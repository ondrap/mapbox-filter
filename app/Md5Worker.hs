{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Create a database of md5
module Md5Worker (
    Md5Queue
  , sendMd5Tile
  , stopMd5Queue
  , tileChanged
  , runQueueThread
) where

import           Control.Concurrent             (forkIO)
import           Control.Concurrent.BoundedChan
import           Control.Concurrent.MVar        (MVar, newEmptyMVar, putMVar,
                                                 takeMVar)
import           Control.Exception.Safe         (catchAny)
import           Control.Monad                  (when)
import           Crypto.Hash.MD5                (hashlazy)
import qualified Data.ByteString                as BS
import           Data.Maybe                     (listToMaybe)
import qualified Data.Pool                      as DP
import qualified Data.Strict.Maybe              as ST
import           Database.SQLite.Simple         (Connection, Only (..), query)
import qualified Database.SQLite.Simple         as SQL
import           System.Directory               (doesFileExist, removeFile,
                                                 renameFile)

import           Types                          (Column (..), TileData (..),
                                                 XyzRow (..), Zoom (..))

data Md5Message =
    Md5AddFile (Zoom, Column, XyzRow) !(ST.Maybe BS.ByteString)
  | Md5Exit (IO ())

data Md5Queue = Md5Queue {
    md5Q         :: BoundedChan Md5Message
  , md5DbOld     :: Maybe (DP.Pool Connection)
  , md5DbName    :: FilePath
  , md5NewDbName :: FilePath
}

sendMd5Tile :: Md5Queue -> (Zoom, Column, XyzRow) -> Maybe TileData -> IO ()
sendMd5Tile queue pos mtdata = do
  let md5 = hashlazy . unTileData <$> mtdata
  writeChan (md5Q queue) $! Md5AddFile pos (toStrict md5)
  where
    toStrict Nothing  = ST.Nothing
    toStrict (Just a) = ST.Just a

stopMd5Queue :: Md5Queue -> IO ()
stopMd5Queue queue = do
  mvar <- newEmptyMVar :: IO (MVar ())
  writeChan (md5Q queue) (Md5Exit (putMVar mvar ()))
  takeMVar mvar
  -- Rename dbname
  oldExists <- doesFileExist (md5DbName queue)
  when oldExists (removeFile (md5DbName queue))
  renameFile (md5NewDbName queue) (md5DbName queue)

tileChanged :: Md5Queue -> (Zoom, Column, XyzRow) -> Maybe TileData -> IO Bool
tileChanged Md5Queue{md5DbOld=Nothing} _ _ = return True
tileChanged Md5Queue{md5DbOld=Just dbpool} (z,x,y) mtile =
  DP.withResource dbpool $ \conn -> do
    res <- query conn "select md5_hash from md5hash where zoom_level=? and tile_column=? and tile_row=?" (z,x,y)
    let mhash = (\(TileData dta) -> hashlazy dta) <$> mtile
    return (mhash /= listToMaybe (fromOnly <$> res))

runQueueThread :: FilePath -> Int -> IO Md5Queue
runQueueThread dbpath thrcount = do
    queue <- newBoundedChan 200
    oldexists <- doesFileExist dbpath
    dbpool <- if oldexists
              then Just <$> DP.createPool (SQL.open dbpath) SQL.close 1 100 thrcount
              else return Nothing

    let newdbpath = dbpath ++ ".new"
    conn <- SQL.open newdbpath
    initDb conn
    _ <- forkIO (handleConn queue conn)
    return (Md5Queue queue dbpool dbpath newdbpath)
  where
    handleConn q conn = do
      msg <- readChan q
      case msg of
        Md5Exit signal -> do
            -- Close db for access
            SQL.close conn
            signal
        Md5AddFile (z,x,y) (ST.Just md5) -> do
          SQL.execute conn "delete from md5hash (zoom_level,tile_column,tile_row) values (?,?,?)" (z, x, y)
            `catchAny` \_ -> return ()
          SQL.execute conn "insert into md5hash (zoom_level,tile_column,tile_row,md5_hash) values (?,?,?,?)" (z, x, y, md5)
          handleConn q conn
        Md5AddFile _ ST.Nothing -> handleConn q conn
      -- Initialize db, return true if it was empty
    initDb :: Connection -> IO ()
    initDb conn =
      (do
         SQL.execute_ conn "create table md5hash (zoom_level int not null, tile_column int not null, tile_row int not null, md5_hash blob not null)"
         SQL.execute_ conn "create index md5hash_index on md5hash (zoom_level, tile_column, tile_row)"
      )`catchAny` \_ -> return ()
