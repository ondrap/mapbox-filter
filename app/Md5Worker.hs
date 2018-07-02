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
import           Control.Concurrent.MVar        (MVar, newEmptyMVar, putMVar)
import           Control.Exception.Safe         (catchAny)
import           Crypto.Hash.MD5                (hashlazy)
import qualified Data.ByteString                as BS
import qualified Data.ByteString.Lazy           as BL
import           Data.Maybe                     (listToMaybe)
import           Database.SQLite.Simple         (Connection, Only (..), query)
import qualified Database.SQLite.Simple         as SQL
import           DbAccess                       (Column (..), TileData (..),
                                                 XyzRow (..), Zoom (..))

data Md5Message =
    Md5AddFile (Zoom, Column, XyzRow) !BS.ByteString
  | Md5Exit (IO ())

data Md5Queue = Md5Queue {
    md5Q        :: BoundedChan Md5Message
  , md5Db       :: Connection
  , md5WasEmpty :: Bool
}

sendMd5Tile :: Md5Queue -> (Zoom, Column, XyzRow) -> TileData -> IO ()
sendMd5Tile queue pos (TileData tdata) = do
  let md5 = hashlazy tdata
  writeChan (md5Q queue) $! Md5AddFile pos md5

stopMd5Queue :: Md5Queue -> IO ()
stopMd5Queue queue = do
  mvar <- newEmptyMVar :: IO (MVar ())
  writeChan (md5Q queue) (Md5Exit (putMVar mvar ()))

tileChanged :: Md5Queue -> (Zoom, Column, XyzRow) -> Maybe BL.ByteString -> IO Bool
tileChanged Md5Queue{md5WasEmpty=True} _ (Just _) = return True
tileChanged Md5Queue{md5WasEmpty=True} _ Nothing = return False
tileChanged Md5Queue{md5Db} (z,x,y) mtile = do
  res <- query md5Db "select md5_hash from md5hash where zoom_level=? and tile_column=? and tile_row=?" (z,x,y)
  let mhash = hashlazy <$> mtile
  case (listToMaybe (fromOnly <$> res), mhash) of
    (Just dbhash, Just hash) -> return (dbhash /= hash)
    (Nothing, Nothing)       -> return False
    _                        -> return True

runQueueThread :: FilePath -> IO Md5Queue
runQueueThread dbpath = do
    queue <- newBoundedChan 100
    conn <- SQL.open dbpath
    wasEmpty <- initDb conn
    _ <- forkIO (handleConn queue conn)
    return (Md5Queue queue conn wasEmpty)
  where
    handleConn q conn = do
      msg <- readChan q
      case msg of
        Md5Exit signal -> do
            -- Close db for access
            SQL.close conn
            signal
        Md5AddFile (z,x,y) md5 -> do
          SQL.execute conn "delete from md5hash where zoom_level=? and tile_column=? and tile_row=?" (z,x,y)
          SQL.execute conn "insert into md5hash (zoom_level,tile_column,tile_row,md5_hash) values (?,?,?,?)" (z, x, y, md5)
          handleConn q conn
    -- Initialize db, return true if it was empty
    initDb :: Connection -> IO Bool
    initDb conn =
      (do
         SQL.execute_ conn "create table md5hash (zoom_level int, tile_column int, tile_row int, md5_hash text)"
         SQL.execute_ conn "create index md5hash_index on md5hash (zoom_level, tile_column, tile_row)"
         return True
      )`catchAny` \_ -> return False
