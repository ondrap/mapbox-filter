{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeApplications           #-}

module DbAccess where

import           Control.Exception.Safe           (MonadThrow, catchAny,
                                                   throwIO)
import           Control.Monad                    (void, when)
import           Control.Monad.Fail               (MonadFail (..))
import           Control.Monad.IO.Class           (MonadIO, liftIO)
import           Control.Monad.Reader             (MonadReader, ReaderT (..),
                                                   ask, asks, runReaderT)
import qualified Data.ByteString.Lazy             as BL
import           Data.Monoid                      ((<>))
import qualified Data.Pool                        as DP
import qualified Data.Text                        as T
import           Database.SQLite.Simple           (Connection, Only (..),
                                                   Query (..), execute,
                                                   executeMany, execute_, query,
                                                   query_, withConnection)
import           Database.SQLite.Simple.FromField (FromField (..))
import           Database.SQLite.Simple.FromRow   (FromRow)
import           Database.SQLite.Simple.ToField   (ToField)
import           Database.SQLite.Simple.ToRow     (ToRow)
import           UnliftIO                         (MonadUnliftIO (..),
                                                   UnliftIO (..), withUnliftIO)
import           Web.Scotty                       (Parsable)

newtype TileId = TileId T.Text
  deriving (Show, ToField, FromField)

newtype TileData = TileData BL.ByteString
  deriving (Show, ToField, FromField)

newtype Zoom = Zoom Int
  deriving (Show, ToField, FromField, Parsable)
newtype XyzRow = XyzRow Int
  deriving (Show, ToField, Parsable)
newtype TmsRow = TmsRow Int
  deriving (Show, ToField, FromField)
newtype Column = Column Int
  deriving (Show, ToField, FromField, Parsable)

-- | Flip y coordinate between xyz and tms schemes
toTmsY :: XyzRow -> Zoom -> TmsRow
toTmsY (XyzRow y) (Zoom z) = TmsRow (2 ^ z - y - 1)

toXyzY :: TmsRow -> Zoom -> XyzRow
toXyzY (TmsRow y) (Zoom z) = XyzRow (2 ^ z - y - 1)

class MonadIO m => HasMbConn m where
  {-# MINIMAL withMbConn #-}
  withMbConn :: (Connection -> m a) -> m a
  mbQuery :: (ToRow q, FromRow r) => Query -> q -> m [r]
  mbQuery q p =
    withMbConn $ \conn ->
      liftIO $ query conn q p
  mbQuery_ :: FromRow r => Query -> m [r]
  mbQuery_ q =
    withMbConn $ \conn ->
      liftIO $ query_ conn q

class MonadIO m => HasJobConn m where
  {-# MINIMAL getJobConn #-}
  getJobConn :: m Connection
  jobQuery :: (ToRow q, FromRow r) => Query -> q -> m [r]
  jobQuery q p = do
    conn <- getJobConn
    liftIO $ query conn q p
  jobQuery_ :: FromRow r => Query -> m [r]
  jobQuery_ q = do
    conn <- getJobConn
    liftIO $ query_ conn q
  jobExecute :: ToRow q => Query -> q -> m ()
  jobExecute q p = do
    conn <- getJobConn
    liftIO $ execute conn q p


fetchTileTid :: (Monad m, HasMbConn m) => TileId -> m (Maybe TileData)
fetchTileTid tid = do
  tres <- mbQuery "select tile_data from images where tile_id=?" (Only tid)
  case tres of
    [Only tdata] -> return (Just tdata)
    _            -> return Nothing

fetchTileZXY :: (Monad m, HasMbConn m) => (Zoom, Column, TmsRow) -> m (Maybe TileData)
fetchTileZXY (z,x,y) = do
  tres <- mbQuery "select tile_data from tiles where zoom_level=? and tile_column=? and tile_row=?"
                  (z,x,y)
  case tres of
    [Only tdata] -> return (Just tdata)
    _            -> return Nothing

getZooms :: (Monad m, HasMbConn m) => m [Zoom]
getZooms =
  fmap fromOnly <$> mbQuery_ "select distinct zoom_level from tiles order by zoom_level"

getTotalCount :: (Monad m, HasMbConn m, MonadFail m) => m Int
getTotalCount = do
  [Only total_count] <- mbQuery_ "select count(*) from tiles"
  return total_count

getZoomColumns :: (Monad m, HasMbConn m) => Zoom -> m [Column]
getZoomColumns z = do
  let colquery = "select distinct tile_column from job where zoom_level=? order by tile_column"
  fmap fromOnly <$> mbQuery colquery (Only z)

getColTiles :: (Monad m, HasMbConn m) => Zoom -> Column -> m [(Zoom, Column, TmsRow, TileId)]
getColTiles z x = do
  let qry = "select zoom_level,tile_column,tile_row,tile_id from map where zoom_level=? AND tile_column=?"
  mbQuery qry (z, x)

getMetaData :: (Monad m, HasMbConn m) => m [(T.Text, String)]
getMetaData = mbQuery_ "select name,value from metadata"

getDbMtime :: (Monad m, HasMbConn m) => m String
getDbMtime = do
  mlines :: [Only String] <- mbQuery_ "select value from metadata where name='mtime'"
  case mlines of
    [Only res] -> return res
    _          -> return ""

class Monad m => WriteMbTile m where
  updateMbtile :: (Zoom, Column, TmsRow, TileId) -> Maybe TileData -> m ()
  vacuumDb :: m ()

-- Automatic instance for incremental jobs when db is available
getIncompleteZooms :: (Monad m, HasJobConn m) => m [Zoom]
getIncompleteZooms = fmap fromOnly <$> jobQuery_ "select distinct zoom_level from jobs order by zoom_level"

getIncompleteColumns :: (Monad m, HasJobConn m) => Zoom -> m [Column]
getIncompleteColumns z =
  fmap fromOnly <$> jobQuery "select tile_column from jobs where zoom_level=?" (Only z)

markColumnComplete :: (Monad m, HasJobConn m) => Zoom -> Column -> m ()
markColumnComplete z x = jobExecute "delete from jobs where zoom_level=? and tile_column=?" (z,x)

markErrorTile :: (Monad m, HasJobConn m) => (Zoom, Column, TmsRow, TileId) -> m ()
markErrorTile (z,x,y, tid) = do
  -- Just for sure, delete
  jobExecute "delete from errors where zoom_level=? and tile_column=? and tile_row=?" (z,x,y)
  jobExecute "insert into errors (zoom_level,tile_column,tile_row, tile_id) values (?,?,?,?)" (z,x,y, tid)

getErrorTiles :: (Monad m, HasJobConn m) => m [(Zoom, Column, TmsRow, TileId)]
getErrorTiles = jobQuery_ "select zoom_level,tile_column,tile_row, tile_id from errors"

clearErrorTile :: (Monad m, HasJobConn m) => (Zoom,Column,TmsRow) -> m ()
clearErrorTile (z,x,y) =
  jobExecute "delete from errors where zoom_level=? and tile_column=? and tile_row=?" (z,x,y)

data SingleEnv = SingleEnv {
    seMbConn  :: Connection
  , seJobConn :: Connection
}

-- | Single-connection (writable) openmaptiles compatibile db
newtype SingleDbRunner a = SingleDbRunner {
    unSingleDbRunner :: ReaderT SingleEnv IO a
  } deriving (Functor, Applicative, Monad, MonadIO, MonadThrow)
deriving instance MonadReader SingleEnv SingleDbRunner

checkJobDb :: Connection -> Connection -> Bool -> IO ()
checkJobDb jobconn mbconn forceFull = do
  exists <- tableExists jobconn "jobs"
  when (not exists || forceFull) $  do
        execute_ jobconn "drop table jobs" `catchAny` \_ -> return ()
        execute_ jobconn "drop table errors" `catchAny` \_ -> return ()
        execute_ jobconn "create table jobs (zoom_level int, tile_column int)"
        execute_ jobconn "create INDEX jobs_index ON jobs (zoom_level,tile_column)"
        execute_ jobconn "create table errors (zoom_level int, tile_column int, tile_row int, )"
        putStrLn "Doing full database work, recreating job list"
        jobs :: [(Zoom, Column)] <- query_ mbconn "select distinct zoom_level, tile_column from map"
        executeMany jobconn "insert into jobs(zomm_level,tile_column) values (?,?)" jobs
        putStrLn "Job list done"
  where
    tableExists conn table =
      (void (query_ @(Only Int) conn (Query ("select count(*) from " <> table))) >> return True)
        `catchAny` \_ -> return False

runSingleDb :: Bool -> FilePath -> FilePath -> SingleDbRunner a -> IO a
runSingleDb forceFull mbpath jobpath (SingleDbRunner code) =
  withConnection mbpath $ \mbconn ->
    withConnection jobpath $ \jobconn -> do
      checkJobDb jobconn mbconn forceFull
      runReaderT code (SingleEnv mbconn jobconn)

instance MonadUnliftIO SingleDbRunner where
  askUnliftIO =
    SingleDbRunner . ReaderT $ \r ->
      withUnliftIO $ \u ->
        return (UnliftIO (unliftIO u . flip runReaderT r . unSingleDbRunner))
  withRunInIO inner =
    SingleDbRunner . ReaderT $ \r ->
      withRunInIO $ \run ->
        inner (run . flip runReaderT r . unSingleDbRunner)

instance MonadFail SingleDbRunner where
  fail str = throwIO (userError str)

instance HasMbConn SingleDbRunner where
  withMbConn f = asks seMbConn >>= f

instance HasJobConn SingleDbRunner where
  getJobConn = asks seJobConn

instance WriteMbTile SingleDbRunner where
  updateMbtile (z,x,y,tid) mdata = do
    conn <- asks seMbConn
    liftIO $ case mdata of
      Just tdata -> execute conn "update images set tile_data=? where tile_id=?" (tdata, tid)
      Nothing -> do
        execute conn "delete from map where zoom_level=? AND tile_column=? AND tile_row=?" (z,x,y)
        execute conn "delete from images where tile_id=?" (Only tid)
  vacuumDb = do
    conn <- asks seMbConn
    liftIO $ execute_ conn "vacuum"

-- | Single-connection (writable) openmaptiles compatibile db
newtype MbRunner m a = MbRunner {
    unMbRunner :: ReaderT (DP.Pool Connection) m a
  } deriving (Functor, Applicative, Monad, MonadIO, MonadThrow)
deriving instance Monad m => MonadReader (DP.Pool Connection) (MbRunner m)

instance MonadUnliftIO m => MonadUnliftIO (MbRunner m) where
  askUnliftIO =
    MbRunner . ReaderT $ \r ->
      withUnliftIO $ \u ->
        return (UnliftIO (unliftIO u . flip runReaderT r . unMbRunner))
  withRunInIO inner =
    MbRunner . ReaderT $ \r ->
      withRunInIO $ \run ->
         inner (run . flip runReaderT r . unMbRunner)

instance MonadUnliftIO m => HasMbConn (MbRunner m) where
  withMbConn f = do
    pool <- ask
    withRunInIO $ \runInIO ->
      DP.withResource pool (\conn -> runInIO (f conn))

instance MonadThrow m => MonadFail (MbRunner m) where
  fail str = throwIO (userError str)

runMb :: Monad m => DP.Pool Connection -> MbRunner m a -> m a
runMb pool (MbRunner code) = runReaderT code pool

data ParallelEnv = ParallelEnv {
    peMbPool  :: DP.Pool Connection
  , peJobConn :: Connection
}

newtype ParallelDbRunner a = ParallelDbRunner {
    unParallelDbRunner :: ReaderT ParallelEnv IO a
  } deriving (Functor, Applicative, Monad, MonadIO, MonadThrow)
deriving instance MonadReader ParallelEnv ParallelDbRunner

instance MonadUnliftIO ParallelDbRunner where
  askUnliftIO =
    ParallelDbRunner . ReaderT $ \r ->
      withUnliftIO $ \u ->
        return (UnliftIO (unliftIO u . flip runReaderT r . unParallelDbRunner))
  withRunInIO inner =
    ParallelDbRunner . ReaderT $ \r ->
      withRunInIO $ \run ->
        inner (run . flip runReaderT r . unParallelDbRunner)

instance MonadFail ParallelDbRunner where
  fail str = throwIO (userError str)

instance HasMbConn ParallelDbRunner where
  withMbConn f = do
    pool <- asks peMbPool
    withRunInIO $ \runInIO ->
      DP.withResource pool (runInIO . f)

instance HasJobConn ParallelDbRunner where
  getJobConn = asks peJobConn

runParallelDb :: Bool -> DP.Pool Connection -> FilePath -> ParallelDbRunner a -> IO a
runParallelDb forceFull mbpool jobpath (ParallelDbRunner code) =
  withConnection jobpath $ \jobconn -> do
    DP.withResource mbpool $ \conn -> checkJobDb jobconn conn forceFull
    runReaderT code (ParallelEnv mbpool jobconn)
