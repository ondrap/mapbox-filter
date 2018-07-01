{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE ViewPatterns        #-}

module Main where

import           Codec.Compression.GZip               (decompress)
import           Control.Concurrent                   (getNumCapabilities,
                                                       threadDelay)
import           Control.Concurrent.ParallelIO.Global (globalPool,
                                                       stopGlobalPool)
import           Control.Concurrent.ParallelIO.Local  (Pool, parallel_,
                                                       withPool)
import           Control.Exception.Safe               (bracket)
import           Control.Lens                         (over, (%~), (&), (.~),
                                                       (<&>), (?~), (^.), (^..),
                                                       (^?), _Just)
import           Control.Monad                        (forever, void)
import           Control.Monad.Fail                   (MonadFail (..))
import           Control.Monad.IO.Class               (liftIO)
import           Data.Aeson                           ((.=))
import qualified Data.Aeson                           as AE
import           Data.Bool                            (bool)
import qualified Data.ByteString                      as BS
import qualified Data.ByteString.Lazy                 as BL
import           Data.Foldable                        (for_)
import qualified Data.HashMap.Strict                  as HMap
import           Data.List                            (nub)
import           Data.List.NonEmpty                   (NonEmpty, nonEmpty)
import           Data.Maybe                           (fromMaybe)
import qualified Data.Pool                            as DP
import           Data.Semigroup                       (sconcat, (<>))
import           Data.String.Conversions              (cs)
import qualified Data.Text                            as T
import qualified Data.Text.IO                         as T
import qualified Data.Text.Lazy                       as TL
import           Data.Traversable                     (for)
import           Database.SQLite.Simple               (Only (..), query_,
                                                       withConnection)
import qualified Database.SQLite.Simple               as SQL
import           Geography.VectorTile                 (layers, linestrings,
                                                       metadata, name, points,
                                                       polygons, tile)
import           Network.AWS                          (AccessKey (..), Credentials (Discover, FromKeys),
                                                       SecretKey (..),
                                                       configure, envManager,
                                                       newEnv, runAWS,
                                                       runResourceT, send,
                                                       setEndpoint, toBody)
import           Network.AWS.S3                       (BucketName (..),
                                                       ObjectKey (..),
                                                       poCacheControl,
                                                       poContentEncoding,
                                                       poContentType, putObject,
                                                       s3)
import           Network.HTTP.Client                  (managerConnCount, managerIdleConnectionCount,
                                                       newManager)
import           Network.HTTP.Client.TLS              (tlsManagerSettings)
import           Options.Applicative                  hiding (header, style)
import           System.Directory                     (createDirectoryIfMissing)
import           System.FilePath.Posix                (takeDirectory, (</>))
import qualified System.Metrics.Counter               as CNT
import           System.Posix.Files                   (getFileStatus,
                                                       modificationTime)
import           System.Posix.Types                   (EpochTime)
import           Text.Read                            (readMaybe)
import           UnliftIO                             (MonadUnliftIO, catchAny,
                                                       race_, withRunInIO)
import           Web.Scotty                           (addHeader, get, header,
                                                       json, param, raise, raw,
                                                       scotty, setHeader)

import           DbAccess
import           Mapbox.Filters
import           Mapbox.Interpret                     (FeatureType (..),
                                                       runFilter)
import           Mapbox.Style                         (MapboxStyle, lMinZoom,
                                                       lSource, msLayers,
                                                       _VectorType)


data PublishTarget = PublishFs FilePath | PublishS3 BucketName
  deriving (Show)

data PublishOpts = PublishOpts {
    pUrlPrefix  :: T.Text
  , pStoreTgt   :: PublishTarget
  , pThreads    :: Maybe Int
  , pForceFull  :: Bool
  , pS3Endpoint :: Maybe BS.ByteString
  , pMbtiles    :: FilePath
}

data CmdLine =
    CmdDump {
      fStyles     :: NonEmpty FilePath
    , fSourceName :: Maybe T.Text
    , fZoomLevel  :: Int
    , fMvtSource  :: FilePath
  }
  | CmdMbtiles {
      fStyles     :: NonEmpty FilePath
    , fSourceName :: Maybe T.Text
    , fForceFull  :: Bool
    , fMbtiles    :: FilePath
  }
  | CmdWebServer {
      fModStyles  :: [FilePath]
    , fSourceName :: Maybe T.Text
    , fWebPort    :: Int
    , fMbtiles    :: FilePath
  }
  | CmdPublish {
      fModStyles   :: [FilePath]
    , fSourceName  :: Maybe T.Text
    , fPublishOpts :: PublishOpts
  }

-- | The same as 'some', but generate NonEmpty list (makes more sense)
nsome :: Alternative f => f a -> f (NonEmpty a)
nsome x = fromMaybe (error "'some' didn't return element") . nonEmpty <$> some x

dumpOptions :: Parser CmdLine
dumpOptions =
  CmdDump <$> nsome (strOption (short 'j' <> long "style" <> metavar "JSFILE" <> help "JSON mapbox style file"))
          <*> optional (strOption (short 's' <> long "source" <> help "Tile source name"))
          <*> option auto (short 'z' <> long "zoom" <> help "Tile zoom level")
          <*> argument str (metavar "SRCFILE" <> help "Source file")

mbtileOptions :: Parser CmdLine
mbtileOptions =
  CmdMbtiles <$> nsome (strOption (short 'j' <> long "style" <> metavar "JSFILE" <> help "JSON mapbox style file"))
            <*> optional (strOption (short 's' <> long "source" <> help "Tile source name"))
            <*> switch (short 'f' <> long "force-full" <> help "Force full recomputation")
            <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")

webOptions :: Parser CmdLine
webOptions =
  CmdWebServer <$> many (strOption (short 'j' <> long "style" <> metavar "JSFILE" <> help "JSON mapbox style file"))
              <*> optional (strOption (short 's' <> long "source" <> help "Tile source name"))
              <*> option auto (short 'p' <> long "port" <> help "Web port number")
              <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")


s3Bucket :: ReadM PublishTarget
s3Bucket = maybeReader s3Reader <|> maybeReader (Just . PublishFs)
  where
    s3Reader txt = PublishS3 . BucketName . stripRightSlash <$> T.stripPrefix "s3://" (T.pack txt)

stripRightSlash :: T.Text -> T.Text
stripRightSlash = T.dropWhileEnd (== '/')

publishOptions :: Parser CmdLine
publishOptions =
  CmdPublish
    <$> many (strOption (short 'j' <> long "style" <> metavar "JSFILE" <> help "JSON mapbox style file"))
    <*> optional (strOption (short 's' <> long "source" <> help "Tile source name"))
    <*> (PublishOpts <$>
         (stripRightSlash <$> strOption (short 'u' <> long "url-prefix" <> help "External tile URL prefix"))
      <*> option s3Bucket (short 't' <> long "target" <> help "S3 target prefix for files (e.g. s3://my-bucket/map) or filesystem path")
      <*> optional (option auto (short 'p' <> long "parallelism" <> metavar "NUMBER" <> help "Spawn multiple threads for faster upload (default: number of cores)"))
      <*> switch (short 'f' <> long "force-full" <> help "Force full recomputation")
      <*> optional (strOption (long "s3-endpoint" <> metavar "HOSTNAME" <> help "Endpoint for S3 operations (use e.g. with Google Cloud Storage)"))
      <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")
    )

cmdLineParser  :: Parser CmdLine
cmdLineParser =
  subparser $
    command "dump" (info (helper <*> dumpOptions) (progDesc "Dump vector files contents."))
    <> command "filter" (info (helper <*> mbtileOptions) (progDesc "Run filtering on a MBTiles database"))
    <> command "web" (info (helper <*> webOptions) (progDesc "Run a web server for serving tiles"))
    <> command "publish" (info (helper <*> publishOptions) (progDesc "Publish mbtile to S3"))

progOpts :: ParserInfo CmdLine
progOpts = info (cmdLineParser <**> helper)
    ( fullDesc <> progDesc "Utilities for working with Mapbox style file")

-- | Return style, update style minzoom levels to maxzoom if bigger
getStyle :: NonEmpty FilePath -> IO MapboxStyle
getStyle fnames =
  fmap sconcat <$> for fnames $ \fname -> do
    bstyle <- BS.readFile fname
    case AE.eitherDecodeStrict bstyle of
      Right res -> return res
      Left err  -> error ("Parsing mapbox style failed: " <> err)

-- | Check that the user correctly specified the source name and filter it out
--
-- It is possible for the mbtiles to provide maxzoom of 14 and style to be for maxzoom 17.
-- Therefore we update minzoom in the styles to be the maximum zoom in DB; this
-- ensures that the features stay in the mbtiles database
checkStyle :: Maybe T.Text -> Int -> MapboxStyle -> IO MapboxStyle
checkStyle mtilesrc dbmaxzoom styl = do
  -- Print vector styles
  let sources = nub (styl ^.. msLayers . traverse . _VectorType . lSource)
  for_ sources $ \s ->
    T.putStrLn $ "Found vector source layer: " <> s
  tilesrc <- case sources of
    [nm] | Just nm == mtilesrc -> return nm
         | Nothing <- mtilesrc -> return nm
    lst | Just nm <- mtilesrc, nm `elem` lst -> return nm
        | otherwise -> error ("Invalid tile source specified, " <> show mtilesrc)
  return $ styl & msLayers %~ filter (\l -> l ^? _VectorType . lSource == Just tilesrc)
                & over (msLayers . traverse . _VectorType . lMinZoom . _Just) (min dbmaxzoom)

-- | Generate metadata json based on modification text + database + other info
genMetadata :: (Monad m, HasMbConn m) => TL.Text -> TL.Text -> m AE.Value
genMetadata modTimeStr urlPrefix = do
    metalines <- getMetaData
    return $ AE.object $
        concatMap addMetaLine metalines
        ++ ["tiles" .= [urlPrefix <> "/tiles/" <> "/{z}/{x}/{y}?" <> modTimeStr],
            "tilejson" .= ("2.0.0" :: T.Text)
            ]
  where
    addMetaLine (key,val)
      | key `elem` ["attribution", "description", "name", "format", "basename", "id"] =
            [key .= val]
      | key `elem` ["minzoom", "maxzoom", "pixel_scale", "maskLevel", "planettime"],
        Just (dnum :: Int) <- readMaybe val =
            [key .= dnum]
      | key == "json", Just (AE.Object obj) <- AE.decode (cs val) =
            HMap.toList obj
      | key == "center", Just (lst :: [Double]) <- decodeArr val =
          [key .= lst]
      | key == "bounds", Just lst@[_ :: Double, _,_,_] <- decodeArr val =
          [key .= lst]
      | otherwise = []
      where
        split _ [] = []
        split c lst =
            let (start,rest) = span (/= c) lst
            in start : split c (drop 1 rest)
        decodeArr = traverse readMaybe . split ','

-- | Dump content of the mbtiles with hints about if the features are removed or retained
dumpPbf :: MapboxStyle -> Int -> FilePath -> IO ()
dumpPbf style zoom fp = do
  mvt <- autoUnzip <$> BL.readFile fp
  case tile (cs mvt) of
    Left err -> error (show err)
    Right vtile ->
      for_ (vtile ^.. layers . traverse) $ \l -> do
          T.putStrLn "-----------------------------"
          T.putStrLn ("Layer: " <> cs (l ^. name))
          let lfilter = cfExpr (getLayerFilter (l ^. name) cfilters)
          for_ (l ^. points) (printCont lfilter Point)
          for_ (l ^. linestrings) (printCont lfilter LineString)
          for_ (l ^. polygons) (printCont lfilter Polygon)
  where
    autoUnzip :: BL.ByteString -> BL.ByteString
    autoUnzip bs | BL.unpack (BL.take 2 bs) == [0x1f,0x8b] = decompress bs
                 | otherwise = bs

    cfilters = styleToCFilters zoom style

    printCont lfilter ptype feature = do
      let include = runFilter lfilter ptype feature
      putStrLn $ bool "- " "  " include <> show ptype <> " " <> show (feature ^. metadata)

type JobAction m = ((Zoom, Column, TmsRow, TileId), Maybe TileData) -> m ()

-- | Run a filtering action on all tiles in the database and perform a JobAction
runFilterJob ::
    forall m. (HasMbConn m, HasJobConn m, MonadUnliftIO m, MonadFail m)
  => Pool -- ^ Threadpool for parallel processing
  -> Maybe MapboxStyle -- ^ Filtering mapboxstyle
  -> JobAction m -- ^ Action to perform on filtered tile
  -> m ()
runFilterJob pool mstyle saveAction = do
    total_count <- getTotalCount
    liftIO $ putStrLn ("Total tiles in the file " <> show total_count <> " tiles")
    counter <- liftIO CNT.new
    emptycnt <- liftIO CNT.new
    -- Take it from tiles directly, we will have more zoom levels but so what
    zlevels <- getZooms
    race_ (showStats total_count counter emptycnt) $
      for_ zlevels $ \zoom -> do
        liftIO $ putStrLn $ "Filtering zoom: " <> show zoom
        cols <- getZoomColumns zoom
        liftWithPool 2 $ \colPool -> -- Do some low limit for columns
          parallelFor_ colPool cols $ \col -> do
            tiles <- getColTiles zoom col
            parallelFor_  pool tiles $ \tid@(z@(Zoom z'),x,y,tileid) ->
              (do
                  liftIO $ CNT.inc counter
                  -- We assume the tile exists in the db...
                  Just tiledta <- fetchTileTid tileid
                  case mstyle of
                      Nothing    -> saveAction ((z,x,y,tileid), Just tiledta)
                      Just style -> shrinkTile emptycnt (styleToCFilters z' style) ((z,x,y,tileid), tiledta)
                ) `catchAny` \err -> do
                      liftIO $ putStrLn ("Error on " <> show tid <> ": " <> show err)
                      markErrorTile tid
            markColumnComplete zoom col
  where
    liftWithPool n f =
      withRunInIO $ \runInIO ->
        withPool n $ \dbpool -> runInIO (f dbpool)
    parallelFor_ pool_ parlist job =
      withRunInIO $ \runInIO ->
        (parallel_ pool_) (runInIO . job <$> parlist)

    showStats total_count counter emptycnt =
      liftIO $ forever $ do
        let delay = 15
        start <- CNT.read counter
        threadDelay (delay * 1000000)
        end <- CNT.read counter
        -- TODO: We should read the system time... but the numbers are not that important
        emptyc <- CNT.read emptycnt
        let percent = round ((100 :: Double) * fromIntegral end / fromIntegral total_count) :: Int
            speed = round (fromIntegral (end - start) / (fromIntegral delay :: Double)) :: Int
        putStrLn $ "Completion status: " <> show percent
                  <> "%, speed: " <> show speed <> " tiles/sec"
                  <> " deleted: " <> show (round @_ @Int $ (100 :: Double) * fromIntegral emptyc / fromIntegral end)
                  <> "%"

    shrinkTile emptycnt filtList (tid, TileData tdata) =
      case filterTileCs filtList tdata of
        Left err -> liftIO $ putStrLn $ "Error when decoding tile " <> show tid <> ": " <> cs err
        Right newdta -> do
          -- Update empty counter
          liftIO $ maybe (CNT.inc emptycnt) (\_ -> return ()) newdta
          -- Call job action
          saveAction (tid, TileData <$> newdta)

-- | Filter all tiles in a database and save the filtered tiles back
convertMbtiles :: MapboxStyle -> FilePath -> Bool -> IO ()
convertMbtiles style mbtiles force = do
  runSingleDb force mbtiles (mbtiles <> ".filter") $ do
    runFilterJob globalPool (Just style) (uncurry updateMbtile)
    -- If we were shrinking, call vacuum on database
    vacuumDb
  stopGlobalPool

-- | Publish the mbtile to an S3 target, make it ready for serving
runPublishJob ::
     Maybe (MapboxStyle, EpochTime) -- ^ Parsed style + modification time of the style
  -> PublishOpts
  -> IO ()
runPublishJob mstyle PublishOpts{pMbtiles, pForceFull, pStoreTgt, pUrlPrefix, pThreads, pS3Endpoint} = do
  -- Create http connection manager with higher limits
  conncount <- maybe getNumCapabilities return pThreads
  manager <- newManager tlsManagerSettings{managerConnCount=conncount, managerIdleConnectionCount=conncount}
  -- Generate AWS environ; fake AWS keys when publishing to filesystem
  let credential = case pStoreTgt of
                    PublishFs{} -> FromKeys (AccessKey "fake") (SecretKey "fake") -- Fake it if we publish to FS
                    PublishS3{} -> Discover
  env <- newEnv credential
        <&> maybe id (\host -> configure (setEndpoint True host 443 s3)) pS3Endpoint
        <&> envManager .~ manager

  dbpool <- DP.createPool (SQL.open pMbtiles) SQL.close 1 100 conncount
  modstr <- runMb dbpool $ makeModtimeStr (snd <$> mstyle)

  withThreads $ \pool ->
    runParallelDb pForceFull dbpool (pMbtiles <> "." <> modstr) $ do
      runFilterJob pool (fst <$> mstyle) $ \((z,x,y,_), mnewdta) ->
        -- Skip empty tiles
        liftIO $ for_ mnewdta $ \(TileData newdta) -> do
          let xyz_y = toXyzY y z
              dstpath = "tiles/" <> show z <> "/" <> show x <> "/" <> show xyz_y
          case pStoreTgt of
            PublishFs root -> do
              let fpath = root </> dstpath
              createDirectoryIfMissing True (takeDirectory fpath)
              BL.writeFile fpath newdta
            PublishS3 bucket ->
              runResourceT $ runAWS env $ do
                let cmd = putObject bucket (ObjectKey (cs dstpath)) (toBody newdta)
                          & poContentType ?~ "application/x-protobuf"
                          & poContentEncoding ?~ "gzip"
                          & poCacheControl ?~ "max-age=31536000"
                void $ send cmd
      meta <- genMetadata (cs modstr) (cs pUrlPrefix)
      case pStoreTgt of
        PublishFs root -> liftIO $ BL.writeFile (root </> "metadata.json") (AE.encode meta)
        PublishS3 bucket -> do
          let cmd = putObject bucket "metadata.json" (toBody (AE.encode meta))
                    & poContentType ?~ "application/json"
          runResourceT $ runAWS env $
            void (send cmd)
  where
    withThreads
      | Just tcount <- pThreads = withPool tcount
      | otherwise = bracket (return globalPool) (const stopGlobalPool)

-- | Make a string containing modification time of db & style
makeModtimeStr :: (Monad m, HasMbConn m) => Maybe EpochTime -> m String
makeModtimeStr mtime = do
    dbmtime <- getDbMtime
    let stmtime = fromMaybe 0 mtime
    return (dbmtime <> "_" <> show stmtime)

-- | Run a web server serving filtered/unfiltered tiles and metadata
runWebServer :: Int -> Maybe (MapboxStyle, EpochTime) -> FilePath -> IO ()
runWebServer port mstyle mbpath = do
  dbpool <- DP.createPool (SQL.open mbpath) SQL.close 1 100 100

  -- Generate a JSON to be included as a metadata file
  -- Run a web server
  scotty port $ do
    get "/tiles/metadata.json" $ do
        -- find out protocol and host
        proto <- fromMaybe "http" <$> header "X-Forwarded-Proto"
        host <- fromMaybe "localhost" <$> header "Host"
        metaJson <- liftIO $ runMb dbpool $ do
            mtime <- makeModtimeStr (snd <$> mstyle)
            genMetadata (cs mtime) (proto <> "://" <> host)
        addHeader "Access-Control-Allow-Origin" "*"
        json metaJson
    get "/tiles/:mt1/:z/:x/:y" $ do
        z@(Zoom z') <- param "z"
        x <- param "x"
        y <- param "y"
        let tms_y = toTmsY y z

        addHeader "Access-Control-Allow-Origin" "*"
        setHeader "Content-Type" "application/x-protobuf"
        setHeader "Cache-Control" "max-age=31536000"

        rnewtile <- liftIO $ runMb dbpool $ fetchTileZXY (z, x, tms_y)
        mnewtile <- case rnewtile of
            Just (TileData dta) ->
              case mstyle of
                Nothing -> return (Just dta)
                Just (style,_) ->
                  case filterTile z' style dta of
                    Left err     -> raise (cs err)
                    Right newdta -> return newdta
            _ -> return Nothing
        case mnewtile of
          Just dta -> do
            addHeader "Content-Encoding" "gzip"
            raw dta
          Nothing -> raw "" -- Empty tile

main :: IO ()
main = do
  opts <- execParser progOpts
  case opts of
    CmdDump{fMvtSource, fZoomLevel, fStyles, fSourceName} -> do
        style <- getStyle fStyles >>= checkStyle fSourceName 14
        dumpPbf style fZoomLevel fMvtSource
    CmdMbtiles{fMbtiles, fStyles, fSourceName, fForceFull} -> do
        maxzoom <- getMaxZoom fMbtiles
        style <- getStyle fStyles >>= checkStyle fSourceName maxzoom
        convertMbtiles style fMbtiles fForceFull
    CmdWebServer{fModStyles, fWebPort, fMbtiles, fSourceName} -> do
        maxzoom <- getMaxZoom fMbtiles
        mstyle <- getMStyle fModStyles fSourceName maxzoom
        runWebServer fWebPort mstyle fMbtiles
    CmdPublish{fModStyles, fSourceName, fPublishOpts} -> do
        maxzoom <- getMaxZoom (pMbtiles fPublishOpts)
        mstyle <- getMStyle fModStyles fSourceName maxzoom
        runPublishJob mstyle fPublishOpts
  where
    -- We need to adjust minZoom in the styles to be at least the maximum zoom level
    -- in the database
    getMaxZoom dbname = do
      [Only maxzoom] <- withConnection dbname $ \conn ->
             query_ conn "select max(zoom_level) from tiles"
      return maxzoom

    getMStyle (nonEmpty -> Just stlist) tilesrc maxzoom = do
        st <- getStyle stlist >>= checkStyle tilesrc maxzoom
        mtimes <- fmap modificationTime <$> traverse getFileStatus stlist
        return (Just (st, maximum mtimes))
    getMStyle _ _ _ = return Nothing
