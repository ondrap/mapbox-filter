{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE ViewPatterns        #-}
{-# LANGUAGE FlexibleContexts        #-}

module Main where

import           Codec.Compression.GZip    (CompressParams (compressLevel),
                                            bestCompression, compressWith,
                                            decompress, defaultCompressParams)
import           Control.Concurrent                   (getNumCapabilities,
                                                       threadDelay)
import           Control.Concurrent.ParallelIO.Global (globalPool,
                                                       stopGlobalPool)
import           Control.Concurrent.ParallelIO.Local  (Pool, parallel_,
                                                       withPool)
import           Control.Exception.Safe               (bracket, throwIO)
import           Control.Lens                         (over, (%~), (&), (.~),
                                                       (<&>), (?~), (^.), (^..),
                                                       (^?), _Just, traverseOf, _1, sequenceOf)
import           Control.Monad                        (forever, unless, void,
                                                       when)
import           Control.Monad.Fail                   (MonadFail (..))
import           Control.Monad.IO.Class               (liftIO)
import           Data.Aeson                           ((.=))
import qualified Data.Aeson                           as AE
import qualified Data.Aeson.Lens                      as AEL
import           Data.Aeson.Encode.Pretty             (encodePretty)
import           Data.Bool                            (bool)
import qualified Data.ByteString                      as BS
import qualified Data.ByteString.Lazy                 as BL
import qualified Data.ByteString.Lazy.Char8           as BL8
import           Data.Foldable                        (for_)
import qualified Data.HashMap.Strict                  as HMap
import           Data.List                            (nub)
import           Data.List.NonEmpty                   (NonEmpty, nonEmpty)
import           Data.Maybe                           (fromMaybe, mapMaybe)
import qualified Data.Pool                            as DP
import           Data.Semigroup                       (sconcat, (<>))
import           Data.String.Conversions              (cs)
import           Control.Newtype                      (Newtype(unpack))
import qualified Data.Text                            as T
import qualified Data.Text.IO                         as T
import qualified Data.Text.Lazy                       as TL
import           Data.Traversable                     (for)
import           Database.SQLite.Simple               (Only (..), query_,
                                                       withConnection)
import qualified Database.SQLite.Simple               as SQL
import           Geography.VectorTile                 (layers, linestrings,
                                                       metadata, name, points,
                                                       polygons, tile, untile,
                                                       VectorTile, geometries)
import           Network.AWS                          (AccessKey (..), Credentials (Discover, FromKeys),
                                                       SecretKey (..),
                                                       configure, envManager,
                                                       newEnv, runAWS,
                                                       runResourceT, send,
                                                       setEndpoint, toBody)
import           Network.AWS.S3                       (BucketName (..),
                                                       ObjectKey (..),
                                                       deleteObject,
                                                       poCacheControl,
                                                       poContentEncoding,
                                                       poContentType, putObject,
                                                       s3)
import           Network.HTTP.Client                  (managerConnCount, managerIdleConnectionCount,
                                                       newManager)
import           Network.HTTP.Client.TLS              (tlsManagerSettings)
import           Options.Applicative                  hiding (header, style)
import           System.Directory                     (createDirectoryIfMissing,
                                                       doesFileExist,
                                                       removeFile)
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
import           Mapbox.OldStyleConvert               (convertToNew)
import           Mapbox.DownCopy                      (DownCopySpec, dDstZoom, copyDown)
import           Types

data PublishTarget = PublishFs FilePath | PublishS3 BucketName
  deriving (Show)

data PublishOpts = PublishOpts {
    pUrlPrefix  :: T.Text
  , pStoreTgt   :: PublishTarget
  , pThreads    :: Maybe Int
  , pForceFull  :: Bool
  , pS3Endpoint :: Maybe BS.ByteString
  , pDiffHashes :: Maybe FilePath
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
    , fRtlConvert :: Bool
    , fSourceName :: Maybe T.Text
    , fForceFull  :: Bool
    , fMbtiles    :: FilePath
  }
  | CmdWebServer {
      fModStyles  :: [FilePath]
    , fCopyDown   :: Maybe FilePath
    , fRtlConvert :: Bool
    , fSourceName :: Maybe T.Text
    , fWebPort    :: Int
    , fMbtiles    :: FilePath
  }
  | CmdPublish {
      fModStyles   :: [FilePath]
    , fCopyDown   :: Maybe FilePath
    , fRtlConvert :: Bool
    , fSourceName  :: Maybe T.Text
    , fPublishOpts :: PublishOpts
  }
  | CmdConvert {
      fStyle :: FilePath
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
            <*> switch (long "rtl-convert" <> help "Apply Right-to-left text conversion on metadata (Arabic etc.)")
            <*> optional (strOption (short 's' <> long "source" <> help "Tile source name"))
            <*> switch (short 'f' <> long "force-full" <> help "Force full recomputation")
            <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")

webOptions :: Parser CmdLine
webOptions =
  CmdWebServer <$> many (strOption (short 'j' <> long "style" <> metavar "JSFILE" <> help "JSON mapbox style file"))
              <*> optional (strOption (short 'c' <> long "copy-down" <> metavar "JSFILE" <> help "JSON copydown specification"))
              <*> switch (long "rtl-convert" <> help "Apply Right-to-left text conversion on metadata (Arabic etc.)")
              <*> optional (strOption (short 's' <> long "source" <> help "Tile source name"))
              <*> option auto (short 'p' <> long "port" <> help "Web port number")
              <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")

convertOptions :: Parser CmdLine
convertOptions =
  CmdConvert <$> argument str (metavar "FILENAME" <> help "Style source")

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
    <*> optional (strOption (short 'c' <> long "copy-down" <> metavar "JSFILE" <> help "JSON copydown specification"))
    <*> switch (long "rtl-convert" <> help "Apply Right-to-left text conversion on metadata (Arabic etc.)")
    <*> optional (strOption (short 's' <> long "source" <> help "Tile source name from mapbox style"))
    <*> (PublishOpts <$>
         (stripRightSlash <$> strOption (short 'u' <> long "url-prefix" <> help "External tile URL prefix"))
      <*> option s3Bucket (short 't' <> long "target" <> help "S3 target prefix for files (e.g. s3://my-bucket/map) or filesystem path")
      <*> optional (option auto (short 'p' <> long "parallelism" <> metavar "NUMBER" <> help "Spawn multiple threads for faster upload (default: number of cores)"))
      <*> switch (short 'f' <> long "force-full" <> help "Force full recomputation")
      <*> optional (strOption (long "s3-endpoint" <> metavar "HOSTNAME" <> help "Endpoint for S3 operations (use e.g. with Google Cloud Storage)"))
      <*> optional (strOption (long "hashes-db" <> metavar "SQLITE" <> help "Old hashes.db for differential upload"))
      <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")
    )

cmdLineParser  :: Parser CmdLine
cmdLineParser =
  subparser $
    command "dump" (info (helper <*> dumpOptions) (progDesc "Dump vector files contents."))
    <> command "filter" (info (helper <*> mbtileOptions) (progDesc "Run filtering on a MBTiles database"))
    <> command "web" (info (helper <*> webOptions) (progDesc "Run a web server for serving tiles"))
    <> command "publish" (info (helper <*> publishOptions) (progDesc "Publish mbtile to S3"))
    <> command "convert-old-filter" (info (helper <*> convertOptions) 
                                    (progDesc "Convert style with deprecated filter to new filter"))

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
        ++ ["tiles" .= [urlPrefix <> "/tiles/{z}/{x}/{y}?" <> modTimeStr],
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
          let lfilter = cfExpr (getLayerFilter False (l ^. name) cfilters)
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
      putStrLn $ show (feature ^. geometries)

type JobAction m = ((Zoom, Column, TmsRow, TileId), Maybe TileData) -> m ()

compressParams :: CompressParams
compressParams = defaultCompressParams{compressLevel=bestCompression}

-- | Return nothing if there are no layers (and therefore no features) on the tile
checkEmptyTile :: VectorTile -> Maybe VectorTile
checkEmptyTile t
  | null (t ^. layers)  = Nothing
  | otherwise = Just t
  

-- | Run a filtering action on all tiles in the database and perform a JobAction
runFilterJob ::
    forall m. (HasMbConn m, HasJobConn m, MonadUnliftIO m, MonadFail m, HasMd5Queue m)
  => Pool -- ^ Threadpool for parallel processing
  -> Maybe MapboxStyle -- ^ Filtering mapboxstyle
  -> Maybe DownCopySpec
  -> Bool -- ^ If true, convert right-to-left texts
  -> JobAction m -- ^ Action to perform on filtered tile
  -> m ()
runFilterJob pool mstyle mdownspec rtlconvert saveAction = do
    total_count <- getIncompleteCount
    liftIO $ putStrLn ("Remaining tiles: " <> show total_count)
    counter <- liftIO CNT.new -- TODO - take complete count from the job file...
    emptycnt <- liftIO CNT.new
    changecnt <- liftIO CNT.new
    skipcnt <- liftIO CNT.new
    zlevels <- getIncompleteZooms

    errors <- getErrorTiles
    unless (null errors) $ do
      liftIO $ putStrLn ("Processing error tiles: " <> show (length errors))
      for_ errors $ \tileArg@(z,x,y,_) ->
        (do
          processTile counter emptycnt changecnt skipcnt tileArg
          clearErrorTile (z,x,y)
          ) `catchAny` \err -> liftIO $ putStrLn ("Tile " <> show tileArg <> " error: " <> show err)

    race_ (showStats total_count counter emptycnt changecnt skipcnt) $
      for_ zlevels $ \zoom -> do
        liftIO $ putStrLn $ "Filtering zoom: " <> show zoom
        cols <- getJobZoomColumns zoom
        liftWithPool 2 $ \colPool -> -- Do some low limit for columns
          parallelFor_ colPool cols $ \col -> do
            tiles <- getColTiles zoom col
            parallelFor_  pool tiles $ \tileArg ->
              processTile counter emptycnt changecnt skipcnt tileArg
                `catchAny` \err -> do
                    liftIO $ putStrLn ("Error on " <> show tileArg <> ": " <> show err)
                    markErrorTile tileArg
            markColumnComplete zoom col
  where
    processTile counter emptycnt changecnt skipcnt pos@(z@(Zoom z'),x,y,tileid) = do
      liftIO $ CNT.inc counter
      -- We assume the tile exists in the db...
      Just tiledta <- fetchTileTid tileid
      -- Fetch downcopy tiles
      duptiles <- fetchDownTiles mdownspec (z, x, toXyzY y z)

      newdta <- case mstyle of
            Nothing -> return (Just tiledta)
            Just style -> do
              (tdta, tuptiles) <- 
                case parseTiles (unTileData tiledta) duptiles of
                    Right (tdta, tuptiles) -> return (tdta, tuptiles)
                    Left err -> liftIO . throwIO . userError . show $ err

              let filtList = styleToCFilters z' style
              let res = filterVectorTile rtlconvert filtList (copyDown mdownspec tdta tuptiles)
              return (TileData . compressWith compressParams . cs . untile <$> checkEmptyTile res)
      liftIO $ whenNothing newdta (CNT.inc emptycnt)
      -- Check changes
      changed <- checkHashChanged (z,x,toXyzY y z) newdta
      if | changed -> do
            -- Call job action
            saveAction (pos, newdta)
            liftIO $ CNT.inc changecnt
          | otherwise -> liftIO $ CNT.inc skipcnt
      addHash (z,x,toXyzY y z) newdta

    liftWithPool n f =
      withRunInIO $ \runInIO ->
        withPool n $ \dbpool -> runInIO (f dbpool)
    parallelFor_ pool_ parlist job =
      withRunInIO $ \runInIO ->
        (parallel_ pool_) (runInIO . job <$> parlist)

    whenNothing Nothing f = f
    whenNothing _ _       = return ()

    showStats total_count counter emptycnt changecnt skipcnt =
      liftIO $ forever $ do
        let delay = 15
        start <- CNT.read counter
        threadDelay (delay * 1000000)
        end <- CNT.read counter
        emptyc <- CNT.read emptycnt
        changec <- CNT.read changecnt
        skipc <- CNT.read skipcnt
        let percent = round ((100 :: Double) * fromIntegral end / fromIntegral total_count) :: Int
            speed = round (fromIntegral (end - start) / (fromIntegral delay :: Double)) :: Int
        putStrLn $ "Completion status: " <> show percent
                  <> "%, speed: " <> show speed <> " tiles/sec"
                  <> " deleted: " <> show (round @_ @Int $ (100 :: Double) * fromIntegral emptyc / fromIntegral end)
                  <> "%, written: " <> show (round @_ @Int $ (100 :: Double) * fromIntegral changec / fromIntegral end)
                  <> "%, skipped: " <> show skipc

-- | Filter all tiles in a database and save the filtered tiles back
convertMbtiles :: MapboxStyle -> Bool -> FilePath -> Bool -> IO ()
convertMbtiles style rtlconvert mbtiles force = do
  runSingleDb force mbtiles (mbtiles <> ".filter") $ do
    runFilterJob globalPool (Just style) Nothing rtlconvert (uncurry updateMbtile)
    -- If we were shrinking, call vacuum on database
    vacuumDb
  stopGlobalPool

-- | Publish the mbtile to an S3 target, make it ready for serving
runPublishJob ::
     Maybe (MapboxStyle, EpochTime) -- ^ Parsed style + modification time of the style
  -> Maybe DownCopySpec
  -> Bool
  -> PublishOpts
  -> IO ()
runPublishJob mstyle mdownspec rtlconvert 
      PublishOpts{pMbtiles, pForceFull, pStoreTgt, pUrlPrefix, pThreads, pS3Endpoint, pDiffHashes} = do
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

  withThreads $ \pool -> do
    let hashfile = pMbtiles <> ".hashes"
    let pConf = ParallelConfig {
        pConnCount = conncount 
      , pJobPath = pMbtiles <> "." <> modstr
      , pMd5Path = hashfile
      , pOldMd5Path = pDiffHashes
    }
    runParallelDb pConf pForceFull dbpool $ do
      runFilterJob pool (fst <$> mstyle) mdownspec rtlconvert $ \((z,x,y,_), mnewdta) ->
        liftIO $ do
          let dstpath = "tiles/" <> mkPath (z,x,y)
          -- Skip empty tiles
          case mnewdta of
            Nothing ->
              case pStoreTgt of
                PublishFs root -> do
                  let fpath = root </> dstpath
                  exists <- doesFileExist fpath
                  when exists (removeFile fpath)
                PublishS3 bucket ->
                  runResourceT (runAWS env $
                      void $ send (deleteObject bucket (ObjectKey (cs dstpath)))
                    ) `catchAny` \e -> liftIO (print e)
            Just (TileData newdta) ->
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
    mkPath (z'@(Zoom z), Column x, tms_y) =
      let (XyzRow xyz_y) = toXyzY tms_y z'
      in show z <> "/" <> show x <> "/" <> show xyz_y
    withThreads
      | Just tcount <- pThreads = withPool tcount
      | otherwise = bracket (return globalPool) (const stopGlobalPool)

-- | Make a string containing modification time of db & style
makeModtimeStr :: (Monad m, HasMbConn m) => Maybe EpochTime -> m String
makeModtimeStr mtime = do
    dbmtime <- getDbMtime
    let stmtime = fromMaybe 0 mtime
    return (dbmtime <> "_" <> show stmtime)

fetchDownTiles :: HasMbConn m => Maybe DownCopySpec -> (Zoom, Column, XyzRow) -> m [(TileData, (Int, Int))]
fetchDownTiles (Just spec) (z, x, y) | spec ^. dDstZoom == unpack z = do
  let newtiles = [((z + 1, 2 * x + Column bx, toTmsY (2 * y + XyzRow by) (z + 1)), (bx, by)) 
                  | bx <- [0..1], by <- [0..1]]
  mapMaybe (sequenceOf _1) <$> traverseOf (traverse . _1) fetchTileZXY newtiles
fetchDownTiles _ _ = return []

-- | Run a web server serving filtered/unfiltered tiles and metadata
runWebServer :: Int -> Maybe (MapboxStyle, EpochTime) -> Maybe DownCopySpec -> Bool -> FilePath -> IO ()
runWebServer port mstyle mdownspec rtlconvert mbpath = do
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
    get "/tiles/:z/:x/:y" $ do
        z@(Zoom z') <- param "z"
        x <- param "x"
        y <- param "y"
        let tms_y = toTmsY y z

        addHeader "Access-Control-Allow-Origin" "*"
        setHeader "Content-Type" "application/x-protobuf"
        setHeader "Cache-Control" "max-age=31536000"

        (rnewtile, duptiles) <- liftIO $ runMb dbpool $ do
            rnewtile <- fetchTileZXY (z, x, tms_y)
            duptiles <- fetchDownTiles mdownspec (z, x, y)
            return (rnewtile, duptiles)

        mnewtile <- case rnewtile of
            Just (TileData dta) ->
              case mstyle of
                Nothing -> return (Just dta)
                Just (style,_) -> do
                  -- decompress, untile all
                  (tdta, tuptiles) <- 
                    case parseTiles dta duptiles of
                        Right (tdta, tuptiles) -> return (tdta, tuptiles)
                        Left err -> raise (cs err)
                  let res = filterTile rtlconvert z' style (copyDown mdownspec tdta tuptiles)
                  return (compressWith compressParams . cs . untile <$> checkEmptyTile res)
            _ -> return Nothing
        case mnewtile of
          Just dta -> do
            addHeader "Content-Encoding" "gzip"
            raw dta
          Nothing -> raw "" -- Empty tile

-- | Parses tiles from a bytestring
parseTiles :: BL8.ByteString -> [(TileData, (Int, Int))] -> Either T.Text (VectorTile, [(VectorTile, (Int, Int))])
parseTiles dta duptiles = do
  t1 <- tile (cs (decompress dta))
  tlist <- traverseOf (traverse . _1) (tile . cs . decompress . unTileData) duptiles
  return (t1, tlist)

-- | Read style from filepath, convert 'filter' to newstyle and write to stdout
runConversion :: FilePath -> IO ()
runConversion fname = do
  bstyle <- BS.readFile fname
  case AE.eitherDecodeStrict bstyle of
    Left err  -> error ("Parsing mapbox style failed: " <> err)
    Right (style :: AE.Value) -> do
      let mnewstyle = (AEL.key "layers" . AEL._Array . traverse . AEL.key "filter") convertToNew style
      case mnewstyle of
        Left err -> error ("Conversion error: " <> err)
        Right res -> BL8.putStrLn (encodePretty res)

main :: IO ()
main = do
  opts <- execParser progOpts
  case opts of
    CmdDump{fMvtSource, fZoomLevel, fStyles, fSourceName} -> do
        style <- getStyle fStyles >>= checkStyle fSourceName 14
        dumpPbf style fZoomLevel fMvtSource
    CmdMbtiles{fMbtiles, fStyles, fSourceName, fForceFull, fRtlConvert} -> do
        maxzoom <- getMaxZoom fMbtiles
        style <- getStyle fStyles >>= checkStyle fSourceName maxzoom
        convertMbtiles style fRtlConvert fMbtiles fForceFull
    CmdWebServer{fModStyles, fCopyDown, fWebPort, fMbtiles, fSourceName, fRtlConvert} -> do
        maxzoom <- getMaxZoom fMbtiles
        mstyle <- getMStyle fModStyles fSourceName maxzoom
        downspec <- readCopyDown fCopyDown
        runWebServer fWebPort mstyle downspec fRtlConvert fMbtiles
    CmdPublish{fModStyles, fCopyDown, fSourceName, fRtlConvert, fPublishOpts} -> do
        maxzoom <- getMaxZoom (pMbtiles fPublishOpts)
        mstyle <- getMStyle fModStyles fSourceName maxzoom
        downspec <- readCopyDown fCopyDown
        runPublishJob mstyle downspec fRtlConvert fPublishOpts
    CmdConvert{fStyle} -> runConversion fStyle
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

    readCopyDown Nothing = return Nothing
    readCopyDown (Just fname) = do
      fspec <- BS.readFile fname
      case AE.eitherDecodeStrict fspec of
        Right res -> return res
        Left err  -> error ("Parsing copydown style failed: " <> err)
  