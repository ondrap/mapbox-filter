{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Control.Concurrent.ParallelIO.Global (globalPool,
                                                       stopGlobalPool)
import           Control.Concurrent.ParallelIO.Local  (Pool, parallel_,
                                                       withPool)
import           Control.Exception.Safe               (bracket, catchAny)
import           Control.Lens                         ((%~), (&), (?~), (^.),
                                                       (^..), _1)
import           Control.Monad                        (void, when, (>=>))
import           Control.Monad.IO.Class               (liftIO)
import           Data.Aeson                           ((.=))
import qualified Data.Aeson                           as AE
import           Data.Bool                            (bool)
import qualified Data.ByteString                      as BS
import qualified Data.ByteString.Lazy                 as BL
import           Data.Foldable                        (for_)
import qualified Data.HashMap.Strict                  as HMap
import           Data.List                            (nub)
import           Data.Maybe                           (fromMaybe)
import           Data.Semigroup                       ((<>))
import           Data.String.Conversions              (cs)
import qualified Data.Text                            as T
import qualified Data.Text.IO                         as T
import qualified Data.Text.Lazy                       as TL
import           Database.SQLite.Simple               (Connection, Only (..),
                                                       execute, execute_, query,
                                                       query_, withConnection)
import           Geography.VectorTile                 (layers, linestrings,
                                                       metadata, name, points,
                                                       polygons, tile)
import           Network.AWS                          (Credentials (Discover),
                                                       newEnv, runAWS,
                                                       runResourceT, send,
                                                       toBody)
import           Network.AWS.S3                       (BucketName (..),
                                                       ObjectKey (..),
                                                       poCacheControl,
                                                       poContentEncoding,
                                                       poContentType, putObject)
import           Options.Applicative                  hiding (header, style)
import           System.Posix.Files                   (getFileStatus,
                                                       modificationTime)
import           System.Posix.Types                   (EpochTime)
import           Text.Read                            (readMaybe)
import           Web.Scotty                           (addHeader, get, header,
                                                       json, param, raise, raw,
                                                       scotty, setHeader)


import           Mapbox.Filters
import           Mapbox.Interpret                     (FeatureType (..),
                                                       runFilter)
import           Mapbox.Style                         (MapboxStyle, lSource,
                                                       msLayers, _VectorLayer)


data CmdLine =
    CmdDump {
      fStyle      :: FilePath
    , fSourceName :: Maybe T.Text
    , fZoomLevel  :: Int
    , fMvtSource  :: FilePath
  }
  | CmdMbtiles {
      fStyle      :: FilePath
    , fSourceName :: Maybe T.Text
    , fMbtiles    :: FilePath
  }
  | CmdWebServer {
      fModStyle   :: Maybe FilePath
    , fSourceName :: Maybe T.Text
    , fWebPort    :: Int
    , fLazyUpdate :: Bool
    , fMbtiles    :: FilePath
  }
  | CmdPublish {
      fModStyle   :: Maybe FilePath
    , fSourceName :: Maybe T.Text
    , fUrlPrefix  :: T.Text
    , fStoreTgt   :: BucketName
    , fThreads    :: Maybe Int
    , fMbtiles    :: FilePath
  }

dumpOptions :: Parser CmdLine
dumpOptions =
  CmdDump <$> strOption (short 'j' <> long "style" <> help "JSON mapbox style file")
          <*> optional (T.pack <$> strOption (short 's' <> long "source" <> help "Tile source name"))
          <*> option auto (short 'z' <> long "zoom" <> help "Tile zoom level")
          <*> argument str (metavar "SRCFILE" <> help "Source file")

mbtileOptions :: Parser CmdLine
mbtileOptions =
  CmdMbtiles <$> strOption (short 'j' <> long "style" <> help "JSON mapbox style file")
            <*> optional (T.pack <$> strOption (short 's' <> long "source" <> help "Tile source name"))
            <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")

webOptions :: Parser CmdLine
webOptions =
  CmdWebServer <$> optional (strOption (short 'j' <> long "style" <> help "JSON mapbox style file"))
              <*> optional (T.pack <$> strOption (short 's' <> long "source" <> help "Tile source name"))
              <*> option auto (short 'p' <> long "port" <> help "Web port number")
              <*> switch (short 'l' <> long "lazy" <> help "Lazily update the database with filtered data")
              <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")


s3Bucket :: ReadM BucketName
s3Bucket = maybeReader s3Reader
  where
    s3Reader txt = BucketName . stripRightSlash <$> T.stripPrefix "s3://" (T.pack txt)
    stripRightSlash txt = fromMaybe txt (T.stripSuffix "/" txt)

publishOptions :: Parser CmdLine
publishOptions =
  CmdPublish <$> optional (strOption (short 'j' <> long "style" <> help "JSON mapbox style file"))
            <*> optional (T.pack <$> strOption (short 's' <> long "source" <> help "Tile source name"))
            <*> (T.pack <$> strOption (short 'u' <> long "url-prefix" <> help "External tile URL prefix"))
            <*> option s3Bucket (short 't' <> long "target" <> help "S3 target prefix for files (e.g. s3://my-bucket/map)")
            <*> optional (option auto (short 'p' <> long "parallelism" <> metavar "NUMBER" <> help "Spawn multiple threads for faster upload (default: number of cores)"))
            <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")

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

getStyle :: FilePath -> IO MapboxStyle
getStyle fname = do
  bstyle <- BS.readFile fname
  case AE.eitherDecodeStrict bstyle of
    Right res -> return res
    Left err -> do
      putStrLn err
      error "Parsing mapbox style failed"


-- | Check that the user correctly specified the source name and filter it out
checkStyle :: Maybe T.Text -> MapboxStyle -> IO MapboxStyle
checkStyle mtilesrc styl = do
  -- Print vector styles
  let sources = nub (styl ^.. msLayers . traverse . _VectorLayer . _1)
  for_ sources $ \s ->
    T.putStrLn $ "Found vector source layer: " <> s
  tilesrc <- case sources of
    [nm] | Just nm == mtilesrc -> return nm
         | Nothing <- mtilesrc -> return nm
    lst | Just nm <- mtilesrc, nm `elem` lst -> return nm
        | otherwise -> error ("Invalid tile source specified, " <> show mtilesrc)
  return $ styl & msLayers %~ filter (\l -> l ^. lSource == tilesrc)

-- | Generate metadata json based on modification text + database + other info
genMetadata :: Connection -> TL.Text -> TL.Text -> IO AE.Value
genMetadata conn modTimeStr urlPrefix = do
    metalines :: [(T.Text,String)] <- query_ conn "select name,value from metadata"
    return $ AE.object $
        concatMap addMetaLine metalines
        ++ ["tiles" .= [urlPrefix <> "/tiles/" <> modTimeStr <> "/{z}/{x}/{y}"],
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


dumpPbf :: MapboxStyle -> Int -> FilePath -> IO ()
dumpPbf style zoom fp = do
  mvt <- BS.readFile fp
  case tile mvt of
    Left err -> error (show err)
    Right vtile ->
      for_ (vtile ^.. layers . traverse) $ \l -> do
          T.putStrLn "-----------------------------"
          T.putStrLn ("Layer: " <> cs (l ^. name))
          let lfilter = getLayerFilter (l ^. name) cfilters
          for_ (l ^. points) (printCont lfilter Point)
          for_ (l ^. linestrings) (printCont lfilter LineString)
          for_ (l ^. polygons) (printCont lfilter Polygon)
  where
    cfilters = styleToCFilters zoom style

    printCont lfilter ptype feature = do
      let include = runFilter lfilter ptype feature
      putStrLn $ bool "- " "  " include <> show ptype <> " " <> show (feature ^. metadata)

type JobAction = ((Int, Int, Int, T.Text), BL.ByteString) -> IO ()

-- | Run a filtering action on all tiles in the database and perform a JobAction
runFilterJob :: Pool -> Connection -> Maybe MapboxStyle -> JobAction -> IO ()
runFilterJob pool conn mstyle saveAction = do
    zlevels <- query_ conn "select distinct zoom_level from map order by zoom_level"
    for_ zlevels $ \(Only zoom) -> do
      putStrLn $ "Filtering zoom: " <> show zoom

      (tiles :: [(Int,Int,Int,T.Text)]) <- query conn "select zoom_level,tile_column,tile_row,tile_id from map where zoom_level=?" (Only zoom)
      putStrLn $ "Tiles: " <> show (length tiles)
      let iaction = case mstyle of
            Just style -> shrinkTile (styleToCFilters zoom style)
            Nothing    -> saveAction
      parallel_ pool $ (fetchTile >=> iaction) <$> tiles
    -- If we were shrinking, call vacuum on database, otherwise skip it
    for_ mstyle $ \_ -> execute_ conn "vacuum"
  where
    fetchTile tid@(_,_,_,tileid) = do
      [Only (tdata :: BL.ByteString)] <- query conn "select tile_data from images where tile_id=?" (Only tileid)
      return (tid, tdata)

    shrinkTile filtList (tid@(_,_,_,tileid), tdata) =
      case filterTileCs filtList tdata of
        Left err -> putStrLn $ "Error when decoding tile " <> show tileid <> ": " <> cs err
        Right newdta -> saveAction (tid, newdta)

-- | Filter all tiles in a database and save the filtered tiles back
convertMbtiles :: MapboxStyle -> FilePath -> IO ()
convertMbtiles style mbtiles = do
  withConnection mbtiles $ \conn ->
    runFilterJob globalPool conn (Just style) $ \((_,_,_,tileid), newdta) ->
      execute conn "update images set tile_data=? where tile_id=?" (newdta, tileid)
  stopGlobalPool

-- | Publish the mbtile to an S3 target, make it ready for serving
runPublishJob :: Maybe Int -> Maybe (MapboxStyle, EpochTime) -> FilePath -> T.Text -> BucketName -> IO ()
runPublishJob mthreads mstyle mbtiles urlPrefix storeTgt = do
  env <- newEnv Discover
  withThreads $ \pool ->
    withConnection mbtiles $ \conn -> do
      modstr <- liftIO $ makeModtimeStr conn (snd <$> mstyle)
      runFilterJob pool conn (fst <$> mstyle) $ \((z,x,y,_), newdta) -> do
        let xyz_y = yzFlipTms y z
            dstpath = ObjectKey (cs ("tiles/" <> modstr <> "/" <> show z <> "/" <> show x <> "/" <> show xyz_y))
        runResourceT $ runAWS env $ do
          let cmd = putObject storeTgt dstpath (toBody newdta)
                    & poContentType ?~ "application/x-protobuf"
                    & poContentEncoding ?~ "gzip"
                    & poCacheControl ?~ "max-age=31536000"
          void $ send cmd
      meta <- genMetadata conn (cs modstr) (cs urlPrefix)
      let cmd = putObject storeTgt "metadata.json" (toBody (AE.encode meta))
                & poContentType ?~ "application/json"
      runResourceT $ runAWS env $
        void (send cmd)
  where
    withThreads
      | Just tcount <- mthreads = withPool tcount
      | otherwise = bracket (return globalPool) (const stopGlobalPool)

-- | Flip y coordinate between xyz and tms schemes
yzFlipTms :: Int -> Int -> Int
yzFlipTms y z = 2 ^ z - y - 1

-- | Make a string containing modification time of db & style
makeModtimeStr :: Connection -> Maybe EpochTime -> IO String
makeModtimeStr conn mtime = do
    dbmtime <- liftIO getDbMtime
    let stmtime = fromMaybe 0 mtime
    return (dbmtime <> "_" <> show stmtime)
  where
    getDbMtime = do
      mlines :: [Only String] <- query_ conn "select value from metadata where name='mtime'"
      case mlines of
        [Only res] -> return res
        _          -> return ""

-- | Run a web server serving filtered/unfiltered tiles and metadata
runWebServer :: Int -> Maybe (MapboxStyle, EpochTime) -> FilePath -> Bool -> IO ()
runWebServer port mstyle mbpath lazyUpdate =
  withConnection mbpath $ \conn -> do
    -- If lazy, add a column to a table
    when lazyUpdate $
      execute_ conn "alter table images add column shrinked default 0"
        `catchAny` \_ -> return ()
    -- Generate a JSON to be included as a metadata file
    -- Run a web server
    scotty port $ do
      get "/tiles/metadata.json" $ do
          -- find out protocol and host
          proto <- fromMaybe "http" <$> header "X-Forwarded-Proto"
          host <- fromMaybe "localhost" <$> header "Host"
          mtime <- liftIO $ makeModtimeStr conn (snd <$> mstyle)
          metaJson <- liftIO $ genMetadata conn (cs mtime) (proto <> "://" <> host)
          addHeader "Access-Control-Allow-Origin" "*"
          json metaJson
      get "/tiles/:mt1/:z/:x/:y" $ do
          z :: Int <- param "z"
          x :: Int <- param "x"
          y :: Int <- param "y"
          let tms_y = yzFlipTms y z

          mnewtile <- case (mstyle, lazyUpdate) of
              (Just (style,_), True) -> getLazyTile conn style z x tms_y
              _                      -> getTile conn z x tms_y

          addHeader "Access-Control-Allow-Origin" "*"
          setHeader "Content-Type" "application/x-protobuf"
          setHeader "Cache-Control" "max-age=31536000"

          case mnewtile of
            Just dta -> do
              addHeader "Content-Encoding" "gzip"
              raw dta
            Nothing -> raw ""
  where
    -- Get tile from modified database; if not already shrinked, shrink it and write to the database
    getLazyTile conn style z x tms_y = do
      rows <- liftIO $ query conn "select tile_data, map.tile_id, shrinked from images,map where zoom_level=? AND tile_column=? AND tile_row=? AND map.tile_id=images.tile_id"
                (z, x, tms_y)
      case rows of
        [(dta, _, 1 :: Int)] -> return (Just dta)
        [(dta, tileid :: T.Text, _)] ->
            case filterTile z style dta of
              Left err -> raise (cs err)
              Right newdta -> do
                liftIO $ execute conn "update images set tile_data=?,shrinked=1 where tile_id=?"
                              (newdta, tileid)
                return (Just newdta)
        _ -> return Nothing

    -- Ordinarily get tile from database; if styling enabled, do the styling
    getTile conn z x tms_y = do
      rows <- liftIO $ query conn "select tile_data from tiles where zoom_level=? AND tile_column=? AND tile_row=?"
                      (z, x, tms_y)
      case rows of
        [Only dta] ->
          case mstyle of
            Nothing -> return (Just dta)
            Just (style,_) ->
              case filterTile z style dta of
                Left err     -> raise (cs err)
                Right newdta -> return (Just newdta)
        _ -> return Nothing

main :: IO ()
main = do
  opts <- execParser progOpts

  case opts of
    CmdDump{fMvtSource, fZoomLevel, fStyle, fSourceName} -> do
        style <- getStyle fStyle >>= checkStyle fSourceName
        dumpPbf style fZoomLevel fMvtSource
    CmdMbtiles{fMbtiles, fStyle, fSourceName} -> do
        style <- getStyle fStyle >>= checkStyle fSourceName
        convertMbtiles style fMbtiles
    CmdWebServer{fModStyle, fWebPort, fMbtiles, fLazyUpdate, fSourceName} -> do
        mstyle <- getMStyle fModStyle fSourceName
        runWebServer fWebPort mstyle fMbtiles fLazyUpdate
    CmdPublish{fModStyle, fSourceName, fUrlPrefix, fStoreTgt, fMbtiles, fThreads } -> do
        mstyle <- getMStyle fModStyle fSourceName
        runPublishJob fThreads mstyle fMbtiles fUrlPrefix fStoreTgt
  where
    getMStyle stname tilesrc =
      case stname of
        Nothing -> return Nothing
        Just fp -> do
          st <- getStyle fp  >>= checkStyle tilesrc
          fstat <- getFileStatus fp
          return (Just (st, modificationTime fstat))
