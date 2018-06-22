{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Codec.Compression.GZip               (compress, decompress)
import           Control.Monad.Trans.Class            (lift)

import           Control.Concurrent.ParallelIO.Global (parallel_,
                                                       stopGlobalPool)
import           Control.Exception.Safe               (catchAny)
import           Control.Lens                         (filtered, over, toListOf,
                                                       (%~), (&), (^.), (^..),
                                                       _1)
import           Control.Monad                        (when)
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
import qualified Data.Vector                          as V
import           Database.SQLite.Simple               (Only (..), execute,
                                                       execute_, query, query_,
                                                       withConnection)
import           Geography.VectorTile                 (Layer, VectorTile,
                                                       layers, linestrings,
                                                       metadata, name, points,
                                                       polygons, tile, untile)
import           Options.Applicative                  hiding (header, style)
import           System.Posix.Files                   (getFileStatus,
                                                       modificationTime)
import           System.Posix.Types                   (EpochTime)
import           Text.Read                            (readMaybe)
import           Web.Scotty                           (addHeader, get, header,
                                                       json, param, raise, raw,
                                                       scotty, setHeader)

import           Mapbox.Interpret                     (CompiledExpr,
                                                       FeatureType (..),
                                                       runFilter)
import           Mapbox.Style                         (MapboxStyle, lSource,
                                                       msLayers, _VectorLayer)


type CFilters = HMap.HashMap BL.ByteString (CompiledExpr Bool)

getLayerFilter :: BL.ByteString -> CFilters -> CompiledExpr Bool
getLayerFilter lname layerFilters = fromMaybe (return False) (HMap.lookup lname layerFilters)

-- | Entry is list of layers with filter (non-existent filter should be replaced with 'return True')
filterVectorTile :: CFilters -> VectorTile -> VectorTile
filterVectorTile layerFilters =
    over layers (HMap.filter (not . nullLayer)) . over (layers . traverse) runLayerFilter
  where
    nullLayer l = null (l ^. points)
                  && null (l ^. linestrings)
                  && null (l ^. polygons)

    runLayerFilter :: Layer -> Layer
    runLayerFilter l =
      let lfilter = getLayerFilter (l ^. name) layerFilters
      in l & over points (V.filter (runFilter lfilter Point))
           & over linestrings (V.filter (runFilter lfilter LineString))
           & over polygons (V.filter (runFilter lfilter Polygon))


-- | Convert style and zoom level to a list of (source_layer, filter)
styleToCFilters :: Int -> MapboxStyle -> CFilters
styleToCFilters zoom =
    HMap.fromListWith combineFilters
  . map (\(_, srclayer, mfilter, _, _) -> (cs srclayer, fromMaybe (return True) mfilter))
  . toListOf (msLayers . traverse . _VectorLayer . filtered acceptFilter)
  where
    -- 'Or' on expressions, but if first fails, still try the second
    combineFilters :: CompiledExpr Bool -> CompiledExpr Bool -> CompiledExpr Bool
    combineFilters fa fb = (fa >>= bool (lift Nothing) (return True)) <|> fb

    acceptFilter (_,_,_,minz,maxz) = zoomMinOk minz && zoomMaxOk maxz

    zoomMinOk Nothing     = True
    zoomMinOk (Just minz) = zoom >= minz
    zoomMaxOk Nothing     = True
    zoomMaxOk (Just maxz) = zoom <= maxz

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

dumpOptions :: Parser CmdLine
dumpOptions = CmdDump <$> strOption (short 'j' <> long "style" <> help "JSON mapbox style file")
                      <*> optional (T.pack <$> strOption (short 's' <> long "source" <> help "Tile source name"))
                      <*> option auto (short 'z' <> long "zoom" <> help "Tile zoom level")
                      <*> argument str (metavar "SRCFILE" <> help "Source file")

mbtileOptions :: Parser CmdLine
mbtileOptions = CmdMbtiles <$> strOption (short 'j' <> long "style" <> help "JSON mapbox style file")
                           <*> optional (T.pack <$> strOption (short 's' <> long "source" <> help "Tile source name"))
                           <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")

webOptions :: Parser CmdLine
webOptions = CmdWebServer <$> optional (strOption (short 'j' <> long "style" <> help "JSON mapbox style file"))
                          <*> optional (T.pack <$> strOption (short 's' <> long "source" <> help "Tile source name"))
                          <*> option auto (short 'p' <> long "port" <> help "Web port number")
                          <*> switch (short 'l' <> long "lazy" <> help "Lazily update the database with filtered data")
                          <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")

cmdLineParser  :: Parser CmdLine
cmdLineParser =
  subparser $
    command "dump" (info (helper <*> dumpOptions) (progDesc "Dump vector files contents."))
    <> command "filter" (info (helper <*> mbtileOptions) (progDesc "Run filtering on a MBTiles database"))
    <> command "web" (info (helper <*> webOptions) (progDesc "Run a web server for serving tiles"))

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

convertMbtiles :: MapboxStyle -> FilePath -> IO ()
convertMbtiles style fp =
  withConnection fp $ \conn -> do
    zlevels <- query_ conn "select distinct zoom_level from map order by zoom_level"
    for_ zlevels $ \(Only zoom) -> do
      putStrLn $ "Filtering zoom: " <> show zoom
      let filtList = styleToCFilters zoom style

      (tiles :: [Only T.Text]) <- query conn "select tile_id from map where zoom_level=?" (Only zoom)
      putStrLn $ "Tiles: " <> show (length tiles)
      parallel_ $ shrinkTile conn filtList <$> tiles
      -- sequence_ $ (shrinkTile conn filtList) <$> tiles
    stopGlobalPool
    execute_ conn "vacuum"
  where
    shrinkTile conn filtList (Only tileid) = do
      [Only (tdata :: BL.ByteString)] <- query conn "select tile_data from images where tile_id=?" (Only tileid)
      case tile (cs $ decompress tdata) of
        Left err -> putStrLn $ "Error when decoding tile " <> show tileid <> ": " <> cs err
        Right vtile -> do
          let restile = filterVectorTile filtList vtile
          execute conn "update images set tile_data=? where tile_id=?" (compress (cs (untile restile)), tileid)

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
          dbmtime <- liftIO $ getDbMtime conn
          let stmtime = maybe 0 snd mstyle
          let mtime = dbmtime <> "_" <> show stmtime
          metaJson <- liftIO $ genJson conn (cs mtime) proto host
          addHeader "Access-Control-Allow-Origin" "*"
          json metaJson
      get "/tiles/:mt1/:z/:x/:y" $ do
          z :: Int <- param "z"
          x :: Int <- param "x"
          y :: Int <- param "y"
          let tms_y = 2 ^ z - y - 1

          mnewtile <- case (mstyle, lazyUpdate) of
              (Just (style,_), True) -> getLazyTile conn style z x tms_y
              _                      -> getTile conn z x tms_y

          addHeader "Access-Control-Allow-Origin" "*"
          setHeader "Content-Type" "application/x-protobuf"
          setHeader "Cache-control" "max-age=31536000"

          case mnewtile of
            Just dta -> do
              addHeader "Content-Encoding" "gzip"
              raw dta
            Nothing -> raw ""
  where
    getDbMtime conn = do
      mlines :: [Only String] <- query_ conn "select value from metadata where name='mtime'"
      case mlines of
        [Only res] -> return res
        _          -> return ""

    genJson conn minfo proto host = do
      metalines :: [(T.Text,String)] <- query_ conn "select name,value from metadata"
      return $ AE.object $
          concatMap addMetaLine metalines
          ++ ["tiles" .= [proto <> "://" <> host <> "/tiles/" <> minfo <> "/{z}/{x}/{y}"],
              "tilejson" .= ("2.0.0" :: T.Text)
              ]

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

    -- Get tile from modified database; if not already shrinked, shrink it and write to the database
    getLazyTile conn style z x tms_y = do
      rows <- liftIO $ query conn "select tile_data, map.tile_id, shrinked from images,map where zoom_level=? AND tile_column=? AND tile_row=? AND map.tile_id=images.tile_id"
                (z, x, tms_y)
      case rows of
        [(dta, _, 1 :: Int)] -> return (Just dta)
        [(dta, tileid :: T.Text, _)] -> do
            -- Shrink
            let cstyles = styleToCFilters z style
            case tile (cs (decompress dta)) of
              Left err -> raise (cs err)
              Right dtile -> do
                let newdta = compress . cs . untile . filterVectorTile cstyles $ dtile
                liftIO $ execute conn "update images set tile_data=?,shrinked=1 where tile_id=?"
                              (newdta, tileid)
                return $ Just newdta
        _ -> return Nothing

    -- Ordinarily get tile from database; if styling enabled, do the styling
    getTile conn z x tms_y = do
      rows <- liftIO $ query conn "select tile_data from tiles where zoom_level=? AND tile_column=? AND tile_row=?"
                      (z, x, tms_y)
      case rows of
        [Only dta] ->
          case mstyle of
            Nothing -> return (Just dta)
            Just (style,_) -> do
              -- Shrink
              let cstyles = styleToCFilters z style
              case tile (cs (decompress dta)) of
                Left err -> raise (cs err)
                Right dtile -> return $ Just (compress . cs . untile . filterVectorTile cstyles $ dtile)
        _ -> return Nothing

main :: IO ()
main = do
  opts <- execParser progOpts
  let tilesrc = fSourceName opts

  case opts of
    CmdDump{fMvtSource, fZoomLevel, fStyle} -> do
        style <- getStyle fStyle >>= checkStyle tilesrc
        dumpPbf style fZoomLevel fMvtSource
    CmdMbtiles{fMbtiles, fStyle} -> do
        style <- getStyle fStyle >>= checkStyle tilesrc
        convertMbtiles style fMbtiles
    CmdWebServer{fModStyle, fWebPort, fMbtiles, fLazyUpdate} -> do
        mstyle <- case fModStyle of
            Nothing -> return Nothing
            Just fp -> do
              st <- getStyle fp  >>= checkStyle tilesrc
              fstat <- getFileStatus fp
              return (Just (st, modificationTime fstat))
        runWebServer fWebPort mstyle fMbtiles fLazyUpdate
