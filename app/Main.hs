{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Codec.Compression.GZip               (compress, decompress)
import           Control.Applicative                  (liftA2)
import           Control.Concurrent.ParallelIO.Global (parallel_,
                                                       stopGlobalPool)
import           Control.Lens                         (filtered, over, toListOf,
                                                       (&), (^.), (^..))
import           Control.Monad                        (when)
import qualified Data.Aeson                           as AE
import           Data.Bool                            (bool)
import qualified Data.ByteString                      as BS
import qualified Data.ByteString.Lazy                 as BL
import           Data.Foldable                        (for_)
import qualified Data.HashMap.Strict                  as HMap
import           Data.List                            (foldl', nub)
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
import           Options.Applicative                  hiding (style)

import           Mapbox.Interpret                     (CompiledExpr,
                                                       FeatureType (..),
                                                       runFilter)
import           Mapbox.Style                         (MapboxStyle, lSource,
                                                       msLayers, _VectorLayer)

-- | Entry is list of layers with filter (non-existent filter should be replaced with 'return True')
filterVectorTile :: [(BL.ByteString, CompiledExpr Bool)] -> VectorTile -> VectorTile
filterVectorTile slist =
    over layers (HMap.filter (not . nullLayer)) . over (layers . traverse) runLayerFilter
  where
    nullLayer l = null (l ^. points)
                  && null (l ^. linestrings)
                  && null (l ^. polygons)

    runLayerFilter :: Layer -> Layer
    runLayerFilter l =
      let lfilter = fromMaybe (return False) (HMap.lookup (l ^. name) layerFilters)
      in l & over points (V.filter (runFilter lfilter Point))
           & over linestrings (V.filter (runFilter lfilter LineString))
           & over polygons (V.filter (runFilter lfilter Polygon))
    layerFilters :: HMap.HashMap BL.ByteString (CompiledExpr Bool)
    layerFilters = HMap.fromListWith (liftA2 (||)) slist

-- | Convert style and zoom level to a list of (source_layer, filter)
styleToSlist :: T.Text -> Int -> MapboxStyle -> [(BL.ByteString, CompiledExpr Bool)]
styleToSlist source zoom =
  map (\(_, srclayer, mfilter, _, _) -> (cs srclayer, fromMaybe (return True) mfilter))
  . toListOf (msLayers . traverse . _VectorLayer . filtered (\(src,_,_, minz, maxz) -> src == source && zoomMinOk minz && zoomMaxOk maxz))
  where
    zoomMinOk Nothing     = True
    zoomMinOk (Just minz) = zoom >= minz
    zoomMaxOk Nothing     = True
    zoomMaxOk (Just maxz) = zoom <= maxz

data CmdLine =
    CmdDump {
      fStyle      :: FilePath
    , fSourceName :: T.Text
    , fZoomLevel  :: Int
    , fMvtSource  :: FilePath
  }
  | CmdMbtiles {
      fStyle      :: FilePath
    , fSourceName :: T.Text
    , fMbtiles    :: FilePath
  }

dumpOptions :: Parser CmdLine
dumpOptions = CmdDump <$> strOption (short 'j' <> long "style" <> help "JSON mapbox style file")
                      <*> (T.pack <$> strOption (short 's' <> long "source" <> help "Tile source name"))
                      <*> option auto (short 'z' <> long "zoom" <> help "Tile zoom level")
                      <*> argument str (metavar "SRCFILE" <> help "Source file")

mbtileOptions :: Parser CmdLine
mbtileOptions = CmdMbtiles <$> strOption (short 'j' <> long "style" <> help "JSON mapbox style file")
                           <*> (T.pack <$> strOption (short 's' <> long "source" <> help "Tile source name"))
                           <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")

cmdLineParser  :: Parser CmdLine
cmdLineParser =
  subparser $
    command "dump" (info (helper <*> dumpOptions) (progDesc "Dump vector files contents."))
    <> command "shrink" (info (helper <*> mbtileOptions) (progDesc "Run filtering on a MBTiles database"))

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

checkStyle :: MapboxStyle -> T.Text -> IO ()
checkStyle styl tilesrc = do
  -- Print vector styles
  let sources = nub (styl ^.. msLayers . traverse . lSource)
  for_ sources $ \s ->
    T.putStrLn $ "Found source layer: " <> s
  when (tilesrc `notElem` sources) $
    error "Invalid tile source specified"

dumpPbf :: MapboxStyle -> T.Text -> Int -> FilePath -> IO ()
dumpPbf style tilesrc zoom fp = do
  mvt <- BS.readFile fp
  case tile mvt of
    Left err -> error (show err)
    Right vtile ->
      for_ (vtile ^.. layers . traverse) $ \l -> do
          T.putStrLn "-----------------------------"
          T.putStrLn ("Layer: " <> cs (l ^. name))
          let lfilter = fromMaybe (return False) (HMap.lookup (l ^. name) layerFilters)
          for_ (l ^. points) (printCont lfilter Point)
          for_ (l ^. linestrings) (printCont lfilter LineString)
          for_ (l ^. polygons) (printCont lfilter Polygon)
  where
    slist = styleToSlist tilesrc zoom style

    layerFilters :: HMap.HashMap BL.ByteString (CompiledExpr Bool)
    layerFilters = foldl' (\hm (k,v) -> HMap.insertWith (liftA2 (||)) (cs k) v hm) mempty slist

    printCont lfilter ptype feature = do
      let include = runFilter lfilter ptype feature
      putStrLn $ bool "- " "  " include <> show ptype <> " " <> show (feature ^. metadata)

convertMbtiles :: MapboxStyle -> T.Text -> FilePath -> IO ()
convertMbtiles style tilesrc fp =
  withConnection fp $ \conn -> do
    zlevels <- query_ conn "select distinct zoom_level from map order by zoom_level"
    for_ zlevels $ \(Only zoom) -> do
      putStrLn $ "Shrinking zoom: " <> show zoom
      let filtList = styleToSlist tilesrc zoom style

      (tiles :: [Only T.Text]) <- query conn "select tile_id from map where zoom_level=?" (Only zoom)
      putStrLn $ "Tiles: " <> show (length tiles)
      parallel_ $ shrinkTile conn filtList <$> tiles
      -- for_ tiles (shrinkTile conn filtList)
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

main :: IO ()
main = do
  opts <- execParser progOpts

  style <- getStyle (fStyle opts)
  let tilesrc = fSourceName opts
  checkStyle style tilesrc

  case opts of
    CmdDump{fMvtSource, fZoomLevel} -> dumpPbf style tilesrc fZoomLevel fMvtSource
    CmdMbtiles{fMbtiles} -> convertMbtiles style tilesrc fMbtiles
