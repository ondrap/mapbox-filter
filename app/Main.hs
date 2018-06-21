{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Control.Applicative     (liftA2)
import           Control.Lens            (filtered, over, toListOf, (&), (^.),
                                          (^..))
import qualified Data.Aeson              as AE
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BL
import           Data.Foldable           (for_)
import qualified Data.HashMap.Strict     as HMap
import           Data.List               (foldl')
import           Data.Maybe              (fromMaybe)
import           Data.String.Conversions (cs)
import qualified Data.Text               as T
import qualified Data.Vector             as V
import           Geography.VectorTile    (Layer, VectorTile, layers,
                                          linestrings, name, points, polygons,
                                          tile, untile)

import           Mapbox.Interpret        (CompiledExpr, FeatureType (..),
                                          runFilter)
import           Mapbox.Style            (MapboxStyle, msLayers, _VectorLayer)

-- | Entry is list of layers with filter (non-existent filter should be replaced with 'return True')
filterVectorTile :: [(T.Text, CompiledExpr Bool)] -> VectorTile -> VectorTile
filterVectorTile slist = over (layers . traverse) runLayerFilter
  where
    runLayerFilter :: Layer -> Layer
    runLayerFilter l =
      let lfilter = fromMaybe (return False) (HMap.lookup (l ^. name) layerFilters)
      in l & over points (V.filter (runFilter lfilter Point))
           & over linestrings (V.filter (runFilter lfilter LineString))
           & over polygons (V.filter (runFilter lfilter Polygon))
    layerFilters :: HMap.HashMap BL.ByteString (CompiledExpr Bool)
    layerFilters = foldl' (\hm (k,v) -> HMap.insertWith (liftA2 (||)) (cs k) v hm) mempty slist

-- | Convert style to a list of (source_layer, filter)
styleToSlist :: T.Text -> MapboxStyle -> [(T.Text, CompiledExpr Bool)]
styleToSlist source =
  map (\(_, srclayer, mfilter) -> (srclayer, fromMaybe (return True) mfilter))
  . toListOf (msLayers . traverse . _VectorLayer . filtered (\(src,_,_) -> src == source))

main :: IO ()
main = do
  bstyle <- BS.readFile "openmaptiles.json.js"
  let Just style = AE.decodeStrict bstyle

  mvt <- BS.readFile "10729.pbf"
  case tile mvt of
    Left err -> error (show err)
    Right vtile -> do
      -- Print statistics
      -- print (length (vtile ^. layers . traverse . points))
      -- print (length (vtile ^. layers . traverse . linestrings))
      -- print (length (vtile ^. layers . traverse . polygons))
      for_ (vtile ^.. layers . traverse) $ \l -> do
        print (l ^. name)
        print ((l ^. linestrings))

      let slist = styleToSlist "openmaptiles" style
          res = filterVectorTile slist vtile
      -- print (length (res ^. layers . traverse . points))
      -- print (length (res ^. layers . traverse . linestrings))
      print "-----------------------------------------"
      for_ (res ^.. layers . traverse) $ \l -> do
            print (l ^. name)
            print ((l ^. linestrings))
      BS.writeFile "10729-new.pbf" (untile res)
