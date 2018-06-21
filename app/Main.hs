module Main where

import           Control.Applicative     (liftA2)
import           Control.Lens            (over, (&), (^.))
import qualified Data.Aeson              as AE
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BL
import qualified Data.HashMap.Strict     as HMap
import           Data.List               (foldl')
import           Data.Maybe              (fromMaybe)
import           Data.String.Conversions (cs)
import qualified Data.Text               as T
import qualified Data.Vector             as V
import           Geography.VectorTile    (Layer, VectorTile, layers,
                                          linestrings, name, points, polygons)

import           Mapbox.Interpret        (CompiledExpr, FeatureType (..),
                                          runFilter)
import           Mapbox.Style            (MapboxStyle)

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
    layerFilters = foldl' (\hm (k,v) -> HMap.insertWith (liftA2 (&&)) (cs k) v hm) mempty slist

main :: IO ()
main = return ()
