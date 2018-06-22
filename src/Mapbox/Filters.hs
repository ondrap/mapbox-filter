-- | Helper module for
module Mapbox.Filters where

import           Codec.Compression.GZip    (CompressParams (compressLevel),
                                            bestCompression, compressWith,
                                            decompress, defaultCompressParams)
import           Control.Applicative       ((<|>))
import           Control.Lens              (filtered, over, toListOf, (&), (^.))
import           Control.Monad.Trans.Class (lift)
import           Data.Bifunctor            (second)
import           Data.Bool                 (bool)
import qualified Data.ByteString.Lazy      as BL
import qualified Data.HashMap.Strict       as HMap
import           Data.Maybe                (fromMaybe)
import           Data.String.Conversions   (cs)
import qualified Data.Text                 as T
import qualified Data.Vector               as V
import           Geography.VectorTile      (Layer, VectorTile, layers,
                                            linestrings, name, points, polygons,
                                            tile, untile)

import           Mapbox.Interpret          (CompiledExpr, FeatureType (..),
                                            runFilter)
import           Mapbox.Style              (MapboxStyle, msLayers, _VectorLayer)


type CFilters = HMap.HashMap BL.ByteString (CompiledExpr Bool)


-- | Return a filter from the CFilters map
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


-- | Convert style and zoom level to a map of (source_layer, filter)
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

-- | Decode, filter, encode based on CFilters map
filterTileCs :: CFilters -> BL.ByteString -> Either T.Text BL.ByteString
filterTileCs cstyles dta =
    second doFilterTile (tile (cs (decompress dta)))
  where
    doFilterTile = compressWith compressParams . cs . untile . filterVectorTile cstyles
    -- | Default compression settings
    compressParams = defaultCompressParams{compressLevel=bestCompression}

-- | Decode, filter, encode based on zoom level and mapbox style
filterTile :: Int -> MapboxStyle -> BL.ByteString -> Either T.Text BL.ByteString
filterTile z style = filterTileCs (styleToCFilters z style)

