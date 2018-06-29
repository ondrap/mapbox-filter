{-# LANGUAGE RankNTypes #-}

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
import qualified Data.HashSet              as HSet
import           Data.Maybe                (fromMaybe)
import           Data.Semigroup            ((<>))
import           Data.String.Conversions   (cs)
import qualified Data.Text                 as T
import qualified Data.Vector               as V
import           Geography.VectorTile      (Feature, Layer, VectorTile, layers,
                                            linestrings, metadata, name, points,
                                            polygons, tile, untile)

import           Mapbox.Interpret          (CompiledExpr, FeatureType (..),
                                            runFilter)
import           Mapbox.Style              (MapboxStyle, lDisplayMeta, lFilter,
                                            lFilterMeta, lMaxZoom, lMinZoom,
                                            lSourceLayer, msLayers, _VectorType)

-- TODO - we can make the metadata selection granular on a per-feature level...

data CFilter = CFilter {
    cfExpr :: CompiledExpr Bool
  , cfMeta :: HSet.HashSet BL.ByteString
}

type CFilters = HMap.HashMap BL.ByteString CFilter

-- | Return a filter from the CFilters map
-- If the layer does not have a record in CFilters, it will be filtered out
getLayerFilter :: BL.ByteString -> CFilters -> CFilter
getLayerFilter lname layerFilters =
  fromMaybe (CFilter (return False) mempty) (HMap.lookup lname layerFilters)

-- | Entry is list of layers with filter (non-existent filter should be replaced with 'return True')
-- Returns nothing if the resulting tile is empty
filterVectorTile :: CFilters -> VectorTile -> Maybe VectorTile
filterVectorTile layerFilters =
    checkEmptyTile . over layers (HMap.filter (not . nullLayer)) . over (layers . traverse) runLayerFilter
  where
    -- Return nothing if there are no layers (and therefore no features) on the tile
    checkEmptyTile t | null (t ^. layers)  = Nothing
                     | otherwise = Just t

    nullLayer l = null (l ^. points)
                  && null (l ^. linestrings)
                  && null (l ^. polygons)

    runLayerFilter :: Layer -> Layer
    runLayerFilter l =
      let lfilter = getLayerFilter (l ^. name) layerFilters
      in l & over points (fmap (clearMeta lfilter) . V.filter (runFilter (cfExpr lfilter) Point))
           & over linestrings (fmap (clearMeta lfilter) . V.filter (runFilter (cfExpr lfilter) LineString))
           & over polygons (fmap (clearMeta lfilter) . V.filter (runFilter (cfExpr lfilter) Polygon))

clearMeta :: CFilter ->  Feature a -> Feature a
clearMeta cf = over metadata (HMap.filterWithKey (\k _ -> k `elem` cfMeta cf))

-- | Convert style and zoom level to a map of (source_layer, filter)
styleToCFilters :: Int -> MapboxStyle -> CFilters
styleToCFilters zoom =
    HMap.fromListWith combineFilters
  . map (\l -> (cs (l ^. lSourceLayer), mkFilter l))
  . toListOf (msLayers . traverse . _VectorType . filtered acceptFilter)
  where
    mkFilter vl = CFilter (fromMaybe (return True) (vl ^. lFilter))
                          (vl ^. lDisplayMeta <> vl ^. lFilterMeta)

    -- 'Or' on expressions, but if first fails, still try the second
    combineFilters :: CFilter -> CFilter -> CFilter
    combineFilters a b =
      let fa = cfExpr a
          fb = cfExpr b
      in CFilter ((fa >>= bool (lift Nothing) (return True)) <|> fb)
                 (cfMeta a <> cfMeta b)

    acceptFilter l = zoomMinOk (l ^. lMinZoom) && zoomMaxOk (l ^. lMaxZoom)

    zoomMinOk Nothing     = True
    zoomMinOk (Just minz) = zoom >= minz
    zoomMaxOk Nothing     = True
    zoomMaxOk (Just maxz) = zoom <= maxz

-- | Decode, filter, encode based on CFilters map
filterTileCs :: CFilters -> BL.ByteString -> Either T.Text (Maybe BL.ByteString)
filterTileCs cstyles dta =
    second doFilterTile (tile (cs (decompress dta)))
  where
    doFilterTile tl = compressWith compressParams . cs . untile <$> filterVectorTile cstyles tl
    -- | Default compression settings
    compressParams = defaultCompressParams{compressLevel=bestCompression}

-- | Decode, filter, encode based on zoom level and mapbox style
filterTile :: Int -> MapboxStyle -> BL.ByteString -> Either T.Text (Maybe BL.ByteString)
filterTile z style = filterTileCs (styleToCFilters z style)

