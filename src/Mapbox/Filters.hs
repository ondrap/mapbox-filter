{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Helper module for
module Mapbox.Filters where

import           Control.Applicative       ((<|>))
import           Control.Lens              (filtered, over, toListOf, (&), (^.))
import           Control.Monad.Trans.Class (lift)
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
                                            polygons, Val(St))
import Data.Text.ICU.Shape (shapeArabic, ShapeOption(..))
import Data.Text.ICU.BiDi (reorderParagraphs, WriteOption(..))

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
getLayerFilter :: Bool -> BL.ByteString -> CFilters -> CFilter
getLayerFilter defval lname layerFilters =
  fromMaybe (CFilter (return defval) mempty) (HMap.lookup lname layerFilters)


simpleFilter' :: Bool -> CFilters -> VectorTile -> VectorTile
simpleFilter' defval layerFilters = over (layers . traverse) runLayerFilter
  where
    runLayerFilter :: Layer -> Layer
    runLayerFilter l =
      let lfilter = getLayerFilter defval (l ^. name) layerFilters
      in l & over points (V.filter (runFilter (cfExpr lfilter) Point))
           & over linestrings (V.filter (runFilter (cfExpr lfilter) LineString))
           & over polygons (V.filter (runFilter (cfExpr lfilter) Polygon))

simpleFilter :: CFilters -> VectorTile -> VectorTile
simpleFilter = simpleFilter' False

simpleNegFilter :: CFilters -> VectorTile -> VectorTile
simpleNegFilter layerFilters = simpleFilter' True (fmap negFilters layerFilters)
  where
    negFilters (CFilter flt meta) = CFilter (not <$> flt) meta

-- | Entry is list of layers with filter (non-existent filter should be replaced with 'return True')
-- Returns nothing if the resulting tile is empty
filterVectorTile :: Bool -> CFilters -> VectorTile -> VectorTile
filterVectorTile rtlconvert layerFilters =
    over layers (HMap.filter (not . nullLayer)) . over (layers . traverse) runMetaFilter . simpleFilter layerFilters
  where
    nullLayer l = null (l ^. points)
                  && null (l ^. linestrings)
                  && null (l ^. polygons)

    runMetaFilter :: Layer -> Layer
    runMetaFilter l =
      let lfilter = getLayerFilter False (l ^. name) layerFilters
      in l & over points (fmap (clearMeta lfilter))
           & over linestrings (fmap (clearMeta lfilter))
           & over polygons (fmap (clearMeta lfilter))

    clearMeta :: CFilter -> Feature a -> Feature a
    clearMeta cf = over metadata (fmap stringConversion . HMap.filterWithKey (\k _ -> k `elem` cfMeta cf))

    stringConversion :: Val -> Val
    stringConversion | rtlconvert = valRtlConvert
                     | otherwise = id
      where
        valRtlConvert (St bstr) = 
          bstr & cs
              & shapeArabic [LettersShape]
              & reorderParagraphs [DoMirroring, RemoveBidiControls]
              & T.intercalate "\n"
              & cs
              & St
        valRtlConvert v = v


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

-- | Decode, filter, encode based on zoom level and mapbox style
filterTile :: Bool -> Int -> MapboxStyle -> VectorTile -> VectorTile
filterTile rtlconvert z style = filterVectorTile rtlconvert (styleToCFilters z style)
