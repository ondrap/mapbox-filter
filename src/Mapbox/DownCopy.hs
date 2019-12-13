{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ApplicativeDo     #-}

module Mapbox.DownCopy where

import           Control.Lens             ((&), (^.), (%~), makeLenses, over, _1, (.~))
import           Data.List                (foldl')
import           Data.Aeson               (FromJSON (..), (.:))
import qualified Data.Aeson               as AE
import qualified Data.Text as T
import qualified Data.ByteString.Lazy     as BL
import qualified Data.HashMap.Strict      as HMap
import           Geography.VectorTile      (VectorTile(..),
                                            linestrings, points,
                                            polygons, Point(..), geometries, layers,
                                            LineString(..), Polygon(..), extent,
                                            featureId)
import Data.String.Conversions.Monomorphic (fromST)
import qualified Data.Vector.Storable as VS
import qualified Data.Vector          as V

import Mapbox.Filters (CFilters, simpleFilter, CFilter(..), simpleNegFilter)
import           Mapbox.Interpret         (CompiledExpr, compileExpr)
import           Mapbox.Expression        (typeCheckFilter)

  
data DownCopySpec = DownCopySpec {
    _dDstZoom  :: Int
  , _dSourceLayer :: BL.ByteString
  , _dFilter   :: CompiledExpr Bool
}
makeLenses ''DownCopySpec

instance FromJSON DownCopySpec where
  parseJSON = AE.withObject "DownCopySpec" $ \o -> do
    _dDstZoom <- o .: "dst-zoom"
    uexp <- o .: "filter"
    -- Directly typecheck and compile filter
    _dFilter <- either (fail . T.unpack) return (compileExpr <$> typeCheckFilter uexp)
    _dSourceLayer <- fromST <$> o .: "source-layer"
    return DownCopySpec{..}

mkCFilters :: DownCopySpec -> CFilters
mkCFilters fspec = HMap.singleton (fspec ^. dSourceLayer) (CFilter (fspec ^. dFilter) mempty)

copyDown :: Maybe DownCopySpec -> VectorTile -> [(VectorTile, (Int, Int))] -> VectorTile
copyDown _ mtile [] = mtile
copyDown Nothing mtile _ = mtile
copyDown (Just fspec) dsttile srcTiles =
    let fltdst = simpleNegFilter lfilters dsttile
        fltsrc = map shrinkTile . over (traverse . _1) (simpleFilter lfilters) $ srcTiles
    in foldl' mergeTile fltdst fltsrc
  where
    lfilters = mkCFilters fspec

    -- Divide by 2 and move according to offset
    shrinkTile (vtile, (dx, dy)) = 
      over (layers . traverse) (applyOperation (dx, dy)) vtile

    applyOperation (dx, dy) layer =
      let ext = fromIntegral (layer ^. extent) -- tile square size
          op (Point x y) = Point ((dx * ext + x) `div` 2) ((dy * ext + y) `div` 2)
      in layer & points %~ over (traverse . geometries) (VS.map op)
               & linestrings %~ over (traverse . geometries . traverse) (applyLine op)
               & polygons %~ over (traverse . geometries . traverse) (applyPolygon op)
    
    applyLine op (LineString plist) = LineString (VS.map op plist)
    applyPolygon op (Polygon pp inp) = Polygon (VS.map op pp) (fmap (applyPolygon op) inp)
    
    -- Merge 2 tiles into one
    mergeTile (VectorTile _l1) (VectorTile _l2) = VectorTile (HMap.unionWith mergeLayer _l1 _l2)
    mergeLayer l1 l2 = l1 & points %~ (`addAndRenumber` (l2 ^. points))
                          & linestrings %~ (`addAndRenumber` (l2 ^. linestrings))
                          & polygons %~ (`addAndRenumber` (l2 ^. polygons))
    addAndRenumber l1 l2
      | V.null l2 = l1
      | otherwise = V.fromListN (V.length l1 + V.length l2) 
                                (zipWith (\idx f -> f & featureId .~ idx) 
                                         [1..] (V.toList l1 <> V.toList l2))
