{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TemplateHaskell   #-}

module Mapbox.Style where

import           Control.Lens      (makeLenses, makePrisms)
import           Data.Aeson        (FromJSON (..), (.:), (.:?))
import qualified Data.Aeson        as AE
import qualified Data.Text         as T

import           Mapbox.Expression (typeCheckFilter)
import           Mapbox.Interpret  (CompiledExpr, compileExpr)

data VectorLayer = VectorLayer {
    _lSource      :: T.Text
  , _lSourceLayer :: T.Text
  , _lFilter      :: Maybe (CompiledExpr Bool)
  , _lMinZoom     :: Maybe Int
  , _lMaxZoom     :: Maybe Int
}
makeLenses ''VectorLayer

data Layer =
    VectorType VectorLayer
  | RasterLayer T.Text
makePrisms ''Layer

instance FromJSON Layer where
  parseJSON = AE.withObject "Layer" $ \o -> do
    _lSource <- o .: "source"
    _lMinZoom <- o .:? "minzoom"
    _lMaxZoom <- o .:? "maxzoom"
    ltype <- o .: "type"
    case (ltype :: T.Text) of
      "raster" -> return (RasterLayer _lSource)
      _ -> do
        _lSourceLayer <- o .: "source-layer"
        flt <- o .:? "filter"
        _lFilter <- case flt of
            Nothing   -> return Nothing
            Just uexp -> either fail (return . Just) (compileExpr <$> typeCheckFilter uexp)
        return (VectorType VectorLayer{..})

data MapboxStyle = MapboxStyle {
    _msLayers :: [Layer]
}
makeLenses ''MapboxStyle

instance FromJSON MapboxStyle where
  parseJSON = AE.withObject "Style" $ \o ->
      MapboxStyle <$> o .: "layers"
