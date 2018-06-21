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

data Layer =
  VectorLayer {
    _lSource      :: T.Text
  , _lSourceLayer :: T.Text
  , _lFilter      :: Maybe (CompiledExpr Bool)
} | RasterLayer {
    _lSource :: T.Text
}
makePrisms ''Layer

instance FromJSON Layer where
  parseJSON = AE.withObject "Layer" $ \o -> do
    _lSource <- o .: "source"
    ltype <- o .: "type"
    case (ltype :: T.Text) of
      "raster" -> return RasterLayer{..}
      _ -> do
        _lSourceLayer <- o .: "source-layer"
        flt <- o .:? "filter"
        _lFilter <- case flt of
            Nothing   -> return Nothing
            Just uexp -> either fail (return . Just) (compileExpr <$> typeCheckFilter uexp)
        return VectorLayer{..}

data MapboxStyle = MapboxStyle {
    _msLayers :: [Layer]
}
makeLenses ''MapboxStyle

instance FromJSON MapboxStyle where
  parseJSON = AE.withObject "Style" $ \o ->
      MapboxStyle <$> o .: "layers"
