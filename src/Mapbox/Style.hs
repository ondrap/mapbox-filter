{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Mapbox.Style where

import           Data.Aeson        (FromJSON (..), (.:), (.:?))
import qualified Data.Aeson        as AE
import qualified Data.Text         as T

import           Mapbox.Expression (TExp, typeCheckFilter)

data Layer =
  VectorLayer {
    _lSource      :: T.Text
  , _lSourceLayer :: T.Text
  , _lFilter      :: Maybe (TExp Bool)
} | RasterLayer {
    _lSource :: T.Text
} deriving (Show)
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
            Just uexp -> either fail (return . Just) (typeCheckFilter uexp)
        return VectorLayer{..}

data MapboxStyle = MapboxStyle {
    msLayers :: [Layer]
} deriving (Show)

instance FromJSON MapboxStyle where
  parseJSON = AE.withObject "Style" $ \o ->
      MapboxStyle <$> o .: "layers"
