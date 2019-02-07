{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}

module Mapbox.Style where

import           Control.Lens             (makeLenses, makePrisms, (^.))
import           Data.Aeson               (FromJSON (..), (.!=), (.:), (.:?))
import qualified Data.Aeson               as AE
import qualified Data.ByteString.Lazy     as BL
import           Data.Functor.Foldable    (Fix (..), para)
import qualified Data.HashMap.Strict      as HMap
import qualified Data.HashSet             as HSet
import           Data.Semigroup           (Semigroup (..))
import           Data.String.Conversions  (cs)
import qualified Data.Text                as T

import           Mapbox.Expression        (UExp, typeCheckFilter)
import           Mapbox.Interpret         (CompiledExpr, compileExpr)
import           Mapbox.UntypedExpression (UExpF (..))

data VectorLayer = VectorLayer {
    _lSource      :: T.Text
  , _lSourceLayer :: T.Text
  , _lFilter      :: Maybe (CompiledExpr Bool)
  , _lMinZoom     :: Maybe Int
  , _lMaxZoom     :: Maybe Int
  , _lDisplayMeta :: HSet.HashSet BL.ByteString -- ^ List of meta attributes needed for displaying
  , _lFilterMeta  :: HSet.HashSet BL.ByteString -- ^ List of meta attributes needed for filtering
}
makeLenses ''VectorLayer
instance Show VectorLayer where
  showsPrec p o = showParen (p > 10) $
      showString "VectorLayer " . showsPrec p (o ^. lSource)
      . showChar ' ' . showsPrec p (o ^. lSourceLayer)
      . showChar ' ' . showString (maybe "no-filter" (const "has-filter") (o ^. lFilter))
      . showChar ' ' . showsPrec p (o ^. lMinZoom)
      . showChar ' ' . showsPrec p (o ^. lMaxZoom)
      . showChar ' ' . showsPrec p (o ^. lDisplayMeta)
      . showChar ' ' . showsPrec p (o ^. lFilterMeta)

data Layer =
    VectorType VectorLayer
  | RasterLayer T.Text
  | BackgroundLayer
  deriving (Show)
makePrisms ''Layer

-- | Go through the parsing tree and find references to metadata attributes
scrapeExprMeta :: UExp -> HSet.HashSet T.Text
scrapeExprMeta = para getMeta
  where
    getMeta (UApp "get" [(Fix (UStr tid),_)]) = HSet.singleton tid
    getMeta (UApp "get" _) = error "Unsupported computation in expression 'get', only direct strings allowed"
    getMeta (UApp "has" [(Fix (UStr tid),_)]) = HSet.singleton tid
    getMeta (UApp "has" _) = error "Unsupported computation in expression 'has', only direct strings allowed"
    getMeta (UApp _ lst) = mconcat (snd <$> lst)
    getMeta (ULet _ (_,s1) (_,s2)) = s1 <> s2
    getMeta (UStr str) = deinterpolate str
    getMeta (UFunction{ufProperty=Just tid}) = HSet.singleton tid
    getMeta _ = mempty
    -- Extract metadata names from {} notation from a string
    deinterpolate txt =
      case T.dropWhile (/= '{') txt of
        "" -> mempty
        rest ->
            let (var,next) = T.span (/= '}') (T.drop 1 rest)
            in HSet.singleton var <> deinterpolate next

instance FromJSON Layer where
  parseJSON = AE.withObject "Layer" $ \o -> do
    ltype <- o .: "type"
    case (ltype :: T.Text) of
      "background" ->  return BackgroundLayer
      "raster" -> do
        source <- o .: "source"
        return (RasterLayer source)
      _ -> do -- Vector layers
        _lMinZoom <- o .:? "minzoom"
        _lMaxZoom <- o .:? "maxzoom"
        _lSource <- o .: "source"
        _lSourceLayer <- o .: "source-layer"
        flt <- o .:? "filter"
        -- Directly typecheck and compile filter
        _lFilter <- case flt of
            Nothing   -> return Nothing
            Just uexp -> either (fail . T.unpack) (return . Just) (compileExpr <$> typeCheckFilter uexp)
        -- Scrape used attributes
        (paint :: HMap.HashMap T.Text UExp) <- o .:? "paint" .!= mempty
        layout <- o .:? "layout" .!= mempty
        let _lDisplayMeta = HSet.map cs $ foldMap (scrapeExprMeta . snd) (HMap.toList (paint <> layout))
        let _lFilterMeta = HSet.map cs $ maybe mempty scrapeExprMeta flt
        return (VectorType VectorLayer{..})

newtype MapboxStyle = MapboxStyle {
    _msLayers :: [Layer]
} deriving (Show)
makeLenses ''MapboxStyle

instance FromJSON MapboxStyle where
  parseJSON = AE.withObject "Style" $ \o ->
      MapboxStyle <$> o .: "layers"

instance Semigroup MapboxStyle where
  MapboxStyle l1 <> MapboxStyle l2 = MapboxStyle (l1 <> l2)
