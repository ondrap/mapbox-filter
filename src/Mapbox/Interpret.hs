{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE TemplateHaskell    #-}

module Mapbox.Interpret (
    compileExpr
  , runFilter
  , FeatureType(..)
  , FeatureInfo(..)
  , CompiledExpr
) where

import           Control.Applicative        ((<|>))
import           Control.Lens               (makeLenses, view)
import           Control.Monad.Trans.Class  (lift)
import           Control.Monad.Trans.Reader
import qualified Data.ByteString.Lazy       as BL
import qualified Data.HashMap.Strict        as HMap
import           Data.Maybe                 (fromMaybe)
import           Data.Scientific            (fromFloatDigits)
import           Data.String.Conversions    (cs)
import           Data.Type.Equality         ((:~:) (..), TestEquality (..))
import           Geography.VectorTile       (Feature (..), Val (..))

import           Mapbox.Expression          (ATExp (..), AnyValue (..),
                                             AttrType (..), BoolFunc (..),
                                             CmpOp (..), OrdOp (..), TExp (..),
                                             TTyp (..), anyValToTVal,
                                             tValToTTyp)

data FeatureType = Point | LineString | Polygon
  deriving (Show)

data FeatureInfo = FeatureInfo {
    _fiId   :: Word
  , _fiType :: FeatureType
  , _fiMeta :: HMap.HashMap BL.ByteString Val
}
makeLenses ''FeatureInfo

type CompiledExpr a = ReaderT FeatureInfo Maybe a

-- | Fail an expression interpretation
failExpr :: CompiledExpr a
failExpr = lift Nothing

-- | Convert a typed expression into a runnable expression
compileExpr :: TExp a -> CompiledExpr a
compileExpr (TNum n)     = return n
compileExpr (TStr s)     = return s
compileExpr (TBool b)    = return b
compileExpr (TNumArr na) = return na
compileExpr (TNegate e)  = not <$> compileExpr e
compileExpr (TReadAttr FeatureId) = ANum . fromIntegral <$> view fiId
compileExpr (TReadAttr GeometryType) = do
  ftype <- view fiType
  return $ case ftype of
    Point      -> "Point"
    LineString -> "LineString"
    Polygon    -> "Polygon"
compileExpr (TCheckMeta tattr) = do
  tname <- compileExpr tattr
  tmeta <- view fiMeta
  return (HMap.member (cs tname) tmeta)
compileExpr (TReadMeta tattr) = do
  tname <- compileExpr tattr
  tmeta <- view fiMeta
  case HMap.lookup (cs tname) tmeta of
    Nothing        -> failExpr
    Just (St aval) -> return (AStr (cs aval))
    Just (Fl n)    -> return (ANum (fromFloatDigits n))
    Just (Do n)    -> return (ANum (fromFloatDigits n))
    Just (I64 n)   -> return (ANum (fromIntegral n))
    Just (W64 n)   -> return (ANum (fromIntegral n))
    Just (S64 n)   -> return (ANum (fromIntegral n))
    Just (B b)     -> return (ABool b)
compileExpr (TConvert False _ []) = failExpr
compileExpr (TConvert False restyp ((vexpr ::: vtyp):rest)) =
  ( case testEquality (tValToTTyp restyp) vtyp of
      Just Refl -> compileExpr vexpr
      Nothing | TTAny <- vtyp -> compileExpr vexpr >>= maybe failExpr return . anyValToTVal restyp
              | otherwise -> failExpr
    ) <|> tryNextArg
  where
    tryNextArg = compileExpr (TConvert False restyp rest)
compileExpr (TConvert True _ _) = error "Not Implemented"
compileExpr (TBoolFunc bf exprs) =
    bop bf <$> traverse compileExpr exprs
  where
    bop BAny = or
    bop BAll = and
compileExpr (TCmpOp op e1 e2) =
    -- The position of 'nulls' is strange, it is actually not possible to get
    -- a null when working with vector tiles; when we get a 'null', the behaviour
    -- is treated as a failure everywhere, except conversion functions (tested)
    top <$> compileExpr e1 <*> compileExpr e2
  where
    top = case op of
      CEq  -> (==)
      CNeq -> (/=)
compileExpr (TOrdOp op e1 e2) =
    top <$> compileExpr e1 <*> compileExpr e2
  where
    top :: Ord a => a -> a -> Bool
    top = case op of
      CGt  -> (>)
      CGeq -> (>=)
      CLt  -> (<)
      CLeq -> (<=)

-- | Run compiled expression on a particular feature
runFilter :: CompiledExpr Bool -> FeatureType -> Feature gs -> Bool
runFilter cexpr ftype f =
  let finfo = FeatureInfo (_featureId f) ftype (_metadata f)
  in fromMaybe False (runReaderT cexpr finfo)
