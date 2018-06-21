{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RankNTypes         #-}
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
    Nothing        -> lift Nothing
    Just (St aval) -> return (AStr (cs aval))
    Just (Fl n)    -> return (ANum (fromFloatDigits n))
    Just (Do n)    -> return (ANum (fromFloatDigits n))
    Just (I64 n)   -> return (ANum (fromIntegral n))
    Just (W64 n)   -> return (ANum (fromIntegral n))
    Just (S64 n)   -> return (ANum (fromIntegral n))
    Just (B b)     -> return (ABool b)
compileExpr (TConvert False _ []) = lift Nothing
compileExpr (TConvert False restyp ((vexpr ::: vtyp):rest)) =
  case testEquality (tValToTTyp restyp) vtyp of
    Just Refl -> compileExpr vexpr <|> tryNextArg
    Nothing | TTAny <- vtyp -> do
                  env <- ask
                  case runReaderT (compileExpr vexpr) env of
                    Nothing -> tryNextArg
                    Just dres ->
                      case anyValToTVal dres restyp of
                        Just v  -> return v
                        Nothing -> tryNextArg
            | otherwise -> tryNextArg
  where
    tryNextArg = compileExpr (TConvert False restyp rest)
compileExpr (TConvert True _ _) = error "Not Implemented"
compileExpr (TBoolFunc bf exprs) = do
  barr <- traverse compileExpr exprs
  case bf of
    BAny -> return (or barr)
    BAll -> return (and barr)
compileExpr (TCmpOp op e1 e2) = do
    -- We have to handle 'nulls' correctly in comparisons
    env <- ask
    let v1 = runReaderT (compileExpr e1) env
    let v2 = runReaderT (compileExpr e2) env
    return (top v1 v2)
  where
    top :: Eq a => Maybe a -> Maybe a -> Bool
    top = case op of
      CEq  -> (==)
      CNeq -> (/=)
compileExpr (TOrdOp op e1 e2) = do
    v1 <- compileExpr e1
    v2 <- compileExpr e2
    return (top v1 v2)
  where
    top :: Ord a => a -> a -> Bool
    top = case op of
      CGt  -> (>)
      CGeq -> (>=)
      CLt  -> (<)
      CLeq -> (<=)

runFilter :: CompiledExpr Bool -> FeatureType -> forall gs. Feature gs -> Bool
runFilter cexpr ftype f =
  let finfo = FeatureInfo (_featureId f) ftype (_metadata f)
  in fromMaybe False (runReaderT cexpr finfo)
