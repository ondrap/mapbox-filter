{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}

module Mapbox.Expression (
    TExp(..)
  , TTyp(..)
  , UExp
  , typeCheckFilter
  , AnyValue(..)
  , AttrType(..)
  , BoolFunc(..)
  , CmpOp(..)
  , OrdOp(..)
  , ATExp(..)
  , tValToTTyp
  , anyValToTVal
) where

import           Control.Monad            ((>=>))
import           Data.Bool                (bool)
import           Data.Functor.Foldable    (Fix (..))
import qualified Data.HashMap.Strict      as HMap
import           Data.Monoid              ((<>))
import           Data.Scientific          (Scientific)
import           Data.String.Conversions  (cs)
import qualified Data.Text                as T
import           Data.Type.Equality       ((:~:) (..), TestEquality (..))

import           Mapbox.UntypedExpression

data CmpOp = CEq | CNeq
  deriving (Show)

data OrdOp = CGt | CGeq | CLt | CLeq
  deriving (Show)

data BoolFunc = BAll | BAny
  deriving (Show)

data AttrType a where
    GeometryType :: AttrType T.Text
    FeatureId :: AttrType AnyValue
instance Show (AttrType a) where
  showsPrec _ GeometryType = showString "geometry-type"
  showsPrec _ FeatureId    = showString "id"

data AnyValue =
    ABool Bool
  | ANum Scientific
  | AStr T.Text
  | ANumArray NumArray
  deriving (Show, Eq)

data TTyp a where
  TTBool :: TTyp Bool
  TTNum :: TTyp Scientific
  TTStr :: TTyp T.Text
  TTNumArr :: TTyp NumArray
  TTAny :: TTyp AnyValue

instance TestEquality TTyp where
  testEquality TTBool TTBool     = Just Refl
  testEquality TTNum TTNum       = Just Refl
  testEquality TTStr TTStr       = Just Refl
  testEquality TTNumArr TTNumArr = Just Refl
  testEquality TTAny TTAny       = Just Refl
  testEquality _ _               = Nothing

instance Show (TTyp a) where
  showsPrec _ TTBool   = showString "Bool"
  showsPrec _ TTNum    = showString "Number"
  showsPrec _ TTStr    = showString "String"
  showsPrec _ TTNumArr = showString "NumArray"
  showsPrec _ TTAny    = showString "Any"

data TValue a where
  TVBool :: TValue Bool
  TVNum :: TValue Scientific
  TVStr :: TValue T.Text
  TVNumArr :: TValue NumArray

tValToTTyp :: TValue a -> TTyp a
tValToTTyp TVBool   = TTBool
tValToTTyp TVNum    = TTNum
tValToTTyp TVStr    = TTStr
tValToTTyp TVNumArr = TTNumArr

anyValToTVal :: TValue a -> AnyValue -> Maybe a
anyValToTVal TVBool (ABool b)        = Just b
anyValToTVal TVNum (ANum n)          = Just n
anyValToTVal TVStr (AStr s)          = Just s
anyValToTVal TVNumArr (ANumArray na) = Just na
anyValToTVal _ _                     = Nothing

instance Show (TValue a) where
  showsPrec _ TVBool   = showString "Bool"
  showsPrec _ TVNum    = showString "Number"
  showsPrec _ TVStr    = showString "String"
  showsPrec _ TVNumArr = showString "NumArray"

class TOrderable a
instance TOrderable Scientific
instance TOrderable T.Text

data ATExp = forall a . TExp a ::: TTyp a
instance Show ATExp where
  showsPrec p (texp ::: ttyp) = showsPrec p texp . showString " :: " . showsPrec p ttyp

data TExp a where
  TNum :: Scientific -> TExp Scientific
  TStr :: T.Text -> TExp T.Text
  TBool :: Bool -> TExp Bool
  TNumArr :: NumArray -> TExp NumArray
  TCmpOp :: Eq a => CmpOp -> TExp a -> TExp a -> TExp Bool
  TOrdOp :: (Ord a, TOrderable a) => OrdOp -> TExp a -> TExp a -> TExp Bool
  TBoolFunc :: BoolFunc -> [TExp Bool] -> TExp Bool
  TReadMeta :: TExp T.Text -> TExp AnyValue
  TCheckMeta :: TExp T.Text -> TExp Bool
  TNegate :: TExp Bool -> TExp Bool
  TConvert :: Bool -> TValue a -> [ATExp] -> TExp a
  TReadAttr :: AttrType a -> TExp a
  TMatch :: Eq a => TExp a -> [(TExp a, TExp b)] -> TExp b -> TExp b

instance Show (TExp a) where
  showsPrec p (TNum d) = showsPrec p d
  showsPrec p (TBool b) = showsPrec p b
  showsPrec p (TStr s) = showsPrec p s
  showsPrec p (TNumArr n) = showsPrec p n
  showsPrec p (TCmpOp _ e1 e2) =
    showString "(" . showsPrec p e1 . showString " == " . showsPrec p e2 . showString ")"
  showsPrec p (TOrdOp op n1 n2) =
    showString "(" . showsPrec p op . showString " " . showsPrec p n1 . showString " " . showsPrec p n2 . showString ")"
  showsPrec p (TBoolFunc func fncs) = showsPrec p func . showString " " . showsPrec p fncs
  showsPrec p (TReadMeta var) = showString "readMeta " . showsPrec p var
  showsPrec p (TNegate e) = showString "!(" . showsPrec p e . showString ")"
  showsPrec p (TCheckMeta var) = showString "hasMeta " . showsPrec p var
  showsPrec p (TConvert force restype exprs) =
    showsPrec p exprs . showString " ->" . bool (showString " ") (showString "! ") force . showsPrec p restype
  showsPrec p (TReadAttr atype) = showString "attr " . showsPrec p atype
  showsPrec p (TMatch inp cond def) = showString "match " . showsPrec p inp  . showString " " . showsPrec p cond . showString " "  . showsPrec p def

type Env = HMap.HashMap T.Text ATExp

-- | Check that the input expression conforms to the requested type
forceType :: TTyp a -> ATExp -> Either T.Text (TExp a)
forceType t1 (mexp ::: t2) =
  case testEquality t1 t2 of
    Just Refl -> return mexp
    Nothing -> Left ("Expression " <> T.pack (show mexp) <> " has type " <> T.pack (show t2)
                      <> ", expected " <> T.pack (show t1))

-- | Convert untyped expression to a typed expression
typeCheck :: Env -> UExp -> Either T.Text ATExp
typeCheck _ (Fix (UNum num)) = Right (TNum num ::: TTNum)
typeCheck _ (Fix (UStr str)) = Right (TStr str ::: TTStr)
typeCheck _ (Fix (UBool b)) = Right (TBool b ::: TTBool)
typeCheck _ (Fix (UNumArr n)) = Right (TNumArr n ::: TTNumArr)
typeCheck env (Fix (UVar var)) =
    maybe (Left ("Variable " <> var <> " not found.")) Right (HMap.lookup var env)
typeCheck env (Fix (ULet var expr next)) = do
    res <- typeCheck env expr
    typeCheck (HMap.insert var res env) next
typeCheck env (Fix (UApp fname args)) =
  case fname of
    "string" -> do
        eargs <- traverse (typeCheck env) args
        return (TConvert False TVStr eargs ::: TTStr)
    "number" -> do
        eargs <- traverse (typeCheck env) args
        return (TConvert False TVNum eargs ::: TTNum)
    "boolean" -> do
        eargs <- traverse (typeCheck env) args
        return (TConvert False TVBool eargs ::: TTBool)
    "get" | [arg] <- args -> do
        mname <- typeCheck env arg >>= forceType TTStr
        return (TReadMeta mname ::: TTAny)
    "!" | [arg] <- args -> do
        mexpr <- typeCheck env arg >>= forceType TTBool
        return (TNegate mexpr ::: TTBool)
    "match" | (inpexp:rest) <- args, odd (length rest) -> do
        (inp ::: intype) <- typeCheck env inpexp
        (def ::: outtype) <- typeCheck env (last rest)
        let mkpairs (a:b:prest) = (a,b) : mkpairs prest
            mkpairs _ = []
        let evalpair (a,b) = (,) <$> (typeCheck env a >>= forceType intype)
                                 <*> (typeCheck env b >>= forceType outtype)
        pairs <- traverse evalpair (mkpairs (init rest))
        -- Add Eq constraint
        case intype of
          TTStr    -> return (TMatch inp pairs def ::: outtype)
          TTNum    -> return (TMatch inp pairs def ::: outtype)
          TTBool   -> return (TMatch inp pairs def ::: outtype)
          TTNumArr -> return (TMatch inp pairs def ::: outtype)
          TTAny    -> return (TMatch inp pairs def ::: outtype)
    "has" | [arg] <- args -> do
        mname <- typeCheck env arg >>= forceType TTStr
        return (TCheckMeta mname ::: TTBool)
    _| Just op <- lookup fname [("==", CEq), ("!=", CNeq)], [arg1, arg2] <- args -> do
            (marg1 ::: t1) <- typeCheck env arg1
            (marg2 ::: t2) <- typeCheck env arg2
            case testEquality t1 t2 of
              Just Refl ->
                case t1 of
                  TTStr    -> return (TCmpOp op marg1 marg2 ::: TTBool)
                  TTNum    -> return (TCmpOp op marg1 marg2 ::: TTBool)
                  TTBool   -> return (TCmpOp op marg1 marg2 ::: TTBool)
                  TTNumArr -> return (TCmpOp op marg1 marg2 ::: TTBool)
                  TTAny    -> return (TCmpOp op marg1 marg2 ::: TTBool)
              Nothing -> Left (cs $ "Comparing unequal things: " <> show arg1 <> ", " <> show arg2
                            <> ": " <> show t1 <> "vs. " <> show t2)
    _| Just op <- lookup fname [("<", CLt), ("<=", CLeq), (">", CGt), (">=", CGeq)],
        [arg1, arg2] <- args -> do
            (marg1 ::: t1) <- typeCheck env arg1
            (marg2 ::: t2) <- typeCheck env arg2
            case testEquality t1 t2 of
              Just Refl ->
                case t1 of
                  TTStr -> return (TOrdOp op marg1 marg2 ::: TTBool)
                  TTNum -> return (TOrdOp op marg1 marg2 ::: TTBool)
                  _     -> Left "Cannot compare other than str/num"
              Nothing -> Left (cs $ "Comparing unequal things: " <> show arg1 <> ", " <> show arg2
                              <> ": " <> show t1 <> "vs. " <> show t2)
    _| Just op <- lookup fname [("any", BAny), ("all", BAll)] -> do
        margs <- traverse (typeCheck env >=> forceType TTBool) args
        return (TBoolFunc op margs ::: TTBool)
    "geometry-type" | [] <- args -> return (TReadAttr GeometryType ::: TTStr)
    _     -> Left ("Unknown function name / wrong param count: " <> fname)

-- | Convert an untyped expression to a filter (Bool) expression
typeCheckFilter :: UExp -> Either String (TExp Bool)
typeCheckFilter uexp =
  case typeCheck mempty uexp of
    Right (mexp ::: TTBool) -> return mexp
    Right (_ ::: otype) -> Left ("Expression has a type " <> show otype <> ", expected Bool")
    Left err -> Left (T.unpack err)
