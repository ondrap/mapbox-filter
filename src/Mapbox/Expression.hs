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
  , tvalToAny
  , TValue(..)
) where

import           Control.Monad            ((>=>))
import           Data.Bool                (bool)
import           Data.Functor.Foldable    (Fix (..))
import qualified Data.HashMap.Strict      as HMap
import           Data.Maybe               (isJust)
import           Data.Monoid              ((<>))
import           Data.Scientific          (Scientific)
import           Data.String.Conversions  (cs)
import qualified Data.Text                as T
import           Data.Type.Equality       ((:~:) (..), TestEquality (..))
import           Data.Foldable            (toList)

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

typeEqual :: TTyp a -> TTyp b -> Bool
typeEqual a b = isJust (testEquality a b)

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

tvalToAny :: TTyp a -> a -> AnyValue
tvalToAny TTBool b = ABool b
tvalToAny TTNum n = ANum n
tvalToAny TTStr t = AStr t
tvalToAny TTNumArr a = ANumArray a
tvalToAny TTAny a = a

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
  TMatch :: (Show a, Eq a) => TExp a -> [([a], TExp b)] -> TExp b -> TExp b
  TToAny :: ATExp -> TExp AnyValue

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
  showsPrec p (TToAny atval) = showString "*to_any* " . showsPrec p atval

type Env = HMap.HashMap T.Text ATExp

-- | Existential witness for equality constraint (add Show constraint for easier Show deriving)
data HasEq a where
  HasEq :: (Show a, Eq a) => HasEq a

-- | Return witness if the type has Eq instance
hasEquality :: TTyp a -> HasEq a
hasEquality TTBool = HasEq
hasEquality TTNum = HasEq
hasEquality TTStr = HasEq
hasEquality TTNumArr = HasEq
hasEquality TTAny = HasEq

-- | Hacky conversion of literal/array of literals to list of literals; check type
convertMatchLabel :: TTyp a -> UExp -> Either T.Text [a]
convertMatchLabel TTNum (Fix (UNum n)) = Right [n]
convertMatchLabel TTNum (Fix (UNumArr arr)) = Right (toList arr)
convertMatchLabel TTStr (Fix (UStr s)) = Right [s]
convertMatchLabel TTStr (Fix (UStrArr args)) = Right args
convertMatchLabel TTBool (Fix (UBool b)) = Right [b]
convertMatchLabel TTAny (Fix (UNum n)) = Right [ANum n]
convertMatchLabel TTAny (Fix (UNumArr arr)) = Right (ANum <$> toList arr)
convertMatchLabel TTAny (Fix (UStr s)) = Right [AStr s]
convertMatchLabel TTAny (Fix (UStrArr args)) = Right (AStr <$> args)
convertMatchLabel TTAny (Fix (UBool b)) = Right [ABool b]
convertMatchLabel _ arg = Left (cs $ "Impossible match label: " <> show arg)

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
typeCheck _ (Fix (UStrArr n)) = Left ("StrArr found in unexpected place, internal error: " <> cs (show n))
typeCheck _ (Fix UFunction{}) = Left "Functions (stops) are not implemented."
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
    "to-number" -> do
      eargs <- traverse (typeCheck env) args
      return (TConvert True TVNum eargs ::: TTNum)
    "to-string" -> do
      eargs <- traverse (typeCheck env) args
      return (TConvert True TVStr eargs ::: TTStr)
    "to-boolean" -> do
      eargs <- traverse (typeCheck env) args
      return (TConvert True TVBool eargs ::: TTBool)
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
        let evalpair (a,b) = (,) <$> convertMatchLabel intype a
                                 <*> (typeCheck env b >>= forceType outtype)
        pairs <- traverse evalpair (mkpairs (init rest))
        -- Add Eq constraint
        case hasEquality intype of
          HasEq -> return (TMatch inp pairs def ::: outtype)
    "has" | [arg] <- args -> do
        mname <- typeCheck env arg >>= forceType TTStr
        return (TCheckMeta mname ::: TTBool)
    _| Just op <- lookup fname [("==", CEq), ("!=", CNeq)], [arg1, arg2] <- args -> do
            (marg1 ::: t1) <- typeCheck env arg1
            (marg2 ::: t2) <- typeCheck env arg2
            -- We could theoretically downgrade to any completely, but let's do at least some
            -- type check and rule out some cases during compilation
            case (testEquality t1 t2, hasEquality t1)  of
              (Just Refl, HasEq) -> return (TCmpOp op marg1 marg2 ::: TTBool)
              (Nothing, _)
                | typeEqual t1 TTAny || typeEqual t2 TTAny ->
                    return (TCmpOp op (TToAny (marg1 ::: t1)) (TToAny (marg2 ::: t2)) ::: TTBool)
                | otherwise ->
                    Left (cs $ "Comparing unequal things: " <> show arg1 <> ", " <> show arg2
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
typeCheckFilter :: UExp -> Either T.Text (TExp Bool)
typeCheckFilter = typeCheck mempty >=> forceType TTBool
