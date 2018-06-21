{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}

module Mapbox.Expression (
    TExp
  , UExp
  , typeCheckFilter
) where

import           Control.Applicative ((<|>))
import           Control.Monad       ((>=>))
import           Data.Aeson          (FromJSON (..))
import qualified Data.Aeson          as AE
import           Data.Bool           (bool)
import qualified Data.HashMap.Strict as HMap
import           Data.Monoid         ((<>))
import           Data.Scientific     (Scientific)
import qualified Data.Text           as T
import           Data.Type.Equality  ((:~:) (..), TestEquality (..))
import qualified Data.Vector         as V

type Id = T.Text
type NumArray = V.Vector Scientific

data UExp =
  UNum Scientific
  | UStr T.Text
  | UBool Bool
  | UNumArr NumArray
  | UVar Id
  | UApp Id [UExp]
  | ULet Id UExp UExp
  deriving (Show)

instance FromJSON UExp where
  parseJSON (AE.String str) = return (UStr str)
  parseJSON (AE.Number num) = return (UNum num)
  parseJSON (AE.Bool b) = return (UBool b)
  parseJSON (AE.Object _) = fail "Objects not supported as expression"
  parseJSON AE.Null = fail "Null not supported as expression"
  parseJSON (AE.Array arr) = numarr <|> expr
    where
      numarr = UNumArr <$> traverse AE.parseJSON arr
      expr | (idn:iargs) <- V.toList arr = do
                    fid <- AE.parseJSON idn
                    case fid of
                      "let" -> letexpr iargs
                      "var" -> varexpr iargs
                      _     -> UApp fid <$> traverse AE.parseJSON iargs
           | otherwise = fail "Empty array not supported"
      letexpr (AE.String vname : val : rest) = do
          uval <- parseJSON val
          next <- letexpr rest
          return (ULet vname uval next)
      letexpr [e] = parseJSON e
      letexpr _ = fail "Invalid let expression"
      varexpr [AE.String nm] = return (UVar nm)
      varexpr _              = fail "Invalid var expression"

data UTyp = UTStr | UTNum | UTBool | UTNumArr
  deriving (Show)

data CmpOp = CEq | CNeq
  deriving (Show)

data OrdOp = CGt | CGeq | CLt | CLeq
  deriving (Show)

data BoolFunc = BAll | BAny
  deriving (Show)

data AttrType = GeometryType | FeatureId
  deriving (Show)

data AnyValue =
    ABool Bool
  | ANum Scientific
  | AStr T.Text
  | ANumArray NumArray
  deriving (Show)

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

instance Show (TValue a) where
  showsPrec _ TVBool   = showString "Bool"
  showsPrec _ TVNum    = showString "Number"
  showsPrec _ TVStr    = showString "String"
  showsPrec _ TVNumArr = showString "NumArray"

data TOrderable a where
  TONum :: TOrderable Scientific
  TOStr :: TOrderable T.Text

instance Show (TOrderable a) where
  showsPrec _ TONum = showString "Number"
  showsPrec _ TOStr = showString "String"

data ATExp = forall a . TExp a ::: TTyp a
instance Show ATExp where
  showsPrec p (texp ::: ttyp) = showsPrec p texp . showString " :: " . showsPrec p ttyp

data TExp a where
  TNum :: Scientific -> TExp Scientific
  TStr :: T.Text -> TExp T.Text
  TBool :: Bool -> TExp Bool
  TNumArr :: NumArray -> TExp NumArray
  TCmpOp :: CmpOp -> TValue a -> TExp a -> TExp a -> TExp Bool
  TOrdOp :: OrdOp -> TOrderable a -> TExp a -> TExp a -> TExp Bool
  TBoolFunc :: BoolFunc -> [TExp Bool] -> TExp Bool
  TReadMeta :: TExp T.Text -> TExp AnyValue
  TCheckMeta :: TExp T.Text -> TExp Bool
  TNegate :: TExp Bool -> TExp Bool
  TConvert :: Bool -> TValue a -> [ATExp] -> TExp a
  TReadAttr :: AttrType -> TExp T.Text

instance Show (TExp a) where
  showsPrec p (TNum d) = showsPrec p d
  showsPrec p (TBool b) = showsPrec p b
  showsPrec p (TStr s) = showsPrec p s
  showsPrec p (TNumArr n) = showsPrec p n
  showsPrec p (TCmpOp _ _ e1 e2) =
    showString "(" . showsPrec p e1 . showString " == " . showsPrec p e2 . showString ")"
  showsPrec p (TOrdOp op _ n1 n2) =
    showString "(" . showsPrec p op . showString " " . showsPrec p n1 . showString " " . showsPrec p n2 . showString ")"
  showsPrec p (TBoolFunc func fncs) = showsPrec p func . showString " " . showsPrec p fncs
  showsPrec p (TReadMeta var) = showString "readMeta " . showsPrec p var
  showsPrec p (TNegate e) = showString "!(" . showsPrec p e . showString ")"
  showsPrec p (TCheckMeta var) = showString "hasMeta " . showsPrec p var
  showsPrec p (TConvert force restype exprs) =
    showsPrec p exprs . showString " ->" . bool (showString " ") (showString "! ") force . showsPrec p restype
  showsPrec p (TReadAttr atype) = showString "attr " . showsPrec p atype

type Env = HMap.HashMap T.Text ATExp

forceType :: TTyp a -> ATExp -> Either T.Text (TExp a)
forceType t1 (mexp ::: t2) =
  case testEquality t1 t2 of
    Just Refl -> return mexp
    Nothing -> Left ("Expression " <> T.pack (show mexp) <> " has type " <> T.pack (show t2)
                      <> ", expected " <> T.pack (show t1))

typeCheck :: Env -> UExp -> Either T.Text ATExp
typeCheck _ (UNum num) = Right (TNum num ::: TTNum)
typeCheck _ (UStr str) = Right (TStr str ::: TTStr)
typeCheck _ (UBool b) = Right (TBool b ::: TTBool)
typeCheck _ (UNumArr n) = Right (TNumArr n ::: TTNumArr)
typeCheck env (UVar var) =
    maybe (Left ("Variable " <> var <> " not found.")) Right (HMap.lookup var env)
typeCheck env (ULet var expr next) =
    case typeCheck env expr of
      Left err  -> Left err
      Right res -> typeCheck (HMap.insert var res env) next
typeCheck env (UApp fname args) =
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
    "has" | [arg] <- args -> do
        mname <- typeCheck env arg >>= forceType TTStr
        return (TCheckMeta mname ::: TTBool)
    _| fname `elem` ["==", "!="], [arg1, arg2] <- args -> do
            (marg1 ::: t1) <- typeCheck env arg1
            (marg2 ::: t2) <- typeCheck env arg2
            let op = if fname == "==" then CEq else CNeq
            case testEquality t1 t2 of
              Just Refl ->
                case t1 of
                  TTStr -> return (TCmpOp op TVStr marg1 marg2 ::: TTBool)
                  TTNum -> return (TCmpOp op TVNum marg1 marg2 ::: TTBool)
                  TTBool -> return (TCmpOp op TVBool marg1 marg2 ::: TTBool)
                  TTNumArr -> return (TCmpOp op TVNumArr marg1 marg2 ::: TTBool)
                  TTAny -> fail "Cannot compare unknown types"
              Nothing -> fail "Comparing unequal things."
    _| Just op <- lookup fname [("<", CLt), ("<=", CLeq), (">", CGt), (">=", CGeq)],
        [arg1, arg2] <- args -> do
            (marg1 ::: t1) <- typeCheck env arg1
            (marg2 ::: t2) <- typeCheck env arg2
            case testEquality t1 t2 of
              Just Refl ->
                case t1 of
                  TTStr -> return (TOrdOp op TOStr marg1 marg2 ::: TTBool)
                  TTNum -> return (TOrdOp op TONum marg1 marg2 ::: TTBool)
                  _     -> fail "Cannot compare other than str/num"
              Nothing -> fail "Comparing unequal things."
    _| fname `elem` ["any", "all"] -> do
        margs <- traverse (typeCheck env >=> forceType TTBool) args
        return (TBoolFunc (if fname == "any" then BAny else BAll) margs ::: TTBool)
    "geometry-type" | [] <- args -> return (TReadAttr GeometryType ::: TTStr)
    _     -> Left ("Unknown function name / wrong param count: " <> fname)


typeCheckFilter :: UExp -> Either String (TExp Bool)
typeCheckFilter uexp =
  case typeCheck mempty uexp of
    Right (mexp ::: TTBool) -> return mexp
    Right (_ ::: otype) -> Left ("Expression has a type " <> show otype <> ", expected Bool")
    Left err -> Left (T.unpack err)
