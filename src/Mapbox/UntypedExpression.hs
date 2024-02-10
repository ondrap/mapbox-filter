{-# LANGUAGE DeriveFunctor        #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE InstanceSigs #-}

-- | Untyped expression parser for the mapbox style expressions
module Mapbox.UntypedExpression where

import           Control.Applicative   ((<|>))
import           Data.Aeson            ((.:?))
import qualified Data.Aeson            as AE
import           Data.Functor.Classes
import Data.Fix (Fix(..))
import           Data.Scientific       (Scientific)
import qualified Data.Text             as T
import qualified Data.Vector           as V
import Data.Aeson.Types (Parser)

type Id = T.Text

type NumArray = V.Vector Scientific

data ULabel =
    LStr T.Text
  | LStrArr [T.Text]
  | LBool Bool
  | LNumArr NumArray
  | LNum Scientific
  deriving (Show)

instance AE.FromJSON ULabel where
  parseJSON v = LStr <$> AE.parseJSON v
              <|> LStrArr <$> AE.parseJSON v
              <|> LNum <$> AE.parseJSON v
              <|> LNumArr <$> AE.parseJSON v
              <|> LBool <$> AE.parseJSON v

data UExpF r =
  UNum Scientific
  | UStr T.Text
  | UStrArr [T.Text]
  | UBool Bool
  | UNumArr NumArray
  | UVar Id
  | UApp Id [r]
  | UMatch r [(ULabel,r)] r
  | ULet Id r r
  | UFunction { ufProperty :: Maybe T.Text } -- Function - we should have 'base', 'stops' etc., currently unimplemented
  deriving (Show, Functor)
type UExp = Fix UExpF

instance Show1 UExpF where
  liftShowsPrec _ _ d (UNum n) = showParen (d > 10) $ showString "UNum " . showsPrec 11 n
  liftShowsPrec _ _ d (UStr n) = showParen (d > 10) $ showString "UStr " . showsPrec 11 n
  liftShowsPrec _ _ d (UStrArr n) = showParen (d > 10) $ showString "UStrArr " . showsPrec 11 n
  liftShowsPrec _ _ d (UBool n) = showParen (d > 10) $ showString "UBool " . showsPrec 11 n
  liftShowsPrec _ _ d (UVar n) = showParen (d > 10) $ showString "UVar " . showsPrec 11 n
  liftShowsPrec _ _ d (UNumArr n) = showParen (d > 10) $ showString "UNumArr " . showsPrec 11 n
  liftShowsPrec sp _ d (ULet tid x y) = showParen (d > 10) $
      showString "ULet " . showsPrec 11 tid . showString " "
      . sp 11 x . showChar ' ' . sp 11 y
  liftShowsPrec sp _ d (UApp tid lst) =  showParen (d > 10) $
      showString "UApp " . showsPrec 11 tid . showString " "
      . mconcat (map (\l -> showChar ' ' . sp 11 l) lst)
  liftShowsPrec _ _ d (UFunction pid) = showParen (d > 10) $ showString "UFunction " . showsPrec 11 pid
  liftShowsPrec sp _ d (UMatch inp lst lelse) =  showParen (d > 10) $
      showString "UApp " . showString "match" . showString " "
      . sp 11 inp . mconcat (map (\(l,v) -> showChar '(' . showString (show l) . showChar ',' . sp 11 v . showChar ')') lst)
      . sp 11 lelse


instance AE.FromJSON1 UExpF where
  liftParseJSON :: forall a. (AE.Value -> Parser a) -> (AE.Value -> Parser [a]) -> AE.Value -> Parser (UExpF a)
  liftParseJSON parse _ = uparse
    where
      uparse (AE.String str) = return (UStr str)
      uparse (AE.Number num) = return (UNum num)
      uparse (AE.Bool b) = return (UBool b)
      uparse AE.Null = fail "Null not supported as expression"
      uparse (AE.Object o) = do
          prop <- o .:? "property"
          return (UFunction prop)
      uparse (AE.Array arr) = numarr <|> expr
        where
          numarr = UNumArr <$> traverse AE.parseJSON arr

          expr | (idn:iargs) <- V.toList arr = do
                        fid <- AE.parseJSON idn
                        case (fid :: T.Text) of
                          "let" -> letexpr iargs
                          "var" -> varexpr iargs
                          "match" -> matchexpr iargs
                          _     -> UApp fid <$> traverse parse iargs
              | otherwise = fail "Empty array not supported"

          letexpr [AE.String vname, val, rest] = do
              uval <- parse val
              next <- parse rest
              return (ULet vname uval next)
          letexpr _ = fail "Invalid let expression"
          varexpr [AE.String nm] = return (UVar nm)
          varexpr _              = fail "Invalid var expression"

          matchexpr (idn:rest) = do
            inp <- parse idn
            (tbl, lastArg) <- parseMatchTable [] rest
            lelse <- parse lastArg
            return $ UMatch inp tbl lelse
          matchexpr args = fail ("Invalid match arguments: " <> show args)

          parseMatchTable :: [(ULabel, a)] -> [AE.Value] -> Parser ([(ULabel, a)], AE.Value)
          parseMatchTable tbl [dlast] = return (tbl, dlast)
          parseMatchTable tbl (lbl:v:rest) = do
              dlabel <- AE.parseJSON lbl
              dval <- parse v
              parseMatchTable ((dlabel, dval):tbl) rest
          parseMatchTable _ [] = fail "Wrong number of arguments to match"
