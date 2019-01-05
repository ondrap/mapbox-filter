{-# LANGUAGE DeriveFunctor        #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TypeSynonymInstances #-}

-- | Untyped expression parser for the mapbox style expressions
module Mapbox.UntypedExpression where

import           Control.Applicative   ((<|>))
import           Data.Aeson            (FromJSON (..), (.:?))
import qualified Data.Aeson            as AE
import           Data.Functor.Classes
import           Data.Functor.Foldable
import           Data.Scientific       (Scientific)
import qualified Data.Text             as T
import qualified Data.Vector           as V

type Id = T.Text

type NumArray = V.Vector Scientific

data UExpF r =
  UNum Scientific
  | UStr T.Text
  | UStrArr [T.Text]
  | UBool Bool
  | UNumArr NumArray
  | UVar Id
  | UApp Id [r]
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

instance FromJSON UExp where
  parseJSON (AE.String str) = return (Fix (UStr str))
  parseJSON (AE.Number num) = return (Fix (UNum num))
  parseJSON (AE.Bool b) = return (Fix (UBool b))
  parseJSON AE.Null = fail "Null not supported as expression"
  parseJSON (AE.Object o) = do
      prop <- o .:? "property"
      return (Fix (UFunction prop))
  parseJSON (AE.Array arr) = numarr <|> expr
    where
      numarr = Fix . UNumArr <$> traverse AE.parseJSON arr
      expr | (idn:iargs) <- V.toList arr = do
                    fid <- AE.parseJSON idn
                    case fid of
                      "let" -> letexpr iargs
                      "var" -> varexpr iargs
                      "match" -> matchexpr iargs
                      _     -> Fix . UApp fid <$> traverse AE.parseJSON iargs
           | otherwise = fail "Empty array not supported"
      letexpr (AE.String vname : val : rest) = do
          uval <- parseJSON val
          next <- letexpr rest
          return (Fix (ULet vname uval next))
      letexpr [e] = parseJSON e
      letexpr _ = fail "Invalid let expression"
      varexpr [AE.String nm] = return (Fix (UVar nm))
      varexpr _              = fail "Invalid var expression"
      -- We have to parse 'match' separately, as it has different types on 'label' - only literal/array of literal allowed
      -- every other is special
      matchexpr args = Fix . UApp "match" <$> traverse parseMatchArg (zip (cycle [False, True]) args)
      parseMatchArg (True, arg) = 
          Fix <$> ((UStrArr <$> AE.parseJSON arg)
                  <|> (UNumArr <$> AE.parseJSON arg)
                  <|> (UNum <$> AE.parseJSON arg)
                  <|> (UStr <$> AE.parseJSON arg)
                  <|> (UBool <$> AE.parseJSON arg)
          )
      parseMatchArg (False, arg) = AE.parseJSON arg
