{-# LANGUAGE OverloadedStrings #-}

-- | Untyped expression parser for the mapbox style expressions
module Mapbox.UntypedExpression where

import           Control.Applicative ((<|>))
import           Data.Aeson          (FromJSON (..))
import qualified Data.Aeson          as AE
import           Data.Scientific     (Scientific)
import qualified Data.Text           as T
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
