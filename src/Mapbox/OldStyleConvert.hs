{-# LANGUAGE OverloadedStrings #-}
module Mapbox.OldStyleConvert where

import qualified Data.Aeson as AE
import Data.Aeson (toJSON, Value(..))
import Data.Foldable (toList)
import qualified Data.Text as T

-- | Convert deprecated old-style filter to a new-style expression filter
convertToNew :: AE.Value -> Either String AE.Value
convertToNew (AE.Array arr) 
  | (String fname:args) <- toList arr = runFunc fname args
  where
    runGet :: T.Text -> AE.Value
    runGet "$type" = toJSON [AE.String "geometry-type"]
    runGet "$id" = toJSON [AE.String "id"]
    runGet other = toJSON ["get", other]

    runFunc :: T.Text -> [AE.Value] -> Either String AE.Value
    runFunc "has" [String key] = Right $ toJSON ["has", key]
    runFunc "!has" [String key] = Right $ toJSON [String "!", toJSON ["has", key]]
    runFunc doper [String key, String val] 
      | doper `elem` ["==", "!=", ">", ">=", "<", "<="] = 
          Right $ toJSON [String doper, toJSON [String "string", runGet key], String val]
    runFunc doper [String key, Number val] 
      | doper `elem` ["==", "!=", ">", ">=", "<", "<="] = 
          Right $ toJSON [String doper, toJSON [String "number", runGet key], Number val]
    runFunc dfunc args
      | dfunc `elem` ["all", "any"] = do
          newargs <- traverse convertToNew args
          Right $ toJSON $ [String dfunc] ++ newargs
      | dfunc == "none" = do
          newargs <- traverse convertToNew args
          Right $ toJSON $ [String "all"] ++ map ((\x -> toJSON [String "!", x])) newargs
    runFunc "!in" (AE.String key:vals) =
      return $ toJSON [String "match", toJSON ["string", runGet key, ""], toJSON vals, toJSON False, toJSON True ]
    runFunc "in" (AE.String key:vals) =
      return $ toJSON [String "match", toJSON ["string", runGet key, ""], toJSON vals, toJSON True, toJSON False ]
    runFunc f args = Left ("Unknown func or params: " <> show f <> ", " <> show args)

convertToNew v = Left ("Parse error: " <> show v)
