{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Types where

import qualified Data.ByteString.Lazy             as BL
import           Database.SQLite.Simple.FromField (FromField (..))
import           Database.SQLite.Simple.ToField   (ToField (..))
import           Web.Scotty                       (Parsable)
import           Control.Newtype                  (Newtype(..))
import           Data.Coerce                      (coerce)

newtype TileId = TileId Int
  deriving (Show, ToField, FromField)

newtype TileData = TileData {
  unTileData :: BL.ByteString
} deriving (Show, ToField, FromField)

newtype Zoom = Zoom Int
  deriving (Show, ToField, FromField, Parsable, Num)
instance Newtype Zoom Int where
  pack = coerce
  unpack = coerce
newtype XyzRow = XyzRow Int
  deriving (Show, ToField, Parsable, Num, Enum)
newtype TmsRow = TmsRow Int
  deriving (Show, ToField, FromField, Num, Enum)
newtype Column = Column Int
  deriving (Show, ToField, FromField, Parsable, Enum, Num)

-- | Flip y coordinate between xyz and tms schemes
toTmsY :: XyzRow -> Zoom -> TmsRow
toTmsY (XyzRow y) (Zoom z) = TmsRow (2 ^ z - y - 1)

toXyzY :: TmsRow -> Zoom -> XyzRow
toXyzY (TmsRow y) (Zoom z) = XyzRow (2 ^ z - y - 1)
