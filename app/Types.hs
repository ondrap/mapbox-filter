{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Types where

import qualified Data.ByteString.Lazy             as BL
import qualified Data.Text                        as T
import           Database.SQLite.Simple.FromField (FromField (..))
import           Database.SQLite.Simple.ToField   (ToField (..))
import           Web.Scotty                       (Parsable)


newtype TileId = TileId T.Text
  deriving (Show, ToField, FromField)

newtype TileData = TileData {
  unTileData :: BL.ByteString
} deriving (Show, ToField, FromField)

newtype Zoom = Zoom Int
  deriving (Show, ToField, FromField, Parsable)
newtype XyzRow = XyzRow Int
  deriving (Show, ToField, Parsable)
newtype TmsRow = TmsRow Int
  deriving (Show, ToField, FromField)
newtype Column = Column Int
  deriving (Show, ToField, FromField, Parsable)

-- | Flip y coordinate between xyz and tms schemes
toTmsY :: XyzRow -> Zoom -> TmsRow
toTmsY (XyzRow y) (Zoom z) = TmsRow (2 ^ z - y - 1)

toXyzY :: TmsRow -> Zoom -> XyzRow
toXyzY (TmsRow y) (Zoom z) = XyzRow (2 ^ z - y - 1)
