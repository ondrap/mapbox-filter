{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE ViewPatterns        #-}

module Main where

import           Codec.Compression.GZip               (decompress)
import           Control.Concurrent.ParallelIO.Global (globalPool,
                                                       stopGlobalPool)
import           Control.Concurrent.ParallelIO.Local  (Pool, parallel_,
                                                       withPool)
import           Control.Exception.Safe               (bracket, catchAny)
import           Control.Lens                         (over, (%~), (&), (<&>),
                                                       (?~), (^.), (^..), (^?),
                                                       _Just)
import           Control.Monad                        (void, when)
import           Control.Monad.IO.Class               (liftIO)
import           Data.Aeson                           ((.=))
import qualified Data.Aeson                           as AE
import           Data.Bool                            (bool)
import qualified Data.ByteString                      as BS
import qualified Data.ByteString.Lazy                 as BL
import           Data.Foldable                        (for_)
import qualified Data.HashMap.Strict                  as HMap
import           Data.List                            (nub)
import           Data.List.NonEmpty                   (NonEmpty, nonEmpty)
import           Data.Maybe                           (fromMaybe)
import           Data.Semigroup                       (sconcat, (<>))
import           Data.String.Conversions              (cs)
import qualified Data.Text                            as T
import qualified Data.Text.IO                         as T
import qualified Data.Text.Lazy                       as TL
import           Data.Traversable                     (for)
import           Database.SQLite.Simple               (Connection, Only (..),
                                                       Query (..), execute,
                                                       execute_, query, query_,
                                                       withConnection)
import           Geography.VectorTile                 (layers, linestrings,
                                                       metadata, name, points,
                                                       polygons, tile)
import           Network.AWS                          (Credentials (Discover),
                                                       configure, newEnv,
                                                       runAWS, runResourceT,
                                                       send, setEndpoint,
                                                       toBody)
import           Network.AWS.S3                       (BucketName (..),
                                                       ObjectKey (..),
                                                       poCacheControl,
                                                       poContentEncoding,
                                                       poContentType, putObject,
                                                       s3)
import           Options.Applicative                  hiding (header, style)
import           System.Posix.Files                   (getFileStatus,
                                                       modificationTime)
import           System.Posix.Types                   (EpochTime)
import           Text.Read                            (readMaybe)
import           Web.Scotty                           (addHeader, get, header,
                                                       json, param, raise, raw,
                                                       scotty, setHeader)


import           Mapbox.Filters
import           Mapbox.Interpret                     (FeatureType (..),
                                                       runFilter)
import           Mapbox.Style                         (MapboxStyle, lMinZoom,
                                                       lSource, msLayers,
                                                       _VectorType)
data PublishOpts = PublishOpts {
    pUrlPrefix  :: T.Text
  , pStoreTgt   :: BucketName
  , pThreads    :: Maybe Int
  , pForceFull  :: Bool
  , pS3Endpoint :: Maybe BS.ByteString
  , pMbtiles    :: FilePath
}

data CmdLine =
    CmdDump {
      fStyles     :: NonEmpty FilePath
    , fSourceName :: Maybe T.Text
    , fZoomLevel  :: Int
    , fMvtSource  :: FilePath
  }
  | CmdMbtiles {
      fStyles     :: NonEmpty FilePath
    , fSourceName :: Maybe T.Text
    , fForceFull  :: Bool
    , fMbtiles    :: FilePath
  }
  | CmdWebServer {
      fModStyles  :: [FilePath]
    , fSourceName :: Maybe T.Text
    , fWebPort    :: Int
    , fLazyUpdate :: Bool
    , fMbtiles    :: FilePath
  }
  | CmdPublish {
      fModStyles   :: [FilePath]
    , fSourceName  :: Maybe T.Text
    , fPublishOpts :: PublishOpts
  }

-- | The same as 'some', but generate NonEmpty list (makes more sense)
nsome :: Alternative f => f a -> f (NonEmpty a)
nsome x = fromMaybe (error "'some' didn't return element") . nonEmpty <$> some x

dumpOptions :: Parser CmdLine
dumpOptions =
  CmdDump <$> nsome (strOption (short 'j' <> long "style" <> metavar "JSFILE" <> help "JSON mapbox style file"))
          <*> optional (strOption (short 's' <> long "source" <> help "Tile source name"))
          <*> option auto (short 'z' <> long "zoom" <> help "Tile zoom level")
          <*> argument str (metavar "SRCFILE" <> help "Source file")

mbtileOptions :: Parser CmdLine
mbtileOptions =
  CmdMbtiles <$> nsome (strOption (short 'j' <> long "style" <> metavar "JSFILE" <> help "JSON mapbox style file"))
            <*> optional (strOption (short 's' <> long "source" <> help "Tile source name"))
            <*> switch (short 'f' <> long "force-full" <> help "Force full recomputation")
            <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")

webOptions :: Parser CmdLine
webOptions =
  CmdWebServer <$> many (strOption (short 'j' <> long "style" <> metavar "JSFILE" <> help "JSON mapbox style file"))
              <*> optional (strOption (short 's' <> long "source" <> help "Tile source name"))
              <*> option auto (short 'p' <> long "port" <> help "Web port number")
              <*> switch (short 'l' <> long "lazy" <> help "Lazily update the database with filtered data")
              <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")


s3Bucket :: ReadM BucketName
s3Bucket = maybeReader s3Reader
  where
    s3Reader txt = BucketName . stripRightSlash <$> T.stripPrefix "s3://" (T.pack txt)

stripRightSlash :: T.Text -> T.Text
stripRightSlash = T.dropWhileEnd (== '/')

publishOptions :: Parser CmdLine
publishOptions =
  CmdPublish
    <$> many (strOption (short 'j' <> long "style" <> metavar "JSFILE" <> help "JSON mapbox style file"))
    <*> optional (strOption (short 's' <> long "source" <> help "Tile source name"))
    <*> (PublishOpts <$>
         (stripRightSlash <$> strOption (short 'u' <> long "url-prefix" <> help "External tile URL prefix"))
      <*> option s3Bucket (short 't' <> long "target" <> help "S3 target prefix for files (e.g. s3://my-bucket/map)")
      <*> optional (option auto (short 'p' <> long "parallelism" <> metavar "NUMBER" <> help "Spawn multiple threads for faster upload (default: number of cores)"))
      <*> switch (short 'f' <> long "force-full" <> help "Force full recomputation")
      <*> optional (strOption (long "s3-endpoint" <> metavar "HOSTNAME" <> help "Endpoint for S3 operations (use e.g. with Google Cloud Storage)"))
      <*> argument str (metavar "MBTILES" <> help "MBTile SQLite database")
    )

cmdLineParser  :: Parser CmdLine
cmdLineParser =
  subparser $
    command "dump" (info (helper <*> dumpOptions) (progDesc "Dump vector files contents."))
    <> command "filter" (info (helper <*> mbtileOptions) (progDesc "Run filtering on a MBTiles database"))
    <> command "web" (info (helper <*> webOptions) (progDesc "Run a web server for serving tiles"))
    <> command "publish" (info (helper <*> publishOptions) (progDesc "Publish mbtile to S3"))

progOpts :: ParserInfo CmdLine
progOpts = info (cmdLineParser <**> helper)
    ( fullDesc <> progDesc "Utilities for working with Mapbox style file")

-- | Return style, update style minzoom levels to maxzoom if bigger
getStyle :: NonEmpty FilePath -> IO MapboxStyle
getStyle fnames =
  fmap sconcat <$> for fnames $ \fname -> do
    bstyle <- BS.readFile fname
    case AE.eitherDecodeStrict bstyle of
      Right res -> return res
      Left err  -> error ("Parsing mapbox style failed: " <> err)

-- | Check that the user correctly specified the source name and filter it out
--
-- It is possible for the mbtiles to provide maxzoom of 14 and style to be for maxzoom 17.
-- Therefore we update minzoom in the styles to be the maximum zoom in DB; this
-- ensures that the features stay in the mbtiles database
checkStyle :: Maybe T.Text -> Int -> MapboxStyle -> IO MapboxStyle
checkStyle mtilesrc dbmaxzoom styl = do
  -- Print vector styles
  let sources = nub (styl ^.. msLayers . traverse . _VectorType . lSource)
  for_ sources $ \s ->
    T.putStrLn $ "Found vector source layer: " <> s
  tilesrc <- case sources of
    [nm] | Just nm == mtilesrc -> return nm
         | Nothing <- mtilesrc -> return nm
    lst | Just nm <- mtilesrc, nm `elem` lst -> return nm
        | otherwise -> error ("Invalid tile source specified, " <> show mtilesrc)
  return $ styl & msLayers %~ filter (\l -> l ^? _VectorType . lSource == Just tilesrc)
                & over (msLayers . traverse . _VectorType . lMinZoom . _Just) (min dbmaxzoom)

-- | Generate metadata json based on modification text + database + other info
genMetadata :: Connection -> TL.Text -> TL.Text -> IO AE.Value
genMetadata conn modTimeStr urlPrefix = do
    metalines :: [(T.Text,String)] <- query_ conn "select name,value from metadata"
    return $ AE.object $
        concatMap addMetaLine metalines
        ++ ["tiles" .= [urlPrefix <> "/tiles/" <> modTimeStr <> "/{z}/{x}/{y}"],
            "tilejson" .= ("2.0.0" :: T.Text)
            ]
  where
    addMetaLine (key,val)
      | key `elem` ["attribution", "description", "name", "format", "basename", "id"] =
            [key .= val]
      | key `elem` ["minzoom", "maxzoom", "pixel_scale", "maskLevel", "planettime"],
        Just (dnum :: Int) <- readMaybe val =
            [key .= dnum]
      | key == "json", Just (AE.Object obj) <- AE.decode (cs val) =
            HMap.toList obj
      | key == "center", Just (lst :: [Double]) <- decodeArr val =
          [key .= lst]
      | key == "bounds", Just lst@[_ :: Double, _,_,_] <- decodeArr val =
          [key .= lst]
      | otherwise = []
      where
        split _ [] = []
        split c lst =
            let (start,rest) = span (/= c) lst
            in start : split c (drop 1 rest)
        decodeArr = traverse readMaybe . split ','

-- | Dump content of the mbtiles with hints about if the features are removed or retained
dumpPbf :: MapboxStyle -> Int -> FilePath -> IO ()
dumpPbf style zoom fp = do
  mvt <- autoUnzip <$> BL.readFile fp
  case tile (cs mvt) of
    Left err -> error (show err)
    Right vtile ->
      for_ (vtile ^.. layers . traverse) $ \l -> do
          T.putStrLn "-----------------------------"
          T.putStrLn ("Layer: " <> cs (l ^. name))
          let lfilter = cfExpr (getLayerFilter (l ^. name) cfilters)
          for_ (l ^. points) (printCont lfilter Point)
          for_ (l ^. linestrings) (printCont lfilter LineString)
          for_ (l ^. polygons) (printCont lfilter Polygon)
  where
    autoUnzip :: BL.ByteString -> BL.ByteString
    autoUnzip bs | BL.unpack (BL.take 2 bs) == [0x1f,0x8b] = decompress bs
                 | otherwise = bs

    cfilters = styleToCFilters zoom style

    printCont lfilter ptype feature = do
      let include = runFilter lfilter ptype feature
      putStrLn $ bool "- " "  " include <> show ptype <> " " <> show (feature ^. metadata)

type JobAction = ((Int, Int, Int), BL.ByteString) -> IO ()

-- | Run a filtering action on all tiles in the database and perform a JobAction
runFilterJob ::
     T.Text  -- ^ Table name that contains tiles that should be processed
  -> Pool -- ^ Threadpool for parallel processing
  -> Connection -- ^ Connection to sqlite db
  -> Maybe MapboxStyle -- ^ Filtering mapboxstyle
  -> JobAction -- ^ Action to perform on filtered tile
  -> IO ()
runFilterJob table pool conn mstyle saveAction = do
    zlevels <- query_ conn (Query ("select distinct zoom_level from " <> table <> " order by zoom_level"))
    for_ zlevels $ \(Only zoom) -> do
      putStrLn $ "Filtering zoom: " <> show zoom
      -- There can be huge amount of tiles, so do it in parallel, column by column
      let colquery = Query ("select distinct tile_column from " <> table <> " where zoom_level=? order by tile_column")
      cols :: [Only Int] <- query conn colquery (Only zoom)
      withPool 5 $ \colPool -> -- Do some low limit for columns
        parallelFor_ colPool cols $ \(Only col) -> do
          let qry = Query ("select zoom_level,tile_column,tile_row from " <> table <> " where zoom_level=? AND tile_column=?")
          (tiles :: [(Int,Int,Int)]) <- query conn qry (zoom, col)
          putStrLn $ "Col: " <> show col <> " Tiles: " <> show (length tiles)
          let iaction = maybe saveAction (\st -> shrinkTile (styleToCFilters zoom st)) mstyle
          parallelFor_  pool tiles $ \tid ->
              (fetchTile tid >>= iaction)
                `catchAny` \err -> putStrLn ("Error on " <> show tid <> ": " <> show err)
  where
    parallelFor_ pool_ parlist job = parallel_ pool_ (job <$> parlist)

    fetchTile tid@(z,x,y) = do
      let q = Query "select tile_data from tiles where zoom_level=? and tile_column=? and tile_row=?"
      [Only (tdata :: BL.ByteString)] <- query conn q (z,x,y)
      return (tid, tdata)

    shrinkTile filtList (tid, tdata) =
      case filterTileCs filtList tdata of
        Left err -> putStrLn $ "Error when decoding tile " <> show tid <> ": " <> cs err
        Right newdta -> saveAction (tid, newdta)

runIncrFilterJob ::
     T.Text -- ^ Table name for handling incremental data
  -> Pool -- ^ Threadpool for parallel processing
  -> Connection -- ^ Connection to sqlite db
  -> Maybe MapboxStyle -- ^ Filtering mapboxstyle
  -> Bool -- ^ Force a full upload (reset saved information)
  -> JobAction -- ^ Action to perform on filtered tile
  -> IO ()
runIncrFilterJob table pool conn mstyle forceFull saveAction = do
    createTable
    runFilterJob table pool conn mstyle $  \p@((z,x,y), _) -> do
      saveAction p
      execute conn
        (Query ("delete from " <> table <> " where zoom_level=? and tile_column=? and tile_row=?"))
        (z,x,y)
      -- delete from table
  where
    -- Return true if the working table exists
    tableExists =
      (void (query_ @(Only Int) conn (Query ("select count(*) from " <> table))) >> return True)
        `catchAny` \_ -> return False
    createTable = do
      exists <- tableExists
      if | not exists || forceFull -> do
            execute_ conn (Query ("drop table " <> table)) `catchAny` \_ -> return ()
            execute_ conn (Query ("create table " <> table <> " as select zoom_level,tile_column,tile_row from tiles")) `catchAny` \_ -> return ()
            execute_ conn (Query ("create INDEX "<> table <> "_index ON " <> table <> " (zoom_level,tile_column,tile_row)"))
            putStrLn "Doing full database work"
         | otherwise ->
            putStrLn "Doing incremental work"
      [Only (cnt :: Int)] <- query_ conn (Query ("select count(*) from " <> table))
      putStrLn ("Need to process " <> show cnt <> " tiles")

-- | Filter all tiles in a database and save the filtered tiles back
convertMbtiles :: MapboxStyle -> FilePath -> Bool -> IO ()
convertMbtiles style mbtiles force = do
  withConnection mbtiles $ \conn -> do
    runIncrFilterJob "shrink_queue" globalPool conn (Just style) force $ \((z,x,y), newdta) -> do
      let q = Query "select tile_id from map where zoom_level=? and tile_column=? and tile_row=?"
      [Only (tileid :: T.Text)] <- query conn q (z,x,y)
      execute conn "update images set tile_data=? where tile_id=?" (newdta, tileid)
    -- If we were shrinking, call vacuum on database
    execute_ conn "vacuum"
  stopGlobalPool

-- | Publish the mbtile to an S3 target, make it ready for serving
runPublishJob ::
     Maybe (MapboxStyle, EpochTime) -- ^ Parsed style + modification time of the style
  -> PublishOpts
  -> IO ()
runPublishJob mstyle PublishOpts{pMbtiles, pForceFull, pStoreTgt, pUrlPrefix, pThreads, pS3Endpoint} = do
  -- Generate AWS environ
  env <- newEnv Discover
        <&> maybe id (\host -> configure (setEndpoint True host 443 s3)) pS3Endpoint

  withThreads $ \pool ->
    withConnection pMbtiles $ \conn -> do
      modstr <- liftIO $ makeModtimeStr conn (snd <$> mstyle)
      runIncrFilterJob ("upload_" <> cs modstr) pool conn (fst <$> mstyle) pForceFull $ \((z,x,y), newdta) -> do
        let xyz_y = yzFlipTms y z
            dstpath = ObjectKey (cs ("tiles/" <> modstr <> "/" <> show z <> "/" <> show x <> "/" <> show xyz_y))
        runResourceT $ runAWS env $ do
          let cmd = putObject pStoreTgt dstpath (toBody newdta)
                    & poContentType ?~ "application/x-protobuf"
                    & poContentEncoding ?~ "gzip"
                    & poCacheControl ?~ "max-age=31536000"
          void $ send cmd
      meta <- genMetadata conn (cs modstr) (cs pUrlPrefix)
      let cmd = putObject pStoreTgt "metadata.json" (toBody (AE.encode meta))
                & poContentType ?~ "application/json"
      runResourceT $ runAWS env $
        void (send cmd)
  where
    withThreads
      | Just tcount <- pThreads = withPool tcount
      | otherwise = bracket (return globalPool) (const stopGlobalPool)

-- | Flip y coordinate between xyz and tms schemes
yzFlipTms :: Int -> Int -> Int
yzFlipTms y z = 2 ^ z - y - 1

-- | Make a string containing modification time of db & style
makeModtimeStr :: Connection -> Maybe EpochTime -> IO String
makeModtimeStr conn mtime = do
    dbmtime <- liftIO getDbMtime
    let stmtime = fromMaybe 0 mtime
    return (dbmtime <> "_" <> show stmtime)
  where
    getDbMtime = do
      mlines :: [Only String] <- query_ conn "select value from metadata where name='mtime'"
      case mlines of
        [Only res] -> return res
        _          -> return ""

-- | Run a web server serving filtered/unfiltered tiles and metadata
runWebServer :: Int -> Maybe (MapboxStyle, EpochTime) -> FilePath -> Bool -> IO ()
runWebServer port mstyle mbpath lazyUpdate =
  withConnection mbpath $ \conn -> do
    -- If lazy, add a column to a table
    when lazyUpdate $
      execute_ conn "alter table images add column shrinked default 0"
        `catchAny` \_ -> return ()
    -- Generate a JSON to be included as a metadata file
    -- Run a web server
    scotty port $ do
      get "/tiles/metadata.json" $ do
          -- find out protocol and host
          proto <- fromMaybe "http" <$> header "X-Forwarded-Proto"
          host <- fromMaybe "localhost" <$> header "Host"
          mtime <- liftIO $ makeModtimeStr conn (snd <$> mstyle)
          metaJson <- liftIO $ genMetadata conn (cs mtime) (proto <> "://" <> host)
          addHeader "Access-Control-Allow-Origin" "*"
          json metaJson
      get "/tiles/:mt1/:z/:x/:y" $ do
          z <- param "z"
          x :: Int <- param "x"
          y <- param "y"
          let tms_y = yzFlipTms y z

          mnewtile <- case (mstyle, lazyUpdate) of
              (Just (style,_), True) -> getLazyTile conn style z x tms_y
              _                      -> getTile conn z x tms_y

          addHeader "Access-Control-Allow-Origin" "*"
          setHeader "Content-Type" "application/x-protobuf"
          setHeader "Cache-Control" "max-age=31536000"

          case mnewtile of
            Just dta -> do
              addHeader "Content-Encoding" "gzip"
              raw dta
            Nothing -> raw ""
  where
    -- Get tile from modified database; if not already shrinked, shrink it and write to the database
    getLazyTile conn style z x tms_y = do
      rows <- liftIO $ query conn "select tile_data, map.tile_id, shrinked from images,map where zoom_level=? AND tile_column=? AND tile_row=? AND map.tile_id=images.tile_id"
                (z, x, tms_y)
      case rows of
        [(dta, _, 1 :: Int)] -> return (Just dta)
        [(dta, tileid :: T.Text, _)] ->
            case filterTile z style dta of
              Left err -> raise (cs err)
              Right newdta -> do
                liftIO $ execute conn "update images set tile_data=?,shrinked=1 where tile_id=?"
                              (newdta, tileid)
                return (Just newdta)
        _ -> return Nothing

    -- Ordinarily get tile from database; if styling enabled, do the styling
    getTile conn z x tms_y = do
      rows <- liftIO $ query conn "select tile_data from tiles where zoom_level=? AND tile_column=? AND tile_row=?"
                      (z, x, tms_y)
      case rows of
        [Only dta] ->
          case mstyle of
            Nothing -> return (Just dta)
            Just (style,_) ->
              case filterTile z style dta of
                Left err     -> raise (cs err)
                Right newdta -> return (Just newdta)
        _ -> return Nothing

main :: IO ()
main = do
  opts <- execParser progOpts
  case opts of
    CmdDump{fMvtSource, fZoomLevel, fStyles, fSourceName} -> do
        style <- getStyle fStyles >>= checkStyle fSourceName 14
        dumpPbf style fZoomLevel fMvtSource
    CmdMbtiles{fMbtiles, fStyles, fSourceName, fForceFull} -> do
        maxzoom <- getMaxZoom fMbtiles
        style <- getStyle fStyles >>= checkStyle fSourceName maxzoom
        convertMbtiles style fMbtiles fForceFull
    CmdWebServer{fModStyles, fWebPort, fMbtiles, fLazyUpdate, fSourceName} -> do
        maxzoom <- getMaxZoom fMbtiles
        mstyle <- getMStyle fModStyles fSourceName maxzoom
        runWebServer fWebPort mstyle fMbtiles fLazyUpdate
    CmdPublish{fModStyles, fSourceName, fPublishOpts} -> do
        maxzoom <- getMaxZoom (pMbtiles fPublishOpts)
        mstyle <- getMStyle fModStyles fSourceName maxzoom
        runPublishJob mstyle fPublishOpts
  where
    -- We need to adjust minZoom in the styles to be at least the maximum zoom level
    -- in the database
    getMaxZoom dbname = do
      [Only maxzoom] <- withConnection dbname $ \conn ->
             query_ conn "select max(zoom_level) from tiles"
      return maxzoom

    getMStyle (nonEmpty -> Just stlist) tilesrc maxzoom = do
        st <- getStyle stlist >>= checkStyle tilesrc maxzoom
        mtimes <- fmap modificationTime <$> traverse getFileStatus stlist
        return (Just (st, maximum mtimes))
    getMStyle _ _ _ = return Nothing
