# Mapbox-filter - filtering mbtiles file according to Mapbox styles

A library that can interpret a subset of the Mapbox style epxression, a very simplified
parser for the Mapbox style JSON and an executable that can:

- Dump the uncompressed tile (.mvt, .pbf files) and show which features will be included given the style file at a particular zoom level (the .pbf files are often gzipped, you might need to unzip them first).
- Iterate throught the `mbtiles` file and filter the tile contents according to the MapBox style, thus making the `mbtiles` file smaller.
- Run a webserver for
  * serving the tiles from the `mbtile` file
  * serving the real-time filtered tiles
  * after serving a tile saving the compressed tile back to the database (Openmaptiles database only is currently supported in this mode)
- Publish mbtile to S3 so that you don't need to run a webserver at all. As this can
  take a very long time, incremental and parallel upload is supported.

This library supports only a subset of the expression language (https://www.mapbox.com/mapbox-gl-js/style-spec/#expressions-types).
It's because I don't need that and most of the language isn't going to be used in the filter expression anyway. If you need
features that are not implemented yet, create an issue.

The filtering first executes the filtering expression and removes features that will not
be displayed. Then it removes metadata that is not used in the styles. The removal
process is currently somewhat crude (it retains all metadata used at the particular layer),
but it should be enough for most usecases.

Currently only the openmaptiles.org mbtile files are supported for `filter` and
`publish` commands. The `web` command in non-lazy mode should be compatibile with
any mbtile file.

## How to compile

1. Install stack - https://docs.haskellstack.org/en/stable/README/
2. `stack setup`
3. `stack build`
4. `stack install` - installs binary `mapbox-filter` to ~/.local/bin

## Examples

Apply the style on all the tiles in the `cz.mbtiles`. The process uses all available CPUs.
You can you use multiple `-j` options to create one file containing data for all styles.
```
$ mapbox-filter filter -j openmaptiles.json.js cz.mbtiles
```

Serve the mbtiles file. The endpoint for MapBox is: http://server_name:3000/tiles/metadata.json
```
$ mapbox-filter web -p 3000 cz.mbtiles
```

Serve the mbtiles file while doing online filtering according to the openmaptiles.json.js file.
```
$ mapbox-filter web -p 3000 -j openmaptiles.json.js cz.mbtiles
```

Serve the mbtiles file, do online filtering on not-yet-filtered tiles, save the filtered tiles back
to the database.
```
$ mapbox-filter web -p 3000 -j openmaptiles.json.js -l cz.mbtiles
```

Publish filtered mbtiles to S3. Higher parallelism might be desirable, use the `-p`
parameter to facilitate more parallel uploads to S3.
```
$ mapbox-filter publish 
  -j openmaptiles.json.js 
  -u https://s3.eu-central-1.amazonaws.com/my-test-bucket/styled-map
  -t s3://my-test-bucket/styled-map -p 10 cz.mbtiles
```

## What next

This started as a way to learn typechecking in Haskell and how to make a typed AST using GADTs.
It took about 1 day to make it work and it practically worked on the first try. Haskell is impressive.
