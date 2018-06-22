# Mapbox-filter - filtering mbtile file according to Mapbox styles

A library that can interpret a subset of the Mapbox style epxression, a very simplified
parser for the Mapbox style JSON and an executable that can:

- dump the uncompressed tile (.mvt, .pbf files) and show which features will be included given the style file at a particular zoom level
- iterate throught the `mbtile` file and filter the tile contents according to the MapBox style, thus making the `mbtile` file smaller
- run a webserver for
  * serving the tiles from the `mbtile` file
  * serving the real-time filtered tiles
  * after serving a tile saving the compressed tile back to the database (Openmaptiles database only is currently supported in this mode)

This library supports only a subset of the expression language (https://www.mapbox.com/mapbox-gl-js/style-spec/#expressions-types).
This is because I don't need that and most of the language isn't going to be used in the filter expression anyway. If you need
features that are not implemented yet, create an issue.

## How to compile

1. Install stack - https://docs.haskellstack.org/en/stable/README/
2. `stack setup`
3. `stack build`
4. `stack install` - installs binary `mapbox-filter` to ~/.local/bin

## Examples

Apply the style on all the tiles in the `cz.mbtiles`. The process uses all available CPUs.
```
$ mapbox-filter filter -j openmaptiles.json.js cz.mbtiles
```

Serve the mbtile file. The endpoint for MapBox is: http://server_name:3000/tiles/metadata.json
```
$ mapbox-filter web -p 3000 cz.mbtiles
```

Serve the mbitle file while doing online filtering according to the openmaptiles.json.js file
```
$ mapbox-filter web -p 3000 -j openmaptiles.json.js cz.mbtiles
```

Serve the mbitle file, do online filtering on not-yet-filtered tiles, save the filtered tiles back
to the database.
```
$ mapbox-filter web -p 3000 -j openmaptiles.json.js -l cz.mbtiles
```

## What next

This started as a way to learn typechecking in Haskell and how to make a typed AST using GADTs.
It took about 1 day to make it work and it practically worked on the first try. Haskell is impressive.
