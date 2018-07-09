# Mapbox-filter - filtering mbtiles file according to Mapbox GL JS styles

A library that can interpret a subset of the Mapbox style epxression, a very simplified
parser for the Mapbox GL JS style and an executable that can:

- Dump the tile (.mvt, .pbf files) and show which features will be included given the style file at a particular zoom level.
- Iterate through the `mbtiles` file and filter the tile contents according to the MapBox style, thus making the `mbtiles` file smaller.
- Run a webserver for
  * serving the tiles from the `mbtile` file
  * serving the real-time filtered tiles
  * after serving a tile saving the compressed tile back to the database (Openmaptiles database only is currently supported in this mode)
- Publish tiles to S3 so that you don't need to run a webserver at all. As this can
  take a very long time, incremental, differential and parallel upload is supported.

This library supports only a subset of the expression language (https://www.mapbox.com/mapbox-gl-js/style-spec/#expressions-types).
It's because I don't need that and most of the language isn't going to be used in the filter expression anyway. If you need
features that are not implemented yet, create an issue.

The filtering first executes the filtering expression and removes features that will not
be displayed. Then it removes metadata that is not used in the styles. The removal
process is currently somewhat crude (it retains all metadata used at the particular layer),
but it should be enough for most usecases.

Currently only the openmaptiles.org mbtile files are supported for `filter` and `publish` commands.
The `web` command should be compatibile with any mbtile file.

## How to compile

1. Install stack - https://docs.haskellstack.org/en/stable/README/
2. `stack setup`
3. `stack build`
4. `stack install` - installs binary `mapbox-filter` to ~/.local/bin
5. or you can run `stack exec -- mapbox-filter` instead without installing

I have not tested it but it will probably work on Windows as well.

## Examples

Show CLI help:
```
$ mapbox-filter -h
$ mapbox-filter publish -h
```

Apply the style on all the tiles in the `cz.mbtiles`. The process uses all available CPUs.
You can you use multiple `-j` options to create one file containing data for all styles.
```
$ mapbox-filter filter -j mapboxstyle.json cz.mbtiles
```

Serve the mbtiles file. The endpoint for MapBox is: http://server_name:3000/tiles/metadata.json
```
$ mapbox-filter web -p 3000 cz.mbtiles
```

Serve the mbtiles file while doing online filtering according to the mapboxstyle.json file.
```
$ mapbox-filter web -p 3000 -j mapboxstyle.json cz.mbtiles
```

Publish filtered mbtiles to S3. Higher parallelism might be desirable, use the `-p`
parameter to facilitate more parallel uploads to S3.
```
$ mapbox-filter publish 
  -j mapboxstyle.json
  -u https://s3.eu-central-1.amazonaws.com/my-test-bucket/styled-map
  -t s3://my-test-bucket/styled-map -p 10 cz.mbtiles
```

## Incremental job

Unless given the `-f` option, the filtering/publishing remembers roughly the last position
and when restarted, the job starts from the last position. The information is retained in a file
`<name>.mbtiles.SOME_NUMBERS`. When the mbtile file is replaced or the style is changed,
the `SOME_NUMBERS` change and a new full job is forced.

## Differential upload

The S3 is billed by a access request; in order to minimize access costs, the program
automatically creates a file `<name>.mbtile.hashes`. When the publishing is complete, copy
the file manually to S3 to have the information available later. 
Upon next job restart (regardless if with or without the `-f` option),
if the file `name.mbtiles.hashes` exists, only the changed tiles will be uploaded or deleted. 
This should minimize costs upon country updates, when only a minority of the tiles is changed.

## Performance considerations

### Parallelism and RTS tuning

The `filter` and `publish` commands by default use as many cores as is available on the computer.
However, sometimes this does not lead to better performance. You can limit the number of cores
with a special RTS (runtime system) command `-N`. It might be also beneficial to tune garbage 
collector with the `-A` parameter; you may need to experiment with the settings.

When publishing directly to S3, the bottleneck is usually the network; in such case it may be
better to use higher parallelism to achieve higher throughput. The following command
will use 16 cores, 80 parallel threads and has an allocation unit set to 1 megabyte:

```
$ mapbox-filter publish -j openmaptiles.json.js -u https://xxx.cloudfront.net/w -t s3://my-map-bucket/w osm-planet.mbtiles -p80 +RTS -N16 -A1m
```

### MD5 database tuning

When publishing the data, a new database of md5 hashes is automatically created to aid with
differential uploads. Unfortunately, the access to the database is serialized. Therefore,
it might be best to run the job in ramdisk. On Linux, this would mean changing directory
somewhere to `tmpfs`, e.g. `/dev/shm`. Create a symlink to the original `mbtiles` file
(e.g. `/dev/shm/world.mbtiles`) and then run the command in the `/dev/shm` directory.
The md5 database will be created on a ramdisk.

Alternatively, SSD disk or some enterprise storage system with write cache
might be fast enough with more assurance in case of power loss. 

## What next

This started as a way to learn typechecking in Haskell and how to make a typed AST using GADTs.
It took about 1 day to make it work and it practically worked on the first try. Haskell is impressive.
Obviously since the first day a lot of functionality and better performance was added.
