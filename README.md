# DuckFS

DuckFS allows you to browse the host filesystem from DuckDB.

## Features
- `pwd() [Scalar Function]`: Get the current working directory.
- `cd(path) [Table Function]`: Change the current working directory.
- `ls(path) [Table Function]`: List files in a directory, `path` is optional and defaults to the current directory.

## Examples
```plaintext
D SELECT pwd();
┌──────────────────────────────────────────┐
│                  pwd()                   │
│                 varchar                  │
├──────────────────────────────────────────┤
│ /Users/paul/workspace/duckFS/build/debug │
└──────────────────────────────────────────┘
```
```plaintext
D PRAGMA cd('/Users/paul/workspace/duckpgq-experiments/scripts/pipeline');
┌────────────────────────────────────────────────────────────┬─────────┐
│                     current_directory                      │ success │
│                          varchar                           │ boolean │
├────────────────────────────────────────────────────────────┼─────────┤
│ /Users/paul/workspace/duckpgq-experiments/scripts/pipeline │ true    │
└────────────────────────────────────────────────────────────┴─────────┘
```
```plaintext
D SELECT *, hsize(size) as hsize FROM ls() WHERE size != 0 ORDER BY last_modified DESC LIMIT 3;
┌───────────────┬───────────┬───────────┬─────────────────────┬───────────┐
│     path      │   size    │ file_type │    last_modified    │   hsize   │
│    varchar    │   int64   │  varchar  │      timestamp      │  varchar  │
├───────────────┼───────────┼───────────┼─────────────────────┼───────────┤
│ ./duckdb      │ 471631560 │ file      │ 2024-10-20 18:16:04 │ 449.78 MB │
│ ./.ninja_log  │     76821 │ file      │ 2024-10-20 18:16:04 │ 75.02 KB  │
│ ./.ninja_deps │   2038000 │ file      │ 2024-10-20 18:15:48 │ 1.94 MB   │
└───────────────┴───────────┴───────────┴─────────────────────┴───────────┘

``` 
```plaintext
D SELECT path FROM ls('.') LIMIT 3;
┌──────────────┐
│     path     │
│   varchar    │
├──────────────┤
│ ./repository │
│ ./tools      │
│ ./extension  │
└──────────────┘
```


## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/duckfs/duckfs.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `duckfs.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. The template contains a single scalar function `duckfs()` that takes a string arguments and returns a string:
```
D select duckfs('Jane') as result;
┌────────────────┐
│     result     │
│    varchar     │
├────────────────┤
│ Duckfs Jane 🐥 │
└────────────────┘
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL duckfs
LOAD duckfs
```
