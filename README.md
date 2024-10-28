
<img src="https://github.com/user-attachments/assets/fa806574-9120-474f-8cb4-b17b1fbc3cd1" width=250 />

# DuckFS
DuckFS allows you to navigate and explore the host filesystem from DuckDB.

Example 1: Navigate to the workspace and list the files.
```plaintext
D PRAGMA cd('/Users/paul/workspace');
D PRAGMA ls;
┌───────────────────────────────┐
│             path              │
│            varchar            │
├───────────────────────────────┤
│ ./duckdb                      │
│ ./playground                  │
│ ./duckfs                      │
...
```
Example 2: List the top 3 file types by total size, with file count, ordered by size.
```plaintext
D SELECT size, count, file_extension AS "type"
  FROM (
      SELECT SUM(file_size(path)) AS size_raw, hsize(size_raw) AS size, COUNT(*) AS count, file_extension(path) AS file_extension
      FROM lsr('/Users/paul/workspace', 10)
      GROUP BY file_extension(path)
  ) AS subquery
  ORDER BY size_raw DESC
  LIMIT 3;
┌───────────┬───────┬─────────┐
│   size    │ count │  type   │
│  varchar  │ int64 │ varchar │
├───────────┼───────┼─────────┤
│ 246.95 GB │    29 │ .duckdb │
│ 90.33 GB  │  3776 │ .tmp    │
│ 26.17 GB  │ 28175 │ .csv    │
└───────────┴───────┴─────────┘
```
Example 3: Find the files you were working on last to continue your analysis.
```plaintext
D SELECT path, file_last_modified(path) AS date FROM ls() WHERE 'csv' IN file_extension(path) ORDER BY date LIMIT 1 ;
┌───────────────────────────┬─────────────────────┐
│           path            │        date         │
│          varchar          │      timestamp      │
├───────────────────────────┼─────────────────────┤
│ ./sketch_results_join.csv │ 2024-07-13 23:25:48 │
└───────────────────────────┴─────────────────────┘
D SELECT n_rows, std, n_duplicates FROM './sketch_results.csv' LIMIT 4;
┌────────┬────────────────────┬────────────────────┐
│ n_rows │        std         │    n_duplicates    │
│ int64  │       double       │       double       │
├────────┼────────────────────┼────────────────────┤
│   1000 │ 26.855167100578615 │ 1.0405827263267429 │
│   1000 │  44.76159067772279 │ 1.1547344110854503 │
│   1000 │ 31.675858315126995 │ 1.5649452269170578 │
│   1000 │  52.60798418491246 │ 3.1545741324921135 │
└────────┴────────────────────┴────────────────────┘
```

## Features

| Function                 | Description                                                                            | Parameters                                                                                     | Type              |
|--------------------------|----------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|-------------------|
| `pwd()`                  | Get the current working directory.                                                      | None                                                                                           | Scalar Function    |
| `cd(path)`               | Change the current working directory.                                                   | `path`: Target directory path (String)                                                          | Table Function     |
| `ls(path)`               | List files in a directory. Defaults to the current directory if `path` is not provided. | `path` (optional): Directory path (String)                                                      | Table Function     |
| `lsr(path, depth)`       | List files in a directory recursively. Defaults to the current directory and no limit.  | `path` (optional): Directory path (String) <br> `depth` (optional): Recursion depth, default is -1 (no limit) | Table Function     |
| `is_file(path)`          | Check if the path is a file.                                                            | `path`: File path (String)                                                                      | Scalar Function    |
| `is_dir(path)`           | Check if the path is a directory.                                                       | `path`: Directory path (String)                                                                 | Scalar Function    |
| `file_name(path)`        | Get the file name from the path.                                                        | `path`: File path (String)                                                                      | Scalar Function    |
| `file_extension(path)`   | Get the file extension from the path.                                                   | `path`: File path (String)                                                                      | Scalar Function    |
| `file_size(path)`        | Get the size of the file.                                                               | `path`: File path (String)                                                                      | Scalar Function    |
| `file_last_modified(path)`| Get the last modified time of the file.                                                | `path`: File path (String)                                                                      | Scalar Function    |
| `absolute_path(path)`    | Get the absolute path of the file.                                                      | `path`: File path (String)                                                                      | Scalar Function    |
| `path_exists(path)`      | Check if the path exists.                                                               | `path`: File or directory path (String)                                                         | Scalar Function    |
| `path_type(path)`        | Get the type of the path (file or directory).                                           | `path`: File or directory path (String)                                                         | Scalar Function    |
| `hsize(bytes)`           | Format file size in human-readable form.                                                | `bytes`: Number of bytes (Integer)                                                              | Scalar Function    |

## Building

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
