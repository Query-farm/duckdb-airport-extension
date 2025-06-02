# Airport Extension for DuckDB

The **Airport** extension brings [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) support to [DuckDB](https://duckdb.org), enabling DuckDB to query, modify, and store data via Arrow Flight servers. A DuckDB extension is a plugin that expands DuckDB's core functionality by adding new capabilities.

To understand the rationale behind the development of this extension, check out the [motivation for creating the extension](https://airport.query.farm/motivation.html).

# Documentation

Visit the [documentation for this extension](https://airport.query.farm).

# Building the extension

```sh
# Clone this repo with submodules.
# duckdb and extension-ci-tools are submodules.
git clone --recursive git@github.com:Query-farm/duckdb-airport-extension

# Clone the vcpkg repo
git clone https://github.com/Microsoft/vcpkg.git

# Bootstrap vcpkg
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake

# Build the extension
make

# If you have ninja installed, you can use it to speed up the build
# GEN=ninja make
```

The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/airport/airport.duckdb_extension
```

- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `airport.duckdb_extension` is the loadable binary as it would be distributed.

## Building on MacOS
If you have difficulties building with the clang provided by the Xcode Command Line Tools, you may want to try installing llvm and using the included clang. Also, some of the dependencies built by `vcpkg` require GNU bison to be installed:
```sh
brew install bison cmake llvm
export CXX=/opt/homebrew/opt/llvm/bin/clang++
```

## Running the extension

To run the extension code, simply start the shell with `./build/release/duckdb`. This duckdb shell will have the extension pre-loaded.

Now we can use the features from the extension directly in DuckDB.

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:

```sh
make test
```

