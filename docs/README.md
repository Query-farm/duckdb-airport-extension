# Airport Extension for DuckDB

This extension "`airport`" enables the use of [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) with [DuckDB](https://duckdb.org).

![Ducks waiting to take a flight at the airport](https://github.com/user-attachments/assets/03b9fea6-171d-4b39-a2de-842860fef69b)

## What is Arrow Flight?

<img src="https://arrow.apache.org/docs/_static/arrow.png" style="float:right" width="200px" alt="Apache Arrow Logo"/>

Arrow Flight is an RPC framework for high-performance data services based on [Apache Arrow](https://arrow.apache.org/docs/index.html) and is built on top of [gRPC](https://grpc.io) and the [Arrow IPC format](https://arrow.apache.org/docs/format/IPC.html).

## API

### Listing Flights

```airport_list_flights(location, criteria, auth_token="token_value", secret="secret_name")```

__Description:__ This function returns a list of Arrow Flights that are available at a particular endpoint.

##### Parameters:

| Parameter Name | Type | Description |
|--------|---|-------------------------------|
| `location` | `VARCHAR` | This is the location of the Flight server |
| `criteria` | `VARCHAR` | This is free-form criteria to pass to the Flight server |

##### Named Parameters:

| Parameter Name | Type | Description |
|--------|---|-------------------------------|
| `auth_token` | `VARCHAR` | A bearer value token to present to the server, the header is formatted like `Authorization: Bearer <auth_token>` |
| `secret` | `VARCHAR` | This is the name of the [DuckDB secret](https://duckdb.org/docs/configuration/secrets_manager.html) to use to supply the value for the `auth_token` |



```sql
> select * from airport_list_flights('http://127.0.0.1:8815', null);

flight_descriptor = [uploaded.parquet]
         endpoint = [{'ticket': uploaded.parquet, 'location': [grpc://0.0.0.0:8815], 'expiration_time': NULL, 'app_metadata': }]
          ordered = false
    total_records = 3
      total_bytes = 363
     app_metadata =
           schema = Character: string
```

In addition to the `criteria` parameter, the Airport extension will pass additional GRPC headers.

#### Serializing Filters

The `airport-duckdb-json-filters` header is sent on the GRPC requests.  The header contains a JSON serialized representation of all of the conditional filters that are going applied to the results.

To illustrate this through an example:

```sql
select * from airport_list_flights('grpc://localhost:8815/', null) where total_bytes = 5;
```

The GRPC header `airport-duckdb-json-filters` will be set to

```json
{
 "filters": [
  {
   "expression_class": "BOUND_COMPARISON",
   "type": "COMPARE_EQUAL",
   "left": {
    "expression_class": "BOUND_COLUMN_REF",
    "type": "BOUND_COLUMN_REF",
    "alias": "total_bytes",
    "return_type": {
     "id": "BIGINT"
    }
   },
   "right": {
    "expression_class": "BOUND_CONSTANT",
    "type": "VALUE_CONSTANT",
    "value": {
     "type": {
      "id": "BIGINT"
     },
     "is_null": false,
     "value": 5
    }
   }
  }
 ]
}
```

The `airport-duckdb-json-filters` header will not contain newlines, but the JSON has been reformatted in this document for ease of comprehension.

It is up to the implementer of the server to use this header to apply optimizations.  The Airport DuckDB extension will still apply the filters to the result returned by the server. This means that the filter logic is purely advisory.  In the author's experience, if Arrow Flight servers implement the filtering logic server side it can unlock some impressive optimizations.  The JSON schema of the serialized filter expressions is not guaranteed to remain unchanged across DuckDB versions, the serialization is performed by the DuckDB code.

#### Projection Optimization

The header `airport-duckdb-column-ids` will contain a comma-separated list of column indexes that are used in the query.  The Arrow Flight server can return nulls for columns that are not requested.  This can be used to reduce the amount of data that is transmitted in the response.

### Taking a Flight

```airport_take_flight(location, descriptor, auth_token="token_value", secret="secret_name")```

__Description:__ This function is a table returning function, it returns the contents of the Arrow Flight.

##### Parameters:

| Parameter Name | Type | Description |
|--------|---|-------------------------------|
| `location` | `VARCHAR` | This is the location of the Flight server |
| `descriptor` | `ANY` | This is the descriptor of the flight.  If it is a `VARCHAR` or `BLOB` it is interpreted as a command, if it is an `ARRAY` or `VARCHAR[]` it is considered a path-based descriptor.  |

##### Named Parameters:

| Parameter Name | Type | Description |
|--------|---|-------------------------------|
| `auth_token` | `VARCHAR` | A bearer value token to present to the server, the header is formatted like `Authorization: Bearer <auth_token>` |
| `secret` | `VARCHAR` | This is the name of the [DuckDB secret](https://duckdb.org/docs/configuration/secrets_manager.html) to use to supply the value for the `auth_token` |
| `ticket` | `BLOB` | This is the ticket (an opaque binary token) supplied to the Flight server it overrides any ticket supplied from GetFlightInfo. |
| `headers` | `MAP(VARCHAR, VARCHAR)` | A map of extra GRPC headers to send with requests to the Flight server. |


```sql
select * from airport_take_flight('grpc://localhost:8815/', ['counter-stream']) limit 5;
┌─────────┐
│ counter │
│  int64  │
├─────────┤
│       0 │
│       1 │
│       2 │
│       3 │
│       4 │
└─────────┘
```

### Creating a secret

To create a secret that can be used by `airport_take_flight` and `airport_list_flight` use the standard DuckDB `CREATE SECRET` command.

```sql
CREATE SECRET airport_hello_world (
      type airport,
      auth_token 'test-token',
      scope 'grpc+tls://server.example.com/'
);
```

The Airport extension respects the scope(s) specified in the secret.  If a value for `auth_token` isn't supplied, but a secret exists with a scope that matches the server location the value for the `auth_token` will be used from the secret.


## Implementation Notes

### TODO

1. Investigate the multithreaded endpoint support.

## Implementation Notes

### Memory Alignment of Apache Arrow Buffers

This extension applied a change to the C interface of Apache Arrow that enforces the 8-byte alignment of the column data.  DuckDB requires that this alignment is present, Apache Arrow and Arrow Flight do not require this.

If other extensions use Apache Arrow please ensure that the patch `align-record-batch.patch` is applied from `vcpkg-overlay/arrow`.

## Building the extension
### Install Apache Arrow
```sh
# Mac OS X Homebrew (Recommended)
brew install apache-arrow

# Windows (via vcpkg)
vcpkg install arrow

# Liunx
sudo apt install -y libarrow-dev
```

### DuckDB Airport Extension
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

## Running the extension

To run the extension code, simply start the shell with `./build/release/duckdb`. This duckdb shell will have the extension pre-loaded.

Now we can use the features from the extension directly in DuckDB.

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:

```sh
make test
```

