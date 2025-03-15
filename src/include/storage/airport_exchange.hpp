#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "airport_flight_stream.hpp"
#include "airport_take_flight.hpp"
#include "airport_table_entry.hpp"

namespace duckdb
{

  struct AirportExchangeTakeFlightBindData : public ArrowScanFunctionData
  {
  public:
    using ArrowScanFunctionData::ArrowScanFunctionData;
    std::unique_ptr<AirportTakeFlightScanData> scan_data = nullptr;
    std::unique_ptr<arrow::flight::FlightClient> flight_client = nullptr;

    string server_location;
    string json_filters;

    // This is the trace id so that calls to GetFlightInfo and DoGet can be traced.
    string trace_id;

    idx_t rowid_column_index = COLUMN_IDENTIFIER_ROW_ID;

    // This is the auth token.
    string auth_token;
    mutable mutex lock;

    vector<string> names;
    vector<LogicalType> return_types;
  };

  // This is all of the state is needed to perform a ArrowScan on a resulting
  // DoExchange stream, this is useful for having RETURNING data work for
  // INSERT, DELETE or UPDATE.
  class AirportExchangeGlobalState
  {
  public:
    std::shared_ptr<arrow::Schema> schema;
    arrow::flight::FlightDescriptor flight_descriptor;

    std::unique_ptr<AirportExchangeTakeFlightBindData> scan_bind_data;
    std::unique_ptr<ArrowArrayStreamWrapper> reader;
    std::unique_ptr<arrow::flight::FlightStreamWriter> writer;

    duckdb::unique_ptr<TableFunctionInput> scan_table_function_input;

    duckdb::unique_ptr<GlobalTableFunctionState> scan_global_state;
    duckdb::unique_ptr<LocalTableFunctionState> scan_local_state;

    vector<LogicalType> send_types;
    vector<string> send_names;
  };

  void AirportExchangeGetGlobalSinkState(ClientContext &context,
                                         const TableCatalogEntry &table,
                                         const AirportTableEntry &airport_table,
                                         AirportExchangeGlobalState *global_state,
                                         ArrowSchema &send_schema,
                                         bool return_chunk,
                                         string exchange_operation,
                                         vector<string> returning_column_names);
}