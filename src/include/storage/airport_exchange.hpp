#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "airport_flight_stream.hpp"
#include "airport_take_flight.hpp"
#include "airport_table_entry.hpp"
#include "airport_schema_utils.h"

namespace duckdb
{

  struct AirportExchangeTakeFlightBindData : public AirportTakeFlightBindData
  {

  public:
    explicit AirportExchangeTakeFlightBindData(stream_factory_produce_t scanner_producer_p,
                                               const string &trace_id,
                                               const int64_t estimated_records,
                                               const AirportTakeFlightParameters &take_flight_params_p,
                                               const std::optional<AirportGetFlightInfoTableFunctionParameters> &table_function_parameters_p,
                                               std::shared_ptr<arrow::Schema> schema,
                                               const flight::FlightDescriptor &descriptor,
                                               std::shared_ptr<arrow::flight::FlightStreamReader> reader,
                                               shared_ptr<DependencyItem> dependency = nullptr) : AirportTakeFlightBindData(scanner_producer_p, trace_id, estimated_records, take_flight_params_p, table_function_parameters_p, schema, descriptor, std::move(reader), std::move(dependency))
    {
    }

    mutable mutex lock;

    void examine_schema(
        ClientContext &context,
        bool skip_rowid_column)
    {
      AirportExamineSchema(
          context,
          schema_root,
          &arrow_table,
          &return_types_,
          &names_,
          nullptr,
          &rowid_column_index,
          skip_rowid_column);
    }

    const vector<string> &names() const
    {
      return names_;
    }

    const vector<LogicalType> &return_types() const
    {
      return return_types_;
    }

  private:
    // these are set in examine_schema().
    vector<string> names_;
    vector<LogicalType> return_types_;
  };

  // This is all of the state is needed to perform a ArrowScan on a resulting
  // DoExchange stream, this is useful for having RETURNING data work for
  // INSERT, DELETE or UPDATE.
  class AirportExchangeGlobalState
  {
  public:
    std::shared_ptr<arrow::Schema> send_schema;

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
                                         const ArrowSchema &send_schema,
                                         const bool return_chunk,
                                         const string exchange_operation,
                                         const vector<string> returning_column_names,
                                         const std::optional<string> transaction_id);
}