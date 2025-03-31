#pragma once

#include "airport_extension.hpp"
#include "duckdb.hpp"

#include "duckdb/function/table/arrow.hpp"
#include "airport_flight_stream.hpp"
#include <msgpack.hpp>

namespace duckdb
{
  struct AirportArrowScanGlobalState : public ArrowScanGlobalState
  {
    vector<flight::FlightEndpoint> endpoints;
    idx_t current_endpoint = 0;
  };

  unique_ptr<ArrowArrayStreamWrapper> AirportProduceArrowScan(const ArrowScanFunctionData &function,
                                                              const vector<column_t> &column_ids,
                                                              TableFilterSet *filters);

  void AirportTakeFlight(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);

  unique_ptr<GlobalTableFunctionState> AirportArrowScanInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input);

  unique_ptr<FunctionData> AirportTakeFlightBindWithFlightDescriptor(
      const AirportTakeFlightParameters &take_flight_params,
      const arrow::flight::FlightDescriptor &descriptor,
      ClientContext &context,
      const TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names,
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<const struct AirportGetFlightInfoTableFunctionParameters> table_function_parameters);

}
