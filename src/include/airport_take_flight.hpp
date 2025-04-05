#pragma once

#include "airport_extension.hpp"
#include "duckdb.hpp"

#include "airport_flight_stream.hpp"

namespace duckdb
{
  struct AirportArrowScanGlobalState : public GlobalTableFunctionState
  {

    unique_ptr<ArrowArrayStreamWrapper> stream;
    mutex main_mutex;
    idx_t batch_index = 0;
    bool done = false;

    idx_t MaxThreads() const override
    {
      return endpoints_.size() == 0 ? 1 : endpoints_.size();
    }

    bool CanRemoveFilterColumns() const
    {
      return !projection_ids_.empty();
    }

    // If there is a list of endpoints this constructor it used.
    AirportArrowScanGlobalState(const vector<flight::FlightEndpoint> &endpoints,
                                const vector<idx_t> &projection_ids,
                                const vector<LogicalType> &scanned_types)
        : endpoints_(endpoints), current_endpoint_(0), projection_ids_(projection_ids),
          scanned_types_(scanned_types)
    {
      D_ASSERT(endpoints_.size() == 1);
    }

    // There are cases where a list of endpoints isn't available, for example
    // the calls to DoExchange, so in that case don't set the endpoints.
    AirportArrowScanGlobalState() : current_endpoint_(0)

    {
    }

    size_t total_endpoints() const
    {
      return endpoints_.size();
    }

    const std::optional<const flight::FlightEndpoint> GetNextEndpoint()
    {
      if (current_endpoint_ >= endpoints_.size())
      {
        return std::nullopt;
      }
      return endpoints_[current_endpoint_++];
    }

    const vector<idx_t> &projection_ids() const
    {
      return projection_ids_;
    }

    const vector<LogicalType> &scanned_types() const
    {
      return scanned_types_;
    }

  private:
    vector<flight::FlightEndpoint> endpoints_;
    idx_t current_endpoint_ = 0;
    const vector<idx_t> projection_ids_;
    const vector<LogicalType> scanned_types_;
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
      const std::optional<AirportGetFlightInfoTableFunctionParameters> &table_function_parameters);

  std::string AirportNameForField(const string &name, const idx_t col_idx);

  unique_ptr<LocalTableFunctionState> AirportArrowScanInitLocal(ExecutionContext &context,
                                                                TableFunctionInitInput &input,
                                                                GlobalTableFunctionState *global_state_p);

}
