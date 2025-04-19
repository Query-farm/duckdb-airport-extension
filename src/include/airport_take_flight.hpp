#pragma once

#include "airport_extension.hpp"
#include "duckdb.hpp"

#include "airport_flight_stream.hpp"

namespace duckdb
{
  struct AirportArrowScanGlobalState : public GlobalTableFunctionState
  {
    // idx_t batch_index = 0;

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
                                const vector<LogicalType> &scanned_types,
                                const std::optional<TableFunctionInitInput> &input)
        : endpoints_(endpoints),
          projection_ids_(projection_ids),
          scanned_types_(scanned_types),
          init_input_(input)
    {
      if (init_input_)
      {
        if (init_input_->filters)
        {
          init_input_->filters = init_input_->filters->Copy();
        }
      }
    }

    // There are cases where a list of endpoints isn't available, for example
    // the calls to DoExchange, so in that case don't set the endpoints.
    explicit AirportArrowScanGlobalState()
    {
    }

    const size_t total_endpoints() const
    {
      return endpoints_.size();
    }

    const std::optional<const flight::FlightEndpoint> GetNextEndpoint()
    {
      size_t index = current_endpoint_.fetch_add(1, std::memory_order_relaxed);
      if (index < endpoints_.size())
      {
        return endpoints_[index];
      }
      return std::nullopt;
    }

    const vector<idx_t> &projection_ids() const
    {
      return projection_ids_;
    }

    const vector<LogicalType> &scanned_types() const
    {
      return scanned_types_;
    }

    const std::optional<TableFunctionInitInput> &init_input() const
    {
      return init_input_;
    }

  private:
    vector<flight::FlightEndpoint> endpoints_;
    std::atomic<size_t> current_endpoint_ = 0;
    const vector<idx_t> projection_ids_;
    const vector<LogicalType> scanned_types_;
    std::optional<TableFunctionInitInput> init_input_ = std::nullopt;
  };

  shared_ptr<ArrowArrayStreamWrapper> AirportProduceArrowScan(
      const ArrowScanFunctionData &function,
      const vector<column_t> &column_ids,
      const TableFilterSet *filters,
      atomic<double> *progress,
      std::shared_ptr<arrow::Buffer> *last_app_metadata,
      const std::shared_ptr<arrow::Schema> &schema,
      const AirportLocationDescriptor &location_descriptor,
      AirportArrowScanLocalState &local_state);

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
