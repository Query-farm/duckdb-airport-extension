#pragma once

#include <limits>
#include <cstdint>
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/c/bridge.h"

#include "arrow/flight/client.h"

#include "duckdb/function/table/arrow.hpp"
#include "msgpack.hpp"
#include "airport_location_descriptor.hpp"
#include "airport_macros.hpp"
namespace flight = arrow::flight;

/// File copied from
/// https://github.com/duckdb/duckdb-wasm/blob/0ad10e7db4ef4025f5f4120be37addc4ebe29618/lib/include/duckdb/web/arrow_stream_buffer.h
namespace duckdb
{

  struct AirportArrowStreamParameters : public ArrowStreamParameters, public AirportLocationDescriptor
  {
    explicit AirportArrowStreamParameters(
        atomic<double> *progress,
        std::shared_ptr<arrow::Buffer> *last_app_metadata,
        const std::shared_ptr<arrow::Schema> &schema,
        const AirportLocationDescriptor &location_descriptor)
        : ArrowStreamParameters(),
          AirportLocationDescriptor(location_descriptor),
          progress(progress),
          last_app_metadata(last_app_metadata),
          schema_(schema)
    {
    }

    // The schema of the stream that will be read.
    const std::shared_ptr<arrow::Schema> &schema() const
    {
      return schema_;
    }

  public:
    atomic<double> *progress = nullptr;
    std::shared_ptr<arrow::Buffer> *last_app_metadata = nullptr;

  private:
    const std::shared_ptr<arrow::Schema> &schema_;
  };

  struct AirportGetFlightInfoTableFunctionParameters
  {
    std::string schema_name;
    std::string action_name;
    std::string parameters;
    std::string table_input_schema;

    MSGPACK_DEFINE_MAP(schema_name, action_name, parameters, table_input_schema)
  };

  class AirportTakeFlightParameters
  {
  public:
    AirportTakeFlightParameters(
        const string &server_location,
        ClientContext &context,
        TableFunctionBindInput &input);

    AirportTakeFlightParameters(
        const string &server_location,
        ClientContext &context);

    const string &server_location() const
    {
      return server_location_;
    }

    const string &auth_token() const
    {
      return auth_token_;
    }

    const string &secret_name() const
    {
      return secret_name_;
    }

    const string &ticket() const
    {
      return ticket_;
    }

    const std::unordered_map<string, std::vector<string>> &user_supplied_headers() const
    {
      return user_supplied_headers_;
    }

    void add_header(const string &key, const string &value)
    {
      user_supplied_headers_[key].push_back({value});
    }

  private:
    const string server_location_;
    string auth_token_;
    string secret_name_;
    // Override the ticket supplied from GetFlightInfo.
    // this is supplied via a named parameter.
    string ticket_;
    std::unordered_map<string, std::vector<string>> user_supplied_headers_;
  };

  struct AirportArrowScanGlobalState;

  struct AirportArrowScanLocalState : public ArrowScanLocalState
  {
  public:
    explicit AirportArrowScanLocalState(unique_ptr<ArrowArrayWrapper> current_chunk,
                                        ClientContext &context,
                                        std::shared_ptr<arrow::flight::FlightStreamReader> reader,
                                        TableFunctionInitInput &input)
        : ArrowScanLocalState(std::move(current_chunk), context),
          reader_(std::move(reader)),
          input_(input)
    {
      D_ASSERT(reader != nullptr);
    }

    explicit AirportArrowScanLocalState(unique_ptr<ArrowArrayWrapper> current_chunk,
                                        ClientContext &context,
                                        TableFunctionInitInput &input)
        : ArrowScanLocalState(std::move(current_chunk), context),
          reader_(std::shared_ptr<arrow::flight::FlightStreamReader>(nullptr)), input_(input)
    {
    }

    const shared_ptr<ArrowArrayStreamWrapper> &stream() const
    {
      D_ASSERT(stream_ != nullptr);
      return stream_;
    }

    const std::variant<
        std::shared_ptr<arrow::flight::FlightStreamReader>,
        std::shared_ptr<arrow::ipc::RecordBatchStreamReader>,
        std::shared_ptr<arrow::ipc::RecordBatchFileReader>> &
    reader() const
    {
      return reader_;
    }

    void set_reader(std::shared_ptr<arrow::flight::FlightStreamReader> reader)
    {
      D_ASSERT(reader != nullptr);
      reader_ = reader;
    }

    void set_reader(std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader)
    {
      D_ASSERT(reader != nullptr);
      reader_ = reader;
    }

    void set_reader(std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader)
    {
      D_ASSERT(reader != nullptr);
      reader_ = reader;
    }

    void set_stream(shared_ptr<ArrowArrayStreamWrapper> stream)
    {
      D_ASSERT(stream != nullptr);
      stream_ = stream;
    }

    const TableFunctionInitInput &input() const
    {
      return input_;
    }

    bool done = false;

  public:
    idx_t lines_read = 0;

  private:
    std::variant<
        std::shared_ptr<arrow::flight::FlightStreamReader>,
        std::shared_ptr<arrow::ipc::RecordBatchStreamReader>,
        std::shared_ptr<arrow::ipc::RecordBatchFileReader>>
        reader_;

    shared_ptr<ArrowArrayStreamWrapper> stream_;
    const TableFunctionInitInput input_;
  };

  struct AirportTakeFlightBindData : public ArrowScanFunctionData, public AirportLocationDescriptor
  {
  public:
    AirportTakeFlightBindData(
        stream_factory_produce_t scanner_producer_p,
        const string &trace_id,
        const int64_t estimated_records,
        const AirportTakeFlightParameters &take_flight_params_p,
        const std::optional<AirportGetFlightInfoTableFunctionParameters> &table_function_parameters_p,
        std::shared_ptr<arrow::Schema> schema,
        const flight::FlightDescriptor &descriptor,
        shared_ptr<DependencyItem> dependency = nullptr)
        : ArrowScanFunctionData(scanner_producer_p, (uintptr_t)this, std::move(dependency)),
          AirportLocationDescriptor(take_flight_params_p.server_location(), descriptor),
          trace_id_(trace_id), estimated_records_(estimated_records),
          take_flight_params_(take_flight_params_p),
          table_function_parameters_(table_function_parameters_p),
          schema_(schema)

    {
      AIRPORT_ARROW_ASSERT_OK_CONTAINER(
          ExportSchema(*schema, &schema_root.arrow_schema),
          this,
          "ExportSchema");
    }

    //    std::unique_ptr<AirportTakeFlightParameters> take_flight_params = nullptr;

    string json_filters;

    idx_t rowid_column_index = COLUMN_IDENTIFIER_ROW_ID;

    // Force no-result
    // When issuing updates and deletes on tables that cannot produce row ids
    // it sometimes make sense that while the LogicalGet node will exist, this
    // Get shouldn't actually produce any rows.
    //
    // Its assumed that the work will be done in the LogicalUpdate or LogicalDelete
    //
    // This will only be modified by the AirportOptimizer.
    bool skip_producing_result_for_update_or_delete = false;

    const string &trace_id() const
    {
      return trace_id_;
    }

    const int64_t estimated_records() const
    {
      return estimated_records_;
    }

    const AirportTakeFlightParameters &take_flight_params() const
    {
      return take_flight_params_;
    }

    const std::optional<AirportGetFlightInfoTableFunctionParameters> &table_function_parameters() const
    {
      return table_function_parameters_;
    }

    const std::shared_ptr<arrow::Schema> &schema() const
    {
      return schema_;
    }

    void set_endpoint_count(const size_t endpoint_count)
    {
      total_endpoints_ = endpoint_count;
      progress_array = std::unique_ptr<std::atomic<double>[]>(new std::atomic<double>[endpoint_count]);
      for (size_t i = 0; i < endpoint_count; i++)
      {
        progress_array[i].store(0.0);
      }
    }

    atomic<double> *get_progress_counter(const idx_t endpoint_index) const
    {
      if (endpoint_index < total_endpoints_)
      {
        return &progress_array[endpoint_index];
      }
      else
      {
        throw InvalidInputException("airport_take_flight: endpoint index out of range");
      }
    }

    double_t total_progress() const
    {
      double_t total = 0.0;
      if (total_endpoints_ == 0)
      {
        return 0.0;
      }
      for (size_t i = 0; i < total_endpoints_; i++)
      {
        total += progress_array[i].load(std::memory_order_relaxed); // or acquire
      }
      return total / (double_t)total_endpoints_;
    }

    std::shared_ptr<arrow::Buffer> last_app_metadata = nullptr;

  private:
    // The total number of endpoints that will be scanned, this is used
    // in calculating the progress of the scan.
    size_t total_endpoints_ = 0;
    // This is the progress of the scan.
    std::unique_ptr<std::atomic<double>[]> progress_array = nullptr;

    // This is the trace id so that calls to GetFlightInfo and DoGet can be traced.
    const string trace_id_;

    // Store the estimated number of records in the flight, typically this is
    // returned from GetFlightInfo, but that could also come from the table itself.
    int64_t estimated_records_ = -1;

    const AirportTakeFlightParameters take_flight_params_;
    const std::optional<AirportGetFlightInfoTableFunctionParameters> table_function_parameters_;

    const std::shared_ptr<arrow::Schema> schema_;
  };

  duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>
  AirportCreateStream(uintptr_t buffer_ptr, ArrowStreamParameters &parameters);

  class AirportArrowArrayStreamWrapper : public duckdb::ArrowArrayStreamWrapper, public AirportLocationDescriptor
  {
  public:
    explicit AirportArrowArrayStreamWrapper(const AirportLocationDescriptor &location_descriptor) : AirportLocationDescriptor(location_descriptor) {}

    shared_ptr<ArrowArrayWrapper> GetNextChunk();
  };

} // namespace duckdb
