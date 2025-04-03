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

  // This is the structure that is passed to the function that can create the stream.
  struct AirportTakeFlightScanData : public AirportLocationDescriptor
  {
  public:
    AirportTakeFlightScanData(
        const AirportLocationDescriptor &location_descriptor,
        const std::shared_ptr<arrow::Schema> &schema,
        std::shared_ptr<flight::FlightStreamReader> stream) : AirportLocationDescriptor(location_descriptor),
                                                              schema_(schema),
                                                              stream_(stream)
    {
    }

    const std::shared_ptr<arrow::Schema> &schema() const
    {
      return schema_;
    }

    const std::shared_ptr<arrow::flight::FlightStreamReader> stream() const
    {
      return stream_;
    }

    void setStream(std::shared_ptr<arrow::flight::FlightStreamReader> stream)
    {
      stream_ = stream;
    }

    atomic<double> progress_;
    string last_app_metadata_;

  private:
    const std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<arrow::flight::FlightStreamReader> stream_;
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

  struct AirportTakeFlightBindData : public ArrowScanFunctionData, public AirportLocationDescriptor
  {
  public:
    AirportTakeFlightBindData(stream_factory_produce_t scanner_producer_p, uintptr_t stream_factory_ptr_p,
                              const string &trace_id,
                              const int64_t estimated_records,
                              const AirportTakeFlightParameters &take_flight_params_p,
                              const std::optional<AirportGetFlightInfoTableFunctionParameters> &table_function_parameters_p,
                              std::shared_ptr<arrow::Schema> schema,
                              const flight::FlightDescriptor &descriptor,
                              std::unique_ptr<AirportTakeFlightScanData> scan_data_p,
                              shared_ptr<DependencyItem> dependency = nullptr) : ArrowScanFunctionData(scanner_producer_p, stream_factory_ptr_p, std::move(dependency)),
                                                                                 AirportLocationDescriptor(take_flight_params_p.server_location(), descriptor),
                                                                                 trace_id_(trace_id), estimated_records_(estimated_records),
                                                                                 take_flight_params_(take_flight_params_p),
                                                                                 table_function_parameters_(table_function_parameters_p),
                                                                                 scan_data_(std::move(scan_data_p)),
                                                                                 schema_(schema)

    {
      AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
          ExportSchema(*schema, &schema_root.arrow_schema),
          take_flight_params_.server_location(),
          descriptor,
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

    const std::unique_ptr<AirportTakeFlightScanData> &scan_data() const
    {
      return scan_data_;
    }

    const std::shared_ptr<arrow::Schema> &schema() const
    {
      return schema_;
    }

    void examine_schema(
        ClientContext &context,
        vector<LogicalType> &return_types,
        vector<string> &names);

  private:
    // This is the trace id so that calls to GetFlightInfo and DoGet can be traced.
    const string trace_id_;

    // Store the estimated number of records in the flight, typically this is
    // returned from GetFlightInfo, but that could also come from the table itself.
    int64_t estimated_records_ = -1;

    const AirportTakeFlightParameters take_flight_params_;
    const std::optional<AirportGetFlightInfoTableFunctionParameters> table_function_parameters_;

    std::unique_ptr<AirportTakeFlightScanData> scan_data_;
    const std::shared_ptr<arrow::Schema> schema_;
  };

  duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>
  AirportCreateStream(uintptr_t buffer_ptr, ArrowStreamParameters &parameters);

  class AirportArrowArrayStreamWrapper : public duckdb::ArrowArrayStreamWrapper, public AirportLocationDescriptor
  {
  public:
    AirportArrowArrayStreamWrapper(const AirportLocationDescriptor &location_descriptor) : AirportLocationDescriptor(location_descriptor) {}

    shared_ptr<ArrowArrayWrapper> GetNextChunk();
  };

} // namespace duckdb
