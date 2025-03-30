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

namespace flight = arrow::flight;

/// File copied from
/// https://github.com/duckdb/duckdb-wasm/blob/0ad10e7db4ef4025f5f4120be37addc4ebe29618/lib/include/duckdb/web/arrow_stream_buffer.h
namespace duckdb
{

  // This is the structure that is passed to the function that can create the stream.
  struct AirportTakeFlightScanData
  {
  public:
    AirportTakeFlightScanData(
        const string &flight_server_location,
        const flight::FlightDescriptor &flight_descriptor,
        std::shared_ptr<const arrow::Schema> schema,
        std::shared_ptr<flight::FlightStreamReader> stream) : progress_(0),
                                                              flight_server_location_(flight_server_location),
                                                              flight_descriptor_(flight_descriptor),
                                                              schema_(schema),
                                                              stream_(stream)
    {
    }

    const std::string &server_location() const
    {
      return flight_server_location_;
    }

    const flight::FlightDescriptor &flight_descriptor() const
    {
      return flight_descriptor_;
    }

    const std::shared_ptr<const arrow::Schema> schema() const
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
    string flight_server_location_;
    flight::FlightDescriptor flight_descriptor_;
    std::shared_ptr<const arrow::Schema> schema_;
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
    string server_location_;
    string auth_token_;
    string secret_name_;
    // Override the ticket supplied from GetFlightInfo.
    // this is supplied via a named parameter.
    string ticket_;
    std::unordered_map<string, std::vector<string>> user_supplied_headers_;
  };

  struct AirportTakeFlightBindData : public ArrowScanFunctionData
  {
  public:
    using ArrowScanFunctionData::ArrowScanFunctionData;
    std::unique_ptr<AirportTakeFlightScanData> scan_data = nullptr;
    std::shared_ptr<arrow::flight::FlightClient> flight_client = nullptr;

    std::unique_ptr<AirportTakeFlightParameters> take_flight_params = nullptr;

    // This is the location of the flight server.
    // string server_location;

    // This is the auth token.
    // string auth_token;

    // unordered_map<string, std::vector<string>> user_supplied_headers;
    //  This is the token to use for the flight as supplied by the user.
    //  if its empty use the token from the server.
    // string ticket;

    string json_filters;

    // This is the trace id so that calls to GetFlightInfo and DoGet can be traced.
    string trace_id;

    idx_t rowid_column_index = COLUMN_IDENTIFIER_ROW_ID;

    // Force no-result
    // When issuing updates and deletes on tables that cannot produce row ids
    // it sometimes make sense that while the LogicalGet node will exist, this
    // Get shouldn't actually produce any rows.
    //
    // Its assumed that the work will be done in the LogicalUpdate or LogicalDelete
    bool skip_producing_result_for_update_or_delete = false;

    // When doing a dynamic table function we need this.
    std::shared_ptr<const AirportGetFlightInfoTableFunctionParameters> table_function_parameters;

    // Store the estimated number of records in the flight, typically this is
    // returned from GetFlightInfo, but that could also come from the table itself.
    int64_t estimated_records = -1;
  };

  struct AirportFlightStreamReader : public arrow::RecordBatchReader
  {
  protected:
    /// The buffer
    string flight_server_location_;
    std::shared_ptr<flight::FlightInfo> flight_info_;
    std::shared_ptr<flight::FlightStreamReader> flight_stream_;

  public:
    /// Constructor
    AirportFlightStreamReader(
        const string &flight_server_location,
        std::shared_ptr<flight::FlightInfo> flight_info,
        std::shared_ptr<flight::FlightStreamReader> flight_stream);

    /// Destructor
    ~AirportFlightStreamReader() = default;

    /// Get the schema
    std::shared_ptr<arrow::Schema> schema() const override;

    /// Read the next record batch in the stream. Return null for batch when
    /// reaching end of stream
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override;

    /// Create arrow array stream wrapper
    static duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>

    CreateStream(uintptr_t buffer_ptr, duckdb::ArrowStreamParameters &parameters);

    /// Create arrow array stream wrapper
    static void GetSchema(uintptr_t buffer_ptr, duckdb::ArrowSchemaWrapper &schema);
  };

  class AirportArrowArrayStreamWrapper : public duckdb::ArrowArrayStreamWrapper
  {
  public:
    AirportArrowArrayStreamWrapper(const string &flight_server_location, const flight::FlightDescriptor &flight_descriptor) : flight_server_location_(flight_server_location), flight_descriptor_(flight_descriptor) {}

    shared_ptr<ArrowArrayWrapper> GetNextChunk();

  private:
    string flight_server_location_;
    flight::FlightDescriptor flight_descriptor_;
  };

} // namespace duckdb
