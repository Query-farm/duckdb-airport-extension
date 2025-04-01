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
        std::shared_ptr<arrow::Schema> schema,
        std::shared_ptr<flight::FlightStreamReader> stream) : AirportLocationDescriptor(location_descriptor),
                                                              schema_(schema),
                                                              stream_(stream)
    {
    }

    const std::shared_ptr<arrow::Schema> schema() const
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
    std::shared_ptr<arrow::Schema> schema_;
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

  duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>
  AirportCreateStream(uintptr_t buffer_ptr, ArrowStreamParameters &parameters);

  class AirportArrowArrayStreamWrapper : public duckdb::ArrowArrayStreamWrapper, public AirportLocationDescriptor
  {
  public:
    AirportArrowArrayStreamWrapper(const AirportLocationDescriptor &location_descriptor) : AirportLocationDescriptor(location_descriptor) {}

    shared_ptr<ArrowArrayWrapper> GetNextChunk();
  };

} // namespace duckdb
