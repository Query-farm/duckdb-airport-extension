#include "airport_flight_stream.hpp"
#include "airport_macros.hpp"
#include "airport_flight_exception.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension_util.hpp"
#include <arrow/c/bridge.h>

#include <arrow/flight/client.h>
#include <arrow/flight/types.h>

#include <iostream>
#include <memory>
#include <arrow/buffer.h>
#include <arrow/util/align_util.h>
#include "msgpack.hpp"
#include "airport_secrets.hpp"

/// File copied from
/// https://github.com/duckdb/duckdb-wasm/blob/0ad10e7db4ef4025f5f4120be37addc4ebe29618/lib/src/arrow_stream_buffer.cc

namespace duckdb
{

  struct AirportScannerProgress
  {
    double progress;

    MSGPACK_DEFINE_MAP(progress)
  };

  class FlightMetadataRecordBatchReaderAdapter : public arrow::RecordBatchReader
  {
  public:
    explicit FlightMetadataRecordBatchReaderAdapter(
        const string &flight_server_location,
        const flight::FlightDescriptor &flight_descriptor,
        atomic<double> *progress,
        string *last_app_metadata,
        std::shared_ptr<arrow::Schema> schema,
        std::shared_ptr<flight::MetadataRecordBatchReader> delegate)
        : flight_server_location_(flight_server_location),
          flight_descriptor_(flight_descriptor),
          schema_(std::move(schema)),
          delegate_(std::move(delegate)),
          progress_(progress),
          last_app_metadata_(last_app_metadata) {}
    std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override
    {
      while (true)
      {
        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(flight::FlightStreamChunk next, delegate_->Next(), flight_server_location_, flight_descriptor_, "");
        if (next.app_metadata)
        {
          // Handle app metadata if needed

          if (last_app_metadata_)
          {
            *last_app_metadata_ = std::string((const char *)next.app_metadata->data(), next.app_metadata->size());
          }

          // This could be changed later on to be more generic.
          // especially since this wrapper will be used by more values.
          if (progress_)
          {
            AIRPORT_MSGPACK_UNPACK(AirportScannerProgress, progress_report, (*next.app_metadata), flight_server_location_, "File to parse msgpack encoded object progress message");
            *progress_ = progress_report.progress; // Update the progress
          }
        }
        if (!next.data && !next.app_metadata)
        {
          // EOS
          *batch = nullptr;
          return arrow::Status::OK();
        }
        else if (next.data)
        {
          *batch = std::move(next.data);
          return arrow::Status::OK();
        }
      }
    }

  private:
    const string flight_server_location_;
    flight::FlightDescriptor flight_descriptor_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<flight::MetadataRecordBatchReader> delegate_;
    atomic<double> *progress_;
    string *last_app_metadata_;
  };

  static arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> FlightMakeRecordBatchReader(
      std::shared_ptr<flight::MetadataRecordBatchReader> reader,
      const string &flight_server_location,
      const flight::FlightDescriptor &flight_descriptor,
      atomic<double> *progress,
      string *last_app_metadata)
  {
    ARROW_ASSIGN_OR_RAISE(auto schema, reader->GetSchema());
    return std::make_shared<FlightMetadataRecordBatchReaderAdapter>(
        flight_server_location,
        flight_descriptor,
        progress,
        last_app_metadata,
        std::move(schema),
        std::move(reader));
  }

  /// Constructor
  AirportFlightStreamReader::AirportFlightStreamReader(
      const string &flight_server_location,
      std::shared_ptr<flight::FlightInfo> flight_info,
      std::shared_ptr<flight::FlightStreamReader> flight_stream)
      : flight_server_location_(flight_server_location), flight_info_(flight_info), flight_stream_(flight_stream) {}

  /// Get the schema
  std::shared_ptr<arrow::Schema> AirportFlightStreamReader::schema() const
  {
    std::shared_ptr<arrow::Schema> info_schema;
    arrow::ipc::DictionaryMemo dictionary_memo;
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(info_schema, flight_info_->GetSchema(&dictionary_memo), flight_server_location_, flight_info_->descriptor(), "");
    return info_schema;
  }

  /// Read the next record batch in the stream. Return null for batch when
  /// reaching end of stream
  arrow::Status AirportFlightStreamReader::ReadNext(
      std::shared_ptr<arrow::RecordBatch> *batch)
  {
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto chunk, flight_stream_.get()->Next(), flight_server_location_, flight_info_->descriptor(), "");
    if (!chunk.data)
    {
      // End of the stream has been reached.
      *batch = nullptr;
      return arrow::Status::OK();
    }

    // DuckDB expects the RecordBatch to be aligned at a pointer offset
    // ensure that this happens.
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        auto aligned_chunk,
        arrow::util::EnsureAlignment(chunk.data, 8, arrow::default_memory_pool()),
        flight_server_location_,
        flight_info_->descriptor(),
        "EnsureRecordBatchAlignment");

    *batch = aligned_chunk;

    return arrow::Status::OK();
  }

  /// Arrow array stream factory function
  duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>
  AirportFlightStreamReader::CreateStream(uintptr_t buffer_ptr,
                                          ArrowStreamParameters &parameters)
  {
    assert(buffer_ptr != 0);

    auto buffer_data = reinterpret_cast<AirportTakeFlightScanData *>(buffer_ptr);

    // We're playing a trick here to recast the FlightStreamReader as a RecordBatchReader,
    // I'm not sure how else to do this.

    // If this doesn't work I can re-implement the ArrowArrayStreamWrapper
    // to take a FlightStreamReader instead of a RecordBatchReader.

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        auto reader,
        FlightMakeRecordBatchReader(
            buffer_data->stream(),
            buffer_data->server_location(),
            buffer_data->flight_descriptor(),
            &buffer_data->progress_,
            &buffer_data->last_app_metadata_),
        buffer_data->server_location(),
        buffer_data->flight_descriptor(),
        "");

    // Create arrow stream
    //    auto stream_wrapper = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
    auto stream_wrapper = duckdb::make_uniq<AirportArrowArrayStreamWrapper>(buffer_data->server_location(), buffer_data->flight_descriptor());
    stream_wrapper->arrow_array_stream.release = nullptr;

    auto maybe_ok = arrow::ExportRecordBatchReader(
        reader, &stream_wrapper->arrow_array_stream);

    if (!maybe_ok.ok())
    {
      if (stream_wrapper->arrow_array_stream.release)
      {
        stream_wrapper->arrow_array_stream.release(
            &stream_wrapper->arrow_array_stream);
      }
      return nullptr;
    }

    return std::move(stream_wrapper);
  }

  void AirportFlightStreamReader::GetSchema(uintptr_t buffer_ptr,
                                            duckdb::ArrowSchemaWrapper &schema)
  {
    assert(buffer_ptr != 0);
    // Rusty: this cast needs to be checked to make sure its valid.
    auto reader = reinterpret_cast<std::shared_ptr<AirportTakeFlightScanData> *>(buffer_ptr);

    arrow::ipc::DictionaryMemo dictionary_memo;
    const auto actual_reader = reader->get();

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(ExportSchema(*actual_reader->schema(), &schema.arrow_schema), actual_reader->server_location(), actual_reader->flight_descriptor(), "ExportSchema");
  }

  shared_ptr<ArrowArrayWrapper> AirportArrowArrayStreamWrapper::GetNextChunk()
  {
    auto current_chunk = make_shared_ptr<ArrowArrayWrapper>();
    if (arrow_array_stream.get_next(&arrow_array_stream, &current_chunk->arrow_array))
    { // LCOV_EXCL_START
      throw AirportFlightException(flight_server_location_, flight_descriptor_, string(GetError()), "");
    } // LCOV_EXCL_STOP

    return current_chunk;
  }

  AirportTakeFlightParameters::AirportTakeFlightParameters(
      const string &server_location,
      ClientContext &context,
      TableFunctionBindInput &input) : server_location_(server_location)
  {
    D_ASSERT(!server_location_.empty());

    for (auto &kv : input.named_parameters)
    {
      auto loption = StringUtil::Lower(kv.first);
      if (loption == "auth_token")
      {
        auth_token_ = StringValue::Get(kv.second);
      }
      else if (loption == "secret")
      {
        secret_name_ = StringValue::Get(kv.second);
      }
      else if (loption == "ticket")
      {
        ticket_ = StringValue::Get(kv.second);
      }
      else if (loption == "headers")
      {
        // Now we need to parse out the map contents.
        auto &children = duckdb::MapValue::GetChildren(kv.second);

        for (auto &value_pair : children)
        {
          auto &child_struct = duckdb::StructValue::GetChildren(value_pair);
          auto key = StringValue::Get(child_struct[0]);
          auto value = StringValue::Get(child_struct[1]);

          user_supplied_headers_[key].push_back(value);
        }
      }
    }

    auth_token_ = AirportAuthTokenForLocation(context, server_location_, secret_name_, auth_token_);
  }

} // namespace duckdb
