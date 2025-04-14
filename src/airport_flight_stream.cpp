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
#include "airport_location_descriptor.hpp"

/// File copied from
/// https://github.com/duckdb/duckdb-wasm/blob/0ad10e7db4ef4025f5f4120be37addc4ebe29618/lib/src/arrow_stream_buffer.cc

namespace duckdb
{

  struct AirportScannerProgress
  {
    double progress;

    MSGPACK_DEFINE_MAP(progress)
  };

  class FlightMetadataRecordBatchReaderAdapter : public arrow::RecordBatchReader, public AirportLocationDescriptor
  {
  public:
    explicit FlightMetadataRecordBatchReaderAdapter(
        const AirportLocationDescriptor &location_descriptor,
        atomic<double> *progress,
        std::shared_ptr<arrow::Buffer> *last_app_metadata,
        const std::shared_ptr<arrow::Schema> &schema,
        std::shared_ptr<flight::FlightStreamReader> delegate)
        : AirportLocationDescriptor(location_descriptor),
          schema_(std::move(schema)),
          delegate_(std::move(delegate)),
          progress_(progress),
          last_app_metadata_(last_app_metadata)
    {
      D_ASSERT(delegate_ != nullptr);
    }

    std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override
    {
      while (true)
      {
        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(flight::FlightStreamChunk chunk,
                                                 delegate_->Next(),
                                                 this,
                                                 "");
        if (chunk.app_metadata)
        {
          // Handle app metadata if needed

          if (last_app_metadata_)
          {
            *last_app_metadata_ = chunk.app_metadata;
          }

          // This could be changed later on to be more generic.
          // especially since this wrapper will be used by more values.
          if (progress_)
          {
            AIRPORT_MSGPACK_UNPACK_CONTAINER(AirportScannerProgress, progress_report, (*chunk.app_metadata), this, "File to parse msgpack encoded object progress message");
            progress_->store(progress_report.progress, std::memory_order_relaxed);
          }
        }
        if (!chunk.data && !chunk.app_metadata)
        {
          // EOS
          *batch = nullptr;
          return arrow::Status::OK();
        }
        else if (chunk.data)
        {
          AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(
              auto aligned_chunk,
              arrow::util::EnsureAlignment(chunk.data, 8, arrow::default_memory_pool()),
              this,
              "EnsureRecordBatchAlignment");

          *batch = aligned_chunk;

          return arrow::Status::OK();
        }
      }
    }

  private:
    const std::shared_ptr<arrow::Schema> schema_;
    const std::shared_ptr<flight::FlightStreamReader> delegate_;

    atomic<double> *progress_;
    std::shared_ptr<arrow::Buffer> *last_app_metadata_;
  };

  /// Arrow array stream factory function
  duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>
  AirportCreateStream(uintptr_t buffer_ptr,
                      ArrowStreamParameters &parameters)
  {
    assert(buffer_ptr != 0);

    auto local_state = reinterpret_cast<const AirportArrowScanLocalState *>(buffer_ptr);
    auto airport_parameters = reinterpret_cast<AirportArrowStreamParameters *>(&parameters);

    // This needs to pull the data off of the local state.
    auto reader = std::make_shared<FlightMetadataRecordBatchReaderAdapter>(
        *airport_parameters,
        airport_parameters->progress,
        airport_parameters->last_app_metadata,
        airport_parameters->schema(),
        local_state->reader());

    // Create arrow stream
    //    auto stream_wrapper = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
    auto stream_wrapper = duckdb::make_uniq<AirportArrowArrayStreamWrapper>(*airport_parameters);
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

    return stream_wrapper;
  }

  shared_ptr<ArrowArrayWrapper> AirportArrowArrayStreamWrapper::GetNextChunk()
  {
    auto current_chunk = make_shared_ptr<ArrowArrayWrapper>();
    if (arrow_array_stream.get_next(&arrow_array_stream, &current_chunk->arrow_array))
    { // LCOV_EXCL_START
      throw AirportFlightException(this->server_location(), this->descriptor(), string(GetError()), "");
    } // LCOV_EXCL_STOP

    return current_chunk;
  }

  AirportTakeFlightParameters::AirportTakeFlightParameters(
      const string &server_location,
      ClientContext &context) : server_location_(server_location)
  {
    D_ASSERT(!server_location_.empty());
    auth_token_ = AirportAuthTokenForLocation(context, server_location_, secret_name_, auth_token_);
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
