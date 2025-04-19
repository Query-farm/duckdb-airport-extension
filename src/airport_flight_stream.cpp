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

  AirportLocalScanData::AirportLocalScanData(std::string uri,
                                             ClientContext &context,
                                             TableFunction &func,
                                             const vector<LogicalType> &expected_return_types,
                                             const vector<string> &expected_return_names,
                                             const TableFunctionInitInput &init_input)
      : table_function(func),
        thread_context(context),
        execution_context(context, thread_context, nullptr),
        finished_chunk(false)
  {
    vector<Value> children;
    children.reserve(1);
    children.push_back(Value(uri));
    named_parameter_map_t named_params;
    vector<LogicalType> input_types;
    vector<string> input_names;

    TableFunctionRef empty;
    TableFunction dummy_table_function;
    dummy_table_function.name = "AirportEndpointParquetScan";
    TableFunctionBindInput bind_input(
        children,
        named_params,
        input_types,
        input_names,
        nullptr,
        nullptr,
        dummy_table_function,
        empty);

    bind_data = func.bind(context,
                          bind_input,
                          return_types,
                          return_names);

    // printf("Parquet names: %s\n", StringUtil::Join(return_names, ", ").c_str());
    // printf("Parquet types: %s\n", StringUtil::Join(return_types, return_types.size(), ", ", [](const LogicalType &type)
    //                                                { return type.ToString(); })
    //                                   .c_str());

    // auto virtual_columns = func.get_virtual_columns(context, bind_data);
    // for (auto &virtual_column : virtual_columns)
    // {
    //   printf("Got virtual column %d name: %s type: %s\n", virtual_column.first, virtual_column.second.name.c_str(), virtual_column.second.type.ToString().c_str());
    // }

    D_ASSERT(return_names.size() == return_types.size());
    std::unordered_map<std::string, std::pair<size_t, LogicalType>> scan_index_map;
    for (size_t i = 0; i < return_names.size(); i++)
    {
      scan_index_map[return_names[i]] = {i, return_types[i]};
    }

    // Reuse the first init input, but override the bind data, that way the predicate
    // pushdown is handled.
    TableFunctionInitInput input(init_input);

    vector<column_t> mapped_column_ids;
    vector<ColumnIndex> mapped_column_indexes;
    for (auto &column_id : input.column_ids)
    {
      if (column_id == COLUMN_IDENTIFIER_ROW_ID || column_id == COLUMN_IDENTIFIER_EMPTY)
      {
        mapped_column_ids.push_back(column_id);
        mapped_column_indexes.emplace_back(column_id);
        continue;
      }

      auto &referenced_column_name = expected_return_names[column_id];
      auto &referenced_column_type = expected_return_types[column_id];

      auto it = scan_index_map.find(referenced_column_name);

      if (it == scan_index_map.end())
      {
        throw BinderException("Airport : The column name " + referenced_column_name + " does not exist in the Arrow Flight schema.  Found column names: " +
                              StringUtil::Join(return_names, ", ") + " expected column names: " +
                              StringUtil::Join(expected_return_names, ", "));
      }

      auto &found_column_index = it->second.first;
      auto &found_column_type = it->second.second;

      if (found_column_type != referenced_column_type)
      {
        throw BinderException("Airport: The data type for column " + referenced_column_name + " does not match the expected data type in the Arrow Flight schema. " +
                              "Found data type: " +
                              found_column_type.ToString() +
                              " expected data type: " +
                              referenced_column_type.ToString());
      }

      mapped_column_ids.push_back(found_column_index);
      mapped_column_indexes.emplace_back(found_column_index);
    }

    input.column_ids = mapped_column_ids;
    input.column_indexes = mapped_column_indexes;

    // printf("Binding data for parquet read\n");
    // printf("Column ids: %s\n", StringUtil::Join(input.column_ids, input.column_ids.size(), ", ",
    //                                             [](const column_t &id)
    //                                             { return std::to_string(id); }

    //                                             )
    //                                .c_str());
    // printf("Column indexes : %s\n", StringUtil::Join(input.column_indexes, input.column_indexes.size(), ", ",
    //                                                  [](const ColumnIndex &type)
    //                                                  { return std::to_string(type.GetPrimaryIndex()); })
    //                                     .c_str());
    // printf("Projection ids: %s\n", StringUtil::Join(input.projection_ids, input.projection_ids.size(), ", ",
    //                                                 [](const idx_t &id)
    //                                                 { return std::to_string(id); })
    //                                    .c_str());
    input.bind_data = bind_data.get();

    global_state = func.init_global(context, input);

    local_state = func.init_local(execution_context, input, global_state.get());
  }

  struct AirportScannerProgress
  {
    double progress;

    MSGPACK_DEFINE_MAP(progress)
  };

  class FlightMetadataRecordBatchReaderAdapter : public arrow::RecordBatchReader, public AirportLocationDescriptor
  {
  public:
    using ReaderDelegate = std::variant<
        std::shared_ptr<arrow::flight::FlightStreamReader>,
        std::shared_ptr<arrow::ipc::RecordBatchStreamReader>,
        std::shared_ptr<arrow::ipc::RecordBatchFileReader>,
        std::shared_ptr<AirportLocalScanData>>;

    explicit FlightMetadataRecordBatchReaderAdapter(
        const AirportLocationDescriptor &location_descriptor,
        atomic<double> *progress,
        std::shared_ptr<arrow::Buffer> *last_app_metadata,
        const std::shared_ptr<arrow::Schema> &schema,
        ReaderDelegate delegate)
        : AirportLocationDescriptor(location_descriptor),
          schema_(std::move(schema)),
          delegate_(std::move(delegate)),
          progress_(progress),
          last_app_metadata_(last_app_metadata),
          batch_index_(0)
    {
    }

    std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override
    {
      const auto using_flight = std::holds_alternative<std::shared_ptr<arrow::flight::FlightStreamReader>>(delegate_);
      const auto using_ipc_stream = std::holds_alternative<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>>(delegate_);
      const auto using_ipc_file = std::holds_alternative<std::shared_ptr<arrow::ipc::RecordBatchFileReader>>(delegate_);

      while (true)
      {

        if (using_ipc_stream)
        {
          auto stream_reader = std::get<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>>(delegate_);
          AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(auto batch_result, stream_reader->Next(), this, "ReadNext");
          if (batch_result)
          {
            AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(
                auto aligned_chunk,
                arrow::util::EnsureAlignment(batch_result, 8, arrow::default_memory_pool()),
                this,
                "EnsureRecordBatchAlignment");

            *batch = aligned_chunk;

            return arrow::Status::OK();
          }
          else
          {
            // EOS
            *batch = nullptr;
            return arrow::Status::OK();
          }
        }
        else if (using_ipc_file)
        {
          auto stream_reader = std::get<std::shared_ptr<arrow::ipc::RecordBatchFileReader>>(delegate_);
          AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(auto batch_result, stream_reader->ReadRecordBatch(batch_index_++), this, "ReadNext");
          if (batch_result)
          {
            AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(
                auto aligned_chunk,
                arrow::util::EnsureAlignment(batch_result, 8, arrow::default_memory_pool()),
                this,
                "EnsureRecordBatchAlignment");

            *batch = aligned_chunk;

            return arrow::Status::OK();
          }
          else
          {
            // EOS
            *batch = nullptr;
            return arrow::Status::OK();
          }
        }
        else if (using_flight)
        {
          auto flight_delegate = std::get<std::shared_ptr<arrow::flight::FlightStreamReader>>(delegate_);
          AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(flight::FlightStreamChunk chunk,
                                                   flight_delegate->Next(),
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
    }

  private:
    const std::shared_ptr<arrow::Schema> schema_;

    const ReaderDelegate delegate_;

    atomic<double> *progress_;
    std::shared_ptr<arrow::Buffer> *last_app_metadata_;

    size_t batch_index_;
  };

  /// Arrow array stream factory function
  duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>
  AirportCreateStream(uintptr_t buffer_ptr,
                      ArrowStreamParameters &parameters)
  {
    assert(buffer_ptr != 0);

    auto local_state = reinterpret_cast<const AirportArrowScanLocalState *>(buffer_ptr);
    auto airport_parameters = reinterpret_cast<AirportArrowStreamParameters *>(&parameters);

    // Depending on type type of the reader of the local state, lets change
    // the type of reader created here.

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
