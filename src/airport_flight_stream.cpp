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
#include <arrow/filesystem/api.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/align_util.h>
#include <arrow/util/uri.h>
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
      : AirportLocalScanData({uri}, {}, context, func, expected_return_types, expected_return_names, init_input)
  {
  }

  AirportDuckDBFunctionCallParsed AirportParseFunctionCallDetails(
      const AirportDuckDBFunctionCall &function_call_data,
      ClientContext &context,
      const AirportTakeFlightBindData &bind_data)
  {
    auto &instance = DatabaseInstance::GetDatabase(context);

    // This should raise an error if the function is unknown.
    auto &function_entry = ExtensionUtil::GetTableFunction(instance, function_call_data.function_name);

    // Now we need to deserialize the Arrow table that was serialized.
    // in IPC format and convert it to a DuckDB DataChunk so we can
    // get the values to pass to the function as arguments and named parameters.

    auto buffer_reader = std::make_shared<arrow::io::BufferReader>(function_call_data.data);

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(
        auto arg_reader,
        arrow::ipc::RecordBatchStreamReader::Open(buffer_reader),
        (&bind_data),
        "airport_take_flight: failed to read DuckDB function call arguments arrow table");

    // Now we need to get the schema from the reader.
    auto const arg_schema = arg_reader->schema();

    // All of the arguments are named arg_0, arg_1, we need to get a list of logical types
    // for all of the arguments.

    // Export the schema to the C api.
    ArrowSchemaWrapper schema_root;

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        ExportSchema(*arg_schema, &schema_root.arrow_schema),
        (&bind_data),
        "ExportSchema");

    auto &config = DBConfig::GetConfig(context);

    // Store all of the types of the arguments.
    vector<LogicalType> arg_types;
    vector<idx_t> arg_indexes;

    vector<LogicalType> all_types;

    vector<std::string> parameter_names;
    vector<idx_t> named_parameter_indexes;

    ArrowTableType arrow_table;

    for (int arg_index = 0;; ++arg_index)
    {
      const std::string column_name = "arg_" + std::to_string(arg_index);
      int col_index = arg_schema->GetFieldIndex(column_name);

      if (col_index == -1)
      {
        break; // No more argument columns
      }

      auto &schema_item = *schema_root.arrow_schema.children[col_index];
      auto arrow_type = ArrowType::GetArrowLogicalType(config, schema_item);

      if (schema_item.dictionary)
      {
        auto dict_type = ArrowType::GetArrowLogicalType(config, *schema_item.dictionary);
        arrow_type->SetDictionary(std::move(dict_type));
      }

      arg_types.push_back(arrow_type->GetDuckType());
      arg_indexes.push_back(col_index);
    }

    for (idx_t col_index = 0; col_index < schema_root.arrow_schema.n_children; col_index++)
    {
      auto &schema_item = *schema_root.arrow_schema.children[col_index];

      auto arrow_type = ArrowType::GetArrowLogicalType(config, schema_item);

      if (schema_item.dictionary)
      {
        auto dictionary_type = ArrowType::GetArrowLogicalType(config, *schema_item.dictionary);
        arrow_type->SetDictionary(std::move(dictionary_type));
      }

      all_types.push_back(arrow_type->GetDuckType());
      arrow_table.AddColumn(col_index, std::move(arrow_type));

      // Skip things that are named parameters.
      if (memcmp(schema_item.name, "arg_", 4) == 0)
      {
        continue;
      }

      parameter_names.push_back(string(schema_item.name));
      named_parameter_indexes.push_back(col_index);
    }

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(
        std::shared_ptr<arrow::RecordBatch> batch,
        arg_reader->Next(),
        (&bind_data),
        "Failed to read batch from DuckDB function call arguments arrow table")

    ArrowSchema c_schema;

    auto current_chunk = make_uniq<ArrowArrayWrapper>();

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        arrow::ExportRecordBatch(*batch, &current_chunk->arrow_array, &c_schema),
        (&bind_data),
        "Failed to export record batch from DuckDB function call arguments arrow table");

    // Extract the values.
    DataChunk args_and_parameters_chunk;
    args_and_parameters_chunk.Initialize(Allocator::Get(context),
                                         all_types,
                                         current_chunk->arrow_array.length);

    args_and_parameters_chunk.SetCardinality(current_chunk->arrow_array.length);

    D_ASSERT(current_chunk->arrow_array.length == 1);

    ArrowScanLocalState fake_local_state(
        std::move(current_chunk),
        context);

    ArrowTableFunction::ArrowToDuckDB(fake_local_state,
                                      arrow_table.GetColumns(),
                                      args_and_parameters_chunk,
                                      0,
                                      false);

    args_and_parameters_chunk.Verify();

    // Try to find the function with the arguments.
    auto scan_function = function_entry.functions.GetFunctionByArguments(context, arg_types);

    // Now get the values and build up the named parameters.
    vector<Value> argument_values;
    for (const auto &col_idx : arg_indexes)
    {
      auto &column = args_and_parameters_chunk.data[col_idx];
      argument_values.push_back(column.GetValue(0));
    }

    // Now we need to get the named parameters.
    named_parameter_map_t named_params;
    for (idx_t parameter_idx = 0; parameter_idx < named_parameter_indexes.size(); parameter_idx++)
    {
      named_params[parameter_names[parameter_idx]] = args_and_parameters_chunk.data[named_parameter_indexes[parameter_idx]].GetValue(0);
    }

    AirportDuckDBFunctionCallParsed result(
        argument_values,
        named_params,
        scan_function);
    return result;
  }

  AirportLocalScanData::AirportLocalScanData(
      vector<Value> argument_values,
      named_parameter_map_t named_params,
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
    vector<LogicalType> input_types;
    vector<string> input_names;

    TableFunctionRef empty;
    TableFunction dummy_table_function;
    dummy_table_function.name = "AirportEndpointScan";
    TableFunctionBindInput bind_input(
        argument_values,
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
