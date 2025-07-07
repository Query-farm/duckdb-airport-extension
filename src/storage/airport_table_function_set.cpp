#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include <arrow/buffer.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/util/key_value_metadata.h>
#include <numeric>
#include "airport_flight_stream.hpp"
#include "airport_request_headers.hpp"
#include "airport_macros.hpp"
#include "airport_macros.hpp"
#include "airport_scalar_function.hpp"
#include "airport_secrets.hpp"
#include "airport_take_flight.hpp"
#include "storage/airport_catalog_api.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_curl_pool.hpp"
#include "storage/airport_exchange.hpp"
#include "storage/airport_schema_entry.hpp"
#include "storage/airport_table_set.hpp"
#include "storage/airport_transaction.hpp"
#include "airport_schema_utils.hpp"
#include "storage/airport_alter_parameters.hpp"

namespace duckdb
{

  class AirportDynamicTableFunctionInfo : public TableFunctionInfo
  {
  public:
    std::shared_ptr<AirportAPITableFunction> function;

  public:
    explicit AirportDynamicTableFunctionInfo(const std::shared_ptr<AirportAPITableFunction> function_p)
        : TableFunctionInfo(), function(function_p)
    {
    }

    ~AirportDynamicTableFunctionInfo() override
    {
    }
  };

  // Serialize data to a arrow::Buffer
  static std::shared_ptr<arrow::Buffer> AirportDynamicSerializeParameters(std::shared_ptr<arrow::Schema> input_schema,
                                                                          ClientContext &context,
                                                                          TableFunctionBindInput &input,
                                                                          const AirportLocationDescriptor &location_descriptor)
  {
    ArrowSchemaWrapper schema_root;

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        ExportSchema(*input_schema, &schema_root.arrow_schema),
        &location_descriptor, "ExportSchema");

    vector<string> input_schema_names;
    vector<LogicalType> input_schema_types;
    vector<idx_t> source_indexes;

    const auto column_count = (idx_t)schema_root.arrow_schema.n_children;

    input_schema_names.reserve(column_count);
    input_schema_types.reserve(column_count);
    source_indexes.reserve(column_count);

    auto &config = DBConfig::GetConfig(context);
    int seen_named_parameters = 0;
    vector<LogicalType> input_schema_any_real_types;
    for (idx_t col_idx = 0;
         col_idx < column_count; col_idx++)
    {
      auto &schema_field = *schema_root.arrow_schema.children[col_idx];
      if (!schema_field.release)
      {
        throw InvalidInputException("airport_dynamic_table_bind: released schema passed");
      }
      auto name = AirportNameForField(schema_field.name, col_idx);

      // If we have a table input skip over it.
      bool is_any_type = false;
      if (schema_field.metadata != nullptr)
      {
        auto column_metadata = ArrowSchemaMetadata(schema_field.metadata);

        auto is_table_type = column_metadata.GetOption("is_table_type");
        if (!is_table_type.empty())
        {
          source_indexes.push_back(-1);
          continue;
        }
        auto is_any_type_metadata = column_metadata.GetOption("is_any_type");
        if (!is_any_type_metadata.empty())
        {
          is_any_type = true;
        }

        if (!column_metadata.GetOption("is_named_parameter").empty())
        {
          seen_named_parameters += 1;
        }
      }
      input_schema_names.push_back(name);
      auto arrow_type = ArrowType::GetArrowLogicalType(config, schema_field);
      if (is_any_type)
      {
        auto &found_type = input.inputs[col_idx - seen_named_parameters].type();
        input_schema_types.push_back(found_type);
        input_schema_any_real_types.push_back(found_type);
      }
      else
      {
        input_schema_types.push_back(arrow_type->GetDuckType());
      }
      // Where does this field come from.
      source_indexes.push_back(col_idx);
    }

    // We need to produce a schema that doesn't contain the is_table_type fields.

    auto appender = make_uniq<ArrowAppender>(input_schema_types,
                                             input_schema_types.size(),
                                             context.GetClientProperties(),
                                             ArrowTypeExtensionData::GetExtensionTypes(context, input_schema_types));

    // Now we need to make a DataChunk from the input bind data so that we can calle the appender.
    DataChunk input_chunk;
    input_chunk.Initialize(Allocator::Get(context),
                           input_schema_types,
                           1);
    input_chunk.SetCardinality(1);

    // Now how do we populate the input_chunk with the input data?
    seen_named_parameters = 0;
    for (idx_t col_idx = 0;
         col_idx < column_count; col_idx++)
    {
      auto &schema_child = *schema_root.arrow_schema.children[col_idx];
      if (!schema_child.release)
      {
        throw InvalidInputException("airport_dynamic_table_bind: released schema passed");
      }

      // So if the parameter is named, we'd get that off of the metadata
      // otherwise its positional.
      auto metadata = ArrowSchemaMetadata(schema_child.metadata);

      if (!metadata.GetOption("is_table_type").empty())
      {
        continue;
      }

      if (!metadata.GetOption("is_named_parameter").empty())
      {
        input_chunk.data[col_idx].SetValue(0, input.named_parameters[schema_child.name]);
        seen_named_parameters += 1;
      }
      else
      {
        // Since named parameters aren't passed in inputs, we need to adjust
        // the offset we're looking at.
        auto &input_data = input.inputs[source_indexes[col_idx - seen_named_parameters]];
        input_chunk.data[col_idx].SetValue(0, input_data);
      }
    }

    // Now that we have the appender append some data.
    appender->Append(input_chunk, 0, input_chunk.size(), input_chunk.size());
    ArrowArray arr = appender->Finalize();

    auto client_properties = context.GetClientProperties();

    // Create a new schema from the logical types and names that were used,
    // this will apply the actual types if any of the fields are marked as
    // being ANY types.
    ArrowSchema temp_schema;
    ArrowConverter::ToArrowSchema(&temp_schema, input_schema_types,
                                  input_schema_names, client_properties);

    std::shared_ptr<arrow::Schema> schema_without_table_fields;

    // Export the schema from the C side to the C++ side.
    AIRPORT_ASSIGN_OR_RAISE_CONTAINER(
        schema_without_table_fields,
        arrow::ImportSchema(&temp_schema),
        &location_descriptor,
        "ExportSchema");

    AIRPORT_ASSIGN_OR_RAISE_CONTAINER(
        auto record_batch,
        arrow::ImportRecordBatch(&arr, schema_without_table_fields),
        &location_descriptor, "");

    AIRPORT_ASSIGN_OR_RAISE_CONTAINER(auto buffer_output_stream,
                                      arrow::io::BufferOutputStream::Create(),
                                      &location_descriptor,
                                      "create buffer output stream");

    AIRPORT_ASSIGN_OR_RAISE_CONTAINER(auto writer,
                                      arrow::ipc::MakeStreamWriter(buffer_output_stream, schema_without_table_fields),
                                      &location_descriptor,
                                      "make stream writer");

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(writer->WriteRecordBatch(*record_batch),
                                      &location_descriptor,
                                      "write record batch");

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(writer->Close(),
                                      &location_descriptor,
                                      "close record batch writer");

    AIRPORT_ASSIGN_OR_RAISE_CONTAINER(
        auto buffer,
        buffer_output_stream->Finish(),
        &location_descriptor,
        "finish buffer output stream");

    return buffer;
  }

  static unique_ptr<FunctionData> AirportDynamicTableBind(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names)
  {
    auto function_info = input.info->Cast<AirportDynamicTableFunctionInfo>();

    auto buffer = AirportDynamicSerializeParameters(function_info.function->input_schema(),
                                                    context,
                                                    input,
                                                    *function_info.function);

    // So save the buffer so we can send it to the server to determine
    // the schema of the flight.

    // Then call the DoAction get_dynamic_flight_info with those arguments.
    AirportTableFunctionFlightInfoParameters tf_params;

    AIRPORT_ASSIGN_OR_RAISE_CONTAINER(
        tf_params.descriptor,
        function_info.function->descriptor().SerializeToString(),
        function_info.function,
        "serialize flight descriptor");
    tf_params.parameters = buffer->ToString();

    // If we are doing an table in_out function we need to serialize the schema of the input.

    // So I think we need to build an ArrowConverter something to build a
    if (input.table_function.in_out_function != nullptr)
    {
      ArrowSchema input_table_schema;
      auto client_properties = context.GetClientProperties();

      ArrowConverter::ToArrowSchema(&input_table_schema,
                                    input.input_table_types,
                                    input.input_table_names,
                                    client_properties);

      AIRPORT_ASSIGN_OR_RAISE_CONTAINER(auto table_input_schema,
                                        arrow::ImportSchema(&input_table_schema),
                                        function_info.function,
                                        "");

      AIRPORT_ASSIGN_OR_RAISE_CONTAINER(
          auto serialized_schema,
          arrow::ipc::SerializeSchema(*table_input_schema, arrow::default_memory_pool()),
          function_info.function,
          "");

      std::string serialized_table_in_schema(reinterpret_cast<const char *>(serialized_schema->data()), serialized_schema->size());

      tf_params.table_input_schema = serialized_table_in_schema;
    }

    auto params = AirportTakeFlightParameters(function_info.function->server_location(),
                                              context,
                                              input);

    return AirportTakeFlightBindWithFlightDescriptor(
        params,
        function_info.function->descriptor(),
        context,
        input, return_types, names, nullptr,
        tf_params,
        nullptr);
  }

  struct ArrowSchemaTableFunctionTypes
  {
    vector<LogicalType> all;
    vector<string> all_names;
    vector<LogicalType> positional;
    vector<string> positional_names;
    std::map<std::string, LogicalType> named;
  };

  static ArrowSchemaTableFunctionTypes
  AirportSchemaToLogicalTypesWithNaming(
      ClientContext &context,
      std::shared_ptr<arrow::Schema> schema,
      const AirportLocationDescriptor &location_descriptor)
  {
    ArrowSchemaWrapper schema_root;

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        ExportSchema(*schema, &schema_root.arrow_schema),
        &location_descriptor,
        "ExportSchema");

    ArrowSchemaTableFunctionTypes result;
    auto &config = DBConfig::GetConfig(context);
    const idx_t column_count = (idx_t)schema_root.arrow_schema.n_children;

    result.all_names.reserve(column_count);
    result.all.reserve(column_count);
    result.positional_names.reserve(column_count);
    result.positional.reserve(column_count);

    for (idx_t col_idx = 0;
         col_idx < column_count; col_idx++)
    {
      auto &schema_item = *schema_root.arrow_schema.children[col_idx];
      if (!schema_item.release)
      {
        throw InvalidInputException("AirportSchemaToLogicalTypes: released schema passed");
      }
      auto arrow_type = ArrowType::GetArrowLogicalType(config, schema_item);

      if (schema_item.dictionary)
      {
        auto dictionary_type = ArrowType::GetArrowLogicalType(config, *schema_item.dictionary);
        arrow_type->SetDictionary(std::move(dictionary_type));
      }

      auto metadata = ArrowSchemaMetadata(schema_item.metadata);

      bool is_any_type = false;
      if (!metadata.GetOption("is_any_type").empty())
      {
        is_any_type = true;
      }
      auto actual_type = is_any_type ? LogicalType(LogicalTypeId::ANY) : arrow_type->GetDuckType();

      if (!metadata.GetOption("is_table_type").empty())
      {
        result.all.emplace_back(LogicalType(LogicalTypeId::TABLE));
      }
      else
      {
        result.all.emplace_back(actual_type);
      }

      result.all_names.emplace_back(string(schema_item.name));

      if (!metadata.GetOption("is_named_parameter").empty())
      {
        result.named[schema_item.name] = actual_type;
      }
      else
      {
        if (!metadata.GetOption("is_table_type").empty())
        {
          result.positional.emplace_back(LogicalType(LogicalTypeId::TABLE));
        }
        else
        {
          result.positional.emplace_back(actual_type);
        }
        result.positional_names.push_back(string(schema_item.name));
      }
    }
    return result;
  }

  struct AirportDynamicTableInOutGlobalState : public GlobalTableFunctionState
  {
      std::string trace_uuid;
      arrow::flight::FlightCallOptions call_options;
      std::shared_ptr<arrow::flight::FlightClient> flight_client;
      std::shared_ptr<arrow::Buffer> parameters_buffer;
      std::shared_ptr<arrow::Schema> send_schema;
      std::unique_ptr<AirportArrowScanGlobalState> scan_global_state;
  };

  struct AirportDynamicTableInOutLocalState : public LocalTableFunctionState, public AirportExchangeGlobalState
  {
  };

  struct AirportTableFunctionInOutParameters
  {
    std::string json_filters;
    std::vector<idx_t> column_ids;

    // The parameters to the table function, which should
    // be included in the opaque ticket data returned
    // for each endpoint.
    std::string parameters;

    std::string at_unit;
    std::string at_value;

    MSGPACK_DEFINE_MAP(json_filters, column_ids, parameters, at_unit, at_value)
  };

  static unique_ptr<GlobalTableFunctionState>
  AirportDynamicTableInOutGlobalInit(ClientContext &context,
                                     TableFunctionInitInput &input)
  {
    auto &bind_data = input.bind_data->Cast<AirportTakeFlightBindData>();
    const auto trace_uuid = airport_trace_id();

    arrow::flight::FlightCallOptions call_options;
    airport_add_normal_headers(call_options,
                               bind_data.take_flight_params(),
                               trace_uuid,
                               bind_data.descriptor());

    auto auth_token = AirportAuthTokenForLocation(context, bind_data.server_location(), "", "");

    call_options.headers.emplace_back("airport-operation", "table_function_in_out");

    D_ASSERT(bind_data.table_function_parameters() != std::nullopt);
    auto &table_function_parameters = *bind_data.table_function_parameters();

    // Indicate if the caller is interested in data being returned.
    call_options.headers.emplace_back("return-chunks", "1");

    auto flight_client = AirportAPI::FlightClientForLocation(bind_data.server_location());

    AirportTableFunctionInOutParameters parameters;
    parameters.json_filters = bind_data.json_filters;
    parameters.column_ids = bind_data.column_ids;
    parameters.parameters = table_function_parameters.parameters;
    parameters.at_unit = bind_data.take_flight_params().at_unit();
    parameters.at_value = bind_data.take_flight_params().at_value();

    msgpack::sbuffer parameters_packed_buffer;
    msgpack::pack(parameters_packed_buffer, parameters);

    auto parameters_buffer = arrow::Buffer::FromString(
        std::string(parameters_packed_buffer.data(), parameters_packed_buffer.size()));


    // Thsi is correct, since we are writing to the server,
    // so the writer has the schema of the table_input_schema.
    std::shared_ptr<arrow::Buffer> serialized_schema_buffer = std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t *>(table_function_parameters.table_input_schema.data()),
        table_function_parameters.table_input_schema.size());

    auto buffer_reader = std::make_shared<arrow::io::BufferReader>(serialized_schema_buffer);

    arrow::ipc::DictionaryMemo in_memo;
    AIRPORT_ASSIGN_OR_RAISE_CONTAINER(
        auto send_schema,
        arrow::ipc::ReadSchema(buffer_reader.get(), &in_memo),
        &bind_data, "ReadSchema");

    auto scan_global_state = make_uniq<AirportArrowScanGlobalState>();

    // Retain the global state.
    unique_ptr<AirportDynamicTableInOutGlobalState> global_state = make_uniq<AirportDynamicTableInOutGlobalState>();

    global_state->scan_global_state = std::move(scan_global_state);
    global_state->send_schema = send_schema;
    global_state->call_options = call_options;
    global_state->flight_client = flight_client;
    global_state->parameters_buffer = parameters_buffer;
    global_state->trace_uuid = trace_uuid;

    return global_state;
  }

  static unique_ptr<LocalTableFunctionState>
      AirportDynamicTableInOutLocalInit(ExecutionContext& context,
          TableFunctionInitInput& input, GlobalTableFunctionState* global_state)
  {
      auto& gstate = global_state->Cast<AirportDynamicTableInOutGlobalState>();

      auto& bind_data = input.bind_data->Cast<AirportTakeFlightBindData>();

      AIRPORT_ASSIGN_OR_RAISE_CONTAINER(
          auto exchange_result,
          gstate.flight_client->DoExchange(gstate.call_options, bind_data.descriptor()),
          &bind_data, "AirportDynamicTableInOutGlobalInit DoExchange");

      AIRPORT_ARROW_ASSERT_OK_CONTAINER(
          exchange_result.writer->WriteMetadata(gstate.parameters_buffer),
          &bind_data,
          "airport_dynamic_table_function: write metadata with parameters");

      // Tell the server the schema that we will be using to write data.
      AIRPORT_ARROW_ASSERT_OK_CONTAINER(
          exchange_result.writer->Begin(gstate.send_schema),
          &bind_data,
          "airport_dynamic_table_function: send schema");

      vector<column_t> column_ids;

      // This is the schema from the server, the output.
      AIRPORT_ASSIGN_OR_RAISE_CONTAINER(auto read_schema,
          exchange_result.reader->GetSchema(),
          &bind_data,
          "");

      auto scan_bind_data = make_uniq<AirportExchangeTakeFlightBindData>(
          (stream_factory_produce_t)&AirportCreateStream,
          gstate.trace_uuid,
          -1,
          bind_data.take_flight_params(),
          std::nullopt,
          read_schema,
          bind_data.descriptor(),
          nullptr);

      // printf("Arrow schema column names are: %s\n", join_vector_of_strings(reading_arrow_column_names, ',').c_str());
      // printf("Expected order of columns to be: %s\n", join_vector_of_strings(destination_chunk_column_names, ',').c_str());

      scan_bind_data->examine_schema(context.client, true);

      // There shouldn't be any projection ids.
      vector<idx_t> projection_ids;

      // Retain the local state.
      unique_ptr<AirportDynamicTableInOutLocalState> local_state = make_uniq<AirportDynamicTableInOutLocalState>();

      // Now simulate the init input.
      auto fake_init_input = TableFunctionInitInput(
          &scan_bind_data->Cast<FunctionData>(),
          column_ids,
          projection_ids,
          nullptr);

      // Local init.

      auto current_chunk = make_uniq<ArrowArrayWrapper>();
      
      auto scan_local_state = make_uniq<AirportArrowScanLocalState>(
          std::move(current_chunk),
          context.client,
          std::move(exchange_result.reader),
          fake_init_input);

      scan_local_state->set_stream(
          AirportProduceArrowScan(
              scan_bind_data->CastNoConst<AirportTakeFlightBindData>(),
              column_ids,
              nullptr,
              // Can't use progress reporting here.
              nullptr,
              &scan_bind_data->last_app_metadata,
              scan_bind_data->schema(),
              *scan_bind_data,
              *scan_local_state));

      scan_local_state->column_ids = fake_init_input.column_ids;
      scan_local_state->filters = fake_init_input.filters.get();

      local_state->scan_local_state = std::move(scan_local_state);

      // Create a parameter is the commonly passed to the other functions.
      local_state->scan_bind_data = std::move(scan_bind_data);
      local_state->writer = std::move(exchange_result.writer);

      local_state->scan_table_function_input = make_uniq<TableFunctionInput>(
          local_state->scan_bind_data.get(),
          local_state->scan_local_state.get(),
          gstate.scan_global_state.get());

      return local_state;
  }

  static OperatorResultType AirportTakeFlightInOut(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                                   DataChunk &output)
  {
    auto &global_state = data_p.global_state->Cast<AirportDynamicTableInOutGlobalState>();
    auto &local_state = data_p.local_state->Cast<AirportDynamicTableInOutLocalState>();

    // We need to send data to the server.

    auto appender = make_uniq<ArrowAppender>(
        input.GetTypes(),
        input.size(),
        context.client.GetClientProperties(),
        ArrowTypeExtensionData::GetExtensionTypes(
            context.client, input.GetTypes()));

    appender->Append(input, 0, input.size(), input.size());
    ArrowArray arr = appender->Finalize();

    AIRPORT_ASSIGN_OR_RAISE_CONTAINER(
        auto record_batch,
        arrow::ImportRecordBatch(&arr, global_state.send_schema),
        local_state.scan_bind_data,
        "airport_dynamic_table_function: import record batch");

    // Now send it
    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        local_state.writer->WriteRecordBatch(*record_batch),
        local_state.scan_bind_data,
        "airport_dynamic_table_function: write record batch");

    // The server could produce results, so we should read them.
    //
    // it would be nice to know if we should expect results or not.
    // but that would require reading more of the stream than what we can
    // do right now.
    //
    // Rusty: for now just produce a chunk for every chunk read.
    output.Reset();
    {
      auto &data = local_state.scan_table_function_input->bind_data->CastNoConst<AirportTakeFlightBindData>();
      auto &state = local_state.scan_table_function_input->local_state->Cast<AirportArrowScanLocalState>();
      //      auto &global_state2 = global_state.scan_table_function_input->global_state->Cast<AirportArrowScanGlobalState>();

      state.chunk = state.stream()->GetNextChunk();

      auto output_size =
          MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(state.chunk->arrow_array.length) - state.chunk_offset);
      output.SetCardinality(state.chunk->arrow_array.length);

      state.lines_read += output_size;
      ArrowTableFunction::ArrowToDuckDB(state,
                                        // I'm not sure if arrow table will be defined
                                        data.arrow_table.GetColumns(),
                                        output,
                                        state.lines_read - output_size,
                                        false);
      output.Verify();
    }

    return OperatorResultType::NEED_MORE_INPUT;
  }

  static OperatorFinalizeResultType AirportTakeFlightInOutFinalize(ExecutionContext &context, TableFunctionInput &data_p,
                                                                   DataChunk &output)
  {
    auto &global_state = data_p.global_state->Cast<AirportDynamicTableInOutGlobalState>();
    auto &local_state = data_p.local_state->Cast<AirportDynamicTableInOutLocalState>();

    const arrow::Buffer finished_buffer("finished");

    // So we need to send data to the server.
    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        local_state.writer->DoneWriting(),
        local_state.scan_bind_data,
        "airport_dynamic_table_function: finalize done writing");

    bool is_finished = false;
    {
      auto &scan_data = local_state.scan_table_function_input;
      auto &data = scan_data->bind_data->CastNoConst<AirportTakeFlightBindData>();
      auto &state = scan_data->local_state->Cast<AirportArrowScanLocalState>();
      //      auto &global_state2 = scan_data->global_state->Cast<AirportArrowScanGlobalState>();

      state.chunk = state.stream()->GetNextChunk();

      auto &last_app_metadata = data.last_app_metadata;
      if (last_app_metadata && last_app_metadata->Equals(finished_buffer))
      {
        is_finished = true;
      }

      auto output_size =
          MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(state.chunk->arrow_array.length) - state.chunk_offset);
      output.SetCardinality(state.chunk->arrow_array.length);

      state.lines_read += output_size;
      if (output_size > 0)
      {
        ArrowTableFunction::ArrowToDuckDB(state,
                                          // I'm not sure if arrow table will be defined
                                          data.arrow_table.GetColumns(),
                                          output,
                                          state.lines_read - output_size,
                                          false);
      }
      output.Verify();
    }

    if (is_finished)
    {
      return OperatorFinalizeResultType::FINISHED;
    }
    // there may be more data.
    return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
  }

  void AirportTableFunctionSet::LoadEntries(ClientContext &context)
  {
    // auto &transaction = AirportTransaction::Get(context, catalog);

    auto &airport_catalog = catalog.Cast<AirportCatalog>();

    auto curl = connection_pool_.acquire();
    auto contents = AirportAPI::GetSchemaItems(
        curl,
        catalog.GetDBPath(),
        schema.name,
        schema.serialized_source(),
        cache_directory_,
        airport_catalog.attach_parameters());

    connection_pool_.release(curl);

    // There can be functions with the same name.
    std::unordered_map<AirportFunctionCatalogSchemaNameKey, std::vector<AirportAPITableFunction>> functions_by_name;

    for (auto &function : contents->table_functions)
    {
      AirportFunctionCatalogSchemaNameKey function_key{function.catalog_name(), function.schema_name(), function.name()};
      functions_by_name[function_key].emplace_back(function);
    }

    for (const auto &pair : functions_by_name)
    {
      TableFunctionSet flight_func_set(pair.first.name);
      vector<FunctionDescription> function_descriptions;

      for (const auto &function : pair.second)
      {
        // These input types are available since they are specified in the metadata, but the
        // schema that is returned likely should be requested dynamically from the dynamic
        // flight function.

        auto input_types = AirportSchemaToLogicalTypesWithNaming(context, function.input_schema(), function);

        // Determine if we have a table input.
        bool has_table_input = false;
        if (std::find(input_types.all.begin(), input_types.all.end(), LogicalType(LogicalTypeId::TABLE)) != input_types.all.end())
        {
          has_table_input = true;
        }

        FunctionDescription description;
        description.parameter_types = input_types.positional;
        description.parameter_names = input_types.positional_names;
        description.description = function.description();
        function_descriptions.push_back(std::move(description));

        TableFunction table_func;
        if (!has_table_input)
        {
          table_func = TableFunction(
              input_types.positional,
              AirportTakeFlight,
              AirportDynamicTableBind,
              AirportArrowScanInitGlobal,
              AirportArrowScanInitLocal);
          table_func.projection_pushdown = true;
          table_func.filter_pushdown = false;
          table_func.pushdown_complex_filter = AirportTakeFlightComplexFilterPushdown;
          table_func.cardinality = AirportTakeFlightCardinality;
          table_func.statistics = AirportTakeFlightStatistics;
          table_func.table_scan_progress = AirportTakeFlightScanProgress;
        }
        else
        {
          table_func = TableFunction(
              input_types.all,
              nullptr,
              // The bind function knows how to handle the in and out.
              AirportDynamicTableBind,
              AirportDynamicTableInOutGlobalInit,
              AirportDynamicTableInOutLocalInit);

          table_func.in_out_function = AirportTakeFlightInOut;
          table_func.in_out_function_final = AirportTakeFlightInOutFinalize;

          // Don't allow projection pushdown here, since it causes
          // issues with the fields being returned.
          table_func.projection_pushdown = false;
          table_func.filter_pushdown = false;
          table_func.pushdown_complex_filter = AirportTakeFlightComplexFilterPushdown;
        }

        for (auto &named_pair : input_types.named)
        {
          table_func.named_parameters.emplace(named_pair.first, named_pair.second);
        }

        // Need to store some function information along with the function so that when its called
        // we know what to pass to it.
        table_func.function_info = make_uniq<AirportDynamicTableFunctionInfo>(std::make_shared<AirportAPITableFunction>(function));

        flight_func_set.AddFunction(table_func);
      }

      CreateTableFunctionInfo info = CreateTableFunctionInfo(flight_func_set);
      info.catalog = pair.first.catalog_name;
      info.schema = pair.first.schema_name;

      for (auto &desc : function_descriptions)
      {
        info.descriptions.push_back(std::move(desc));
      }

      auto function_entry = make_uniq_base<StandardEntry, TableFunctionCatalogEntry>(catalog, schema,
                                                                                     info.Cast<CreateTableFunctionInfo>());
      CreateEntry(std::move(function_entry));
    }
  }

} // namespace duckdb
