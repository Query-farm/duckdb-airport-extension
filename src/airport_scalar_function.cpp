#include "duckdb.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include <arrow/c/bridge.h>
#include "duckdb/common/types/uuid.hpp"
#include "airport_scalar_function.hpp"
#include "airport_request_headers.hpp"
#include "airport_macros.hpp"
#include "airport_secrets.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "airport_flight_stream.hpp"
#include "storage/airport_exchange.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "airport_location_descriptor.hpp"
#include <numeric>

namespace duckdb
{

  // So the local state of an airport provided scalar function is going to setup a
  // lot of the functionality necessary.
  //
  // Its going to create the flight client, call the DoExchange endpoint,
  //
  // Its going to send the schema of the stream that we're going to write to the server
  // and its going to read the schema of the strema that is returned.
  struct AirportScalarFunctionLocalState : public FunctionLocalState, public AirportLocationDescriptor
  {
    explicit AirportScalarFunctionLocalState(ClientContext &context,
                                             const AirportLocationDescriptor &location_descriptor,
                                             std::shared_ptr<arrow::Schema> function_output_schema,
                                             std::shared_ptr<arrow::Schema> function_input_schema) : AirportLocationDescriptor(location_descriptor),
                                                                                                     function_output_schema_(function_output_schema),
                                                                                                     function_input_schema_(function_input_schema)
    {
      const auto trace_id = airport_trace_id();

      // Create the client
      auto flight_client = AirportAPI::FlightClientForLocation(this->server_location());

      arrow::flight::FlightCallOptions call_options;

      // Lookup the auth token from the secret storage.

      auto auth_token = AirportAuthTokenForLocation(context,
                                                    this->server_location(),
                                                    "", "");
      // FIXME: there may need to be a way for the user to supply the auth token
      // but since scalar functions are defined by the server, just assume the user
      // has the token persisted in their secret store.
      airport_add_standard_headers(call_options, this->server_location());
      airport_add_authorization_header(call_options, auth_token);
      airport_add_trace_id_header(call_options, trace_id);

      // Indicate that we are doing a delete.
      call_options.headers.emplace_back("airport-operation", "scalar_function");

      // Indicate if the caller is interested in data being returned.
      call_options.headers.emplace_back("return-chunks", "1");

      airport_add_flight_path_header(call_options, this->descriptor());

      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(
          auto exchange_result,
          flight_client->DoExchange(call_options, this->descriptor()),
          this, "");

      // Tell the server the schema that we will be using to write data.
      AIRPORT_ARROW_ASSERT_OK_CONTAINER(
          exchange_result.writer->Begin(function_input_schema_),
          this,
          "Begin schema");

      auto scan_data = make_uniq<AirportTakeFlightScanData>(
          *this,
          function_output_schema_,
          std::move(exchange_result.reader));

      scan_bind_data = make_uniq<AirportExchangeTakeFlightBindData>(
          (stream_factory_produce_t)&AirportCreateStream,
          (uintptr_t)scan_data.get());

      scan_bind_data->scan_data = std::move(scan_data);

      // Read the schema for the results being returned.
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(auto read_schema,
                                               scan_bind_data->scan_data->stream()->GetSchema(),
                                               this, "");

      // Ensure that the schema of the response matches the one that was
      // returned on the flight info object.
      AIRPORT_ASSERT_OK_CONTAINER(function_output_schema_->Equals(*read_schema),
                                  this,
                                  "Schema equality check");

      // Convert the Arrow schema to the C format schema.
      auto &data = *scan_bind_data;
      AIRPORT_ARROW_ASSERT_OK_CONTAINER(
          ExportSchema(*read_schema, &data.schema_root.arrow_schema),
          this,
          "ExportSchema");

      vector<string> reading_arrow_column_names;
      vector<string> arrow_types;

      for (idx_t col_idx = 0;
           col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++)
      {
        auto &schema = *data.schema_root.arrow_schema.children[col_idx];
        if (!schema.release)
        {
          throw InvalidInputException("airport_scalar_function: released schema passed");
        }
        auto name = string(schema.name);
        if (name.empty())
        {
          name = string("v") + to_string(col_idx);
        }

        reading_arrow_column_names.push_back(name);

        auto arrow_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), schema);
        arrow_types.push_back(arrow_type->GetDuckType().ToString());
      }

      // There should only be one column returned, since this is a call
      // to a scalar function.
      D_ASSERT(reading_arrow_column_names.size() == 1);
      D_ASSERT(arrow_types.size() == 1);

      for (idx_t col_idx = 0;
           col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++)
      {
        auto &schema = *data.schema_root.arrow_schema.children[col_idx];
        if (!schema.release)
        {
          throw InvalidInputException("airport_scalar_function: released schema passed");
        }
        auto arrow_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), schema);

        if (schema.dictionary)
        {
          auto dictionary_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), *schema.dictionary);
          arrow_type->SetDictionary(std::move(dictionary_type));
        }
        scan_bind_data->return_types.emplace_back(arrow_type->GetDuckType());

        scan_bind_data->arrow_table.AddColumn(col_idx, std::move(arrow_type));

        auto format = string(schema.format);

        auto name = to_string(col_idx);
        scan_bind_data->names.push_back(name);
      }

      writer = std::move(exchange_result.writer);

      // Just fake a single column index.
      vector<column_t> column_ids = {0};
      scan_global_state = make_uniq<AirportArrowScanGlobalState>();
      scan_global_state->stream = AirportProduceArrowScan(scan_bind_data->CastNoConst<AirportTakeFlightBindData>(), column_ids, nullptr);

      // There shouldn't be any projection ids.
      vector<idx_t> projection_ids;

      auto fake_init_input = TableFunctionInitInput(
          &scan_bind_data->Cast<FunctionData>(),
          column_ids,
          projection_ids,
          nullptr);

      auto current_chunk = make_uniq<ArrowArrayWrapper>();
      scan_local_state = make_uniq<ArrowScanLocalState>(std::move(current_chunk), context);
      scan_local_state->column_ids = fake_init_input.column_ids;
      scan_local_state->filters = fake_init_input.filters.get();
    }

  public:
    std::shared_ptr<arrow::Schema> function_input_schema() const
    {
      return function_input_schema_;
    }

    std::shared_ptr<arrow::Schema> function_output_schema() const
    {
      return function_output_schema_;
    }

    const bool &input_schema_includes_any_types() const
    {
      return input_schema_includes_any_types_;
    }

    std::unique_ptr<AirportExchangeTakeFlightBindData> scan_bind_data;
    std::unique_ptr<AirportArrowScanGlobalState> scan_global_state;
    std::unique_ptr<ArrowArrayStreamWrapper> reader;
    std::unique_ptr<ArrowScanLocalState> scan_local_state;
    std::unique_ptr<arrow::flight::FlightStreamWriter> writer;

  private:
    bool input_schema_includes_any_types_;
    std::shared_ptr<arrow::Schema> function_output_schema_;
    std::shared_ptr<arrow::Schema> function_input_schema_;
    unique_ptr<arrow::flight::FlightClient> flight_client;
  };

  struct AirportScalarFunctionBindData : public FunctionData
  {
  public:
    explicit AirportScalarFunctionBindData(std::shared_ptr<arrow::Schema> input_schema) : input_schema_(input_schema)
    {
    }

    unique_ptr<FunctionData> Copy() const override
    {
      return make_uniq<AirportScalarFunctionBindData>(input_schema_);
    };

    bool Equals(const FunctionData &other_p) const override
    {
      auto &other = other_p.Cast<AirportScalarFunctionBindData>();
      return input_schema_ == other.input_schema();
    }

    std::shared_ptr<arrow::Schema> input_schema() const
    {
      return input_schema_;
    }

  private:
    std::shared_ptr<arrow::Schema> input_schema_;
  };

  unique_ptr<FunctionData> AirportScalarFunctionBind(ClientContext &context, ScalarFunction &bound_function,
                                                     vector<unique_ptr<Expression>> &arguments)
  {
    // FIXME check for the number of arguments.
    auto &info = bound_function.function_info->Cast<AirportScalarFunctionInfo>();

    if (!info.input_schema_includes_any_types())
    {
      return make_uniq<AirportScalarFunctionBindData>(info.input_schema());
    }

    // So we need to create the schema dynamically based on the types passed.
    vector<string> send_names;
    vector<LogicalType> return_types;

    auto input_schema = info.input_schema();

    ArrowSchemaWrapper schema_root;

    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        ExportSchema(*info.input_schema(), &schema_root.arrow_schema),
        (&info),
        "ExportSchema");

    for (idx_t col_idx = 0;
         col_idx < (idx_t)schema_root.arrow_schema.n_children; col_idx++)
    {
      auto &schema = *schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("AirportSchemaToLogicalTypes: released schema passed");
      }
      send_names.push_back(string(schema.name));

      auto arrow_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), schema);

      if (schema.dictionary)
      {
        auto dictionary_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), *schema.dictionary);
        arrow_type->SetDictionary(std::move(dictionary_type));
      }

      // Indicate that the field should select any type.
      bool is_any_type = false;
      if (schema.metadata != nullptr)
      {
        auto column_metadata = ArrowSchemaMetadata(schema.metadata);
        if (!column_metadata.GetOption("is_any_type").empty())
        {
          is_any_type = true;
        }
      }

      if (is_any_type)
      {
        return_types.push_back(arguments[col_idx]->return_type);
      }
      else
      {
        return_types.emplace_back(arrow_type->GetDuckType());
      }
    }

    // Now convert the list of names and LogicalTypes to an ArrowSchema
    ArrowSchema send_schema;
    auto client_properties = context.GetClientProperties();
    ArrowConverter::ToArrowSchema(&send_schema, return_types, send_names, client_properties);

    std::shared_ptr<arrow::Schema> cpp_schema;

    // Export the C based schema to the C++ one.
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(
        cpp_schema,
        arrow::ImportSchema(&send_schema),
        (&info),
        "ExportSchema");

    return make_uniq<AirportScalarFunctionBindData>(cpp_schema);
  }

  void AirportScalarFunctionProcessChunk(DataChunk &args, ExpressionState &state, Vector &result)
  {
    auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<AirportScalarFunctionLocalState>();
    auto &context = state.GetContext();

    // So the send schema can contain ANY fields, if it does, we want to dynamically create the schema from
    // what was supplied.

    auto appender = make_uniq<ArrowAppender>(args.GetTypes(), args.size(), context.GetClientProperties(),
                                             ArrowTypeExtensionData::GetExtensionTypes(context, args.GetTypes()));

    // Now that we have the appender append some data.
    appender->Append(args, 0, args.size(), args.size());
    ArrowArray arr = appender->Finalize();

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        auto record_batch,
        arrow::ImportRecordBatch(&arr, lstate.function_input_schema()),
        lstate.server_location(),
        lstate.descriptor(), "");

    // Now send that record batch to the remove server.
    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        lstate.writer->WriteRecordBatch(*record_batch),
        lstate.server_location(),
        lstate.descriptor(), "");

    lstate.scan_local_state->Reset();

    auto current_chunk = lstate.scan_global_state->stream->GetNextChunk();
    lstate.scan_local_state->chunk = std::move(current_chunk);

    auto output_size =
        MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(lstate.scan_local_state->chunk->arrow_array.length) - lstate.scan_local_state->chunk_offset);

    DataChunk returning_data_chunk;
    returning_data_chunk.Initialize(Allocator::Get(context),
                                    lstate.scan_bind_data->return_types,
                                    output_size);

    returning_data_chunk.SetCardinality(output_size);

    ArrowTableFunction::ArrowToDuckDB(*(lstate.scan_local_state.get()),
                                      lstate.scan_bind_data->arrow_table.GetColumns(),
                                      returning_data_chunk,
                                      0,
                                      false);

    returning_data_chunk.Verify();

    result.Reference(returning_data_chunk.data[0]);
  }

  // Lets work on initializing the local state
  unique_ptr<FunctionLocalState> AirportScalarFunctionInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data)
  {
    auto &info = expr.function.function_info->Cast<AirportScalarFunctionInfo>();
    auto &data = bind_data->Cast<AirportScalarFunctionBindData>();

    return make_uniq<AirportScalarFunctionLocalState>(
        state.GetContext(),
        info,
        info.output_schema(),
        // Use this schema that should have the proper types for the any columns.
        data.input_schema());
  }
}