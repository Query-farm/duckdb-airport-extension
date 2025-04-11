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
#include "airport_schema_utils.h"
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
                                             const std::shared_ptr<arrow::Schema> &function_output_schema,
                                             const std::shared_ptr<arrow::Schema> &function_input_schema)
        : AirportLocationDescriptor(location_descriptor),
          function_output_schema_(function_output_schema),
          function_input_schema_(function_input_schema)
    {
      const auto trace_id = airport_trace_id();

      auto &server_location = this->server_location();
      // Create the client
      auto flight_client = AirportAPI::FlightClientForLocation(server_location);

      arrow::flight::FlightCallOptions call_options;

      // Lookup the auth token from the secret storage.

      auto auth_token = AirportAuthTokenForLocation(context,
                                                    server_location,
                                                    "", "");
      // FIXME: there may need to be a way for the user to supply the auth token
      // but since scalar functions are defined by the server, just assume the user
      // has the token persisted in their secret store.
      airport_add_standard_headers(call_options, server_location);
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

      scan_bind_data_ = make_uniq<AirportExchangeTakeFlightBindData>(
          (stream_factory_produce_t)&AirportCreateStream,
          (uintptr_t)scan_data.get(),
          trace_id,
          -1,
          AirportTakeFlightParameters(server_location, context),
          std::nullopt,
          function_output_schema_,
          this->descriptor(),
          std::move(scan_data));

      // Read the schema for the results being returned.
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(auto read_schema,
                                               scan_bind_data_->scan_data()->stream()->GetSchema(),
                                               this,
                                               "");

      // Ensure that the schema of the response matches the one that was
      // returned on the flight info object.
      AIRPORT_ASSERT_OK_CONTAINER(function_output_schema_->Equals(*read_schema),
                                  this,
                                  "Schema equality check");

      // Convert the Arrow schema to the C format schema.

      scan_bind_data_->examine_schema(context, false);

      // There should only be a single output column.
      D_ASSERT(scan_bind_data_->names().size() == 1);

      writer_ = std::move(exchange_result.writer);

      // Just fake a single column index.
      vector<column_t> column_ids = {0};

      // So you need some endpoints here.
      scan_global_state_ = make_uniq<AirportArrowScanGlobalState>(
          AirportProduceArrowScan(
              *scan_bind_data_,
              column_ids,
              nullptr,
              // No progress reporting.
              nullptr,
              // No need for the last metadata message
              nullptr,
              scan_bind_data_->schema()));

      // There shouldn't be any projection ids.
      vector<idx_t> projection_ids;

      auto fake_init_input = TableFunctionInitInput(
          &scan_bind_data_->Cast<FunctionData>(),
          column_ids,
          projection_ids,
          nullptr);

      auto current_chunk = make_uniq<ArrowArrayWrapper>();
      scan_local_state_ = make_uniq<AirportArrowScanLocalState>(std::move(current_chunk), context);
      scan_local_state_->column_ids = fake_init_input.column_ids;
      scan_local_state_->filters = fake_init_input.filters.get();
    }

  public:
    const std::shared_ptr<arrow::Schema> &function_input_schema() const
    {
      return function_input_schema_;
    }

    const std::shared_ptr<arrow::Schema> &function_output_schema() const
    {
      return function_output_schema_;
    }

    void process_chunk(DataChunk &args, ExpressionState &state, Vector &result);

  private:
    std::unique_ptr<AirportExchangeTakeFlightBindData> scan_bind_data_;
    std::unique_ptr<AirportArrowScanGlobalState> scan_global_state_;
    std::unique_ptr<AirportArrowScanLocalState> scan_local_state_;
    std::unique_ptr<arrow::flight::FlightStreamWriter> writer_;

    const std::shared_ptr<arrow::Schema> function_output_schema_;
    const std::shared_ptr<arrow::Schema> function_input_schema_;
    const unique_ptr<arrow::flight::FlightClient> flight_client_;
  };

  struct AirportScalarFunctionBindData : public FunctionData
  {
  public:
    explicit AirportScalarFunctionBindData(const std::shared_ptr<arrow::Schema> &input_schema) : input_schema_(input_schema)
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

    const std::shared_ptr<arrow::Schema> &input_schema() const
    {
      return input_schema_;
    }

  private:
    const std::shared_ptr<arrow::Schema> input_schema_;
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

    auto &config = DBConfig::GetConfig(context);

    for (idx_t col_idx = 0;
         col_idx < (idx_t)schema_root.arrow_schema.n_children; col_idx++)
    {
      auto &schema = *schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("AirportSchemaToLogicalTypes: released schema passed");
      }
      send_names.push_back(string(schema.name));
      auto arrow_type = ArrowType::GetArrowLogicalType(config, schema);

      if (schema.dictionary)
      {
        auto dictionary_type = ArrowType::GetArrowLogicalType(config, *schema.dictionary);
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
    lstate.process_chunk(args, state, result);
  }

  void AirportScalarFunctionLocalState::process_chunk(DataChunk &args, ExpressionState &state, Vector &result)
  {
    auto &context = state.GetContext();

    // So the send schema can contain ANY fields, if it does, we want to dynamically create the schema from
    // what was supplied.

    auto appender = make_uniq<ArrowAppender>(args.GetTypes(),
                                             args.size(),
                                             context.GetClientProperties(),
                                             ArrowTypeExtensionData::GetExtensionTypes(context, args.GetTypes()));

    // Now that we have the appender append some data.
    appender->Append(args, 0, args.size(), args.size());
    ArrowArray arr = appender->Finalize();

    // Copy from the Appender into the RecordBatch.
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(
        auto record_batch,
        arrow::ImportRecordBatch(&arr, function_input_schema_),
        this, "");

    // Now send that record batch to the remove server.
    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        writer_->WriteRecordBatch(*record_batch),
        this, "");

    scan_local_state_->Reset();

    scan_local_state_->chunk = scan_global_state_->stream()->GetNextChunk();

    auto output_size =
        MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(scan_local_state_->chunk->arrow_array.length) - scan_local_state_->chunk_offset);

    DataChunk returning_data_chunk;
    returning_data_chunk.Initialize(Allocator::Get(context),
                                    scan_bind_data_->return_types(),
                                    output_size);

    returning_data_chunk.SetCardinality(output_size);

    ArrowTableFunction::ArrowToDuckDB(*(scan_local_state_.get()),
                                      scan_bind_data_->arrow_table.GetColumns(),
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