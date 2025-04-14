#include "duckdb.hpp"
#include "storage/airport_delete.hpp"
#include "storage/airport_table_entry.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "airport_macros.hpp"
#include "airport_request_headers.hpp"
#include "airport_flight_exception.hpp"
#include "airport_secrets.hpp"
#include "airport_flight_stream.hpp"
#include "airport_take_flight.hpp"
#include "storage/airport_exchange.hpp"
#include "airport_schema_utils.h"

#include <numeric>

namespace duckdb
{

  static int findIndex(const std::vector<std::string> &vec, const std::string &target)
  {
    auto it = std::find(vec.begin(), vec.end(), target);

    if (it == vec.end())
    {
      throw std::runtime_error("String not found in vector");
    }

    return std::distance(vec.begin(), it);
  }

  void AirportExchangeGetGlobalSinkState(ClientContext &context,
                                         const TableCatalogEntry &table,
                                         const AirportTableEntry &airport_table,
                                         AirportExchangeGlobalState *global_state,
                                         const ArrowSchema &send_schema,
                                         const bool return_chunk,
                                         const string exchange_operation,
                                         const vector<string> destination_chunk_column_names,
                                         const std::optional<string> transaction_id)
  {
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(
        global_state->send_schema,
        arrow::ImportSchema((ArrowSchema *)&send_schema),
        airport_table.table_data,
        "");

    const auto &server_location = airport_table.table_data->server_location();
    const auto &descriptor = airport_table.table_data->descriptor();

    // global_state->flight_descriptor = descriptor;

    auto auth_token = AirportAuthTokenForLocation(context, server_location, "", "");

    D_ASSERT(airport_table.table_data != nullptr);

    auto flight_client = AirportAPI::FlightClientForLocation(server_location);

    auto trace_uuid = airport_trace_id();

    arrow::flight::FlightCallOptions call_options;
    airport_add_standard_headers(call_options, server_location);
    airport_add_authorization_header(call_options, auth_token);
    airport_add_trace_id_header(call_options, trace_uuid);

    // Indicate that we are doing a delete.
    call_options.headers.emplace_back("airport-operation", exchange_operation);

    if (transaction_id.has_value() && !transaction_id.value().empty())
    {
      call_options.headers.emplace_back("airport-transaction-id", transaction_id.value());
    }

    // Indicate if the caller is interested in data being returned.
    call_options.headers.emplace_back("return-chunks", return_chunk ? "1" : "0");

    airport_add_flight_path_header(call_options, descriptor);

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(
        auto exchange_result,
        flight_client->DoExchange(call_options, descriptor),
        airport_table.table_data, "");

    // Tell the server the schema that we will be using to write data.
    AIRPORT_ARROW_ASSERT_OK_CONTAINER(
        exchange_result.writer->Begin(global_state->send_schema),
        airport_table.table_data,
        "Begin schema");

    // Now that there is a reader stream and a writer stream, we want to reuse the Arrow
    // scan code as much as possible, but the problem is it assumes its being called as
    // part of a table returning function, with the life cycle of bind, init global, init local
    // and scan.
    //
    // But we can simulate most of that here.

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_CONTAINER(auto read_schema,
                                             exchange_result.reader->GetSchema(),
                                             airport_table.table_data,
                                             "");

    auto scan_bind_data = make_uniq<AirportExchangeTakeFlightBindData>(
        (stream_factory_produce_t)&AirportCreateStream,
        trace_uuid,
        -1,
        AirportTakeFlightParameters(server_location, context),
        std::nullopt,
        read_schema,
        descriptor,
        nullptr);

    vector<column_t> column_ids;

    if (return_chunk)
    {
      // printf("Schema of reader stream is:\n----------\n%s\n---------\n", read_schema->ToString().c_str());

      vector<string> reading_arrow_column_names;

      const auto column_count = (idx_t)scan_bind_data->schema_root.arrow_schema.n_children;

      reading_arrow_column_names.reserve(column_count);

      for (idx_t col_idx = 0;
           col_idx < column_count; col_idx++)
      {
        const auto &schema_item = *scan_bind_data->schema_root.arrow_schema.children[col_idx];
        if (!schema_item.release)
        {
          throw InvalidInputException("airport_exchange: released schema passed");
        }
        auto name = AirportNameForField(schema_item.name, col_idx);

        reading_arrow_column_names.push_back(name);
      }

      column_ids.reserve(destination_chunk_column_names.size());

      for (size_t output_index = 0; output_index < destination_chunk_column_names.size(); output_index++)
      {
        auto found_index = findIndex(reading_arrow_column_names, destination_chunk_column_names[output_index]);
        if (exchange_operation != "update")
        {
          column_ids.push_back(found_index);
        }
        else
        {
          // This is right for outputs, because it allowed the read chunk to happen.
          column_ids.push_back(output_index);
        }
        // printf("Output data chunk column %s (type=%s) (%d) comes from arrow column index index %d\n",
        //        destination_chunk_column_names[output_index].c_str(),
        //        arrow_types[found_index].c_str(),
        //        output_index,
        //        found_index);
      }

      scan_bind_data->examine_schema(context, true);
    }

    // For each index in the arrow table, the column_ids is asked what
    // where to map that column, the row id can be expressed there.

    // There shouldn't be any projection ids.
    vector<idx_t> projection_ids;

    // Now to initialize the Arrow scan from the reader stream we need to do the steps
    // that the normal table returning function does.

    // bind
    // init global state
    // init local state
    // scan...

    // Init the global state.

    // Retain the global state.
    global_state->scan_global_state = make_uniq<AirportArrowScanGlobalState>();

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
        context,
        std::move(exchange_result.reader), fake_init_input);
    scan_local_state->set_stream(AirportProduceArrowScan(
        scan_bind_data->CastNoConst<AirportTakeFlightBindData>(),
        column_ids,
        nullptr,
        nullptr, // No progress reporting.
        &scan_bind_data->last_app_metadata,
        scan_bind_data->schema(),
        *scan_bind_data,
        *scan_local_state));

    scan_local_state->column_ids = fake_init_input.column_ids;
    scan_local_state->filters = fake_init_input.filters.get();

    global_state->scan_local_state = std::move(scan_local_state);

    // Create a parameter is the commonly passed to the other functions.
    global_state->scan_bind_data = std::move(scan_bind_data);
    global_state->writer = std::move(exchange_result.writer);

    global_state->scan_table_function_input = make_uniq<TableFunctionInput>(
        global_state->scan_bind_data.get(),
        global_state->scan_local_state.get(),
        global_state->scan_global_state.get());
  }
}