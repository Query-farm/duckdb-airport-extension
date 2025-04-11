#include "airport_extension.hpp"
#include "duckdb.hpp"

// Arrow includes.
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/buffer.h>

#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "airport_take_flight.hpp"
#include "airport_json_common.hpp"
#include "airport_json_serializer.hpp"
#include "airport_flight_stream.hpp"
#include "airport_macros.hpp"
#include "airport_request_headers.hpp"
#include "airport_flight_exception.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "msgpack.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "storage/airport_catalog.hpp"
#include "airport_flight_statistics.hpp"
#include "airport_schema_utils.h"

namespace duckdb
{
  // Create a FlightDescriptor from a DuckDB value which can be one of a few different
  // types.
  static flight::FlightDescriptor flight_descriptor_from_value(duckdb::Value &flight_descriptor)
  {
    switch (flight_descriptor.type().id())
    {
    case LogicalTypeId::BLOB:
    case LogicalTypeId::VARCHAR:
      return flight::FlightDescriptor::Command(flight_descriptor.ToString());
    case LogicalTypeId::LIST:
    {
      auto &list_values = ListValue::GetChildren(flight_descriptor);
      vector<string> components;
      for (idx_t i = 0; i < list_values.size(); i++)
      {
        auto &child = list_values[i];
        if (child.type().id() != LogicalTypeId::VARCHAR)
        {
          throw InvalidInputException("airport_take_flight: when specifying a path all list components must be a varchar");
        }
        components.emplace_back(child.ToString());
      }
      return flight::FlightDescriptor::Path(components);
    }
    case LogicalTypeId::ARRAY:
    {
      auto &array_values = ArrayValue::GetChildren(flight_descriptor);
      vector<string> components;
      for (idx_t i = 0; i < array_values.size(); i++)
      {
        auto &child = array_values[i];
        if (child.type().id() != LogicalTypeId::VARCHAR)
        {
          throw InvalidInputException("airport_take_flight: when specifying a path all list components must be a varchar");
        }
        components.emplace_back(child.ToString());
      }
      return flight::FlightDescriptor::Path(components);
    }
    // FIXME: deal with the union type returned by Arrow list flights.
    default:
      throw InvalidInputException("airport_take_flight: unknown descriptor type passed");
    }
  }

  unique_ptr<FunctionData>
  AirportTakeFlightBindWithFlightDescriptor(
      const AirportTakeFlightParameters &take_flight_params,
      const flight::FlightDescriptor &descriptor,
      ClientContext &context,
      const TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names,
      // So rather than the cached_flight_info_ptr here we can just have the cached schema.
      std::shared_ptr<arrow::Schema> schema,
      const std::optional<AirportGetFlightInfoTableFunctionParameters> &table_function_parameters)
  {
    // Create a UID for tracing.
    const auto trace_uuid = airport_trace_id();

    auto &server_location = take_flight_params.server_location();

    // The main thing that needs to be answered in this function
    // are the names and return types and establishing the bind data.
    //
    // If the cached_flight_info_ptr is not null we should use the schema
    // from that flight info otherwise it should be requested.

    arrow::flight::FlightCallOptions call_options;
    airport_add_normal_headers(call_options, take_flight_params, trace_uuid,
                               descriptor);

    int64_t estimated_records = -1;

    // Get the information about the flight, this will allow the
    // endpoint information to be returned.
    unique_ptr<AirportTakeFlightScanData> scan_data;

    if (schema != nullptr)
    {
      scan_data = make_uniq<AirportTakeFlightScanData>(
          AirportLocationDescriptor(take_flight_params.server_location(), descriptor),
          nullptr);
    }
    else
    {
      std::unique_ptr<arrow::flight::FlightInfo> retrieved_flight_info;
      auto flight_client = AirportAPI::FlightClientForLocation(server_location);

      if (table_function_parameters != std::nullopt)
      {
        // Rather than calling GetFlightInfo we will call DoAction and get
        // get the flight info that way, since it allows us to serialize
        // all of the data we need to send instead of just the flight name.

        AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, "get_flight_info_table_function", table_function_parameters);

        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto action_results, flight_client->DoAction(call_options, action), server_location, "airport_get_flight_info_table_function");

        // The only item returned is a serialized flight info.
        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto serialized_flight_info_buffer, action_results->Next(), server_location, "reading get_flight_info for table function");

        std::string_view serialized_flight_info(reinterpret_cast<const char *>(serialized_flight_info_buffer->body->data()), serialized_flight_info_buffer->body->size());

        // Now deserialize that flight info so we can use it.
        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(retrieved_flight_info, arrow::flight::FlightInfo::Deserialize(serialized_flight_info), server_location, "deserialize flight info");

        AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), server_location, "");
      }
      else
      {
        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(retrieved_flight_info,
                                                           flight_client->GetFlightInfo(call_options, descriptor),
                                                           server_location,
                                                           descriptor,
                                                           "");
      }

      // Assert that the descriptor is the same as the one that was passed in.
      if (descriptor != retrieved_flight_info->descriptor())
      {
        throw InvalidInputException("airport_take_flight: descriptor returned from server does not match the descriptor that was passed in to GetFlightInfo, check with Flight server implementation.");
      }

      estimated_records = retrieved_flight_info->total_records();

      arrow::ipc::DictionaryMemo dictionary_memo;
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(schema,
                                                         retrieved_flight_info->GetSchema(&dictionary_memo),
                                                         server_location,
                                                         descriptor,
                                                         "");

      scan_data = make_uniq<AirportTakeFlightScanData>(
          AirportLocationDescriptor(server_location, descriptor),
          nullptr);
    }

    auto ret = make_uniq<AirportTakeFlightBindData>(
        (stream_factory_produce_t)&AirportCreateStream,
        (uintptr_t)scan_data.get(),
        trace_uuid,
        estimated_records,
        take_flight_params,
        table_function_parameters,
        schema,
        descriptor,
        std::move(scan_data));

    AirportExamineSchema(context,
                         ret->schema_root,
                         &ret->arrow_table,
                         &return_types,
                         &names,
                         nullptr,
                         &ret->rowid_column_index,
                         true);

    return ret;
  }

  static unique_ptr<FunctionData> take_flight_bind(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names)
  {
    auto server_location = input.inputs[0].ToString();
    AirportTakeFlightParameters params(server_location, context, input);
    auto descriptor = flight_descriptor_from_value(input.inputs[1]);

    return AirportTakeFlightBindWithFlightDescriptor(
        params,
        descriptor,
        context,
        input, return_types, names, nullptr, std::nullopt);
  }

  static unique_ptr<FunctionData> take_flight_bind_with_pointer(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names)
  {
    if (input.inputs[0].IsNull())
    {
      throw BinderException("airport: take_flight_with_pointer, pointers to AirportTable cannot be null");
    }

    const auto info = reinterpret_cast<const duckdb::AirportAPITable *>(input.inputs[0].GetPointer());

    AirportTakeFlightParameters params(info->server_location(), context, input);

    // The transaction identifier is passed as the 2nd argument.
    if (!input.inputs[1].IsNull())
    {
      auto id = input.inputs[1].ToString();
      if (!id.empty())
      {
        params.add_header("airport-transaction-id", id);
      }
    }

    return AirportTakeFlightBindWithFlightDescriptor(
        params,
        info->descriptor(),
        context,
        input,
        return_types,
        names,
        info->schema(),
        std::nullopt);
  }

  static bool AirportArrowScanParallelStateNext(const ClientContext &context, const FunctionData *bind_data_p,
                                                AirportArrowScanLocalState &state, AirportArrowScanGlobalState &parallel_state)
  {
    lock_guard<mutex> parallel_lock(parallel_state.main_mutex);
    if (parallel_state.done)
    {
      return false;
    }
    state.Reset();
    state.batch_index = ++parallel_state.batch_index;

    auto current_chunk = parallel_state.stream()->GetNextChunk();
    while (current_chunk->arrow_array.length == 0 && current_chunk->arrow_array.release)
    {
      current_chunk = parallel_state.stream()->GetNextChunk();
    }
    state.chunk = std::move(current_chunk);
    //! have we run out of chunks? we are done
    if (!state.chunk->arrow_array.release)
    {
      parallel_state.done = true;
      return false;
    }
    return true;
  }

  void AirportTakeFlight(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
  {
    if (!data_p.local_state)
    {
      return;
    }
    auto &state = data_p.local_state->Cast<AirportArrowScanLocalState>();
    auto &global_state = data_p.global_state->Cast<AirportArrowScanGlobalState>();
    auto &airport_bind_data = data_p.bind_data->CastNoConst<AirportTakeFlightBindData>();

    //! Out of tuples in this chunk
    if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length)
    {
      if (!AirportArrowScanParallelStateNext(
              context,
              &airport_bind_data,
              state,
              global_state))
      {
        return;
      }
    }
    auto output_size =
        MinValue<int64_t>(STANDARD_VECTOR_SIZE,
                          state.chunk->arrow_array.length - state.chunk_offset);

    airport_bind_data.lines_read += output_size;

    if (global_state.CanRemoveFilterColumns())
    {
      state.all_columns.Reset();
      state.all_columns.SetCardinality(output_size);
      ArrowTableFunction::ArrowToDuckDB(state, airport_bind_data.arrow_table.GetColumns(), state.all_columns,
                                        airport_bind_data.lines_read - output_size, false, airport_bind_data.rowid_column_index);
      output.ReferenceColumns(state.all_columns, global_state.projection_ids());
    }
    else
    {
      output.SetCardinality(output_size);
      ArrowTableFunction::ArrowToDuckDB(state, airport_bind_data.arrow_table.GetColumns(), output,
                                        airport_bind_data.lines_read - output_size, false, airport_bind_data.rowid_column_index);
    }

    output.Verify();
    state.chunk_offset += output.size();
  }

  static unique_ptr<NodeStatistics> airport_take_flight_cardinality(ClientContext &context, const FunctionData *data)
  {
    // To estimate the cardinality of the flight, we can peek at the flight information
    // that was retrieved during the bind function.
    //
    // This estimate does not take into account any filters that may have been applied
    //
    auto &bind_data = data->Cast<AirportTakeFlightBindData>();
    auto flight_estimated_records = bind_data.estimated_records();

    if (flight_estimated_records != -1)
    {
      return make_uniq<NodeStatistics>(flight_estimated_records);
    }
    // If we don't have an estimated number of records, just use an assumption.
    return make_uniq<NodeStatistics>(100000);
  }

  static void take_flight_complex_filter_pushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                                  vector<unique_ptr<Expression>> &filters)
  {
    auto allocator = AirportJSONAllocator(BufferAllocator::Get(context));

    auto alc = allocator.GetYYAlc();

    auto doc = AirportJSONCommon::CreateDocument(alc);
    auto result_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, result_obj);

    auto filters_arr = yyjson_mut_arr(doc);

    for (auto &f : filters)
    {
      auto serializer = AirportJsonSerializer(doc, false, false, false);
      f->Serialize(serializer);
      yyjson_mut_arr_append(filters_arr, serializer.GetRootObject());
    }

    yyjson_mut_val *column_id_names = yyjson_mut_arr(doc);
    for (auto id : get.GetColumnIds())
    {
      // So there can be a special column id specified called rowid.
      yyjson_mut_arr_add_str(doc, column_id_names, id.IsRowIdColumn() ? "rowid" : get.names[id.GetPrimaryIndex()].c_str());
    }

    yyjson_mut_obj_add_val(doc, result_obj, "filters", filters_arr);
    yyjson_mut_obj_add_val(doc, result_obj, "column_binding_names_by_index", column_id_names);
    idx_t len;
    auto data = yyjson_mut_val_write_opts(
        result_obj,
        AirportJSONCommon::WRITE_FLAG,
        alc, reinterpret_cast<size_t *>(&len), nullptr);

    if (data == nullptr)
    {
      throw SerializationException(
          "Failed to serialize json, perhaps the query contains invalid utf8 characters?");
    }

    auto json_result = string(data, (size_t)len);

    auto &bind_data = bind_data_p->Cast<AirportTakeFlightBindData>();

    bind_data.json_filters = json_result;
  }

  unique_ptr<ArrowArrayStreamWrapper> AirportProduceArrowScan(
      const ArrowScanFunctionData &function,
      const vector<column_t> &column_ids,
      TableFilterSet *filters,
      atomic<double> *progress,
      std::shared_ptr<arrow::Buffer> *last_app_metadata,
      const std::shared_ptr<arrow::Schema> &schema)
  {
    AirportArrowStreamParameters parameters(progress, last_app_metadata, schema);

    auto &projected = parameters.projected_columns;
    // Preallocate space for efficiency
    projected.columns.reserve(column_ids.size());
    projected.projection_map.reserve(column_ids.size());
    projected.filter_to_col.reserve(column_ids.size());

    for (const auto col_idx : column_ids)
    {
      if (col_idx == COLUMN_IDENTIFIER_ROW_ID)
        continue;

      const auto &schema = *function.schema_root.arrow_schema.children[col_idx];
      projected.projection_map.emplace(col_idx, schema.name);
      projected.columns.emplace_back(schema.name);
      projected.filter_to_col.emplace(col_idx, col_idx);
    }

    parameters.filters = filters;

    // TODO
    // RUSTY: replace this.
    return function.scanner_producer(function.stream_factory_ptr, parameters);
  }

  // static string CompressString(const string &input, const string &location, const flight::FlightDescriptor &descriptor)
  // {
  //   auto codec = arrow::util::Codec::Create(arrow::Compression::ZSTD, 1).ValueOrDie();

  //   // Estimate the maximum compressed size (usually larger than original size)
  //   int64_t max_compressed_len = codec->MaxCompressedLen(input.size(), reinterpret_cast<const uint8_t *>(input.data()));

  //   // Allocate a buffer to hold the compressed data

  //   AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto compressed_buffer, arrow::AllocateBuffer(max_compressed_len), location, descriptor, "");

  //   // Perform the compression
  //   AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto compressed_size,
  //                                                      codec->Compress(
  //                                                          input.size(),
  //                                                          reinterpret_cast<const uint8_t *>(input.data()),
  //                                                          max_compressed_len,
  //                                                          compressed_buffer->mutable_data()),
  //                                                      location, descriptor, "");

  //   // If you want to write the compressed data to a string
  //   std::string compressed_str(reinterpret_cast<const char *>(compressed_buffer->data()), compressed_size);
  //   return compressed_str;
  // }

  struct AirportTicketMetadataParameters
  {
    std::string json_filters;
    std::vector<idx_t> column_ids;

    MSGPACK_DEFINE_MAP(json_filters, column_ids)
  };

  // static string BuildCompressedTicketMetadata(const string &json_filters, const vector<idx_t> &column_ids, uint32_t *uncompressed_length, const string &location, const flight::FlightDescriptor &descriptor)
  // {
  //   AirportTicketMetadataParameters params;
  //   params.json_filters = json_filters;
  //   params.column_ids = column_ids;

  //   std::stringstream packed_buffer;
  //   msgpack::pack(packed_buffer, params);

  //   auto metadata_doc_string = packed_buffer.str();
  //   *uncompressed_length = metadata_doc_string.size();
  //   auto compressed_metadata = CompressString(metadata_doc_string, location, descriptor);

  //   return compressed_metadata;
  // }

  struct AirportGetFlightEndpointsRequest
  {
    std::string descriptor;
    AirportTicketMetadataParameters parameters;

    MSGPACK_DEFINE_MAP(descriptor, parameters)
  };

  static vector<flight::FlightEndpoint> AirportGetFlightEndpoints(
      const AirportTakeFlightParameters &take_flight_params,
      const string &trace_id,
      const flight::FlightDescriptor &descriptor,
      const std::shared_ptr<flight::FlightClient> &flight_client,
      const std::string &json_filters,
      const vector<idx_t> &column_ids)
  {
    vector<flight::FlightEndpoint> endpoints;
    arrow::flight::FlightCallOptions call_options;
    auto &server_location = take_flight_params.server_location();

    airport_add_normal_headers(call_options, take_flight_params, trace_id,
                               descriptor);

    AirportGetFlightEndpointsRequest endpoints_request;

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(
        endpoints_request.descriptor,
        descriptor.SerializeToString(),
        server_location,
        "get_flight_endpoints serialize flight descriptor");
    endpoints_request.parameters.json_filters = json_filters;
    endpoints_request.parameters.column_ids = column_ids;

    AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, "get_flight_endpoints", endpoints_request);

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto action_results,
                                            flight_client->DoAction(call_options, action),
                                            server_location,
                                            "airport get_flight_endpoints action");

    // The only item returned is a serialized flight info.
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto serialized_endpoint_info_buffer,
                                            action_results->Next(),
                                            server_location,
                                            "reading get_flight_endpoints");

    std::string_view serialized_endpoint_info(reinterpret_cast<const char *>(serialized_endpoint_info_buffer->body->data()), serialized_endpoint_info_buffer->body->size());

    AIRPORT_MSGPACK_UNPACK(std::vector<std::string>,
                           serialized_endpoints,
                           serialized_endpoint_info,
                           server_location,
                           "File to parse msgpack encoded endpoints");

    AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), server_location, "get_flight_endpoints drain");

    endpoints.reserve(serialized_endpoints.size());

    for (const auto &endpoint : serialized_endpoints)
    {
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto deserialized_endpoint,
                                              arrow::flight::FlightEndpoint::Deserialize(endpoint),
                                              server_location,
                                              "deserialize flight endpoint");
      endpoints.push_back(std::move(deserialized_endpoint));
    }
    return endpoints;
  }

  unique_ptr<GlobalTableFunctionState> AirportArrowScanInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input)
  {
    auto &bind_data = input.bind_data->CastNoConst<AirportTakeFlightBindData>();

    // Ideally this is where we call GetFlightInfo to obtain the endpoints, but
    // GetFlightInfo can't take the predicate information, so we'll need to call an
    // action called get_flight_endpoints.
    //
    // FIXME: somehow the flight should be marked if it supports predicate pushdown.
    // right now I'm not sure what this is.
    //
    auto flight_client = AirportAPI::FlightClientForLocation(bind_data.server_location());

    vector<idx_t> projection_ids;
    vector<LogicalType> scanned_types;

    if (!input.projection_ids.empty())
    {
      projection_ids = input.projection_ids;
      for (const auto &col_idx : input.column_ids)
      {
        if (col_idx == COLUMN_IDENTIFIER_ROW_ID)
        {
          auto rowid_type = AirportAPI::GetRowIdType(
              context,
              bind_data.schema(),
              bind_data);
          scanned_types.emplace_back(rowid_type);
        }
        else
        {
          scanned_types.push_back(bind_data.all_types[col_idx]);
        }
      }
    }

    auto result = make_uniq<AirportArrowScanGlobalState>(
        AirportGetFlightEndpoints(bind_data.take_flight_params(),
                                  bind_data.trace_id(),
                                  bind_data.descriptor(),
                                  flight_client,
                                  bind_data.json_filters,
                                  input.column_ids),
        projection_ids, scanned_types);

    // Store the total number of endpoints in the bind data so progress
    // can be reported across all endpoints.
    bind_data.set_endpoint_count(result->total_endpoints());

    auto &first_endpoint_opt = result->GetNextEndpoint();
    if (!first_endpoint_opt)
    {
      throw InvalidInputException("airport_take_flight: no endpoints returned from server");
    }
    auto &first_endpoint = *first_endpoint_opt;
    auto &first_location = first_endpoint.locations.front();

    auto server_location = first_location.ToString();
    if (first_location != flight::Location::ReuseConnection())
    {
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(flight_client,
                                              flight::FlightClient::Connect(first_location),
                                              first_location.ToString(),
                                              "");
      server_location = bind_data.server_location();
    }

    const auto &descriptor = bind_data.descriptor();

    arrow::flight::FlightCallOptions call_options;
    airport_add_normal_headers(call_options,
                               bind_data.take_flight_params(),
                               bind_data.trace_id(),
                               descriptor);

    if (bind_data.skip_producing_result_for_update_or_delete)
    {
      // This is a special case where the result of the scan should be skipped.
      // This is useful when the scan is being used to update or delete rows.
      // For a table that doesn't actually produce row ids, so filtering cannot be applied.
      call_options.headers.emplace_back("airport-skip-producing-results", "1");
    }

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        auto stream,
        flight_client->DoGet(
            call_options,
            first_endpoint.ticket),
        server_location,
        descriptor,
        "");

    // FIXME: make sure that the schema returned from the server is the same as
    // what we were expecting.

    bind_data.scan_data()->setStream(std::move(stream));

    result->stream_ = AirportProduceArrowScan(bind_data,
                                              input.column_ids,
                                              input.filters.get(),
                                              bind_data.get_progress_counter(0),
                                              // No need for the last metadata message.
                                              nullptr,
                                              bind_data.schema());

    return result;
  }

  static double take_flight_scan_progress(ClientContext &, const FunctionData *data, const GlobalTableFunctionState *global_state)
  {
    return data->Cast<AirportTakeFlightBindData>().total_progress();
  }

  static unique_ptr<LocalTableFunctionState>
  AirportArrowScanInitLocalInternal(ClientContext &context, TableFunctionInitInput &input,
                                    GlobalTableFunctionState *global_state_p)
  {
    auto &global_state = global_state_p->Cast<AirportArrowScanGlobalState>();
    auto current_chunk = make_uniq<ArrowArrayWrapper>();
    auto result = make_uniq<AirportArrowScanLocalState>(std::move(current_chunk), context);
    result->column_ids = input.column_ids;
    result->filters = input.filters.get();
    auto &bind_data = input.bind_data->Cast<AirportTakeFlightBindData>();
    if (!bind_data.projection_pushdown_enabled)
    {
      result->column_ids.clear();
    }
    else if (!input.projection_ids.empty())
    {
      auto &asgs = global_state_p->Cast<AirportArrowScanGlobalState>();
      result->all_columns.Initialize(context, asgs.scanned_types());
    }
    if (!AirportArrowScanParallelStateNext(context, input.bind_data.get(), *result, global_state))
    {
      return nullptr;
    }
    return result;
  }

  unique_ptr<LocalTableFunctionState> AirportArrowScanInitLocal(ExecutionContext &context,
                                                                TableFunctionInitInput &input,
                                                                GlobalTableFunctionState *global_state_p)
  {
    return AirportArrowScanInitLocalInternal(context.client, input, global_state_p);
  }

  void AddTakeFlightFunction(DatabaseInstance &instance)
  {

    auto take_flight_function_set = TableFunctionSet("airport_take_flight");

    auto take_flight_function_with_descriptor = TableFunction(
        "airport_take_flight",
        {LogicalType::VARCHAR, LogicalType::ANY},
        AirportTakeFlight,
        take_flight_bind,
        AirportArrowScanInitGlobal,
        AirportArrowScanInitLocal);

    take_flight_function_with_descriptor.named_parameters["auth_token"] = LogicalType::VARCHAR;
    take_flight_function_with_descriptor.named_parameters["secret"] = LogicalType::VARCHAR;
    take_flight_function_with_descriptor.named_parameters["ticket"] = LogicalType::BLOB;
    take_flight_function_with_descriptor.named_parameters["headers"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
    take_flight_function_with_descriptor.pushdown_complex_filter = take_flight_complex_filter_pushdown;

    take_flight_function_with_descriptor.cardinality = airport_take_flight_cardinality;
    //    take_flight_function_with_descriptor.get_batch_index = nullptr;
    take_flight_function_with_descriptor.projection_pushdown = true;
    take_flight_function_with_descriptor.filter_pushdown = false;
    take_flight_function_with_descriptor.table_scan_progress = take_flight_scan_progress;
    take_flight_function_set.AddFunction(take_flight_function_with_descriptor);

    auto take_flight_function_with_pointer = TableFunction(
        "airport_take_flight",
        {LogicalType::POINTER, LogicalType::VARCHAR},
        AirportTakeFlight,
        take_flight_bind_with_pointer,
        AirportArrowScanInitGlobal,
        AirportArrowScanInitLocal);

    take_flight_function_with_pointer.named_parameters["auth_token"] = LogicalType::VARCHAR;
    take_flight_function_with_pointer.named_parameters["secret"] = LogicalType::VARCHAR;
    take_flight_function_with_pointer.pushdown_complex_filter = take_flight_complex_filter_pushdown;

    // Add support for optional named paraemters that would be appended to the descriptor
    // of the flight, ideally parameters would be JSON encoded.

    take_flight_function_with_pointer.cardinality = airport_take_flight_cardinality;
    //    take_flight_function_with_pointer.get_batch_index = nullptr;
    take_flight_function_with_pointer.projection_pushdown = true;
    take_flight_function_with_pointer.filter_pushdown = false;
    take_flight_function_with_pointer.table_scan_progress = take_flight_scan_progress;
    take_flight_function_with_pointer.statistics = airport_take_flight_statistics;

    take_flight_function_set.AddFunction(take_flight_function_with_pointer);

    ExtensionUtil::RegisterFunction(instance, take_flight_function_set);
  }

  std::string AirportNameForField(const string &name, idx_t col_idx)
  {
    if (name.empty())
    {
      return string("v") + to_string(col_idx);
    }
    return name;
  }

}