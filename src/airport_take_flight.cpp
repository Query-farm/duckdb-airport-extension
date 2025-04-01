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

namespace duckdb
{

  template <typename T>
  static std::vector<std::string> convert_to_strings(const std::vector<T> &vec)
  {
    std::vector<std::string> result(vec.size());
    std::transform(vec.begin(), vec.end(), result.begin(), [](const T &elem)
                   { return std::to_string(elem); });
    return result;
  }

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

  static void AirportTakeFlightDetermineNamesAndTypes(
      const ArrowSchemaWrapper &schema_root,
      ClientContext &context,
      vector<LogicalType> &return_types,
      vector<string> &names,
      ArrowTableType &arrow_table,
      idx_t &rowid_column_index)
  {
    rowid_column_index = COLUMN_IDENTIFIER_ROW_ID;
    for (idx_t col_idx = 0;
         col_idx < (idx_t)schema_root.arrow_schema.n_children; col_idx++)
    {
      auto &schema = *schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("airport_take_flight: released schema passed");
      }
      auto arrow_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), schema);

      // Determine if the column is the rowid column by looking at the metadata
      // on the column.
      bool is_rowid_column = false;
      if (schema.metadata != nullptr)
      {
        auto column_metadata = ArrowSchemaMetadata(schema.metadata);

        auto comment = column_metadata.GetOption("is_rowid");
        if (!comment.empty())
        {
          is_rowid_column = true;
          rowid_column_index = col_idx;
        }
      }

      if (schema.dictionary)
      {
        auto dictionary_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), *schema.dictionary);
        arrow_type->SetDictionary(std::move(dictionary_type));
      }

      if (!is_rowid_column)
      {
        return_types.emplace_back(arrow_type->GetDuckType());
      }

      arrow_table.AddColumn(is_rowid_column ? COLUMN_IDENTIFIER_ROW_ID : col_idx, std::move(arrow_type));

      auto format = string(schema.format);
      auto name = string(schema.name);
      if (name.empty())
      {
        name = string("v") + to_string(col_idx);
      }
      if (!is_rowid_column)
      {
        names.push_back(name);
      }
      // printf("Added column with id %lld %s\n", is_rowid_column ? COLUMN_IDENTIFIER_ROW_ID : col_idx, name.c_str());
    }
    QueryResult::DeduplicateColumns(names);
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
      std::shared_ptr<const AirportGetFlightInfoTableFunctionParameters> table_function_parameters)
  {
    // Create a UID for tracing.
    const auto trace_uuid = airport_trace_id();

    // The main thing that needs to be answered in this function
    // are the names and return types and establishing the bind data.
    //
    // If the cached_flight_info_ptr is not null we should use the schema
    // from that flight info otherwise it should be requested.

    auto flight_client = AirportAPI::FlightClientForLocation(take_flight_params.server_location());

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
          schema,
          nullptr);
    }
    else
    {

      std::unique_ptr<arrow::flight::FlightInfo> retrieved_flight_info;

      if (table_function_parameters != nullptr)
      {
        // Rather than calling GetFlightInfo we will call DoAction and get
        // get the flight info that way, since it allows us to serialize
        // all of the data we need to send instead of just the flight name.
        std::stringstream packed_buffer;
        msgpack::pack(packed_buffer, table_function_parameters);
        arrow::flight::Action action{"get_flight_info_table_function",
                                     arrow::Buffer::FromString(packed_buffer.str())};

        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto action_results, flight_client->DoAction(call_options, action), take_flight_params.server_location(), "airport_get_flight_info_table_function");

        // The only item returned is a serialized flight info.
        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto serialized_flight_info_buffer, action_results->Next(), take_flight_params.server_location(), "reading get_flight_info for table function");

        std::string_view serialized_flight_info(reinterpret_cast<const char *>(serialized_flight_info_buffer->body->data()), serialized_flight_info_buffer->body->size());

        // Now deserialize that flight info so we can use it.
        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(retrieved_flight_info, arrow::flight::FlightInfo::Deserialize(serialized_flight_info), take_flight_params.server_location(), "deserialize flight info");

        AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), take_flight_params.server_location(), "");
      }
      else
      {
        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(retrieved_flight_info,
                                                           flight_client->GetFlightInfo(call_options, descriptor),
                                                           take_flight_params.server_location(),
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
                                                         take_flight_params.server_location(),
                                                         descriptor,
                                                         "");

      scan_data = make_uniq<AirportTakeFlightScanData>(
          AirportLocationDescriptor(take_flight_params.server_location(), descriptor),
          schema,
          nullptr);
    }

    // Store the flight info on the bind data.

    // After doing a little bit of examination of the DuckDb sources, I learned that
    // that DuckDb supports the "C" interface of Arrow, this means that DuckDB doens't
    // actually have a dependency on Arrow.
    //
    // Arrow Flight requires a dependency on the full Arrow library, because of all of
    // the dependencies.
    //
    // Thankfully there is a "bridge" interface between the C++ based Arrow types returned
    // by the C++ Arrow library and the C based Arrow types that DuckDB already knows how to
    // consume.
    auto ret = make_uniq<AirportTakeFlightBindData>(
        (stream_factory_produce_t)&AirportCreateStream,
        (uintptr_t)scan_data.get());

    ret->estimated_records = estimated_records;
    ret->scan_data = std::move(scan_data);
    ret->flight_client = flight_client;
    ret->take_flight_params = make_uniq<AirportTakeFlightParameters>(take_flight_params);

    ret->trace_id = trace_uuid;
    ret->table_function_parameters = table_function_parameters;

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        ExportSchema(*schema, &ret->schema_root.arrow_schema),
        take_flight_params.server_location(),
        descriptor,
        "ExportSchema");

    AirportTakeFlightDetermineNamesAndTypes(
        ret->schema_root,
        context,
        return_types,
        names,
        ret->arrow_table,
        ret->rowid_column_index);

    return std::move(ret);
  }

  static unique_ptr<FunctionData> take_flight_bind(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names)
  {
    auto server_location = input.inputs[0].ToString();
    auto params = AirportTakeFlightParameters(server_location, context, input);
    auto descriptor = flight_descriptor_from_value(input.inputs[1]);

    return AirportTakeFlightBindWithFlightDescriptor(
        params,
        descriptor,
        context,
        input, return_types, names, nullptr, nullptr);
  }

  static unique_ptr<FunctionData> take_flight_bind_with_pointer(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names)
  {
    //    auto server_location = input.inputs[0].ToString();

    if (input.inputs[0].IsNull())
    {
      throw BinderException("airport: take_flight_with_pointer, pointers to AirportTable cannot be null");
    }

    const auto info = reinterpret_cast<duckdb::AirportAPITable *>(input.inputs[0].GetPointer());

    auto params = AirportTakeFlightParameters(info->server_location(), context, input);

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
        nullptr);
  }

  void AirportTakeFlight(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
  {
    if (!data_p.local_state)
    {
      return;
    }
    auto &state = data_p.local_state->Cast<ArrowScanLocalState>();
    auto &global_state = data_p.global_state->Cast<AirportArrowScanGlobalState>();
    auto &airport_bind_data = data_p.bind_data->CastNoConst<AirportTakeFlightBindData>();

    //! Out of tuples in this chunk
    if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length)
    {
      if (!ArrowTableFunction::ArrowScanParallelStateNext(
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
      output.ReferenceColumns(state.all_columns, global_state.projection_ids);
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

  struct GetFlightColumnStatistics
  {
    std::string flight_descriptor;
    std::string column_name;
    std::string type;

    MSGPACK_DEFINE_MAP(flight_descriptor, column_name, type)
  };

  struct GetFlightColumnStatisticsNumericValue
  {
    bool boolean;
    int8_t tinyint;
    int16_t smallint;
    int32_t integer;
    int64_t bigint;
    uint8_t utinyint;
    uint16_t usmallint;
    uint32_t uinteger;
    uint64_t ubigint;

    uint64_t hugeint_high;
    uint64_t hugeint_low;

    float float_;   // NOLINT
    double double_; // NOLINT

    MSGPACK_DEFINE_MAP(boolean, tinyint, smallint, integer, bigint, utinyint, usmallint, uinteger, ubigint,
                       hugeint_high, hugeint_low,
                       float_, double_)
  };

  Value GetValueForType(LogicalTypeId type, const GetFlightColumnStatisticsNumericValue &v)
  {
    switch (type)
    {
    case LogicalTypeId::BOOLEAN:
      return Value::BOOLEAN(v.boolean);
    case LogicalTypeId::TINYINT:
      return Value::TINYINT(v.tinyint);
    case LogicalTypeId::SMALLINT:
      return Value::SMALLINT(v.smallint);
    case LogicalTypeId::INTEGER:
      return Value::INTEGER(v.integer);
    case LogicalTypeId::BIGINT:
      return Value::BIGINT(v.bigint);
    case LogicalTypeId::UTINYINT:
      return Value::UTINYINT(v.utinyint);
    case LogicalTypeId::USMALLINT:
      return Value::USMALLINT(v.usmallint);
    case LogicalTypeId::UINTEGER:
      return Value::UINTEGER(v.uinteger);
    case LogicalTypeId::UBIGINT:
      return Value::UBIGINT(v.ubigint);
    case LogicalTypeId::TIMESTAMP_TZ:
      return Value::TIMESTAMPTZ(timestamp_tz_t(v.bigint));
    case LogicalTypeId::HUGEINT:
    {
      hugeint_t t = ((hugeint_t)v.hugeint_high << 64) | v.hugeint_low;
      return Value::HUGEINT(t);
    }
    case LogicalTypeId::UHUGEINT:
    {
      uhugeint_t t = ((uhugeint_t)v.hugeint_high << 64) | v.hugeint_low;
      return Value::UHUGEINT(t);
    }
    case LogicalTypeId::FLOAT:
      return Value::FLOAT(v.float_);
    case LogicalTypeId::DOUBLE:
      return Value::DOUBLE(v.double_);
    default:
      throw InvalidInputException("Unknown type passed to GetValueForType");
    }
  }

  struct GetFlightColumnStatisticsNumericStatsData
  {
    //! Whether or not the value has a max value
    bool has_min;
    //! Whether or not the segment has a min value
    bool has_max;
    //! The minimum value of the segment
    GetFlightColumnStatisticsNumericValue min;
    //! The maximum value of the segment
    GetFlightColumnStatisticsNumericValue max;

    MSGPACK_DEFINE_MAP(has_min, has_max, min, max)
  };

  struct GetFlightColumnStatisticsStringData
  {
    std::string min;
    std::string max;
    MSGPACK_DEFINE_MAP(min, max)
  };

  struct GetFlightColumnStatisticsResult
  {
    //! Whether or not the segment can contain NULL values
    bool has_null;
    //! Whether or not the segment can contain values that are not null
    bool has_no_null;
    // estimate that one may have even if distinct_stats==nullptr
    idx_t distinct_count;

    //! Numeric and String stats
    GetFlightColumnStatisticsNumericStatsData numeric_stats;
    GetFlightColumnStatisticsStringData string_stats;

    MSGPACK_DEFINE_MAP(has_null, has_no_null, distinct_count, numeric_stats, string_stats)
  };

  static unique_ptr<BaseStatistics> take_flight_statistics(ClientContext &context, const FunctionData *bind_data, column_t column_index)
  {
    auto &data = bind_data->Cast<AirportTakeFlightBindData>();

    // printf("Requesting statistics for column %llu\n", column_index);

    if (column_index == COLUMN_IDENTIFIER_ROW_ID)
    {
      return make_uniq<BaseStatistics>(BaseStatistics::CreateEmpty(LogicalType::BIGINT));
    }

    // So we need to map the column id to the logical type.
    auto &schema = data.schema_root.arrow_schema.children[column_index];

    // printf("Column name is %s\n", schema->name);
    auto arrow_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), *schema);
    if (schema->dictionary)
    {
      auto dictionary_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), *schema->dictionary);
      arrow_type->SetDictionary(std::move(dictionary_type));
    }
    auto duck_type = arrow_type->GetDuckType();

    // Talk to the server to get the statistics for the column.
    // this will be done with a do_action call.
    // if it fails just assume the server doesn't implement statistics.
    if (!data.schema_root.arrow_schema.metadata)
    {
      // printf("No metadata attached to flight schema, so cannot produce stats\n");
      return make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(duck_type));
    }

    auto metadata = ArrowSchemaMetadata(data.schema_root.arrow_schema.metadata);
    auto stats = metadata.GetOption("can_produce_statistics");
    if (stats.empty())
    {
      // printf("Cannot produce stats for this column\n");
      return make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(duck_type));
    }

    // printf("Can produce statistics for this flight\n");

    arrow::flight::FlightCallOptions call_options;
    airport_add_normal_headers(call_options, *data.take_flight_params, data.trace_id);

    std::stringstream packed_buffer;

    auto &server_location = data.take_flight_params->server_location();

    GetFlightColumnStatistics params;
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(
        params.flight_descriptor,
        data.scan_data->descriptor().SerializeToString(),
        server_location,
        "take_flight_statistics");
    params.column_name = schema->name;
    params.type = duck_type.ToString();

    msgpack::pack(packed_buffer, params);
    arrow::flight::Action action{"get_flight_column_statistics",
                                 arrow::Buffer::FromString(packed_buffer.str())};

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto action_results,
                                            data.flight_client->DoAction(call_options, action),
                                            server_location,
                                            "take_flight_statitics");

    // The only item returned is a serialized flight info.
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto stats_buffer,
                                            action_results->Next(),
                                            server_location,
                                            "reading take_flight_statistics for a column");

    std::string_view serialized_column_statistics(reinterpret_cast<const char *>(stats_buffer->body->data()), stats_buffer->body->size());

    AIRPORT_MSGPACK_UNPACK(GetFlightColumnStatisticsResult,
                           col_stats,
                           (*(stats_buffer->body)),
                           server_location,
                           "File to parse msgpack encoded column statistics");

    AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(),
                                     server_location,
                                     "");

    switch (duck_type.id())
    {
    case LogicalTypeId::VARCHAR:
    {
      auto result = StringStats::CreateEmpty(LogicalType(duck_type.id()));
      if (col_stats.has_no_null)
      {
        result.SetHasNoNull();
      }
      if (col_stats.has_null)
      {
        result.SetHasNull();
      }
      result.SetDistinctCount(col_stats.distinct_count);
      StringStats::Update(result, col_stats.string_stats.min);
      StringStats::Update(result, col_stats.string_stats.max);
      return result.ToUnique();
    }
    case LogicalTypeId::BOOLEAN:
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
    case LogicalTypeId::UBIGINT:
    case LogicalTypeId::HUGEINT:
    case LogicalTypeId::UHUGEINT:
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
    case LogicalTypeId::TIMESTAMP_TZ:
    {
      auto result = NumericStats::CreateEmpty(LogicalType(duck_type.id()));
      if (col_stats.has_no_null)
      {
        result.SetHasNoNull();
      }
      if (col_stats.has_null)
      {
        result.SetHasNull();
      }
      result.SetDistinctCount(col_stats.distinct_count);
      NumericStats::SetMin(result, GetValueForType(duck_type.id(), col_stats.numeric_stats.min));
      NumericStats::SetMax(result, GetValueForType(duck_type.id(), col_stats.numeric_stats.max));
      return result.ToUnique();
    }
    case LogicalTypeId::ARRAY:
    case LogicalTypeId::LIST:
    case LogicalTypeId::MAP:
    case LogicalTypeId::STRUCT:
      return make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(duck_type));

    default:
      throw NotImplementedException("Statistics for type %s not implemented", duck_type.ToString());
    }
  }

  static unique_ptr<NodeStatistics> take_flight_cardinality(ClientContext &context, const FunctionData *data)
  {
    // To estimate the cardinality of the flight, we can peek at the flight information
    // that was retrieved during the bind function.
    //
    // This estimate does not take into account any filters that may have been applied
    //
    auto &bind_data = data->Cast<AirportTakeFlightBindData>();
    auto flight_estimated_records = bind_data.estimated_records;

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

  unique_ptr<ArrowArrayStreamWrapper> AirportProduceArrowScan(const ArrowScanFunctionData &function,
                                                              const vector<column_t> &column_ids, TableFilterSet *filters)
  {
    //! Generate Projection Pushdown Vector
    ArrowStreamParameters parameters;
    for (idx_t idx = 0; idx < column_ids.size(); idx++)
    {
      auto col_idx = column_ids[idx];
      if (col_idx != COLUMN_IDENTIFIER_ROW_ID)
      {
        auto &schema = *function.schema_root.arrow_schema.children[col_idx];
        parameters.projected_columns.projection_map[idx] = schema.name;
        parameters.projected_columns.columns.emplace_back(schema.name);
        parameters.projected_columns.filter_to_col[idx] = col_idx;
      }
    }
    parameters.filters = filters;

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

  static vector<flight::FlightEndpoint> GetFlightEndpoints(
      const std::unique_ptr<AirportTakeFlightParameters> &take_flight_params,
      const string &trace_id,
      const flight::FlightDescriptor &descriptor,
      std::shared_ptr<flight::FlightClient> flight_client,
      const std::string &json_filters,
      const vector<idx_t> &column_ids)
  {
    vector<flight::FlightEndpoint> endpoints;
    arrow::flight::FlightCallOptions call_options;
    auto &server_location = take_flight_params->server_location();

    airport_add_normal_headers(call_options, *take_flight_params, trace_id,
                               descriptor);

    AirportGetFlightEndpointsRequest endpoints_request;

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(
        endpoints_request.descriptor,
        descriptor.SerializeToString(),
        server_location,
        "get_flight_endpoints serialize flight descriptor");
    endpoints_request.parameters.json_filters = json_filters;
    endpoints_request.parameters.column_ids = column_ids;

    std::stringstream packed_buffer;
    msgpack::pack(packed_buffer, endpoints_request);
    arrow::flight::Action action{"get_flight_endpoints",
                                 arrow::Buffer::FromString(packed_buffer.str())};

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

    AIRPORT_MSGPACK_UNPACK(std::vector<std::string>, serialized_endpoints, serialized_endpoint_info, server_location, "File to parse msgpack encoded endpoints");

    AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), server_location, "get_flight_endpoints drain");

    for (const auto &endpoint : serialized_endpoints)
    {
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto deserialized_endpoint,
                                              arrow::flight::FlightEndpoint::Deserialize(endpoint),
                                              server_location,
                                              "deserialize flight endpoint");
      endpoints.push_back(deserialized_endpoint);
    }
    return endpoints;
  }

  unique_ptr<GlobalTableFunctionState> AirportArrowScanInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input)
  {
    auto &bind_data = input.bind_data->Cast<AirportTakeFlightBindData>();
    auto result = make_uniq<AirportArrowScanGlobalState>();

    // Ideally this is where we call GetFlightInfo to obtain the endpoints, but
    // GetFlightInfo can't take the predicate information, so we'll need to call an
    // action called get_flight_endpoints.
    //
    // FIXME: somehow the flight should be marked if it supports predicate pushdown.
    // right now I'm not sure what this is.
    //
    result->endpoints = GetFlightEndpoints(bind_data.take_flight_params,
                                           bind_data.trace_id,
                                           bind_data.scan_data->descriptor(),
                                           bind_data.flight_client,
                                           bind_data.json_filters,
                                           input.column_ids);

    D_ASSERT(result->endpoints.size() == 1);

    // Each endpoint can be processed on a different thread.
    result->max_threads = result->endpoints.size();

    auto &first_endpoint = result->endpoints[0];
    auto &first_location = first_endpoint.locations[0];

    std::shared_ptr<flight::FlightClient> client = bind_data.flight_client;
    auto server_location = first_location.ToString();
    if (first_location != flight::Location::ReuseConnection())
    {
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto location_client,
                                              flight::FlightClient::Connect(first_location),
                                              first_location.ToString(),
                                              "");
      client = std::move(location_client);
      server_location = bind_data.take_flight_params->server_location();
    }

    auto &descriptor = bind_data.scan_data->descriptor();

    arrow::flight::FlightCallOptions call_options;
    airport_add_normal_headers(call_options, *bind_data.take_flight_params, bind_data.trace_id,
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
        client->DoGet(
            call_options,
            first_endpoint.ticket),
        server_location,
        bind_data.scan_data->descriptor(),
        "");

    bind_data.scan_data->setStream(std::move(stream));

    result->stream = AirportProduceArrowScan(bind_data, input.column_ids, input.filters.get());

    return std::move(result);
  }

  static double take_flight_scan_progress(ClientContext &, const FunctionData *data, const GlobalTableFunctionState *global_state)
  {
    auto &bind_data = data->Cast<AirportTakeFlightBindData>();
    //    lock_guard<mutex> guard(bind_data.lock);

    // FIXME: this will have to be adapted for multiple endpoints
    return bind_data.scan_data->progress_ * 100.0;
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
        ArrowTableFunction::ArrowScanInitLocal);

    take_flight_function_with_descriptor.named_parameters["auth_token"] = LogicalType::VARCHAR;
    take_flight_function_with_descriptor.named_parameters["secret"] = LogicalType::VARCHAR;
    take_flight_function_with_descriptor.named_parameters["ticket"] = LogicalType::BLOB;
    take_flight_function_with_descriptor.named_parameters["headers"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
    take_flight_function_with_descriptor.pushdown_complex_filter = take_flight_complex_filter_pushdown;

    take_flight_function_with_descriptor.cardinality = take_flight_cardinality;
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
        ArrowTableFunction::ArrowScanInitLocal);

    take_flight_function_with_pointer.named_parameters["auth_token"] = LogicalType::VARCHAR;
    take_flight_function_with_pointer.named_parameters["secret"] = LogicalType::VARCHAR;
    take_flight_function_with_pointer.pushdown_complex_filter = take_flight_complex_filter_pushdown;

    // Add support for optional named paraemters that would be appended to the descriptor
    // of the flight, ideally parameters would be JSON encoded.

    take_flight_function_with_pointer.cardinality = take_flight_cardinality;
    //    take_flight_function_with_pointer.get_batch_index = nullptr;
    take_flight_function_with_pointer.projection_pushdown = true;
    take_flight_function_with_pointer.filter_pushdown = false;
    take_flight_function_with_pointer.table_scan_progress = take_flight_scan_progress;
    take_flight_function_with_pointer.statistics = take_flight_statistics;

    take_flight_function_set.AddFunction(take_flight_function_with_pointer);

    ExtensionUtil::RegisterFunction(instance, take_flight_function_set);
  }
}