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

  struct AirportGetFlightColumnStatistics
  {
    std::string flight_descriptor;
    std::string column_name;
    std::string type;

    MSGPACK_DEFINE_MAP(flight_descriptor, column_name, type)
  };

  struct AirportGetFlightColumnStatisticsNumericValue
  {
    bool boolean = false;
    int8_t tinyint = 0;
    int16_t smallint = 0;
    int32_t integer = 0;
    int64_t bigint = 0;
    uint8_t utinyint = 0;
    uint16_t usmallint = 0;
    uint32_t uinteger = 0;
    uint64_t ubigint = 0;

    uint64_t hugeint_high = 0;
    uint64_t hugeint_low = 0;

    float float_ = 0.0;   // NOLINT
    double double_ = 0.0; // NOLINT

    MSGPACK_DEFINE_MAP(boolean, tinyint, smallint, integer, bigint, utinyint, usmallint, uinteger, ubigint,
                       hugeint_high, hugeint_low,
                       float_, double_)
  };

  static Value AirportGetValueForDuckDBType(LogicalTypeId type, const AirportGetFlightColumnStatisticsNumericValue &v)
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

  struct AirportGetFlightColumnStatisticsNumericStatsData
  {
    //! Whether or not the value has a max value
    bool has_min = false;
    //! Whether or not the segment has a min value
    bool has_max = false;
    //! The minimum value of the segment
    AirportGetFlightColumnStatisticsNumericValue min;
    //! The maximum value of the segment
    AirportGetFlightColumnStatisticsNumericValue max;

    MSGPACK_DEFINE_MAP(has_min, has_max, min, max)
  };

  struct AirportGetFlightColumnStatisticsStringData
  {
    std::string min;
    std::string max;
    MSGPACK_DEFINE_MAP(min, max)
  };

  struct AirportGetFlightColumnStatisticsResult
  {
    //! Whether or not the segment can contain NULL values
    bool has_null = false;
    //! Whether or not the segment can contain values that are not null
    bool has_no_null = false;
    // estimate that one may have even if distinct_stats==nullptr
    idx_t distinct_count = 0;

    //! Numeric and String stats
    AirportGetFlightColumnStatisticsNumericStatsData numeric_stats;
    AirportGetFlightColumnStatisticsStringData string_stats;

    MSGPACK_DEFINE_MAP(has_null, has_no_null, distinct_count, numeric_stats, string_stats)
  };

  unique_ptr<BaseStatistics> airport_take_flight_statistics(ClientContext &context, const FunctionData *bind_data, column_t column_index)
  {
    auto &data = bind_data->Cast<AirportTakeFlightBindData>();

    // printf("Requesting statistics for column %llu\n", column_index);

    if (column_index == COLUMN_IDENTIFIER_ROW_ID)
    {
      return make_uniq<BaseStatistics>(BaseStatistics::CreateEmpty(LogicalType::BIGINT));
    }
    else if (column_index == COLUMN_IDENTIFIER_EMPTY)
    {
      return make_uniq<BaseStatistics>(BaseStatistics::CreateEmpty(LogicalType::BOOLEAN));
    }

    // So we need to map the column id to the logical type.
    auto &schema = data.schema_root.arrow_schema.children[column_index];

    auto &config = DBConfig::GetConfig(context);

    // printf("Column name is %s\n", schema->name);
    auto arrow_type = ArrowType::GetArrowLogicalType(config, *schema);
    if (schema->dictionary)
    {
      auto dictionary_type = ArrowType::GetArrowLogicalType(config, *schema->dictionary);
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
    airport_add_normal_headers(call_options, data.take_flight_params(), data.trace_id());

    auto &server_location = data.take_flight_params().server_location();

    AirportGetFlightColumnStatistics params;
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(
        params.flight_descriptor,
        data.descriptor().SerializeToString(),
        server_location,
        "take_flight_statistics");
    params.column_name = schema->name;
    params.type = duck_type.ToString();

    auto flight_client = AirportAPI::FlightClientForLocation(server_location);

    AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, "get_flight_column_statistics", params);

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto action_results,
                                            flight_client->DoAction(call_options, action),
                                            server_location,
                                            "take_flight_statitics");

    // The only item returned is a serialized flight info.
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto stats_buffer,
                                            action_results->Next(),
                                            server_location,
                                            "reading take_flight_statistics for a column");

    std::string_view serialized_column_statistics(reinterpret_cast<const char *>(stats_buffer->body->data()), stats_buffer->body->size());

    AIRPORT_MSGPACK_UNPACK(AirportGetFlightColumnStatisticsResult,
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
      NumericStats::SetMin(result, AirportGetValueForDuckDBType(duck_type.id(), col_stats.numeric_stats.min));
      NumericStats::SetMax(result, AirportGetValueForDuckDBType(duck_type.id(), col_stats.numeric_stats.max));
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

}