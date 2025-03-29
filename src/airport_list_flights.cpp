#include "airport_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"

// Arrow includes.
#include <arrow/flight/client.h>
#include "duckdb/main/extension_util.hpp"

#include "duckdb/main/secret/secret_manager.hpp"

#include "airport_json_common.hpp"
#include "airport_json_serializer.hpp"
#include "airport_macros.hpp"
#include "airport_secrets.hpp"
#include "airport_request_headers.hpp"
#include "storage/airport_catalog.hpp"

namespace flight = arrow::flight;

namespace duckdb
{

  struct ListFlightsBindData : public TableFunctionData
  {
    // This is is the location of the server
    string server_location;

    // This is the auth token.
    string auth_token;

    // This is the criteria that will be passed the list flights.
    string criteria;

    // A JSON representation of filters being applied to the results,
    // which will be passed to the server as a GRPC header.
    string json_filters;
  };

  struct ListFlightsGlobalState : public GlobalTableFunctionState
  {
  public:
    std::shared_ptr<flight::FlightClient> flight_client_;
    std::unique_ptr<flight::FlightListing> listing;

    ListFlightsGlobalState(std::shared_ptr<flight::FlightClient> flight_client)
    {
      flight_client_ = flight_client;
    }

    idx_t MaxThreads() const override
    {
      return 1;
    }

    static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input)
    {
      auto &bind_data = input.bind_data->Cast<ListFlightsBindData>();

      auto flight_client = AirportAPI::FlightClientForLocation(bind_data.server_location);

      return make_uniq<ListFlightsGlobalState>(flight_client);
    }
  };

  static unique_ptr<FunctionData> list_flights_bind(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types, vector<string> &names)
  {
    auto server_location = input.inputs[0].ToString();
    string criteria = "";
    if (input.inputs.size() > 1)
    {
      criteria = input.inputs[1].ToString();
    }

    string auth_token = "";
    string secret_name = "";

    for (auto &kv : input.named_parameters)
    {
      auto loption = StringUtil::Lower(kv.first);
      if (loption == "auth_token")
      {
        auth_token = StringValue::Get(kv.second);
      }
      else if (loption == "secret")
      {
        secret_name = StringValue::Get(kv.second);
      }
    }

    auth_token = AirportAuthTokenForLocation(context, server_location, secret_name, auth_token);

    auto ret = make_uniq<ListFlightsBindData>();
    ret->server_location = server_location;
    ret->auth_token = auth_token;
    ret->criteria = criteria;

    // ordered - boolean
    // total_records - BIGINT
    // total_bytes - BIGINT
    // metadata - bytes

    child_list_t<LogicalType> flight_descriptor_members = {
        {"cmd", LogicalTypeId::BLOB},
        {"path", LogicalType::LIST(LogicalTypeId::VARCHAR)}};

    auto endpoint_type = LogicalType::STRUCT({{"ticket", LogicalTypeId::BLOB},
                                              {"location", LogicalType::LIST(LogicalTypeId::VARCHAR)},
                                              {"expiration_time", LogicalTypeId::TIMESTAMP},
                                              {"app_metadata", LogicalTypeId::BLOB}});

    std::initializer_list<duckdb::LogicalType> table_types = {
        LogicalType::UNION(flight_descriptor_members),
        LogicalType::LIST(endpoint_type),
        LogicalType::BOOLEAN,
        LogicalType::BIGINT,
        LogicalType::BIGINT,
        LogicalType::BLOB,
        LogicalType::VARCHAR};
    return_types.insert(return_types.end(), table_types.begin(), table_types.end());

    auto list_flights_field_names = {
        "flight_descriptor",
        "endpoint",
        "ordered",
        "total_records",
        "total_bytes",
        "app_metadata",
        "schema"};
    names.insert(names.end(), list_flights_field_names.begin(), list_flights_field_names.end());

    return std::move(ret);
  }

  static void list_flights(ClientContext &context, TableFunctionInput &data, DataChunk &output)
  {
    auto &bind_data = data.bind_data->Cast<ListFlightsBindData>();
    auto &global_state = data.global_state->Cast<ListFlightsGlobalState>();

    if (global_state.listing == nullptr)
    {
      // Now send a list flights request.
      arrow::flight::FlightCallOptions call_options;
      airport_add_standard_headers(call_options, bind_data.server_location);

      // FIXME: this will fail with large filter sizes, so its best not to pass it here.
      call_options.headers.emplace_back("airport-duckdb-json-filters", bind_data.json_filters);

      airport_add_authorization_header(call_options, bind_data.auth_token);
      // printf("Calling with filters: %s\n", bind_data.json_filters.c_str());

      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(global_state.listing, global_state.flight_client_->ListFlights(call_options, {bind_data.criteria}), bind_data.server_location, "");
    }

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto flight_info,
                                            global_state.listing->Next(),
                                            bind_data.server_location,
                                            "");

    if (flight_info == nullptr)
    {
      // There are no more flights to return.
      output.SetCardinality(0);
      return;
    }

    const auto max_rows = STANDARD_VECTOR_SIZE;

    auto &descriptor_entries = StructVector::GetEntries(output.data[0]);
    auto descriptor_type_tag_data = FlatVector::GetData<uint8_t>(*descriptor_entries[0]);
    auto descriptor_cmd_data = FlatVector::GetData<string_t>(*descriptor_entries[1]);

    // Flat vector of list entries.
    auto descriptor_path_data = ListVector::GetData(*descriptor_entries[2]);
    auto endpoint_data = ListVector::GetData(output.data[1]);

    unsigned int output_row_index = 0;
    while (flight_info != nullptr && output_row_index < max_rows)
    {
      auto descriptor = flight_info->descriptor();

      switch (descriptor.type)
      {
      case flight::FlightDescriptor::CMD:
      {
        descriptor_type_tag_data[output_row_index] = 0;
        descriptor_cmd_data[output_row_index] = StringVector::AddStringOrBlob(*descriptor_entries[1], descriptor.cmd);
        FlatVector::Validity(*descriptor_entries[2]).SetInvalid(output_row_index);
        descriptor_path_data[output_row_index].length = 0;
        descriptor_path_data[output_row_index].offset = 0;
      };
      break;
      case flight::FlightDescriptor::PATH:
      {
        descriptor_type_tag_data[output_row_index] = 1;
        FlatVector::Validity(*descriptor_entries[1]).SetInvalid(output_row_index);

        auto current_size = ListVector::GetListSize(*descriptor_entries[2]);
        auto new_size = current_size + descriptor.path.size();

        if (ListVector::GetListCapacity(*descriptor_entries[2]) < new_size)
        {
          ListVector::Reserve(*descriptor_entries[2], new_size);
        }

        auto path_values = ListVector::GetEntry(*descriptor_entries[2]);
        auto path_parts = FlatVector::GetData<string_t>(path_values);

        for (size_t i = 0; i < descriptor.path.size(); i++)
        {
          path_parts[current_size + i] = StringVector::AddString(ListVector::GetEntry(*descriptor_entries[2]), descriptor.path[i]);
        }

        descriptor_path_data[output_row_index].length = descriptor.path.size();
        descriptor_path_data[output_row_index].offset = current_size;

        ListVector::SetListSize(*descriptor_entries[2], new_size);
      }
      break;
      default:
        throw InvalidInputException("Unknown Arrow Flight descriptor type encountered while listing flights");
      }

      // Now lets make a fake endpoint struct.
      auto endpoint_list_current_size = ListVector::GetListSize(output.data[1]);
      auto endpoint_list_new_size = endpoint_list_current_size + flight_info->endpoints().size();

      if (ListVector::GetListCapacity(output.data[1]) < endpoint_list_new_size)
      {
        ListVector::Reserve(output.data[1], endpoint_list_new_size);
      }

      auto &endpoint_entries = StructVector::GetEntries(ListVector::GetEntry(output.data[1]));

      auto endpoint_ticket_data = FlatVector::GetData<string_t>(*endpoint_entries[0]);
      auto endpoint_location_data = ListVector::GetData(*endpoint_entries[1]);
      auto endpoint_expiration_data = FlatVector::GetData<int64_t>(*endpoint_entries[2]);
      auto endpoint_metadata_data = FlatVector::GetData<string_t>(*endpoint_entries[3]);

      // Lets deal with the endpoints.
      for (size_t endpoint_index = 0; endpoint_index < flight_info->endpoints().size(); endpoint_index++)
      {
        auto endpoint = flight_info->endpoints()[endpoint_index];
        endpoint_ticket_data[endpoint_list_current_size + endpoint_index] = StringVector::AddStringOrBlob(*endpoint_entries[0], endpoint.ticket.ticket);
        if (endpoint.expiration_time.has_value())
        {
          endpoint_expiration_data[endpoint_list_current_size + endpoint_index] = endpoint.expiration_time.value().time_since_epoch().count();
        }
        else
        {
          // No expiration is set.
          FlatVector::Validity(*endpoint_entries[2]).SetInvalid(endpoint_list_current_size + endpoint_index);
        }
        endpoint_metadata_data[endpoint_list_current_size + endpoint_index] = StringVector::AddStringOrBlob(*endpoint_entries[3], endpoint.app_metadata);

        // Now deal with the locations of this endpoint.
        auto endpoint_location_current_size = ListVector::GetListSize(*endpoint_entries[1]);
        auto endpoint_location_new_size = endpoint_location_current_size + endpoint.locations.size();
        if (ListVector::GetListCapacity(*endpoint_entries[1]) < endpoint_location_new_size)
        {
          ListVector::Reserve(*endpoint_entries[1], endpoint_location_new_size);
        }

        auto endpoint_location_parts = FlatVector::GetData<string_t>(ListVector::GetEntry(*endpoint_entries[1]));

        for (size_t location_index = 0; location_index < endpoint.locations.size(); location_index++)
        {
          endpoint_location_parts[endpoint_location_current_size + location_index] = StringVector::AddStringOrBlob(ListVector::GetEntry(*endpoint_entries[1]), endpoint.locations[location_index].ToString());
        }

        endpoint_location_data[endpoint_list_current_size].length = endpoint.locations.size();
        endpoint_location_data[endpoint_list_current_size].offset = endpoint_location_current_size;

        ListVector::SetListSize(*endpoint_entries[1], endpoint_location_new_size);
      }

      endpoint_data[output_row_index].length = flight_info->endpoints().size();
      endpoint_data[output_row_index].offset = endpoint_list_current_size;

      ListVector::SetListSize(output.data[1], endpoint_list_new_size);

      FlatVector::GetData<bool>(output.data[2])[output_row_index] = flight_info->ordered();
      FlatVector::GetData<uint64_t>(output.data[3])[output_row_index] = flight_info->total_records();
      FlatVector::GetData<uint64_t>(output.data[4])[output_row_index] = flight_info->total_bytes();
      FlatVector::GetData<string_t>(output.data[5])[output_row_index] = StringVector::AddStringOrBlob(output.data[5], flight_info->app_metadata());

      std::shared_ptr<arrow::Schema> info_schema;
      arrow::ipc::DictionaryMemo dictionary_memo;
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(info_schema, flight_info->GetSchema(&dictionary_memo), bind_data.server_location, descriptor, "");
      FlatVector::GetData<string_t>(output.data[6])[output_row_index] = StringVector::AddStringOrBlob(output.data[6], info_schema->ToString());

      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(flight_info, global_state.listing->Next(), bind_data.server_location, "");
      output_row_index++;
    }

    output.SetCardinality(output_row_index);
  }

  static void list_flights_complex_filter_pushdown(
      ClientContext &context,
      LogicalGet &get,
      FunctionData *bind_data_p,
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
      auto serializer = AirportJsonSerializer(doc, true, true, true);
      f->Serialize(serializer);
      yyjson_mut_arr_append(filters_arr, serializer.GetRootObject());
    }

    yyjson_mut_obj_add_val(doc, result_obj, "filters", filters_arr);
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

    auto &bind_data = bind_data_p->Cast<ListFlightsBindData>();

    bind_data.json_filters = json_result;
  }

  void AddListFlightsFunction(DatabaseInstance &instance)
  {
    auto list_flights_functions = TableFunctionSet("airport_list_flights");

    auto with_criteria = TableFunction(
        "airport_list_flights",
        {LogicalType::VARCHAR, LogicalType::VARCHAR},
        list_flights,
        list_flights_bind,
        ListFlightsGlobalState::Init);

    with_criteria.named_parameters["auth_token"] = LogicalType::VARCHAR;
    with_criteria.named_parameters["secret"] = LogicalType::VARCHAR;

    with_criteria.pushdown_complex_filter = list_flights_complex_filter_pushdown;
    with_criteria.filter_pushdown = false;
    list_flights_functions.AddFunction(with_criteria);

    auto without_criteria = TableFunction(
        "airport_list_flights",
        {LogicalType::VARCHAR},
        list_flights,
        list_flights_bind,
        ListFlightsGlobalState::Init);

    without_criteria.named_parameters["auth_token"] = LogicalType::VARCHAR;
    without_criteria.named_parameters["secret"] = LogicalType::VARCHAR;
    without_criteria.pushdown_complex_filter = list_flights_complex_filter_pushdown;
    without_criteria.filter_pushdown = false;
    list_flights_functions.AddFunction(without_criteria);

    ExtensionUtil::RegisterFunction(instance, list_flights_functions);
  }

}
