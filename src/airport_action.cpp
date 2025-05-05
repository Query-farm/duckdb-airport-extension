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

  struct ActionBindData : public TableFunctionData
  {
    // This is is the location of the server
    std::string server_location;

    // This is the auth token.
    std::string auth_token;

    std::string action_name;

    // The parameters that will be passed to the action.
    std::optional<std::string> parameter;

    std::unordered_map<string, std::vector<string>> user_supplied_headers;

    explicit ActionBindData(std::string server_location,
                            std::string auth_token,
                            std::string action_name,
                            std::optional<std::string> parameter,
                            std::unordered_map<string, std::vector<string>> user_supplied_headers)
        : server_location(std::move(server_location)),
          auth_token(std::move(auth_token)),
          action_name(std::move(action_name)),
          parameter(std::move(parameter)),
          user_supplied_headers(std::move(user_supplied_headers))
    {
    }
  };

  struct ActionGlobalState : public GlobalTableFunctionState
  {
  public:
    const std::shared_ptr<flight::FlightClient> flight_client_;

    // If the action returns a lot of items, we need to keep the result stream here.
    std::unique_ptr<arrow::flight::ResultStream> result_stream;

    explicit ActionGlobalState(std::shared_ptr<flight::FlightClient> flight_client) : flight_client_(flight_client)
    {
    }

    idx_t MaxThreads() const override
    {
      return 1;
    }

    static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input)
    {
      const auto &bind_data = input.bind_data->Cast<ActionBindData>();

      auto flight_client = AirportAPI::FlightClientForLocation(bind_data.server_location);

      return make_uniq<ActionGlobalState>(flight_client);
    }
  };

  static unique_ptr<FunctionData> do_action_bind(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types, vector<string> &names)
  {
    if (input.inputs.size() < 2)
    {
      throw BinderException("airport_action requires at least 2 arguments");
    }

    auto server_location = input.inputs[0].ToString();
    auto action_name = input.inputs[1].ToString();

    std::optional<std::string> parameter = std::nullopt;
    if (input.inputs.size() > 2)
    {
      parameter = input.inputs[2].ToString();
    }

    string auth_token = "";
    string secret_name = "";
    std::unordered_map<string, std::vector<string>> user_supplied_headers;

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
      else if (loption == "headers")
      {
        // Now we need to parse out the map contents.
        auto &children = duckdb::MapValue::GetChildren(kv.second);

        for (auto &value_pair : children)
        {
          auto &child_struct = duckdb::StructValue::GetChildren(value_pair);
          auto key = StringValue::Get(child_struct[0]);
          auto value = StringValue::Get(child_struct[1]);

          user_supplied_headers[key].push_back(value);
        }
      }
    }

    auth_token = AirportAuthTokenForLocation(context, server_location, secret_name, auth_token);

    auto ret = make_uniq<ActionBindData>(server_location,
                                         auth_token,
                                         action_name,
                                         parameter,
                                         user_supplied_headers);

    return_types.emplace_back(LogicalType::BLOB);
    names.emplace_back("result");

    return ret;
  }

  static void do_action(ClientContext &context, TableFunctionInput &data, DataChunk &output)
  {
    auto &bind_data = data.bind_data->Cast<ActionBindData>();
    auto &global_state = data.global_state->Cast<ActionGlobalState>();

    auto &server_location = bind_data.server_location;

    if (global_state.result_stream == nullptr)
    {
      // Now send a list flights request.
      arrow::flight::FlightCallOptions call_options;
      airport_add_standard_headers(call_options, bind_data.server_location);

      // FIXME: this will fail with large filter sizes, so its best not to pass it here.
      call_options.headers.emplace_back("airport-action-name", bind_data.action_name);
      airport_add_authorization_header(call_options, bind_data.auth_token);
      // printf("Calling with filters: %s\n", bind_data.json_filters.c_str());

      arrow::flight::Action action{
          bind_data.action_name,
          bind_data.parameter.has_value() ? std::make_shared<arrow::Buffer>(
                                                reinterpret_cast<const uint8_t *>(bind_data.parameter.value().data()),
                                                bind_data.parameter.value().size())
                                          : std::make_shared<arrow::Buffer>(nullptr, 0)};

      AIRPORT_ASSIGN_OR_RAISE_LOCATION(global_state.result_stream,
                                       global_state.flight_client_->DoAction(call_options, action),
                                       server_location,
                                       "airport_action");
    }

    AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto action_result, global_state.result_stream->Next(), server_location, "airport_action next item");

    if (action_result == nullptr)
    {
      // There are no results on the stream.
      AIRPORT_ARROW_ASSERT_OK_LOCATION(global_state.result_stream->Drain(), server_location, "airport_action drain");
      output.SetCardinality(0);
      return;
    }

    FlatVector::GetData<string_t>(output.data[0])[0] = StringVector::AddStringOrBlob(output.data[0],
                                                                                     action_result->body->ToString());

    output.SetCardinality(1);
  }

  void AirportAddActionFlightFunction(DatabaseInstance &instance)
  {
    auto do_action_functions = TableFunctionSet("airport_action");

    auto with_parameter = TableFunction(
        "airport_action",
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        do_action,
        do_action_bind,
        ActionGlobalState::Init);

    with_parameter.named_parameters["auth_token"] = LogicalType::VARCHAR;
    with_parameter.named_parameters["secret"] = LogicalType::VARCHAR;
    with_parameter.named_parameters["headers"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);

    do_action_functions.AddFunction(with_parameter);

    auto without_parameter = TableFunction(
        "airport_action",
        {LogicalType::VARCHAR, LogicalType::VARCHAR},
        do_action,
        do_action_bind,
        ActionGlobalState::Init);

    without_parameter.named_parameters["auth_token"] = LogicalType::VARCHAR;
    without_parameter.named_parameters["secret"] = LogicalType::VARCHAR;
    without_parameter.named_parameters["headers"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);

    do_action_functions.AddFunction(without_parameter);

    ExtensionUtil::RegisterFunction(instance, do_action_functions);
  }

}
