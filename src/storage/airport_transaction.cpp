#include "airport_extension.hpp"
#include "storage/airport_transaction.hpp"
#include "storage/airport_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "airport_request_headers.hpp"
#include "airport_macros.hpp"
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/buffer.h>
#include "airport_macros.hpp"

namespace duckdb
{

  AirportTransaction::AirportTransaction(AirportCatalog &airport_catalog, TransactionManager &manager, ClientContext &context)
      : Transaction(manager, context), access_mode(airport_catalog.access_mode)
  {
    credentials = airport_catalog.credentials;
    catalog_name = airport_catalog.internal_name;
  }

  AirportTransaction::~AirportTransaction() = default;

  struct GetTransactionIdentifierResult
  {
    std::optional<std::string> identifier;
    MSGPACK_DEFINE(identifier)
  };

  std::optional<string> AirportTransaction::GetTransactionIdentifier()
  {
    auto flight_client = AirportAPI::FlightClientForLocation(credentials->location);

    arrow::flight::FlightCallOptions call_options;
    airport_add_standard_headers(call_options, credentials->location);
    airport_add_authorization_header(call_options, credentials->auth_token);

    arrow::flight::Action action{"get_transaction_identifier", arrow::Buffer::FromString(catalog_name)};

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto action_results,
                                            flight_client->DoAction(call_options, action),
                                            credentials->location,
                                            "calling get_transaction_identifier action");

    // The only item returned is a serialized flight info.
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto result_buffer,
                                            action_results->Next(),
                                            credentials->location,
                                            "reading get_transaction_identifier action result");

    AIRPORT_MSGPACK_UNPACK(
        GetTransactionIdentifierResult, result,
        (*(result_buffer->body)),
        credentials->location,
        "File to parse msgpack encoded get_transaction_identifier response");

    AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), credentials->location, "");

    return result.identifier;
  }

  void AirportTransaction::Start()
  {
    transaction_state = AirportTransactionState::TRANSACTION_NOT_YET_STARTED;
    // Get an identifier from the server that will be passed as airport-transaction-id
    // in requests.
    identifier = GetTransactionIdentifier();
  }
  void AirportTransaction::Commit()
  {
    if (transaction_state == AirportTransactionState::TRANSACTION_STARTED)
    {
      transaction_state = AirportTransactionState::TRANSACTION_FINISHED;
    }
  }
  void AirportTransaction::Rollback()
  {
    if (transaction_state == AirportTransactionState::TRANSACTION_STARTED)
    {
      transaction_state = AirportTransactionState::TRANSACTION_FINISHED;
    }
  }

  AirportTransaction &AirportTransaction::Get(ClientContext &context, Catalog &catalog)
  {
    return Transaction::Get(context, catalog).Cast<AirportTransaction>();
  }

} // namespace duckdb
