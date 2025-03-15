#include "storage/airport_catalog_set.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/airport_schema_entry.hpp"
#include "airport_request_headers.hpp"
#include "storage/airport_catalog.hpp"
#include "airport_macros.hpp"
#include <arrow/buffer.h>
#include <msgpack.hpp>

namespace duckdb
{

  AirportCatalogSet::AirportCatalogSet(Catalog &catalog) : catalog(catalog), is_loaded(false)
  {
  }

  optional_ptr<CatalogEntry> AirportCatalogSet::GetEntry(ClientContext &context, const string &name)
  {
    lock_guard<mutex> l(entry_lock);
    if (!is_loaded)
    {
      is_loaded = true;
      LoadEntries(context);
    }
    auto entry = entries.find(name);
    if (entry == entries.end())
    {
      return nullptr;
    }
    return entry->second.get();
  }

  struct DropTableAction
  {
    std::string schema_name;
    std::string table_name;

    MSGPACK_DEFINE(schema_name, table_name)
  };

  void AirportCatalogSet::DropEntry(ClientContext &context, DropInfo &info)
  {
    // printf("AirportCatalogSet::DropEntry\n");
    // printf("Dropping entry: %s\n", info.ToString().c_str());
    // printf("Name: %s\n", info.name.c_str());
    // printf("Schema: %s\n", info.schema.c_str());

    if (!is_loaded)
    {
      is_loaded = true;
      LoadEntries(context);
    }

    auto &airport_catalog = catalog.Cast<AirportCatalog>();

    arrow::flight::FlightCallOptions call_options;

    airport_add_standard_headers(call_options, airport_catalog.credentials.location);
    airport_add_authorization_header(call_options, airport_catalog.credentials.auth_token);

    std::unique_ptr<arrow::flight::FlightClient> &flight_client = AirportAPI::FlightClientForLocation(airport_catalog.credentials.location);

    switch (info.type)
    {
    case CatalogType::TABLE_ENTRY:
    {
      auto schema_entry = entries.find(info.name);
      if (schema_entry == entries.end())
      {
        throw InvalidInputException("Table %s is not found", info.schema);
      }

      call_options.headers.emplace_back("airport-action-name", "drop_table");

      DropTableAction drop_table_action = {info.schema, info.name};
      std::stringstream packed_buffer;
      msgpack::pack(packed_buffer, drop_table_action);
      arrow::flight::Action action{"drop_table",
                                   arrow::Buffer::FromString(packed_buffer.str())};

      std::unique_ptr<arrow::flight::ResultStream> action_results;
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(action_results, flight_client->DoAction(call_options, action), airport_catalog.credentials.location, "airport_create_schema");

      // We aren't interested in anything from this call.
      AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), airport_catalog.credentials.location, "");

      entries.erase(info.name);
    }
    break;
    case CatalogType::SCHEMA_ENTRY:
    {
      call_options.headers.emplace_back("airport-action-name", "drop_schema");
      auto entry = entries.find(info.name);
      if (entry == entries.end())
      {
        throw InvalidInputException("Schema %s is not found", info.name);
      }

      arrow::flight::Action action{"drop_schema",
                                   arrow::Buffer::FromString(info.name)};
      std::unique_ptr<arrow::flight::ResultStream> action_results;
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(action_results, flight_client->DoAction(call_options, action), airport_catalog.credentials.location, "airport_create_schema");

      // We aren't interested in anything from this call.
      AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), airport_catalog.credentials.location, "");

      entries.erase(info.name);
    }
    break;
      //      throw NotImplementedException("AirportCatalogSet::DropEntry for schema");
      //      break;
    default:
      throw NotImplementedException("AirportCatalogSet::DropEntry for type");
    }
  }

  void AirportCatalogSet::EraseEntryInternal(const string &name)
  {
    lock_guard<mutex> l(entry_lock);
    entries.erase(name);
  }

  void AirportCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback)
  {
    lock_guard<mutex> l(entry_lock);
    if (!is_loaded)
    {
      is_loaded = true;
      LoadEntries(context);
    }
    for (auto &entry : entries)
    {
      callback(*entry.second);
    }
  }

  optional_ptr<CatalogEntry> AirportCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry)
  {
    auto result = entry.get();
    if (result->name.empty())
    {
      throw InternalException("AirportCatalogSet::CreateEntry called with empty name");
    }
    //    printf("Creating catalog entry\n");
    entries.insert(make_pair(result->name, std::move(entry)));
    return result;
  }

  void AirportCatalogSet::ClearEntries()
  {
    lock_guard<mutex> l(entry_lock);
    entries.clear();
    is_loaded = false;
  }

  AirportInSchemaSet::AirportInSchemaSet(AirportSchemaEntry &schema) : AirportCatalogSet(schema.ParentCatalog()), schema(schema)
  {
  }

  optional_ptr<CatalogEntry> AirportInSchemaSet::CreateEntry(unique_ptr<CatalogEntry> entry)
  {
    if (!entry->internal)
    {
      entry->internal = schema.internal;
    }
    return AirportCatalogSet::CreateEntry(std::move(entry));
  }

} // namespace duckdb
