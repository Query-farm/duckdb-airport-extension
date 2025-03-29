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

  struct DropItemActionParameters
  {
    // the type of the item to drop, table, schema.
    std::string type;

    std::string catalog_name;
    std::string schema_name;
    std::string name;

    bool ignore_not_found;

    MSGPACK_DEFINE_MAP(type, catalog_name, schema_name, name, ignore_not_found)
  };

  void AirportCatalogSet::DropEntry(ClientContext &context, DropInfo &info)
  {
    if (!is_loaded)
    {
      is_loaded = true;
      LoadEntries(context);
    }

    auto &airport_catalog = catalog.Cast<AirportCatalog>();
    arrow::flight::FlightCallOptions call_options;

    airport_add_standard_headers(call_options, airport_catalog.attach_parameters()->location());
    airport_add_authorization_header(call_options, airport_catalog.attach_parameters()->auth_token());

    auto flight_client = AirportAPI::FlightClientForLocation(airport_catalog.attach_parameters()->location());

    // Common parameters
    DropItemActionParameters params;
    params.catalog_name = info.catalog;
    params.schema_name = info.schema;
    params.name = (info.type == CatalogType::TABLE_ENTRY) ? info.name : "";
    params.ignore_not_found = (info.if_not_found == OnEntryNotFound::RETURN_NULL) ? true : false;

    std::string action_type;

    switch (info.type)
    {
    case CatalogType::TABLE_ENTRY:
      params.type = "table";
      action_type = "drop_table";
      break;
    case CatalogType::SCHEMA_ENTRY:
      params.type = "schema";
      action_type = "drop_schema";
      call_options.headers.emplace_back("airport-action-name", action_type);
      break;
    default:
      throw NotImplementedException("AirportCatalogSet::DropEntry for type");
    }

    std::stringstream packed_buffer;
    msgpack::pack(packed_buffer, params);

    arrow::flight::Action action{action_type, arrow::Buffer::FromString(packed_buffer.str())};

    auto &server_location = airport_catalog.attach_parameters()->location();

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto action_results, flight_client->DoAction(call_options, action), server_location, "airport_create_schema");

    AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), server_location, "");

    entries.erase(info.name);
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
