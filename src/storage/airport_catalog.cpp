#include "airport_extension.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_schema_entry.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/airport_delete.hpp"
#include "storage/airport_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/buffer.h>
#include "airport_macros.hpp"

#include "airport_request_headers.hpp"

namespace duckdb
{

  AirportCatalog::AirportCatalog(AttachedDatabase &db_p, const string &internal_name, AccessMode access_mode,
                                 AirportAttachParameters attach_params)
      : Catalog(db_p), access_mode_(access_mode), attach_parameters_(std::make_shared<AirportAttachParameters>(std::move(attach_params))),
        internal_name_(internal_name), schemas(*this)
  {
    flight_client_ = AirportAPI::FlightClientForLocation(this->attach_parameters_->location());
  }

  AirportCatalog::~AirportCatalog() = default;

  void AirportCatalog::Initialize(bool load_builtin)
  {
  }

  optional_idx AirportCatalog::GetCatalogVersion(ClientContext &context)
  {
    if (loaded_catalog_version.has_value() && loaded_catalog_version.value().is_fixed)
    {
      return loaded_catalog_version.value().catalog_version;
    }

    arrow::flight::FlightCallOptions call_options;
    airport_add_standard_headers(call_options, attach_parameters_->location());
    airport_add_authorization_header(call_options, attach_parameters_->auth_token());

    // Might want to cache this though if a server declares the server catalog will not change.
    arrow::flight::Action action{"catalog_version", arrow::Buffer::FromString(internal_name_)};

    auto &server_location = attach_parameters_->location();

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto action_results,
                                            flight_client_->DoAction(call_options, action),
                                            server_location,
                                            "calling catalog_version action");

    // The only item returned is a serialized flight info.
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto serialized_catalog_version_buffer,
                                            action_results->Next(),
                                            server_location,
                                            "reading catalog_version action result");

    AIRPORT_MSGPACK_UNPACK(AirportGetCatalogVersionResult, result,
                           (*serialized_catalog_version_buffer->body),
                           server_location,
                           "File to parse msgpack encoded catalog_version response");

    loaded_catalog_version = result;

    return result.catalog_version;
  }

  optional_ptr<CatalogEntry> AirportCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info)
  {
    if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT)
    {
      DropInfo try_drop;
      try_drop.type = CatalogType::SCHEMA_ENTRY;
      try_drop.name = info.schema;
      try_drop.if_not_found = OnEntryNotFound::RETURN_NULL;
      try_drop.cascade = false;
      schemas.DropEntry(transaction.GetContext(), try_drop);
    }
    return schemas.CreateSchema(transaction.GetContext(), info);
  }

  void AirportCatalog::DropSchema(ClientContext &context, DropInfo &info)
  {
    return schemas.DropEntry(context, info);
  }

  void AirportCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback)
  {
    // If there is a contents_url for all schemas make sure it is present and decompressed on the disk, so that the
    // schema loaders will grab it.

    schemas.LoadEntireSet(context);

    schemas.Scan(context, [&](CatalogEntry &schema)
                 { callback(schema.Cast<AirportSchemaEntry>()); });
  }

  optional_ptr<SchemaCatalogEntry> AirportCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name,
                                                             OnEntryNotFound if_not_found, QueryErrorContext error_context)
  {
    if (schema_name == DEFAULT_SCHEMA)
    {
      if (if_not_found == OnEntryNotFound::RETURN_NULL)
      {
        // There really isn't a default way to handle this, so just return null.
        return nullptr;
      }
      throw BinderException("Schema with name \"%s\" not found", schema_name);
    }
    auto entry = schemas.GetEntry(transaction.GetContext(), schema_name);
    if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL)
    {
      throw BinderException("Schema with name \"%s\" not found", schema_name);
    }
    return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
  }

  bool AirportCatalog::InMemory()
  {
    return false;
  }

  string AirportCatalog::GetDBPath()
  {
    return internal_name_;
  }

  DatabaseSize AirportCatalog::GetDatabaseSize(ClientContext &context)
  {
    DatabaseSize size;
    return size;
  }

  void AirportCatalog::ClearCache()
  {
    schemas.ClearEntries();
  }

  unique_ptr<PhysicalOperator> AirportCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
                                                                 unique_ptr<PhysicalOperator> plan)
  {
    auto insert = make_uniq<AirportInsert>(op, op.schema, std::move(op.info), false);
    insert->children.push_back(std::move(plan));
    return std::move(insert);
  }

  unique_ptr<LogicalOperator> AirportCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                              unique_ptr<LogicalOperator> plan)
  {
    throw NotImplementedException("AirportCatalog BindCreateIndex");
  }

} // namespace duckdb
