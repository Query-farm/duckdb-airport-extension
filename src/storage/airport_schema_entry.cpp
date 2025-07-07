#include "storage/airport_schema_entry.hpp"
#include "storage/airport_table_entry.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "storage/airport_curl_pool.hpp"
#include "airport_request_headers.hpp"
#include "storage/airport_catalog.hpp"
#include "airport_macros.hpp"
#include <arrow/buffer.h>
#include <msgpack.hpp>

namespace duckdb
{

  AirportSchemaEntry::AirportSchemaEntry(Catalog &catalog,
                                         CreateSchemaInfo &info, AirportCurlPool &connection_pool, const string &cache_directory,
                                         const AirportAPISchema &schema_data)
      : SchemaCatalogEntry(catalog, info), schema_data_(schema_data), tables(connection_pool, *this, cache_directory), scalar_functions(connection_pool, *this, cache_directory), table_functions(connection_pool, *this, cache_directory)
  {
  }

  AirportSchemaEntry::~AirportSchemaEntry()
  {
  }

  AirportTransaction &GetAirportTransaction(CatalogTransaction transaction)
  {
    if (!transaction.transaction)
    {
      throw InternalException("No transaction in GetAirportTransaction!?");
    }
    return transaction.transaction->Cast<AirportTransaction>();
  }

  optional_ptr<CatalogEntry> AirportSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info)
  {
    const auto &base_info = info.Base();
    auto table_name = base_info.table;
    if (base_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT)
    {
      throw NotImplementedException("REPLACE ON CONFLICT in CreateTable");
    }
    return tables.CreateTable(transaction.GetContext(), info);
  }

  optional_ptr<CatalogEntry> AirportSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info)
  {
    throw BinderException("Airport databases do not support creating functions");
  }

  void AirportUnqualifyColumnRef(ParsedExpression &expr)
  {
    if (expr.type == ExpressionType::COLUMN_REF)
    {
      auto &colref = expr.Cast<ColumnRefExpression>();
      auto name = std::move(colref.column_names.back());
      colref.column_names = {std::move(name)};
      return;
    }
    ParsedExpressionIterator::EnumerateChildren(expr, AirportUnqualifyColumnRef);
  }

  optional_ptr<CatalogEntry> AirportSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                             TableCatalogEntry &table)
  {
    throw NotImplementedException("CreateIndex");
  }

  struct AirportCreateViewParameters
  {
    std::string catalog_name;
    std::string schema_name;
    std::string view_name;
    
    // The SQL query for the materialized view
    std::string sql_query;
    
    // This will be "error", "ignore", or "replace"
    std::string on_conflict;

    MSGPACK_DEFINE_MAP(catalog_name, schema_name, view_name, sql_query, on_conflict)
  };

  optional_ptr<CatalogEntry> AirportSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info)
  {
    if (info.sql.empty())
    {
      throw BinderException("Cannot create view in Airport that originated from an "
                            "empty SQL statement");
    }
    if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT ||
        info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT)
    {
      auto current_entry = GetEntry(transaction, CatalogType::VIEW_ENTRY, info.view_name);
      if (current_entry)
      {
        if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT)
        {
          return current_entry;
        }
        throw NotImplementedException("REPLACE ON CONFLICT in CreateView");
      }
    }

    auto &airport_catalog = catalog.Cast<AirportCatalog>();
    auto &server_location = airport_catalog.attach_parameters()->location();

    // Prepare parameters for create_view action
    AirportCreateViewParameters params;
    params.catalog_name = airport_catalog.internal_name();
    params.schema_name = info.schema;
    params.view_name = info.view_name;
    params.sql_query = info.sql;
    
    switch (info.on_conflict)
    {
    case OnCreateConflict::ERROR_ON_CONFLICT:
      params.on_conflict = "error";
      break;
    case OnCreateConflict::IGNORE_ON_CONFLICT:
      params.on_conflict = "ignore";
      break;
    case OnCreateConflict::REPLACE_ON_CONFLICT:
      params.on_conflict = "replace";
      break;
    default:
      throw NotImplementedException("Unimplemented conflict type");
    }

    arrow::flight::FlightCallOptions call_options;

    airport_add_standard_headers(call_options, airport_catalog.attach_parameters()->location());
    airport_add_authorization_header(call_options, airport_catalog.attach_parameters()->auth_token());

    call_options.headers.emplace_back("airport-action-name", "create_view");

    auto flight_client = AirportAPI::FlightClientForLocation(airport_catalog.attach_parameters()->location());

    AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(action, "create_view", params);

    std::unique_ptr<arrow::flight::ResultStream> action_results;
    AIRPORT_ASSIGN_OR_RAISE_LOCATION(action_results, flight_client->DoAction(call_options, action), server_location, "airport_create_view");

    AIRPORT_ASSIGN_OR_RAISE_LOCATION(auto result_buffer, action_results->Next(), server_location, "");
    AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), server_location, "");

    if (result_buffer == nullptr)
    {
      throw AirportFlightException(server_location, "No flight info returned from create_view action");
    }

    std::string_view serialized_flight_info(reinterpret_cast<const char *>(result_buffer->body->data()), result_buffer->body->size());

    // Deserialize the flight info from create_view RPC
    AIRPORT_ASSIGN_OR_RAISE_LOCATION(
        auto flight_info,
        arrow::flight::FlightInfo::Deserialize(serialized_flight_info),
        server_location,
        "Error deserializing flight info from create_view RPC");

    auto table_entry = AirportCatalogEntryFromFlightInfo(
        std::move(flight_info),
        server_location,
        *this,
        catalog,
        transaction.GetContext());

    return tables.CreateEntry(std::move(table_entry));
  }

  optional_ptr<CatalogEntry> AirportSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info)
  {
    throw BinderException("Airport databases do not support creating types");
  }

  optional_ptr<CatalogEntry> AirportSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info)
  {
    throw BinderException("Airport databases do not support creating sequences");
  }

  optional_ptr<CatalogEntry> AirportSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                     CreateTableFunctionInfo &info)
  {
    throw BinderException("Airport databases do not support creating table functions");
  }

  optional_ptr<CatalogEntry> AirportSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                    CreateCopyFunctionInfo &info)
  {
    throw BinderException("Airport databases do not support creating copy functions");
  }

  optional_ptr<CatalogEntry> AirportSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                      CreatePragmaFunctionInfo &info)
  {
    throw BinderException("Airport databases do not support creating pragma functions");
  }

  optional_ptr<CatalogEntry> AirportSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info)
  {
    throw BinderException("Airport databases do not support creating collations");
  }

  void AirportSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info)
  {
    if (info.type != AlterType::ALTER_TABLE)
    {
      throw BinderException("Only altering tables is supported for now");
    }
    auto &alter = info.Cast<AlterTableInfo>();
    tables.AlterTable(transaction.GetContext(), alter);
  }

  bool CatalogTypeIsSupported(CatalogType type)
  {
    switch (type)
    {
    case CatalogType::SCALAR_FUNCTION_ENTRY:
    case CatalogType::TABLE_ENTRY:
    case CatalogType::TABLE_FUNCTION_ENTRY:
      return true;

    default:
      return false;
    }
  }

  void AirportSchemaEntry::Scan(ClientContext &context, CatalogType type,
                                const std::function<void(CatalogEntry &)> &callback)
  {
    if (!CatalogTypeIsSupported(type))
    {
      return;
    }

    GetCatalogSet(type).Scan(context, callback);
  }
  void AirportSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback)
  {
    throw NotImplementedException("Scan without context not supported");
  }

  void AirportSchemaEntry::DropEntry(ClientContext &context, DropInfo &info)
  {
    switch (info.type)
    {
    case CatalogType::TABLE_ENTRY:
    {
      tables.DropEntry(context, info);
      break;
    }
    default:
      throw NotImplementedException("AirportSchemaEntry::DropEntry for type");
    }
  }

  optional_ptr<CatalogEntry> AirportSchemaEntry::LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info)
  {
    if (!CatalogTypeIsSupported(lookup_info.GetCatalogType()))
    {
      return nullptr;
    }
    return GetCatalogSet(lookup_info.GetCatalogType()).GetEntry(transaction.GetContext(), lookup_info);
  }

  AirportCatalogSet &AirportSchemaEntry::GetCatalogSet(CatalogType type)
  {
    switch (type)
    {
    case CatalogType::TABLE_ENTRY:
    case CatalogType::VIEW_ENTRY:
      return tables;
    case CatalogType::SCALAR_FUNCTION_ENTRY:
      return scalar_functions;
    case CatalogType::TABLE_FUNCTION_ENTRY:
      return table_functions;
    default:
      string error_message = "Airport: Type not supported for GetCatalogSet: " + CatalogTypeToString(type);
      throw NotImplementedException(error_message);
    }
  }

} // namespace duckdb
