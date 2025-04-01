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
    auto &base_info = info.Base();
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

  string GetAirportCreateView(CreateViewInfo &info)
  {
    throw NotImplementedException("GetCreateView");
  }

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
    // auto &airport_transaction = GetAirportTransaction(transaction);
    //	uc_transaction.Query(GetAirportCreateView(info));
    return tables.RefreshTable(transaction.GetContext(), info.view_name);
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

  optional_ptr<CatalogEntry> AirportSchemaEntry::GetEntry(CatalogTransaction transaction, CatalogType type,
                                                          const string &name)
  {
    if (!CatalogTypeIsSupported(type))
    {
      return nullptr;
    }
    return GetCatalogSet(type).GetEntry(transaction.GetContext(), name);
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
      string error_message = "Type not supported for GetCatalogSet: " + CatalogTypeToString(type);
      throw InternalException(error_message);
    }
  }

} // namespace duckdb
