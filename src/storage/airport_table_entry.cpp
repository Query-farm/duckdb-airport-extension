#include "airport_extension.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_schema_entry.hpp"
#include "storage/airport_table_entry.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "storage/airport_catalog_api.hpp"
#include "../../duckdb/third_party/catch/catch.hpp"
#include "storage/airport_transaction.hpp"

namespace flight = arrow::flight;

namespace duckdb
{

  AirportTableEntry::AirportTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, LogicalType rowid_type)
      : TableCatalogEntry(catalog, schema, info), rowid_type(rowid_type), catalog(catalog)
  {
    this->internal = false;
  }

  AirportTableEntry::AirportTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, AirportTableInfo &info, LogicalType rowid_type)
      : TableCatalogEntry(catalog, schema, *info.create_info), rowid_type(rowid_type), catalog(catalog)
  {
    this->internal = false;
  }

  unique_ptr<BaseStatistics> AirportTableEntry::GetStatistics(ClientContext &context, column_t column_id)
  {
    // TODO: Rusty implement this from the flight server.
    // printf("Getting column statistics for column %d\n", column_id);
    return nullptr;
  }

  TableFunction AirportTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data)
  {
    auto &db = DatabaseInstance::GetDatabase(context);
    auto &airport_take_flight_function_set = ExtensionUtil::GetTableFunction(db, "airport_take_flight");
    auto airport_take_flight_function = airport_take_flight_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR, LogicalType::POINTER, LogicalType::VARCHAR});

    D_ASSERT(table_data);
    D_ASSERT(table_data->flight_info != nullptr);
    D_ASSERT(!table_data->location.empty());

    // How can we send the current transaction information.
    auto &transaction = AirportTransaction::Get(context, catalog);

    // Rusty: this is the place where the transformation happens between table functions and tables.
    vector<Value> inputs = {
        table_data->location,
        Value::POINTER((uintptr_t)&table_data->flight_info),
        transaction.identifier.has_value() ? transaction.identifier.value() : ""};

    named_parameter_map_t param_map;
    vector<LogicalType> return_types;
    vector<string> names;
    TableFunctionRef empty_ref;

    TableFunctionBindInput bind_input(inputs,
                                      param_map,
                                      return_types,
                                      names,
                                      nullptr,
                                      nullptr,
                                      airport_take_flight_function,
                                      empty_ref);

    auto result = airport_take_flight_function.bind(context, bind_input, return_types, names);
    bind_data = std::move(result);

    return airport_take_flight_function;
  }

  TableStorageInfo AirportTableEntry::GetStorageInfo(ClientContext &context)
  {
    TableStorageInfo result;
    // TODO fill info
    return result;
  }

} // namespace duckdb
