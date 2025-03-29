#pragma once

#include "storage/airport_catalog_set.hpp"
#include "storage/airport_table_entry.hpp"

namespace duckdb
{
  struct CreateTableInfo;
  class AirportResult;
  class AirportSchemaEntry;
  class AirportCurlPool;

  class AirportTableSet : public AirportInSchemaSet
  {
  private:
    AirportCurlPool &connection_pool_;
    string cache_directory_;

  public:
    explicit AirportTableSet(AirportCurlPool &connection_pool, AirportSchemaEntry &schema, const string &cache_directory);
    ~AirportTableSet() {}

  public:
    optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);

    static unique_ptr<AirportTableInfo> GetTableInfo(ClientContext &context, AirportSchemaEntry &schema,
                                                     const string &table_name);
    optional_ptr<CatalogEntry> RefreshTable(ClientContext &context, const string &table_name);

    void AlterTable(ClientContext &context, AlterTableInfo &info);

  protected:
    void LoadEntries(ClientContext &context) override;

    void AlterTable(ClientContext &context, RenameTableInfo &info);
    void AlterTable(ClientContext &context, RenameColumnInfo &info);
    void AlterTable(ClientContext &context, AddColumnInfo &info);
    void AlterTable(ClientContext &context, RemoveColumnInfo &info);

    static void AddColumn(ClientContext &context, AirportResult &result, AirportTableInfo &table_info, idx_t column_offset = 0);
  };

  class AirportScalarFunctionSet : public AirportInSchemaSet
  {

  protected:
    void LoadEntries(ClientContext &context) override;

  private:
    AirportCurlPool &connection_pool_;
    string cache_directory_;

  public:
    explicit AirportScalarFunctionSet(AirportCurlPool &connection_pool, AirportSchemaEntry &schema, const string &cache_directory);
    ~AirportScalarFunctionSet() {}
  };

  class AirportTableFunctionSet : public AirportInSchemaSet
  {

  protected:
    void LoadEntries(ClientContext &context) override;

  private:
    AirportCurlPool &connection_pool_;
    string cache_directory_;

  public:
    explicit AirportTableFunctionSet(AirportCurlPool &connection_pool, AirportSchemaEntry &schema, const string &cache_directory);
    ~AirportTableFunctionSet() {}
  };

} // namespace duckdb
