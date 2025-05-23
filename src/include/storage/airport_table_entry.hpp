#pragma once

#include "airport_catalog_api.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "storage/airport_table_set.hpp"

namespace duckdb
{
  class AirportTableSet;

  struct AirportTableInfo
  {
    AirportTableInfo() : create_info(make_uniq<CreateTableInfo>())
    {
    }

    AirportTableInfo(const string &schema, const string &table)
        : create_info(make_uniq<CreateTableInfo>(string(), schema, table))
    {
    }

    AirportTableInfo(const SchemaCatalogEntry &schema, const string &table)
        : create_info(make_uniq<CreateTableInfo>((SchemaCatalogEntry &)schema, table))
    {
    }

    const string &GetTableName() const
    {
      return create_info->table;
    }

    const unique_ptr<CreateTableInfo> create_info;
  };

  class AirportTableEntry : public TableCatalogEntry
  {
  public:
    AirportTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, const LogicalType &rowid_type);
    AirportTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, AirportTableInfo &info, const LogicalType &rowid_type);

    unique_ptr<AirportAPITable> table_data;

    virtual_column_map_t GetVirtualColumns() const override
    {
      virtual_column_map_t virtual_columns;
      if (rowid_type.id() != LogicalTypeId::SQLNULL)
      {
        virtual_columns.insert(make_pair(COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", rowid_type)));
      }
      // virtual_columns.insert(make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));

      return virtual_columns;
    }

    const LogicalType &GetRowIdType() const
    {
      return rowid_type;
    }

  public:
    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data, const EntryLookupInfo &lookup) override;

    TableStorageInfo GetStorageInfo(ClientContext &context) override;

    unique_ptr<AirportTableEntry> AlterEntryDirect(ClientContext &context, AlterInfo &info);
    // void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
    //                            ClientContext &context) override;

    Catalog &GetCatalog() const
    {
      return catalog_;
    }

  private:
    //! A logical type for the rowid of this table.
    const LogicalType rowid_type;

    Catalog &catalog_;
  };

} // namespace duckdb
