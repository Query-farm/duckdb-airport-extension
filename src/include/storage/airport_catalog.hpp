#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "storage/airport_schema_set.hpp"

namespace duckdb
{
  class AirportSchemaEntry;

  struct AirportCredentials
  {
    // The criteria to pass to the flight server when listing flights.
    string criteria;
    // The location of the flight server.
    string location;
    // The authorization token to use.
    string auth_token;
    // The name of the secret to use
    string secret_name;
  };

  class AirportClearCacheFunction : public TableFunction
  {
  public:
    AirportClearCacheFunction();

    static void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);
  };

  class AirportCatalog : public Catalog
  {
  public:
    explicit AirportCatalog(AttachedDatabase &db_p, const string &internal_name, AccessMode access_mode,
                            AirportCredentials credentials);
    ~AirportCatalog();

    string internal_name;
    AccessMode access_mode;
    shared_ptr<AirportCredentials> credentials;
    std::shared_ptr<arrow::flight::FlightClient> flight_client;

  public:
    void Initialize(bool load_builtin) override;
    string GetCatalogType() override
    {
      return "airport";
    }

    optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

    void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

    optional_ptr<SchemaCatalogEntry> GetSchema(CatalogTransaction transaction, const string &schema_name,
                                               OnEntryNotFound if_not_found,
                                               QueryErrorContext error_context = QueryErrorContext()) override;

    unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op,
                                            unique_ptr<PhysicalOperator> plan) override;
    unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
                                                   unique_ptr<PhysicalOperator> plan) override;
    unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op,
                                            unique_ptr<PhysicalOperator> plan) override;
    unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                            unique_ptr<PhysicalOperator> plan) override;
    unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                unique_ptr<LogicalOperator> plan) override;

    DatabaseSize GetDatabaseSize(ClientContext &context) override;

    //! Whether or not this is an in-memory database
    bool InMemory() override;
    string GetDBPath() override;

    void ClearCache();

    optional_idx GetCatalogVersion(ClientContext &context) override;

    std::optional<string> GetTransactionIdentifier();

    // Track what version of the catalog has been loaded.
    std::optional<AirportGetCatalogVersionResult> loaded_catalog_version;

  private:
    void DropSchema(ClientContext &context, DropInfo &info) override;

  private:
    AirportSchemaSet schemas;
    string default_schema;
  };
}
