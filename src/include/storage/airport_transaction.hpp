#pragma once

#include "airport_extension.hpp"
#include "airport_catalog.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb
{
  class AirportCatalog;
  class AirportSchemaEntry;
  class AirportTableEntry;

  enum class AirportTransactionState
  {
    TRANSACTION_NOT_YET_STARTED,
    TRANSACTION_STARTED,
    TRANSACTION_FINISHED
  };

  class AirportTransaction : public Transaction
  {
  public:
    AirportTransaction(AirportCatalog &airport_catalog, TransactionManager &manager, ClientContext &context);
    ~AirportTransaction() override;

    void Start();
    void Commit();
    void Rollback();

    //	UCConnection &GetConnection();
    //	unique_ptr<UCResult> Query(const string &query);
    static AirportTransaction &Get(ClientContext &context, Catalog &catalog);
    AccessMode GetAccessMode() const
    {
      return access_mode;
    }

    // The identifier returned from the Arrow flight server.
    std::optional<std::string> identifier;

  private:
    std::optional<std::string> GetTransactionIdentifier();

    AirportTransactionState transaction_state;
    AccessMode access_mode;

    // The name of the catalog where this transaction is running.
    std::string catalog_name;
    // Copied from the airport catalog, since it can't keep a reference.
    shared_ptr<AirportCredentials> credentials;
  };

} // namespace duckdb
