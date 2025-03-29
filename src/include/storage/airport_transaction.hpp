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
      return access_mode_;
    }

    // The identifier returned from the Arrow flight server.
    const std::optional<std::string> &identifier() const
    {
      return identifier_;
    }

  private:
    // The identifier returned from the Arrow flight server.
    std::optional<std::string> identifier_;

    std::optional<std::string> GetTransactionIdentifier();

    AirportTransactionState transaction_state;
    AccessMode access_mode_;

    // The name of the catalog where this transaction is running.
    std::string catalog_name;
    // Copied from the airport catalog, since it can't keep a reference.
    std::shared_ptr<const AirportAttachParameters> attach_parameters;
  };

} // namespace duckdb
