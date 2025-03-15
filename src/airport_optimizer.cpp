#include "airport_extension.hpp"
#include "storage/airport_catalog.hpp"
#include "airport_optimizer.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "airport_flight_stream.hpp"

namespace duckdb
{
  static void MarkAirportTakeFlightAsSkipProducing(unique_ptr<LogicalOperator> &op)
  {
    auto markAirportTakeFlightSkipResults = [](LogicalGet &get)
    {
      if (get.function.name == "airport_take_flight")
      {
        auto &bind_data = get.bind_data->Cast<AirportTakeFlightBindData>();
        bind_data.skip_producing_result_for_update_or_delete = true;
      }
    };

    reference<LogicalOperator> child = *op->children[0];
    if (child.get().type == LogicalOperatorType::LOGICAL_GET)
    {
      markAirportTakeFlightSkipResults(child.get().Cast<LogicalGet>());
    }
    else if (child.get().type == LogicalOperatorType::LOGICAL_FILTER ||
             child.get().type == LogicalOperatorType::LOGICAL_PROJECTION)
    {
      for (auto &child : op->children)
      {
        MarkAirportTakeFlightAsSkipProducing(child);
      }
    }
    else
    {
      throw NotImplementedException("Unsupported child type for LogicalUpdate type is" + LogicalOperatorToString(child.get().type));
    }
  }

  void OptimizeAirportUpdate(unique_ptr<LogicalOperator> &op)
  {
    if (op->type != LogicalOperatorType::LOGICAL_UPDATE)
      return;

    auto &update = op->Cast<LogicalUpdate>();

    // If the table produced rowids we cannot optimize it.
    if (update.table.GetRowIdType() != LogicalType::SQLNULL)
      return;

    MarkAirportTakeFlightAsSkipProducing(op);
  }

  void OptimizeAirportDelete(unique_ptr<LogicalOperator> &op)
  {
    if (op->type != LogicalOperatorType::LOGICAL_DELETE)
      return;

    auto &del = op->Cast<LogicalDelete>();

    // If the table produced rowids we cannot optimize it.
    if (del.table.GetRowIdType() != LogicalType::SQLNULL)
      return;

    MarkAirportTakeFlightAsSkipProducing(op);
  }

  void AirportOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan)
  {
    OptimizeAirportUpdate(plan);
    OptimizeAirportDelete(plan);
  }
}