#include "duckdb.hpp"
#include "storage/airport_delete_parameterized.hpp"
#include "storage/airport_table_entry.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_transaction.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "airport_macros.hpp"
#include "airport_request_headers.hpp"
#include "airport_flight_exception.hpp"
#include "airport_secrets.hpp"

#include "duckdb/common/arrow/schema_metadata.hpp"

#include "airport_flight_stream.hpp"
#include "airport_take_flight.hpp"
#include "storage/airport_exchange.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

namespace duckdb
{

  //===--------------------------------------------------------------------===//
  // Plan
  //===--------------------------------------------------------------------===//
  static string ExtractFilters(PhysicalOperator &child, const string &statement)
  {
    // FIXME - all of this is pretty gnarly, we should provide a hook earlier on
    // in the planning process to convert this into a SQL statement
    if (child.type == PhysicalOperatorType::FILTER)
    {
      auto &filter = child.Cast<PhysicalFilter>();
      auto result = ExtractFilters(*child.children[0], statement);
      auto filter_str = filter.expression->ToString();
      if (result.empty())
      {
        return filter_str;
      }
      else
      {
        return result + " AND " + filter_str;
      }
    }
    else if (child.type == PhysicalOperatorType::TABLE_SCAN)
    {
      auto &table_scan = child.Cast<PhysicalTableScan>();
      if (!table_scan.table_filters)
      {
        return string();
      }
      throw NotImplementedException("Pushed down table filters not supported currently");
    }
    else
    {
      throw NotImplementedException("Unsupported operator type %s in %s statement - only simple expressions "
                                    "(e.g. %s "
                                    "FROM tbl WHERE x=y) are supported in AirportParameterizedDelete",
                                    PhysicalOperatorToString(child.type), statement, statement);
    }
  }

  AirportDeleteParameterized::AirportDeleteParameterized(LogicalOperator &op, TableCatalogEntry &table, PhysicalOperator &plan)
      : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(table)
  {
    sql_filters = ExtractFilters(plan, "DELETE");
    printf("Got SQL filters: %s\n", sql_filters.c_str());
  }

  //===--------------------------------------------------------------------===//
  // States
  //===--------------------------------------------------------------------===//
  class AirportDeleteParameterizedGlobalState : public GlobalSinkState
  {
  public:
    explicit AirportDeleteParameterizedGlobalState() : affected_rows(0)
    {
    }

    idx_t affected_rows;
  };

  unique_ptr<GlobalSinkState> AirportDeleteParameterized::GetGlobalSinkState(ClientContext &context) const
  {
    printf("AirportDeleteParameterized::GetGlobalSinkState\n");
    return make_uniq<AirportDeleteParameterizedGlobalState>();
  }

  //===--------------------------------------------------------------------===//
  // Sink
  //===--------------------------------------------------------------------===//
  SinkResultType AirportDeleteParameterized::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const
  {
    printf("AirportDeleteParameterized::Sink\n");
    return SinkResultType::FINISHED;
  }

  //===--------------------------------------------------------------------===//
  // Finalize
  //===--------------------------------------------------------------------===//
  SinkFinalizeType AirportDeleteParameterized::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                        OperatorSinkFinalizeInput &input) const
  {
    auto &gstate = input.global_state.Cast<AirportDeleteParameterizedGlobalState>();
    gstate.affected_rows = 0;
    // auto &transaction = MySQLTransaction::Get(context, table.catalog);
    // auto &connection = transaction.GetConnection();
    // auto result = connection.Query(query);
    // gstate.affected_rows = result->AffectedRows();
    printf("AirportDeleteParameterized::Finalize\n");
    return SinkFinalizeType::READY;
  }

  //===--------------------------------------------------------------------===//
  // GetData
  //===--------------------------------------------------------------------===//
  SourceResultType AirportDeleteParameterized::GetData(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const
  {
    auto &insert_gstate = sink_state->Cast<AirportDeleteParameterizedGlobalState>();
    chunk.SetCardinality(1);
    chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.affected_rows));
    printf("AirportDeleteParameterized::GetData\n");
    return SourceResultType::FINISHED;
  }

  //===--------------------------------------------------------------------===//
  // Helpers
  //===--------------------------------------------------------------------===//
  string AirportDeleteParameterized::GetName() const
  {
    return "AIRPORT_DELETE_PARAMETERIZED";
  }

  InsertionOrderPreservingMap<string> AirportDeleteParameterized::ParamsToString() const
  {
    InsertionOrderPreservingMap<string> result;
    result["Table Name"] = table.name;
    return result;
  }

  // string ConstructDeleteStatement(LogicalDelete &op, PhysicalOperator &child)
  // {
  //   string result = "DELETE FROM ";
  //   result += MySQLUtils::WriteIdentifier(op.table.schema.name);
  //   result += ".";
  //   result += MySQLUtils::WriteIdentifier(op.table.name);
  //   auto filters = ExtractFilters(child, "DELETE");
  //   if (!filters.empty())
  //   {
  //     result += " WHERE " + filters;
  //   }
  //   return result;
  // }

  // unique_ptr<PhysicalOperator> MySQLCatalog::PlanDelete(ClientContext &context, LogicalDelete &op,
  //                                                       unique_ptr<PhysicalOperator> plan)
  // {
  //   if (op.return_chunk)
  //   {
  //     throw BinderException("RETURNING clause not yet supported for deletion of a MySQL table");
  //   }

  //   auto result = make_uniq<AirportDeleteParameterized>(op, "DELETE", op.table, ConstructDeleteStatement(op, *plan));
  //   result->children.push_back(std::move(plan));
  //   return std::move(result);
  // }

  // string ConstructUpdateStatement(LogicalUpdate &op, PhysicalOperator &child)
  // {
  //   // FIXME - all of this is pretty gnarly, we should provide a hook earlier on
  //   // in the planning process to convert this into a SQL statement
  //   string result = "UPDATE";
  //   result += MySQLUtils::WriteIdentifier(op.table.schema.name);
  //   result += ".";
  //   result += MySQLUtils::WriteIdentifier(op.table.name);
  //   result += " SET ";
  //   if (child.type != PhysicalOperatorType::PROJECTION)
  //   {
  //     throw NotImplementedException("MySQL Update not supported - Expected the "
  //                                   "child of an update to be a projection");
  //   }
  //   auto &proj = child.Cast<PhysicalProjection>();
  //   for (idx_t c = 0; c < op.columns.size(); c++)
  //   {
  //     if (c > 0)
  //     {
  //       result += ", ";
  //     }
  //     auto &col = op.table.GetColumn(op.table.GetColumns().PhysicalToLogical(op.columns[c]));
  //     result += MySQLUtils::WriteIdentifier(col.GetName());
  //     result += " = ";
  //     if (op.expressions[c]->type == ExpressionType::VALUE_DEFAULT)
  //     {
  //       result += "DEFAULT";
  //       continue;
  //     }
  //     if (op.expressions[c]->type != ExpressionType::BOUND_REF)
  //     {
  //       throw NotImplementedException("MySQL Update not supported - Expected a bound reference expression");
  //     }
  //     auto &ref = op.expressions[c]->Cast<BoundReferenceExpression>();
  //     result += proj.select_list[ref.index]->ToString();
  //   }
  //   result += " ";
  //   auto filters = ExtractFilters(*child.children[0], "UPDATE");
  //   if (!filters.empty())
  //   {
  //     result += " WHERE " + filters;
  //   }
  //   return result;
  // }

  // unique_ptr<PhysicalOperator> MySQLCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
  //                                                       unique_ptr<PhysicalOperator> plan)
  // {
  //   if (op.return_chunk)
  //   {
  //     throw BinderException("RETURNING clause not yet supported for updates of a MySQL table");
  //   }
  //   auto result = make_uniq<AirportDeleteParameterized>(op, "UPDATE", op.table, ConstructUpdateStatement(op, *plan));
  //   result->children.push_back(std::move(plan));
  //   return std::move(result);
  // }

} // namespace duckdb
