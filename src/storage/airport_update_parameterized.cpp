#include "duckdb.hpp"
#include "storage/airport_update_parameterized.hpp"
#include "storage/airport_table_entry.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
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
#include "duckdb/execution/operator/projection/physical_projection.hpp"

namespace duckdb
{

  static string ExtractFilters(PhysicalOperator &child, const string &statement)
  {
    // Check for FILTER operator type
    if (child.type == PhysicalOperatorType::FILTER)
    {
      auto &filter = child.Cast<PhysicalFilter>();
      string result = ExtractFilters(*child.children[0], statement);
      return result.empty() ? filter.expression->ToString() : result + " AND " + filter.expression->ToString();
    }

    // Check for TABLE_SCAN operator type
    if (child.type == PhysicalOperatorType::TABLE_SCAN)
    {
      auto &table_scan = child.Cast<PhysicalTableScan>();
      if (!table_scan.table_filters)
      {
        return {};
      }
      throw NotImplementedException("Pushed down table filters not supported currently");
    }

    if (child.type == PhysicalOperatorType::PROJECTION)
    {
      //      auto &proj = child.Cast<PhysicalProjection>();
      return ExtractFilters(*child.children[0], statement);
    }

    // Handle unsupported operator types
    throw NotImplementedException(
        "Unsupported operator type %s in %s statement - only simple expressions "
        "(e.g. %s FROM tbl WHERE x=y) are supported in AirportParameterizedUpdate",
        PhysicalOperatorToString(child.type), statement, statement);
  }

  static std::pair<string, string> ConstructUpdateStatement(LogicalUpdate &op, PhysicalOperator &child)
  {
    if (child.type != PhysicalOperatorType::PROJECTION)
    {
      throw NotImplementedException(
          "Airport Parameterized Update not supported - Expected the child of an update to be a projection");
    }

    auto &proj = child.Cast<PhysicalProjection>();
    string expressions;

    for (idx_t c = 0; c < op.columns.size(); ++c)
    {
      if (c > 0)
      {
        expressions += ", ";
      }

      const auto &col = op.table.GetColumn(op.table.GetColumns().PhysicalToLogical(op.columns[c]));
      expressions += col.GetName() + " = ";

      switch (op.expressions[c]->type)
      {
      case ExpressionType::VALUE_DEFAULT:
        expressions += "DEFAULT";
        break;
      case ExpressionType::BOUND_REF:
      {
        const auto &ref = op.expressions[c]->Cast<BoundReferenceExpression>();
        expressions += proj.select_list[ref.index]->ToString();
        break;
      }
      default:
        throw NotImplementedException(
            "Airport Parameterized Update not supported - Expected a bound reference expression");
      }
    }

    auto filters = ExtractFilters(*child.children[0], "UPDATE");
    return {expressions, filters};
  }

  AirportUpdateParameterized::AirportUpdateParameterized(LogicalOperator &op, TableCatalogEntry &table, PhysicalOperator &plan)
      : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(table)
  {
    auto result = ConstructUpdateStatement(op.Cast<LogicalUpdate>(), plan);
    printf("Got expressions: %s\n", result.first.c_str());
    printf("Got filters: %s\n", result.second.c_str());
  }

  //===--------------------------------------------------------------------===//
  // States
  //===--------------------------------------------------------------------===//
  class AirportUpdateParameterizedGlobalState : public GlobalSinkState
  {
  public:
    explicit AirportUpdateParameterizedGlobalState() : affected_rows(0)
    {
    }

    idx_t affected_rows;
  };

  unique_ptr<GlobalSinkState> AirportUpdateParameterized::GetGlobalSinkState(ClientContext &context) const
  {
    printf("AirportUpdateParameterized::GetGlobalSinkState\n");
    return make_uniq<AirportUpdateParameterizedGlobalState>();
  }

  //===--------------------------------------------------------------------===//
  // Sink
  //===--------------------------------------------------------------------===//
  SinkResultType AirportUpdateParameterized::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const
  {
    printf("AirportUpdateParameterized::Sink\n");
    return SinkResultType::FINISHED;
  }

  //===--------------------------------------------------------------------===//
  // Finalize
  //===--------------------------------------------------------------------===//
  SinkFinalizeType AirportUpdateParameterized::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                        OperatorSinkFinalizeInput &input) const
  {
    auto &gstate = input.global_state.Cast<AirportUpdateParameterizedGlobalState>();
    gstate.affected_rows = 0;
    // auto &transaction = MySQLTransaction::Get(context, table.catalog);
    // auto &connection = transaction.GetConnection();
    // auto result = connection.Query(query);
    // gstate.affected_rows = result->AffectedRows();
    printf("AirportUpdateParameterized::Finalize\n");
    return SinkFinalizeType::READY;
  }

  //===--------------------------------------------------------------------===//
  // GetData
  //===--------------------------------------------------------------------===//
  SourceResultType AirportUpdateParameterized::GetData(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const
  {
    auto &insert_gstate = sink_state->Cast<AirportUpdateParameterizedGlobalState>();
    chunk.SetCardinality(1);
    chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.affected_rows));
    printf("AirportUpdateParameterized::GetData\n");
    return SourceResultType::FINISHED;
  }

  //===--------------------------------------------------------------------===//
  // Helpers
  //===--------------------------------------------------------------------===//
  string AirportUpdateParameterized::GetName() const
  {
    return "AIRPORT_UPDATE_PARAMETERIZED";
  }

  InsertionOrderPreservingMap<string> AirportUpdateParameterized::ParamsToString() const
  {
    InsertionOrderPreservingMap<string> result;
    result["Table Name"] = table.name;
    return result;
  }

} // namespace duckdb
