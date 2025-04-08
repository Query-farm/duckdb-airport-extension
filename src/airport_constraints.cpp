#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"

#include "duckdb/storage/table/append_state.hpp"

#include "airport_extension.hpp"
#include "storage/airport_insert.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_transaction.hpp"
#include "storage/airport_table_entry.hpp"

namespace duckdb
{
  // Need to Adapt this for Airport.

  static void VerifyNotNullConstraint(TableCatalogEntry &table, Vector &vector, idx_t count, const string &col_name)
  {
    if (!VectorOperations::HasNull(vector, count))
    {
      return;
    }

    throw ConstraintException("NOT NULL constraint failed: %s.%s", table.name, col_name);
  }

  // To avoid throwing an error at SELECT, instead this moves the error detection to INSERT
  // static void VerifyGeneratedExpressionSuccess(ClientContext &context, TableCatalogEntry &table, DataChunk &chunk,
  //                                              Expression &expr, column_t index)
  // {
  //   auto &col = table.GetColumn(LogicalIndex(index));
  //   D_ASSERT(col.Generated());
  //   ExpressionExecutor executor(context, expr);
  //   Vector result(col.Type());
  //   try
  //   {
  //     executor.ExecuteExpression(chunk, result);
  //   }
  //   catch (InternalException &ex)
  //   {
  //     throw;
  //   }
  //   catch (std::exception &ex)
  //   {
  //     ErrorData error(ex);
  //     throw ConstraintException("Incorrect value for generated column \"%s %s AS (%s)\" : %s", col.Name(),
  //                               col.Type().ToString(), col.GeneratedExpression().ToString(), error.RawMessage());
  //   }
  // }

  static void VerifyCheckConstraint(ClientContext &context, TableCatalogEntry &table, Expression &expr,
                                    DataChunk &chunk, CheckConstraint &check)
  {
    ExpressionExecutor executor(context, expr);
    Vector result(LogicalType::INTEGER);
    try
    {
      executor.ExecuteExpression(chunk, result);
    }
    catch (std::exception &ex)
    {
      ErrorData error(ex);
      throw ConstraintException("CHECK constraint failed on table %s with expression %s (Error: %s)", table.name, check.ToString(), error.RawMessage());
    }
    catch (...)
    {
      // LCOV_EXCL_START
      throw ConstraintException("CHECK constraint failed on table %s with expression %s (Unknown Error)", table.name, check.ToString());
    } // LCOV_EXCL_STOP
    UnifiedVectorFormat vdata;
    result.ToUnifiedFormat(chunk.size(), vdata);

    auto dataptr = UnifiedVectorFormat::GetData<int32_t>(vdata);
    for (idx_t i = 0; i < chunk.size(); i++)
    {
      auto idx = vdata.sel->get_index(i);
      if (vdata.validity.RowIsValid(idx) && dataptr[idx] == 0)
      {
        throw ConstraintException("CHECK constraint failed on table %s with expression: %s", table.name, check.ToString());
      }
    }
  }

  std::optional<size_t> findStringIndex(const std::vector<std::string> &vec, const std::string &target)
  {
    auto it = std::find(vec.begin(), vec.end(), target);
    if (it != vec.end())
    {
      return std::distance(vec.begin(), it); // Return the index
    }
    return std::nullopt; // Return empty if not found
  }

  void AirportVerifyAppendConstraints(ConstraintState &state, ClientContext &context, DataChunk &chunk,
                                      optional_ptr<ConflictManager> conflict_manager,
                                      vector<string> column_names)
  {
    auto &table = state.table;

    // Airport currently doesn't have generated columns

    // if (table.HasGeneratedColumns()) {
    // 	// Verify that the generated columns expression work with the inserted values
    // 	auto binder = Binder::CreateBinder(context);
    // 	physical_index_set_t bound_columns;
    // 	CheckBinder generated_check_binder(*binder, context, table.name, table.GetColumns(), bound_columns);
    // 	for (auto &col : table.GetColumns().Logical()) {
    // 		if (!col.Generated()) {
    // 			continue;
    // 		}
    // 		D_ASSERT(col.Type().id() != LogicalTypeId::ANY);
    // 		generated_check_binder.target_type = col.Type();
    // 		auto to_be_bound_expression = col.GeneratedExpression().Copy();
    // 		auto bound_expression = generated_check_binder.Bind(to_be_bound_expression);
    // 		VerifyGeneratedExpressionSuccess(context, table, chunk, *bound_expression, col.Oid());
    // 	}
    // }

    // if (HasUniqueIndexes(info->indexes)) {
    // 	VerifyUniqueIndexes(info->indexes, context, chunk, conflict_manager);
    // }

    auto &constraints = table.GetConstraints();
    for (idx_t i = 0; i < state.bound_constraints.size(); i++)
    {
      auto &base_constraint = constraints[i];
      switch (base_constraint->type)
      {
      case ConstraintType::NOT_NULL:
      {
        // auto &bound_not_null = constraint->Cast<BoundNotNullConstraint>();
        auto &not_null = base_constraint->Cast<NotNullConstraint>();
        auto &col = table.GetColumns().GetColumn(LogicalIndex(not_null.index));

        // Since the column names that are send can be in a different order
        // look them up by name.
        auto col_index = findStringIndex(column_names, col.Name());
        if (col_index.has_value())
        {
          VerifyNotNullConstraint(table, chunk.data[col_index.value()], chunk.size(), col.Name());
        }
        break;
      }
      case ConstraintType::CHECK:
      {
        auto &constraint = state.bound_constraints[i];

        auto &check = constraint->Cast<BoundCheckConstraint>();
        auto &unbound_constraint = base_constraint->Cast<CheckConstraint>();
        VerifyCheckConstraint(context, table, *check.expression, chunk, unbound_constraint);
        break;
      }
      case ConstraintType::UNIQUE:
      {
        // These were handled earlier on
        break;
      }
      // case ConstraintType::FOREIGN_KEY: {
      //  auto &bfk = constraint->Cast<BoundForeignKeyConstraint>();
      //  if (bfk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
      //      bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
      //  	VerifyAppendForeignKeyConstraint(bfk, context, chunk);
      //  }
      //	break;
      //}
      default:
        throw NotImplementedException("Constraint type not implemented!");
      }
    }
  }

}