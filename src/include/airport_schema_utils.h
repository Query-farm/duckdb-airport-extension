#include "airport_extension.hpp"
#include "duckdb.hpp"

namespace duckdb
{

  void AirportExamineSchema(
      ClientContext &context,
      const ArrowSchemaWrapper &schema_root,
      ArrowTableType *arrow_table,
      vector<LogicalType> *return_types,
      vector<string> *names,
      vector<string> *duckdb_type_names,
      idx_t *rowid_column_index,
      bool skip_rowid_column);
}