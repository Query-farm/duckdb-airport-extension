#include "airport_extension.hpp"
#include "duckdb.hpp"

#include "duckdb/common/arrow/schema_metadata.hpp"
#include "airport_take_flight.hpp"

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
      bool skip_rowid_column)
  {
    if (rowid_column_index)
    {
      *rowid_column_index = COLUMN_IDENTIFIER_ROW_ID;
    }

    auto &config = DBConfig::GetConfig(context);

    const idx_t num_columns = static_cast<idx_t>(schema_root.arrow_schema.n_children);

    if (num_columns > 0)
    {
      if (return_types)
      {
        return_types->reserve(num_columns);
      }
      if (names)
      {
        names->reserve(num_columns);
      }
      if (duckdb_type_names)
      {
        duckdb_type_names->reserve(num_columns);
      }
    }

    for (idx_t col_idx = 0; col_idx < num_columns; col_idx++)
    {
      auto &schema = *schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("airport_take_flight: released schema passed");
      }
      auto arrow_type = ArrowType::GetArrowLogicalType(config, schema);

      // Determine if the column is the rowid column by looking at the metadata
      // on the column.
      bool is_rowid_column = false;
      if (schema.metadata)
      {
        ArrowSchemaMetadata column_metadata(schema.metadata);

        if (!column_metadata.GetOption("is_rowid").empty())
        {
          is_rowid_column = true;
          if (rowid_column_index)
          {
            *rowid_column_index = col_idx;
          }
        }
      }

      if (schema.dictionary)
      {
        auto dictionary_type = ArrowType::GetArrowLogicalType(config, *schema.dictionary);
        arrow_type->SetDictionary(std::move(dictionary_type));
      }

      const idx_t column_id = is_rowid_column ? COLUMN_IDENTIFIER_ROW_ID : col_idx;

      const string column_name = AirportNameForField(schema.name, col_idx);

      if (!skip_rowid_column || !is_rowid_column)
      {
        if (return_types)
        {
          return_types->emplace_back(arrow_type->GetDuckType());
        }
        if (names)
        {
          names->push_back(std::move(column_name));
        }
        if (duckdb_type_names)
        {
          duckdb_type_names->push_back(arrow_type->GetDuckType().ToString());
        }
      }

      if (arrow_table)
      {
        arrow_table->AddColumn(column_id, std::move(arrow_type));
      }
    }
    QueryResult::DeduplicateColumns(*names);
  }

}