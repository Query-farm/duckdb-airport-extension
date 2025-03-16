#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "yyjson.hpp"
#include <arrow/buffer.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/util/key_value_metadata.h>
#include <numeric>
#include "airport_flight_stream.hpp"
#include "airport_request_headers.hpp"
#include "airport_macros.hpp"
#include "airport_macros.hpp"
#include "airport_scalar_function.hpp"
#include "airport_secrets.hpp"
#include "airport_take_flight.hpp"
#include "storage/airport_catalog_api.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_curl_pool.hpp"
#include "storage/airport_exchange.hpp"
#include "storage/airport_schema_entry.hpp"
#include "storage/airport_table_set.hpp"
#include "storage/airport_transaction.hpp"

using namespace duckdb_yyjson; // NOLINT

struct FunctionCatalogSchemaName
{
  std::string catalog_name;
  std::string schema_name;
  std::string name;

  // Define equality operator to compare two keys
  bool operator==(const FunctionCatalogSchemaName &other) const
  {
    return catalog_name == other.catalog_name && schema_name == other.schema_name && name == other.name;
  }
};

namespace std
{
  template <>
  struct hash<FunctionCatalogSchemaName>
  {
    size_t operator()(const FunctionCatalogSchemaName &k) const
    {
      // Combine the hash of all 3 strings
      return hash<std::string>()(k.catalog_name) ^ (hash<std::string>()(k.schema_name) << 1) ^ (hash<std::string>()(k.name) << 2);
    }
  };
}

namespace duckdb
{

  static std::string join_vector_of_strings(const std::vector<std::string> &vec, const char joiner)
  {
    if (vec.empty())
      return "";

    return std::accumulate(
        std::next(vec.begin()), vec.end(), vec.front(),
        [joiner](const std::string &a, const std::string &b)
        {
          return a + joiner + b;
        });
  }

  AirportTableFunctionSet::AirportTableFunctionSet(AirportCurlPool &connection_pool, AirportSchemaEntry &schema, const string &cache_directory_) : AirportInSchemaSet(schema), connection_pool(connection_pool), cache_directory(cache_directory_)
  {
  }

  AirportScalarFunctionSet::AirportScalarFunctionSet(AirportCurlPool &connection_pool, AirportSchemaEntry &schema, const string &cache_directory_) : AirportInSchemaSet(schema), connection_pool(connection_pool), cache_directory(cache_directory_)
  {
  }

  AirportTableSet::AirportTableSet(AirportCurlPool &connection_pool, AirportSchemaEntry &schema, const string &cache_directory_) : AirportInSchemaSet(schema), connection_pool(connection_pool), cache_directory(cache_directory_)
  {
  }

  struct AirportTableCheckConstraints
  {
    std::vector<std::string> constraints;

    MSGPACK_DEFINE_MAP(constraints)
  };

  void AirportTableSet::LoadEntries(ClientContext &context)
  {
    // auto &transaction = AirportTransaction::Get(context, catalog);

    auto &airport_catalog = catalog.Cast<AirportCatalog>();

    // TODO: handle out-of-order columns using position property
    auto curl = connection_pool.acquire();
    auto contents = AirportAPI::GetSchemaItems(
        curl,
        catalog.GetDBPath(),
        schema.name,
        schema.schema_data->source,
        cache_directory,
        airport_catalog.credentials);
    connection_pool.release(curl);

    for (auto &table : contents->tables)
    {
      // D_ASSERT(schema.name == table.schema_name);
      CreateTableInfo info;

      info.table = table.name;
      info.comment = table.comment;

      std::shared_ptr<arrow::Schema> info_schema;
      arrow::ipc::DictionaryMemo dictionary_memo;
      string error_location = "(" + airport_catalog.credentials.location + ")";
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(info_schema, table.flight_info->GetSchema(&dictionary_memo), airport_catalog.credentials.location, table.flight_info->descriptor(), "");

      ArrowSchema arrow_schema;

      AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(ExportSchema(*info_schema, &arrow_schema), airport_catalog.credentials.location, table.flight_info->descriptor(), "ExportSchema");

      vector<string> column_names;
      vector<duckdb::LogicalType> return_types;
      vector<string> not_null_columns;

      LogicalType rowid_type = LogicalType(LogicalType::ROW_TYPE);

      if (arrow_schema.metadata != nullptr)
      {
        auto schema_metadata = ArrowSchemaMetadata(arrow_schema.metadata);

        auto check_constraints = schema_metadata.GetOption("check_constraints");
        if (!check_constraints.empty())
        {

          AirportTableCheckConstraints table_constraints;
          try
          {
            msgpack::object_handle oh = msgpack::unpack(
                (const char *)check_constraints.c_str(),
                check_constraints.size(),
                0);
            msgpack::object obj = oh.get();
            obj.convert(table_constraints);
          }
          catch (const std::exception &e)
          {
            throw AirportFlightException(airport_catalog.credentials.location,
                                         table.flight_info->descriptor(),
                                         "File to parse msgpack encoded table check constraints.",
                                         string(e.what()));
          }

          for (auto &expression : table_constraints.constraints)
          {
            auto expression_list = Parser::ParseExpressionList(expression, context.GetParserOptions());
            if (expression_list.size() != 1)
            {
              throw ParserException("Failed to parse CHECK constraint expression: " + expression + " for table " + table.name);
            }
            info.constraints.push_back(make_uniq<CheckConstraint>(std::move(expression_list[0])));
          }
        }
      }

      for (idx_t col_idx = 0;
           col_idx < (idx_t)arrow_schema.n_children; col_idx++)
      {
        auto &column = *arrow_schema.children[col_idx];
        if (!column.release)
        {
          throw InvalidInputException("AirportTableSet::LoadEntries: released schema passed");
        }

        if (column.metadata != nullptr)
        {
          auto column_metadata = ArrowSchemaMetadata(column.metadata);

          auto is_rowid = column_metadata.GetOption("is_rowid");
          if (!is_rowid.empty())
          {
            rowid_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), column)->GetDuckType();

            // So the skipping here is a problem, since its assumed
            // that the return_type and column_names can be easily indexed.
            continue;
          }
        }

        auto column_name = string(column.name);
        if (column_name.empty())
        {
          column_name = string("v") + to_string(col_idx);
        }

        column_names.emplace_back(column_name);

        auto arrow_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), column);
        if (column.dictionary)
        {
          auto dictionary_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), *column.dictionary);
          return_types.emplace_back(dictionary_type->GetDuckType());
        }
        else
        {
          return_types.emplace_back(arrow_type->GetDuckType());
        }

        if (!(column.flags & ARROW_FLAG_NULLABLE))
        {
          not_null_columns.emplace_back(column_name);
        }
      }

      QueryResult::DeduplicateColumns(column_names);
      idx_t rowid_adjust = 0;
      for (idx_t col_idx = 0;
           col_idx < (idx_t)arrow_schema.n_children; col_idx++)
      {
        auto &column = *arrow_schema.children[col_idx];
        if (column.metadata != nullptr)
        {
          auto column_metadata = ArrowSchemaMetadata(column.metadata);

          auto is_rowid = column_metadata.GetOption("is_rowid");
          if (!is_rowid.empty())
          {
            rowid_adjust = 1;
            continue;
          }
        }

        auto column_def = ColumnDefinition(column_names[col_idx - rowid_adjust], return_types[col_idx - rowid_adjust]);
        if (column.metadata != nullptr)
        {
          auto column_metadata = ArrowSchemaMetadata(column.metadata);

          auto comment = column_metadata.GetOption("comment");
          if (!comment.empty())
          {
            column_def.SetComment(duckdb::Value(comment));
          }

          auto default_value = column_metadata.GetOption("default");

          if (!default_value.empty())
          {
            auto expressions = Parser::ParseExpressionList(default_value);
            if (expressions.empty())
            {
              throw InternalException("Expression list is empty when parsing default value for column %s", column.name);
            }
            column_def.SetDefaultValue(std::move(expressions[0]));
          }
        }

        info.columns.AddColumn(std::move(column_def));
      }
      arrow_schema.release(&arrow_schema);

      for (auto col_name : not_null_columns)
      {
        auto not_null_index = info.columns.GetColumnIndex(col_name);
        info.constraints.emplace_back(make_uniq<NotNullConstraint>(not_null_index));
      }

      // printf("Creating a table in catalog %s, schema %s, name %s\n", catalog.GetName().c_str(), schema.name.c_str(), info.table.c_str());

      auto table_entry = make_uniq<AirportTableEntry>(catalog, schema, info, rowid_type);
      table_entry->table_data = make_uniq<AirportAPITable>(table);
      CreateEntry(std::move(table_entry));
    }
  }

  optional_ptr<CatalogEntry> AirportTableSet::RefreshTable(ClientContext &context, const string &table_name)
  {
    throw NotImplementedException("AirportTableSet::RefreshTable");
    // auto table_info = GetTableInfo(context, schema, table_name);
    // auto table_entry = make_uniq<AirportTableEntry>(catalog, schema, *table_info);
    // auto table_ptr = table_entry.get();
    // CreateEntry(std::move(table_entry));
    // return table_ptr;
  }

  unique_ptr<AirportTableInfo> AirportTableSet::GetTableInfo(ClientContext &context, AirportSchemaEntry &schema,
                                                             const string &table_name)
  {
    throw NotImplementedException("AirportTableSet::GetTableInfo");
  }

  optional_ptr<CatalogEntry> AirportTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info)
  {
    auto &airport_catalog = catalog.Cast<AirportCatalog>();
    auto &base = info.base->Cast<CreateTableInfo>();

    vector<LogicalType> column_types;
    vector<string> column_names;
    for (auto &col : base.columns.Logical())
    {
      column_types.push_back(col.GetType());
      column_names.push_back(col.Name());
    }

    // To perform this creation we likely want to serialize the schema and send it to the server.
    // so the table can be created as part of a DoAction call.

    // So to convert all of the columns an arrow schema we need to look into the code for
    // doing inserts.

    ArrowSchema schema;
    auto client_properties = context.GetClientProperties();

    ArrowConverter::ToArrowSchema(&schema,
                                  column_types,
                                  column_names,
                                  client_properties);

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto real_schema, arrow::ImportSchema(&schema), airport_catalog.credentials.location, "");

    std::shared_ptr<arrow::KeyValueMetadata> schema_metadata = std::make_shared<arrow::KeyValueMetadata>();

    AIRPORT_ARROW_ASSERT_OK_LOCATION(schema_metadata->Set("table_name", base.table), airport_catalog.credentials.location, "");
    AIRPORT_ARROW_ASSERT_OK_LOCATION(schema_metadata->Set("schema_name", base.schema), airport_catalog.credentials.location, "");
    AIRPORT_ARROW_ASSERT_OK_LOCATION(schema_metadata->Set("catalog_name", base.catalog), airport_catalog.credentials.location, "");

    real_schema = real_schema->WithMetadata(schema_metadata);

    // Not make the call, need to include the schema name.

    arrow::flight::FlightCallOptions call_options;

    airport_add_standard_headers(call_options, airport_catalog.credentials.location);
    airport_add_authorization_header(call_options, airport_catalog.credentials.auth_token);

    call_options.headers.emplace_back("airport-action-name", "create_table");

    std::unique_ptr<flight::FlightClient> &flight_client = AirportAPI::FlightClientForLocation(airport_catalog.credentials.location);

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(
        auto serialized_schema,
        arrow::ipc::SerializeSchema(*real_schema, arrow::default_memory_pool()),
        airport_catalog.credentials.location,
        "");

    arrow::flight::Action action{"create_table",
                                 serialized_schema};
    std::unique_ptr<arrow::flight::ResultStream> action_results;
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(action_results, flight_client->DoAction(call_options, action), airport_catalog.credentials.location, "airport_create_table");

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto flight_info_buffer, action_results->Next(), airport_catalog.credentials.location, "");

    if (flight_info_buffer == nullptr)
    {
      throw InternalException("No flight info returned from create_table action");
    }

    std::string_view serialized_flight_info(reinterpret_cast<const char *>(flight_info_buffer->body->data()), flight_info_buffer->body->size());

    // Now how to we deserialize the flight info from that buffer...
    std::shared_ptr<flight::FlightInfo> flight_info;
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(flight_info, arrow::flight::FlightInfo::Deserialize(serialized_flight_info), airport_catalog.credentials.location, "");

    // We aren't interested in anything after the first result.
    AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), airport_catalog.credentials.location, "");

    // FIXME: check to make sure the rowid column is the correct type, this seems to be missing here.
    auto table_entry = make_uniq<AirportTableEntry>(catalog, this->schema, base, LogicalType(LogicalTypeId::BIGINT));
    AirportAPITable new_table(
        airport_catalog.credentials.location,
        flight_info,
        base.catalog,
        base.schema,
        base.table,
        string(""));

    new_table.flight_info = std::move(flight_info);
    table_entry->table_data = make_uniq<AirportAPITable>(new_table);

    return CreateEntry(std::move(table_entry));
  }

  void AirportTableSet::AlterTable(ClientContext &context, RenameTableInfo &info)
  {
    throw NotImplementedException("AirportTableSet::AlterTable");
  }

  void AirportTableSet::AlterTable(ClientContext &context, RenameColumnInfo &info)
  {
    throw NotImplementedException("AirportTableSet::AlterTable");
  }

  void AirportTableSet::AlterTable(ClientContext &context, AddColumnInfo &info)
  {
    throw NotImplementedException("AirportTableSet::AlterTable");
  }

  void AirportTableSet::AlterTable(ClientContext &context, RemoveColumnInfo &info)
  {
    throw NotImplementedException("AirportTableSet::AlterTable");
  }

  void AirportTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter)
  {
    throw NotImplementedException("AirportTableSet::AlterTable");
  }

  // Given an Arrow schema return a vector of the LogicalTypes for that schema.
  static vector<LogicalType> AirportSchemaToLogicalTypes(
      ClientContext &context,
      std::shared_ptr<arrow::Schema> schema,
      const string &server_location,
      const flight::FlightDescriptor &flight_descriptor)
  {
    ArrowSchemaWrapper schema_root;

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        ExportSchema(*schema, &schema_root.arrow_schema),
        server_location,
        flight_descriptor,
        "ExportSchema");

    vector<LogicalType> return_types;

    for (idx_t col_idx = 0;
         col_idx < (idx_t)schema_root.arrow_schema.n_children; col_idx++)
    {
      auto &schema = *schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("AirportSchemaToLogicalTypes: released schema passed");
      }
      auto arrow_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), schema);

      if (schema.dictionary)
      {
        auto dictionary_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), *schema.dictionary);
        arrow_type->SetDictionary(std::move(dictionary_type));
      }

      // Indicate that the field should select any type.
      bool is_any_type = false;
      if (schema.metadata != nullptr)
      {
        auto column_metadata = ArrowSchemaMetadata(schema.metadata);
        if (!column_metadata.GetOption("is_any_type").empty())
        {
          is_any_type = true;
        }
      }

      if (is_any_type)
      {
        // This will be sorted out in the bind of the function.
        return_types.push_back(LogicalType::ANY);
      }
      else
      {
        return_types.emplace_back(arrow_type->GetDuckType());
      }
    }
    return return_types;
  }

  void AirportScalarFunctionSet::LoadEntries(ClientContext &context)
  {
    // auto &transaction = AirportTransaction::Get(context, catalog);

    auto &airport_catalog = catalog.Cast<AirportCatalog>();

    // TODO: handle out-of-order columns using position property
    auto curl = connection_pool.acquire();
    auto contents = AirportAPI::GetSchemaItems(
        curl,
        catalog.GetDBPath(),
        schema.name,
        schema.schema_data->source,
        cache_directory,
        airport_catalog.credentials);

    connection_pool.release(curl);

    //    printf("AirportScalarFunctionSet loading entries\n");
    //    printf("Total functions: %lu\n", tables_and_functions.second.size());

    // There can be functions with the same name.
    std::unordered_map<FunctionCatalogSchemaName, std::vector<AirportAPIScalarFunction>> functions_by_name;

    for (auto &function : contents->scalar_functions)
    {
      FunctionCatalogSchemaName function_key{function.catalog_name, function.schema_name, function.name};
      functions_by_name[function_key].emplace_back(function);
    }

    for (const auto &pair : functions_by_name)
    {
      ScalarFunctionSet flight_func_set(pair.first.name);

      // FIXME: need a way to specify the function stability.
      for (const auto &function : pair.second)
      {
        auto input_types = AirportSchemaToLogicalTypes(context, function.input_schema, function.location, function.flight_info->descriptor());

        arrow::ipc::DictionaryMemo dictionary_memo;

        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
            auto output_schema,
            function.flight_info->GetSchema(&dictionary_memo),
            function.location,
            function.flight_info->descriptor(),
            "GetSchema");

        auto output_types = AirportSchemaToLogicalTypes(context, output_schema, function.location, function.flight_info->descriptor());
        D_ASSERT(output_types.size() == 1);

        auto scalar_func = ScalarFunction(input_types, output_types[0],
                                          AirportScalarFun,
                                          AirportScalarFunBind,
                                          nullptr,
                                          nullptr,
                                          AirportScalarFunInitLocalState,
                                          LogicalTypeId::INVALID,
                                          duckdb::FunctionStability::VOLATILE,
                                          duckdb::FunctionNullHandling::DEFAULT_NULL_HANDLING,
                                          nullptr);
        scalar_func.function_info = make_uniq<AirportScalarFunctionInfo>(function.location, function.name, function.flight_info,
                                                                         function.input_schema);

        flight_func_set.AddFunction(scalar_func);
      }

      CreateScalarFunctionInfo info = CreateScalarFunctionInfo(flight_func_set);
      info.catalog = pair.first.catalog_name;
      info.schema = pair.first.schema_name;

      auto function_entry = make_uniq_base<StandardEntry, ScalarFunctionCatalogEntry>(catalog, schema,
                                                                                      info.Cast<CreateScalarFunctionInfo>());
      CreateEntry(std::move(function_entry));
    }
  }

  class AirportDynamicTableFunctionInfo : public TableFunctionInfo
  {
  public:
    std::shared_ptr<AirportAPITableFunction> function;

  public:
    AirportDynamicTableFunctionInfo(const std::shared_ptr<AirportAPITableFunction> function_p)
        : TableFunctionInfo(), function(function_p)
    {
    }

    ~AirportDynamicTableFunctionInfo() override
    {
    }
  };

  // Create a new arrow schema where all is_table_fields are removed, since they will be
  // serialized outside of the parameters.
  static std::shared_ptr<arrow::Schema> AirportSchemaWithoutTableFields(std::shared_ptr<arrow::Schema> schema)
  {
    vector<std::shared_ptr<arrow::Field>> keep_fields;
    for (const auto &field : schema->fields())
    {
      auto metadata = field->metadata();

      if (metadata == nullptr || !metadata->Contains("is_table_input"))
      {
        keep_fields.push_back(field);
      }
    }

    // Create a new schema with the remaining fields
    return arrow::schema(keep_fields);
  }

  // Serialize data to a arrow::Buffer
  static std::shared_ptr<arrow::Buffer> AirportDynamicSerializeParameters(std::shared_ptr<arrow::Schema> input_schema,
                                                                          ClientContext &context,
                                                                          TableFunctionBindInput &input,
                                                                          string server_location,
                                                                          arrow::flight::FlightDescriptor flight_descriptor)
  {
    ArrowSchemaWrapper schema_root;

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        ExportSchema(*input_schema, &schema_root.arrow_schema),
        server_location,
        flight_descriptor,
        "ExportSchema");

    vector<string> input_schema_names;
    vector<LogicalType> input_schema_types;
    vector<idx_t> source_indexes;

    for (idx_t col_idx = 0;
         col_idx < (idx_t)schema_root.arrow_schema.n_children; col_idx++)
    {
      auto &schema = *schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("airport_dynamic_table_bind: released schema passed");
      }
      auto name = string(schema.name);
      if (name.empty())
      {
        name = string("v") + to_string(col_idx);
      }

      // If we have a table input skip over it.
      if (schema.metadata != nullptr)
      {
        auto column_metadata = ArrowSchemaMetadata(schema.metadata);

        auto is_table_input = column_metadata.GetOption("is_table_input");
        if (!is_table_input.empty())
        {
          source_indexes.push_back(-1);
          continue;
        }
      }
      input_schema_names.push_back(name);
      auto arrow_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), schema);
      input_schema_types.push_back(arrow_type->GetDuckType());
      // Where does this field come from.
      source_indexes.push_back(col_idx);
    }

    // We need to produce a schema that doesn't contain the is_table_input fields.

    auto appender = make_uniq<ArrowAppender>(input_schema_types, input_schema_types.size(), context.GetClientProperties(),
                                             ArrowTypeExtensionData::GetExtensionTypes(context, input_schema_types));

    // Now we need to make a DataChunk from the input bind data so that we can calle the appender.
    DataChunk input_chunk;
    input_chunk.Initialize(Allocator::Get(context),
                           input_schema_types,
                           1);
    input_chunk.SetCardinality(1);

    // Now how do we populate the input_chunk with the input data?
    int seen_named_parameters = 0;
    for (idx_t col_idx = 0;
         col_idx < (idx_t)schema_root.arrow_schema.n_children; col_idx++)
    {
      auto &schema = *schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("airport_dynamic_table_bind: released schema passed");
      }

      // So if the parameter is named, we'd get that off of the metadata
      // otherwise its positional.
      auto metadata = ArrowSchemaMetadata(schema.metadata);

      if (!metadata.GetOption("is_table_input").empty())
      {
        continue;
      }

      if (!metadata.GetOption("is_named_parameter").empty())
      {
        input_chunk.data[col_idx].SetValue(0, input.named_parameters[schema.name]);
        seen_named_parameters += 1;
      }
      else
      {
        // Since named parameters aren't passed in inputs, we need to adjust
        // the offset we're looking at.
        auto &input_data = input.inputs[source_indexes[col_idx - seen_named_parameters]];
        input_chunk.data[col_idx].SetValue(0, input_data);
      }
    }

    // Now that we have the appender append some data.
    appender->Append(input_chunk, 0, input_chunk.size(), input_chunk.size());
    ArrowArray arr = appender->Finalize();

    auto schema_without_table_fields = AirportSchemaWithoutTableFields(input_schema);

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        auto record_batch,
        arrow::ImportRecordBatch(&arr, schema_without_table_fields),
        server_location,
        flight_descriptor, "");

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto buffer_output_stream,
                                            arrow::io::BufferOutputStream::Create(),
                                            server_location,
                                            "create buffer output stream");

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto writer,
                                            arrow::ipc::MakeStreamWriter(buffer_output_stream, schema_without_table_fields),
                                            server_location,
                                            "make stream writer");

    AIRPORT_ARROW_ASSERT_OK_LOCATION(writer->WriteRecordBatch(*record_batch),
                                     server_location,
                                     "write record batch");

    AIRPORT_ARROW_ASSERT_OK_LOCATION(writer->Close(),
                                     server_location,
                                     "close record batch writer");

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(
        auto buffer,
        buffer_output_stream->Finish(),
        server_location,
        "finish buffer output stream");

    return buffer;
  }

  static unique_ptr<FunctionData> AirportDynamicTableBind(
      ClientContext &context,
      TableFunctionBindInput &input,
      vector<LogicalType> &return_types,
      vector<string> &names)
  {
    auto function_info = input.info->Cast<AirportDynamicTableFunctionInfo>();

    auto buffer = AirportDynamicSerializeParameters(function_info.function->input_schema,
                                                    context,
                                                    input,
                                                    function_info.function->location,
                                                    function_info.function->flight_info->descriptor());

    // So save the buffer so we can send it to the server to determine
    // the schema of the flight.

    // Then call the DoAction get_dynamic_flight_info with those arguments.
    GetFlightInfoTableFunctionParameters tf_params;
    tf_params.parameters = string((char *)buffer->data(), buffer->size());
    tf_params.schema_name = function_info.function->schema_name;
    tf_params.action_name = function_info.function->action_name;

    // If we are doing an table in_out function we need to serialize the schema of the input.

    // So I think we need to build an ArrowConverter something to build a
    if (input.table_function.in_out_function != nullptr)
    {
      ArrowSchema input_table_schema;
      auto client_properties = context.GetClientProperties();

      ArrowConverter::ToArrowSchema(&input_table_schema,
                                    input.input_table_types,
                                    input.input_table_names,
                                    client_properties);

      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto table_input_schema,
                                                         arrow::ImportSchema(&input_table_schema),
                                                         function_info.function->location,
                                                         function_info.function->flight_info->descriptor(),
                                                         "");

      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
          auto serialized_schema,
          arrow::ipc::SerializeSchema(*table_input_schema, arrow::default_memory_pool()),
          function_info.function->location,
          function_info.function->flight_info->descriptor(),
          "");

      std::string serialized_table_in_schema(reinterpret_cast<const char *>(serialized_schema->data()), serialized_schema->size());

      tf_params.table_input_schema = serialized_table_in_schema;
    }

    auto params = AirportParseTakeFlightParameters(function_info.function->location,
                                                   context, input);

    return AirportTakeFlightBindWithFlightDescriptor(
        params,
        function_info.function->flight_info->descriptor(),
        context,
        input, return_types, names, nullptr,
        std::make_shared<GetFlightInfoTableFunctionParameters>(tf_params));
  }

  struct ArrowSchemaTableFunctionTypes
  {
    vector<LogicalType> all;
    vector<string> all_names;
    vector<LogicalType> positional;
    vector<string> positional_names;
    std::map<std::string, LogicalType> named;
  };

  static ArrowSchemaTableFunctionTypes
  AirportSchemaToLogicalTypesWithNaming(
      ClientContext &context,
      std::shared_ptr<arrow::Schema> schema,
      const string &server_location,
      const flight::FlightDescriptor &flight_descriptor)
  {
    ArrowSchemaWrapper schema_root;

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        ExportSchema(*schema, &schema_root.arrow_schema),
        server_location,
        flight_descriptor,
        "ExportSchema");

    ArrowSchemaTableFunctionTypes result;

    for (idx_t col_idx = 0;
         col_idx < (idx_t)schema_root.arrow_schema.n_children; col_idx++)
    {
      auto &schema = *schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("AirportSchemaToLogicalTypes: released schema passed");
      }
      auto arrow_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), schema);

      if (schema.dictionary)
      {
        auto dictionary_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), *schema.dictionary);
        arrow_type->SetDictionary(std::move(dictionary_type));
      }

      auto metadata = ArrowSchemaMetadata(schema.metadata);

      if (!metadata.GetOption("is_table_input").empty())
      {
        result.all.emplace_back(LogicalType(LogicalTypeId::TABLE));
      }
      else
      {
        result.all.emplace_back(arrow_type->GetDuckType());
      }

      result.all_names.emplace_back(string(schema.name));

      if (!metadata.GetOption("is_named_parameter").empty())
      {
        result.named[schema.name] = arrow_type->GetDuckType();
      }
      else
      {
        if (!metadata.GetOption("is_table_input").empty())
        {
          result.positional.emplace_back(LogicalType(LogicalTypeId::TABLE));
        }
        else
        {
          result.positional.emplace_back(arrow_type->GetDuckType());
        }
        result.positional_names.push_back(string(schema.name));
      }
    }
    return result;
  }

  struct AirportDynamicTableInOutGlobalState : public GlobalTableFunctionState, public AirportExchangeGlobalState
  {
  };

  static unique_ptr<GlobalTableFunctionState>
  AirportDynamicTableInOutGlobalInit(ClientContext &context,
                                     TableFunctionInitInput &input)
  {
    auto &bind_data = input.bind_data->Cast<AirportTakeFlightBindData>();

    auto auth_token = AirportAuthTokenForLocation(context, bind_data.server_location, "", "");

    auto trace_uuid = UUID::ToString(UUID::GenerateRandomUUID());

    arrow::flight::FlightCallOptions call_options;
    airport_add_standard_headers(call_options, bind_data.server_location);
    airport_add_authorization_header(call_options, auth_token);

    call_options.headers.emplace_back("airport-trace-id", trace_uuid);

    call_options.headers.emplace_back("airport-operation", "table_in_out_function");

    D_ASSERT(bind_data.table_function_parameters != nullptr);
    call_options.headers.emplace_back("airport-action-name", bind_data.table_function_parameters->action_name);

    // Indicate if the caller is interested in data being returned.
    call_options.headers.emplace_back("return-chunks", "1");

    auto &flight_descriptor = bind_data.scan_data->flight_descriptor();

    if (flight_descriptor.type == arrow::flight::FlightDescriptor::PATH)
    {
      auto path_parts = flight_descriptor.path;
      std::string joined_path_parts = join_vector_of_strings(path_parts, '/');
      call_options.headers.emplace_back("airport-flight-path", joined_path_parts);
    }

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        auto exchange_result,
        bind_data.flight_client->DoExchange(call_options, flight_descriptor),
        bind_data.server_location,
        flight_descriptor, "");

    // We have the serialized schema that we sent the server earlier so deserialize so we can
    // send it again.
    std::shared_ptr<arrow::Buffer> serialized_schema_buffer = std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t *>(bind_data.table_function_parameters->table_input_schema.data()),
        bind_data.table_function_parameters->table_input_schema.size());

    auto buffer_reader = std::make_shared<arrow::io::BufferReader>(serialized_schema_buffer);

    arrow::ipc::DictionaryMemo in_memo;
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        auto send_schema,
        arrow::ipc::ReadSchema(buffer_reader.get(), &in_memo),
        bind_data.server_location,
        flight_descriptor, "ReadSchema");

    // Send the input set of parameters to the server.
    std::shared_ptr<arrow::Buffer> parameters_buffer = std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t *>(bind_data.table_function_parameters->parameters.data()),
        bind_data.table_function_parameters->parameters.size());

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        exchange_result.writer->WriteMetadata(parameters_buffer),
        bind_data.server_location,
        flight_descriptor,
        "airport_dynamic_table_function: write metadata with parameters");

    // Tell the server the schema that we will be using to write data.
    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        exchange_result.writer->Begin(send_schema),
        bind_data.server_location,
        flight_descriptor,
        "airport_dynamic_table_function: send schema");

    auto scan_data = make_uniq<AirportTakeFlightScanData>(
        bind_data.server_location,
        // This flight info will be correctly populated from the bind.
        bind_data.scan_data->flight_info_,
        std::move(exchange_result.reader));

    auto scan_bind_data = make_uniq<AirportExchangeTakeFlightBindData>(
        (stream_factory_produce_t)&AirportFlightStreamReader::CreateStream,
        (uintptr_t)scan_data.get());

    scan_bind_data->scan_data = std::move(scan_data);
    //    scan_bind_data->flight_client = bind_data.flight_client;
    scan_bind_data->server_location = bind_data.server_location;
    scan_bind_data->trace_id = trace_uuid;

    vector<column_t> column_ids;

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(auto read_schema,
                                                       scan_bind_data->scan_data->stream_->GetSchema(),
                                                       bind_data.server_location,
                                                       flight_descriptor, "");

    auto &data = *scan_bind_data;
    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        ExportSchema(*read_schema, &data.schema_root.arrow_schema),
        bind_data.server_location,
        flight_descriptor,
        "ExportSchema");

    vector<string> reading_arrow_column_names;
    for (idx_t col_idx = 0;
         col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++)
    {
      auto &schema = *data.schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("airport_exchange: released schema passed");
      }
      auto name = string(schema.name);
      if (name.empty())
      {
        name = string("v") + to_string(col_idx);
      }

      reading_arrow_column_names.push_back(name);
    }

    // printf("Arrow schema column names are: %s\n", join_vector_of_strings(reading_arrow_column_names, ',').c_str());
    // printf("Expected order of columns to be: %s\n", join_vector_of_strings(destination_chunk_column_names, ',').c_str());

    vector<string> arrow_types;
    for (idx_t col_idx = 0;
         col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++)
    {
      auto &schema = *data.schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("airport_exchange: released schema passed");
      }
      auto arrow_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), schema);
      arrow_types.push_back(arrow_type->GetDuckType().ToString());

      // Determine if the column is the rowid column by looking at the metadata
      // on the column.
      bool is_rowid_column = false;
      if (schema.metadata != nullptr)
      {
        auto column_metadata = ArrowSchemaMetadata(schema.metadata);

        auto comment = column_metadata.GetOption("is_rowid");
        if (!comment.empty())
        {
          is_rowid_column = true;
          scan_bind_data->rowid_column_index = col_idx;
        }
      }

      if (schema.dictionary)
      {
        auto dictionary_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), *schema.dictionary);
        arrow_type->SetDictionary(std::move(dictionary_type));
      }

      if (!is_rowid_column)
      {
        scan_bind_data->return_types.emplace_back(arrow_type->GetDuckType());
      }

      auto name = string(schema.name);

      // printf("Setting arrow column index %llu to data %s\n", is_rowid_column ? COLUMN_IDENTIFIER_ROW_ID : col_idx, arrow_type->GetDuckType().ToString().c_str());
      scan_bind_data->arrow_table.AddColumn(is_rowid_column ? COLUMN_IDENTIFIER_ROW_ID : col_idx, std::move(arrow_type));

      auto format = string(schema.format);
      if (name.empty())
      {
        name = string("v") + to_string(col_idx);
      }

      if (!is_rowid_column)
      {
        scan_bind_data->names.push_back(name);
      }
    }

    // There shouldn't be any projection ids.
    vector<idx_t> projection_ids;

    auto scan_global_state = make_uniq<ArrowScanGlobalState>();
    scan_global_state->stream = AirportProduceArrowScan(scan_bind_data->CastNoConst<ArrowScanFunctionData>(), column_ids, nullptr);
    scan_global_state->max_threads = 1;

    // Retain the global state.
    unique_ptr<AirportDynamicTableInOutGlobalState> global_state = make_uniq<AirportDynamicTableInOutGlobalState>();

    global_state->scan_global_state = std::move(scan_global_state);
    global_state->schema = send_schema;

    // Now simulate the init input.
    auto fake_init_input = TableFunctionInitInput(
        &scan_bind_data->Cast<FunctionData>(),
        column_ids,
        projection_ids,
        nullptr);

    // Local init.

    auto current_chunk = make_uniq<ArrowArrayWrapper>();
    auto scan_local_state = make_uniq<ArrowScanLocalState>(std::move(current_chunk), context);
    scan_local_state->column_ids = fake_init_input.column_ids;
    scan_local_state->filters = fake_init_input.filters.get();

    global_state->scan_local_state = std::move(scan_local_state);

    // Create a parameter is the commonly passed to the other functions.
    global_state->scan_bind_data = std::move(scan_bind_data);
    global_state->writer = std::move(exchange_result.writer);

    global_state->scan_table_function_input = make_uniq<TableFunctionInput>(
        global_state->scan_bind_data.get(),
        global_state->scan_local_state.get(),
        global_state->scan_global_state.get());

    unique_ptr<duckdb::GlobalTableFunctionState> base_wrapper = std::move(global_state);

    return base_wrapper;
  }

  static OperatorResultType AirportTakeFlightInOut(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                                   DataChunk &output)
  {
    auto &global_state = data_p.global_state->Cast<AirportDynamicTableInOutGlobalState>();

    // We need to send data to the server.

    auto appender = make_uniq<ArrowAppender>(
        input.GetTypes(),
        input.size(),
        context.client.GetClientProperties(),
        ArrowTypeExtensionData::GetExtensionTypes(
            context.client, input.GetTypes()));

    appender->Append(input, 0, input.size(), input.size());
    ArrowArray arr = appender->Finalize();

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        auto record_batch,
        arrow::ImportRecordBatch(&arr, global_state.schema),
        global_state.scan_bind_data->server_location,
        global_state.flight_descriptor, "airport_dynamic_table_function: import record batch");

    // Now send it
    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        global_state.writer->WriteRecordBatch(*record_batch),
        global_state.scan_bind_data->server_location,
        global_state.flight_descriptor, "airport_dynamic_table_function: write record batch");

    // The server could produce results, so we should read them.
    //
    // it would be nice to know if we should expect results or not.
    // but that would require reading more of the stream than what we can
    // do right now.
    //
    // Rusty: for now just produce a chunk for every chunk read.
    output.Reset();
    {
      auto &data = global_state.scan_table_function_input->bind_data->CastNoConst<ArrowScanFunctionData>();
      auto &state = global_state.scan_table_function_input->local_state->Cast<ArrowScanLocalState>();
      auto &global_state2 = global_state.scan_table_function_input->global_state->Cast<ArrowScanGlobalState>();

      auto current_chunk = global_state2.stream->GetNextChunk();
      state.chunk = std::move(current_chunk);

      auto output_size =
          MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(state.chunk->arrow_array.length) - state.chunk_offset);
      output.SetCardinality(state.chunk->arrow_array.length);

      data.lines_read += output_size;
      ArrowTableFunction::ArrowToDuckDB(state,
                                        // I'm not sure if arrow table will be defined
                                        data.arrow_table.GetColumns(),
                                        output,
                                        data.lines_read - output_size,
                                        false);
      output.Verify();
    }

    return OperatorResultType::NEED_MORE_INPUT;
  }

  static OperatorFinalizeResultType AirportTakeFlightInOutFinalize(ExecutionContext &context, TableFunctionInput &data_p,
                                                                   DataChunk &output)
  {
    auto &global_state = data_p.global_state->Cast<AirportDynamicTableInOutGlobalState>();

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        global_state.writer->DoneWriting(),
        global_state.scan_bind_data->server_location,
        global_state.flight_descriptor, "airport_dynamic_table_function: finalize done writing");

    bool is_finished = false;
    {
      auto &scan_data = global_state.scan_table_function_input;
      auto &data = scan_data->bind_data->CastNoConst<ArrowScanFunctionData>();
      auto &state = scan_data->local_state->Cast<ArrowScanLocalState>();
      auto &global_state2 = scan_data->global_state->Cast<ArrowScanGlobalState>();

      auto current_chunk = global_state2.stream->GetNextChunk();
      state.chunk = std::move(current_chunk);

      auto &last_app_metadata = global_state.scan_bind_data->scan_data->last_app_metadata_;
      if (last_app_metadata == "finished")
      {
        is_finished = true;
      }

      auto output_size =
          MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(state.chunk->arrow_array.length) - state.chunk_offset);
      output.SetCardinality(state.chunk->arrow_array.length);

      data.lines_read += output_size;
      if (output_size > 0)
      {
        ArrowTableFunction::ArrowToDuckDB(state,
                                          // I'm not sure if arrow table will be defined
                                          data.arrow_table.GetColumns(),
                                          output,
                                          data.lines_read - output_size,
                                          false);
      }
      output.Verify();
    }

    if (is_finished)
    {
      return OperatorFinalizeResultType::FINISHED;
    }
    // there may be more data.
    return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
  }

  void AirportTableFunctionSet::LoadEntries(ClientContext &context)
  {
    // auto &transaction = AirportTransaction::Get(context, catalog);

    auto &airport_catalog = catalog.Cast<AirportCatalog>();

    auto curl = connection_pool.acquire();
    auto contents = AirportAPI::GetSchemaItems(
        curl,
        catalog.GetDBPath(),
        schema.name,
        schema.schema_data->source,
        cache_directory,
        airport_catalog.credentials);

    connection_pool.release(curl);

    // There can be functions with the same name.
    std::unordered_map<FunctionCatalogSchemaName, std::vector<AirportAPITableFunction>> functions_by_name;

    for (auto &function : contents->table_functions)
    {
      FunctionCatalogSchemaName function_key{function.catalog_name, function.schema_name, function.name};
      functions_by_name[function_key].emplace_back(function);
    }

    for (const auto &pair : functions_by_name)
    {
      TableFunctionSet flight_func_set(pair.first.name);
      vector<FunctionDescription> function_descriptions;

      for (const auto &function : pair.second)
      {
        // These input types are available since they are specified in the metadata, but the
        // schema that is returned likely should be requested dynamically from the dynamic
        // flight function.
        auto input_types = AirportSchemaToLogicalTypesWithNaming(context, function.input_schema, function.location, function.flight_info->descriptor());

        // Determine if we have a table input.
        bool has_table_input = false;
        if (std::find(input_types.all.begin(), input_types.all.end(), LogicalType(LogicalTypeId::TABLE)) != input_types.all.end())
        {
          has_table_input = true;
        }

        FunctionDescription description;
        description.parameter_types = input_types.positional;
        description.parameter_names = input_types.positional_names;
        description.description = function.description;
        function_descriptions.push_back(std::move(description));

        TableFunction table_func;
        if (!has_table_input)
        {
          table_func = TableFunction(
              input_types.positional,
              AirportTakeFlight,
              AirportDynamicTableBind,
              AirportArrowScanInitGlobal,
              ArrowTableFunction::ArrowScanInitLocal);
        }
        else
        {
          table_func = TableFunction(
              input_types.all,
              nullptr,
              // The bind function knows how to handle the in and out.
              AirportDynamicTableBind,
              AirportDynamicTableInOutGlobalInit,
              nullptr);

          table_func.in_out_function = AirportTakeFlightInOut;
          table_func.in_out_function_final = AirportTakeFlightInOutFinalize;
        }

        // Add all of t
        for (auto &named_pair : input_types.named)
        {
          table_func.named_parameters.emplace(named_pair.first, named_pair.second);
        }

        // Need to store some function information along with the function so that when its called
        // we know what to pass to it.
        table_func.function_info = make_uniq<AirportDynamicTableFunctionInfo>(std::make_shared<AirportAPITableFunction>(function));

        flight_func_set.AddFunction(table_func);
      }

      CreateTableFunctionInfo info = CreateTableFunctionInfo(flight_func_set);
      info.catalog = pair.first.catalog_name;
      info.schema = pair.first.schema_name;

      for (auto &desc : function_descriptions)
      {
        info.descriptions.push_back(std::move(desc));
      }

      auto function_entry = make_uniq_base<StandardEntry, TableFunctionCatalogEntry>(catalog, schema,
                                                                                     info.Cast<CreateTableFunctionInfo>());
      CreateEntry(std::move(function_entry));
    }
  }

} // namespace duckdb
