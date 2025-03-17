#include "airport_extension.hpp"
#include "storage/airport_insert.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_transaction.hpp"
#include "storage/airport_table_entry.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "airport_extension.hpp"
#include "duckdb/execution/operator/persistent/physical_insert.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"

#include "airport_flight_stream.hpp"
#include "airport_take_flight.hpp"
#include "storage/airport_exchange.hpp"
#include "airport_macros.hpp"
#include "airport_request_headers.hpp"
#include "airport_flight_exception.hpp"
#include "airport_secrets.hpp"
#include "airport_constraints.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "msgpack.hpp"

namespace duckdb
{

  AirportInsert::AirportInsert(LogicalOperator &op, TableCatalogEntry &table,
                               physical_index_vector_t<idx_t> column_index_map_p,
                               bool return_chunk,
                               vector<unique_ptr<Expression>> bound_defaults,
                               vector<unique_ptr<BoundConstraint>> bound_constraints_p)
      : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), insert_table(&table),
        insert_types(table.GetTypes()),
        schema(nullptr),
        column_index_map(std::move(column_index_map_p)),
        return_chunk(return_chunk),
        bound_defaults(std::move(bound_defaults)),
        bound_constraints(std::move(bound_constraints_p))
  {
  }

  AirportInsert::AirportInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info_p,
                               idx_t estimated_cardinality)
      : PhysicalOperator(PhysicalOperatorType::CREATE_TABLE_AS, op.types, estimated_cardinality), insert_table(nullptr), schema(&schema),
        info(std::move(info_p)), return_chunk(false)
  {
    PhysicalInsert::GetInsertInfo(*info, insert_types, bound_defaults);
  }

  //===--------------------------------------------------------------------===//
  // States
  //===--------------------------------------------------------------------===//
  class AirportInsertGlobalState : public GlobalSinkState, public AirportExchangeGlobalState
  {
  public:
    explicit AirportInsertGlobalState(
        ClientContext &context,
        AirportTableEntry *table,
        const vector<LogicalType> &return_types,
        bool return_chunk)
        : table(table), insert_count(0),
          return_collection(context, return_types), return_chunk(return_chunk)
    {
      if (!table)
      {
        throw NotImplementedException("AirportInsertGlobalState: table is null");
      }
    }

    AirportTableEntry *table;
    idx_t insert_count;
    mutex insert_lock;

    ColumnDataCollection return_collection;

    bool return_chunk;
  };

  class AirportInsertLocalState : public LocalSinkState
  {
  public:
    AirportInsertLocalState(ClientContext &context,
                            const vector<LogicalType> &types,
                            //                            const TableCatalogEntry &table,
                            const vector<unique_ptr<Expression>> &bound_defaults,
                            const vector<unique_ptr<BoundConstraint>> &bound_constraints)
        : default_executor(context, bound_defaults), bound_constraints(bound_constraints)
    {
      returning_data_chunk.Initialize(Allocator::Get(context), types);
    }

    ConstraintState &GetConstraintState(TableCatalogEntry &table, TableCatalogEntry &tableref);
    ExpressionExecutor default_executor;
    const vector<unique_ptr<BoundConstraint>> &bound_constraints;
    unique_ptr<ConstraintState> constraint_state;

    DataChunk returning_data_chunk;
  };

  ConstraintState &AirportInsertLocalState::GetConstraintState(TableCatalogEntry &table, TableCatalogEntry &tableref)
  {
    if (!constraint_state)
    {
      constraint_state = make_uniq<ConstraintState>(table, bound_constraints);
    }
    return *constraint_state;
  }

  static pair<vector<string>, vector<LogicalType>> AirportGetInsertColumns(const AirportInsert &insert, AirportTableEntry &entry)
  {
    vector<string> column_names;
    vector<LogicalType> column_types;
    auto &columns = entry.GetColumns();
    if (!insert.column_index_map.empty())
    {
      vector<PhysicalIndex> column_indexes;
      column_indexes.resize(columns.LogicalColumnCount(), PhysicalIndex(DConstants::INVALID_INDEX));
      for (idx_t c = 0; c < insert.column_index_map.size(); c++)
      {
        auto column_index = PhysicalIndex(c);
        auto mapped_index = insert.column_index_map[column_index];
        if (mapped_index == DConstants::INVALID_INDEX)
        {
          // column not specified
          continue;
        }
        column_indexes[mapped_index] = column_index;
      }
    }

    // Since we are supporting default values now, we want to end all columns of the table.
    // rather than just the columns the user has specified.
    for (auto &col : entry.GetColumns().Logical())
    {
      column_types.push_back(col.GetType());
      column_names.push_back(col.GetName());
    }
    return make_pair(column_names, column_types);
  }

  unique_ptr<GlobalSinkState> AirportInsert::GetGlobalSinkState(ClientContext &context) const
  {
    optional_ptr<AirportTableEntry> table;
    //    AirportTableEntry *insert_table;
    if (info)
    {
      // Create table as
      D_ASSERT(!insert_table);
      auto &schema_ref = *schema.get_mutable();
      table = &schema_ref.CreateTable(schema_ref.GetCatalogTransaction(context), *info)->Cast<AirportTableEntry>();
    }
    else
    {
      D_ASSERT(insert_table);
      table = &insert_table.get_mutable()->Cast<AirportTableEntry>();
    }

    auto insert_global_state = make_uniq<AirportInsertGlobalState>(context, table.get(), GetTypes(), return_chunk);

    auto &transaction = AirportTransaction::Get(context, insert_table->catalog);
    // auto &connection = transaction.GetConnection();
    auto [send_names, send_types] = AirportGetInsertColumns(*this, *table);

    insert_global_state->send_types = send_types;
    insert_global_state->send_names = send_names;

    // FIXME: so if the user doesn't specify the column list
    // it means that the send_names/send_types is empty.

    D_ASSERT(send_names.size() == send_types.size());
    D_ASSERT(send_names.size() > 0);
    D_ASSERT(send_types.size() > 0);

    ArrowSchema send_schema;
    auto client_properties = context.GetClientProperties();
    ArrowConverter::ToArrowSchema(&send_schema, insert_global_state->send_types, send_names,
                                  client_properties);

    D_ASSERT(table != nullptr);

    vector<string> returning_column_names;
    for (auto &cd : table->GetColumns().Logical())
    {
      returning_column_names.push_back(cd.GetName());
    }

    AirportExchangeGetGlobalSinkState(context,
                                      *table.get(),
                                      *table,
                                      insert_global_state.get(),
                                      send_schema,
                                      return_chunk,
                                      "insert",
                                      returning_column_names,
                                      transaction.identifier);

    return std::move(insert_global_state);
  }

  unique_ptr<LocalSinkState> AirportInsert::GetLocalSinkState(ExecutionContext &context) const
  {
    return make_uniq<AirportInsertLocalState>(context.client,
                                              insert_types,
                                              bound_defaults,
                                              bound_constraints);
  }

  idx_t AirportInsert::OnConflictHandling(TableCatalogEntry &table,
                                          ExecutionContext &context,
                                          AirportInsertGlobalState &gstate,
                                          AirportInsertLocalState &lstate,
                                          DataChunk &chunk) const
  {
    if (action_type == OnConflictAction::THROW)
    {
      auto &constraint_state = lstate.GetConstraintState(table, table);
      AirportVerifyAppendConstraints(constraint_state, context.client, chunk, nullptr, gstate.send_names);
      return 0;
    }
    return 0;
  }

  void AirportInsert::ResolveDefaults(const TableCatalogEntry &table, DataChunk &chunk,
                                      const physical_index_vector_t<idx_t> &column_index_map,
                                      ExpressionExecutor &default_executor, DataChunk &result)
  {
    chunk.Flatten();
    default_executor.SetChunk(chunk);

    result.Reset();
    result.SetCardinality(chunk);

    if (!column_index_map.empty())
    {
      // columns specified by the user, use column_index_map
      for (auto &col : table.GetColumns().Physical())
      {
        auto storage_idx = col.StorageOid();
        auto mapped_index = column_index_map[col.Physical()];
        if (mapped_index == DConstants::INVALID_INDEX)
        {
          // insert default value
          default_executor.ExecuteExpression(storage_idx, result.data[storage_idx]);
        }
        else
        {
          // get value from child chunk
          D_ASSERT((idx_t)mapped_index < chunk.ColumnCount());
          D_ASSERT(result.data[storage_idx].GetType() == chunk.data[mapped_index].GetType());
          result.data[storage_idx].Reference(chunk.data[mapped_index]);
        }
      }
    }
    else
    {
      // no columns specified, just append directly
      for (idx_t i = 0; i < result.ColumnCount(); i++)
      {
        D_ASSERT(result.data[i].GetType() == chunk.data[i].GetType());
        result.data[i].Reference(chunk.data[i]);
      }
    }
  }

  //===--------------------------------------------------------------------===//
  // Sink
  //===--------------------------------------------------------------------===//
  SinkResultType AirportInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const
  {
    auto &gstate = input.global_state.Cast<AirportInsertGlobalState>();
    auto &ustate = input.local_state.Cast<AirportInsertLocalState>();

    // So this is going to write the data into the returning_data_chunk
    // which has all table columns.
    AirportInsert::ResolveDefaults(*gstate.table, chunk, column_index_map, ustate.default_executor, ustate.returning_data_chunk);

    // So there is some confusion about which columns are at a particular index.
    OnConflictHandling(*gstate.table, context, gstate, ustate, ustate.returning_data_chunk);

    auto appender = make_uniq<ArrowAppender>(gstate.send_types, ustate.returning_data_chunk.size(), context.client.GetClientProperties(),
                                             ArrowTypeExtensionData::GetExtensionTypes(
                                                 context.client, gstate.send_types));
    appender->Append(ustate.returning_data_chunk, 0, ustate.returning_data_chunk.size(), ustate.returning_data_chunk.size());
    ArrowArray arr = appender->Finalize();

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
        auto record_batch,
        arrow::ImportRecordBatch(&arr, gstate.schema),
        gstate.table->table_data->location,
        gstate.flight_descriptor, "");

    // Acquire a lock because we don't want other threads to be writing to the same streams
    // at the same time.
    lock_guard<mutex> delete_guard(gstate.insert_lock);

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        gstate.writer->WriteRecordBatch(*record_batch),
        gstate.table->table_data->location,
        gstate.flight_descriptor, "");

    // Since we wrote a batch I'd like to read the data returned if we are returning chunks.
    if (gstate.return_chunk)
    {
      ustate.returning_data_chunk.Reset();

      {
        auto &data = gstate.scan_table_function_input->bind_data->CastNoConst<ArrowScanFunctionData>(); // FIXME
        auto &state = gstate.scan_table_function_input->local_state->Cast<ArrowScanLocalState>();
        auto &global_state = gstate.scan_table_function_input->global_state->Cast<ArrowScanGlobalState>();

        state.Reset();

        auto current_chunk = global_state.stream->GetNextChunk();
        state.chunk = std::move(current_chunk);

        auto output_size =
            MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(state.chunk->arrow_array.length) - state.chunk_offset);
        data.lines_read += output_size;
        ustate.returning_data_chunk.SetCardinality(state.chunk->arrow_array.length);

        ArrowTableFunction::ArrowToDuckDB(state,
                                          data.arrow_table.GetColumns(),
                                          ustate.returning_data_chunk,
                                          data.lines_read - output_size,
                                          false);
        ustate.returning_data_chunk.Verify();
        gstate.return_collection.Append(ustate.returning_data_chunk);
      }
    }

    return SinkResultType::NEED_MORE_INPUT;
  }

  struct AirportInsertFinalMetadata
  {
    uint64_t total_inserted;
    MSGPACK_DEFINE_MAP(total_inserted)
  };

  //===--------------------------------------------------------------------===//
  // Finalize
  //===--------------------------------------------------------------------===//
  SinkFinalizeType AirportInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                           OperatorSinkFinalizeInput &input) const
  {
    auto &gstate = input.global_state.Cast<AirportInsertGlobalState>();

    // printf("AirportDelete::Finalize started, indicating that writing is done\n");
    auto flight_descriptor = gstate.table->table_data->flight_info->descriptor();

    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(
        gstate.writer->DoneWriting(),
        gstate.table->table_data->location,
        gstate.flight_descriptor, "");

    {
      //      auto &data = gstate.scan_table_function_input->bind_data->CastNoConst<ArrowScanFunctionData>(); // FIXME
      auto &state = gstate.scan_table_function_input->local_state->Cast<ArrowScanLocalState>();
      auto &global_state = gstate.scan_table_function_input->global_state->Cast<ArrowScanGlobalState>();

      state.Reset();

      auto current_chunk = global_state.stream->GetNextChunk();
      state.chunk = std::move(current_chunk);

      if (!gstate.scan_bind_data->scan_data->last_app_metadata_.empty())
      {
        auto metadata = *&gstate.scan_bind_data->scan_data->last_app_metadata_;

        AirportInsertFinalMetadata final_metadata;
        try
        {
          msgpack::object_handle oh = msgpack::unpack(
              (const char *)metadata.data(),
              metadata.size(),
              0);
          msgpack::object obj = oh.get();
          obj.convert(final_metadata);
          gstate.insert_count = final_metadata.total_inserted;
        }
        catch (const std::exception &e)
        {
          throw AirportFlightException(gstate.table->table_data->location,
                                       gstate.flight_descriptor,
                                       "Failed to parse msgpack encoded object for final insert metadata.",
                                       string(e.what()));
        }
      }
    }

    return SinkFinalizeType::READY;
  }

  //===--------------------------------------------------------------------===//
  // Source
  //===--------------------------------------------------------------------===//
  class AirportInsertSourceState : public GlobalSourceState
  {
  public:
    explicit AirportInsertSourceState(const AirportInsert &op)
    {
      if (op.return_chunk)
      {
        D_ASSERT(op.sink_state);
        auto &g = op.sink_state->Cast<AirportInsertGlobalState>();
        g.return_collection.InitializeScan(scan_state);
      }
    }

    ColumnDataScanState scan_state;
  };

  unique_ptr<GlobalSourceState> AirportInsert::GetGlobalSourceState(ClientContext &context) const
  {
    return make_uniq<AirportInsertSourceState>(*this);
  }

  //===--------------------------------------------------------------------===//
  // GetData
  //===--------------------------------------------------------------------===//
  SourceResultType AirportInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                          OperatorSourceInput &input) const
  {
    auto &state = input.global_state.Cast<AirportInsertSourceState>();
    auto &g = sink_state->Cast<AirportInsertGlobalState>();
    if (!return_chunk)
    {
      chunk.SetCardinality(1);
      chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.insert_count)));
      return SourceResultType::FINISHED;
    }

    g.return_collection.Scan(state.scan_state, chunk);

    return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
  }

  //===--------------------------------------------------------------------===//
  // Helpers
  //===--------------------------------------------------------------------===//
  string AirportInsert::GetName() const
  {
    return info ? "AIRPORT_INSERT" : "AIRPORT_CREATE_TABLE_AS";
  }

  InsertionOrderPreservingMap<string> AirportInsert::ParamsToString() const
  {
    InsertionOrderPreservingMap<string> result;
    result["Table Name"] = !info ? insert_table->name : info->Base().table;
    return result;
  }

  unique_ptr<PhysicalOperator> AirportCatalog::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                          unique_ptr<PhysicalOperator> plan)
  {

    if (op.action_type != OnConflictAction::THROW)
    {
      throw BinderException("ON CONFLICT clause not yet supported for insertion into Airport table");
    }

    //    plan = AddCastToAirportTypes(context, std::move(plan));

    auto insert = make_uniq<AirportInsert>(
        op,
        op.table,
        op.column_index_map,
        op.return_chunk,
        std::move(op.bound_defaults),
        std::move(op.bound_constraints));

    insert->children.push_back(std::move(plan));
    return std::move(insert);
  }

  // unique_ptr<PhysicalOperator> AirportCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
  //                                                                unique_ptr<PhysicalOperator> plan)
  // {
  //   // plan = AddCastToAirportTypes(context, std::move(plan));

  //   auto insert = make_uniq<AirportInsert>(op, op.schema, std::move(op.info));
  //   insert->children.push_back(std::move(plan));
  //   return std::move(insert);
  // }

}
