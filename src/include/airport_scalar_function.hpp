#pragma once

#include "duckdb.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"

#include "airport_request_headers.hpp"
#include "airport_macros.hpp"
#include "airport_secrets.hpp"
#include "arrow/util/key_value_metadata.h"

namespace duckdb
{

  class AirportScalarFunctionInfo : public ScalarFunctionInfo, public AirportLocationDescriptor
  {
  private:
    const string function_name_;
    const std::shared_ptr<arrow::Schema> output_schema_;
    const std::shared_ptr<arrow::Schema> input_schema_;

  public:
    AirportScalarFunctionInfo(const string &location,
                              const string &name,
                              const flight::FlightDescriptor &flight_descriptor,
                              const std::shared_ptr<arrow::Schema> &output_schema,
                              const std::shared_ptr<arrow::Schema> &input_schema)
        : ScalarFunctionInfo(),
          AirportLocationDescriptor(location, flight_descriptor),
          function_name_(name),
          output_schema_(output_schema),
          input_schema_(input_schema)
    {
    }

    AirportScalarFunctionInfo(
        const string &name,
        const AirportLocationDescriptor &location,
        const std::shared_ptr<arrow::Schema> &output_schema,
        const std::shared_ptr<arrow::Schema> &input_schema)
        : ScalarFunctionInfo(),
          AirportLocationDescriptor(location),
          function_name_(name),
          output_schema_(output_schema),
          input_schema_(input_schema)
    {
    }

    ~AirportScalarFunctionInfo() override
    {
    }

    const string &function_name() const
    {
      return function_name_;
    }

    const bool input_schema_includes_any_types() const
    {
      for (int i = 0; i < input_schema_->num_fields(); ++i)
      {
        const auto &field = input_schema_->field(i);
        auto field_metadata = field->metadata();

        if (field_metadata != nullptr && field_metadata->Contains("is_any_type"))
        {
          return true;
        }
      }
      return false;
    }

    const std::shared_ptr<arrow::Schema> &output_schema() const
    {
      return output_schema_;
    }

    const std::shared_ptr<arrow::Schema> &input_schema() const
    {
      return input_schema_;
    }
  };

  void AirportScalarFunctionProcessChunk(DataChunk &args, ExpressionState &state, Vector &result);
  unique_ptr<FunctionLocalState> AirportScalarFunctionInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data);

  unique_ptr<FunctionData> AirportScalarFunctionBind(ClientContext &context, ScalarFunction &bound_function,
                                                     vector<unique_ptr<Expression>> &arguments);
}