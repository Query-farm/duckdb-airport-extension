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

  class AirportScalarFunctionInfo : public ScalarFunctionInfo
  {
  private:
    string location_;
    string name_;
    flight::FlightDescriptor flight_descriptor_;
    std::shared_ptr<arrow::Schema> output_schema_;
    std::shared_ptr<arrow::Schema> input_schema_;

  public:
    AirportScalarFunctionInfo(const string &location,
                              const string &name,
                              const flight::FlightDescriptor &flight_descriptor,
                              std::shared_ptr<arrow::Schema> output_schema,
                              std::shared_ptr<arrow::Schema> input_schema)
        : ScalarFunctionInfo(), location_(location), name_(name), flight_descriptor_(flight_descriptor),
          output_schema_(output_schema),
          input_schema_(input_schema)
    {
    }

    ~AirportScalarFunctionInfo() override
    {
    }

    const string &location() const
    {
      return location_;
    }

    const string &name() const
    {
      return name_;
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

    const flight::FlightDescriptor &flight_descriptor() const
    {
      return flight_descriptor_;
    }

    std::shared_ptr<arrow::Schema> output_schema() const
    {
      return output_schema_;
    }

    std::shared_ptr<arrow::Schema> input_schema() const
    {
      return input_schema_;
    }
  };

  void AirportScalarFun(DataChunk &args, ExpressionState &state, Vector &result);
  unique_ptr<FunctionLocalState> AirportScalarFunInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data);

  unique_ptr<FunctionData> AirportScalarFunBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments);
}