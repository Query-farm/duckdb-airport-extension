#include "airport_extension.hpp"
#include "duckdb.hpp"

#pragma once

namespace duckdb
{
  unique_ptr<BaseStatistics> airport_take_flight_statistics(ClientContext &context, const FunctionData *bind_data, column_t column_index);

}