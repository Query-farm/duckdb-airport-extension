#pragma once

#include <vector>
#include <string>
#include "arrow/flight/client.h"
#include "airport_flight_stream.hpp"

namespace duckdb
{
  void airport_add_standard_headers(arrow::flight::FlightCallOptions &options, const std::string &server_location);
  void airport_add_authorization_header(arrow::flight::FlightCallOptions &options, const std::string &auth_token);

  void airport_add_normal_headers(arrow::flight::FlightCallOptions &options,
                                  const AirportTakeFlightParameters &params,
                                  const std::string &trace_id);

  // Generate a random id that is used for request tracking.
  string airport_trace_id();

}