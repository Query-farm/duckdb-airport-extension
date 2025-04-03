#pragma once

#include "duckdb.hpp"
#include "arrow/flight/client.h"

namespace flight = arrow::flight;

namespace duckdb
{

  // Capture a server_location and a Flight descriptor, make this a struct
  // since many other classes/structs use this combination of members.
  struct AirportLocationDescriptor
  {
  public:
    AirportLocationDescriptor(const string &server_location,
                              const flight::FlightDescriptor &descriptor)
        : server_location_(server_location), descriptor_(descriptor)
    {
    }

    const string &server_location() const
    {
      return server_location_;
    }

    const flight::FlightDescriptor &descriptor() const
    {
      return descriptor_;
    }

  private:
    const string server_location_;
    const flight::FlightDescriptor descriptor_;
  };

}