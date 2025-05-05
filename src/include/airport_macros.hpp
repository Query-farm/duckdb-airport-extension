#include "airport_flight_exception.hpp"

#pragma once

#define AIRPORT_ARROW_ASSERT_OK_LOCATION(expr, location, message)                    \
  for (::arrow::Status _st = ::arrow::internal::GenericToStatus((expr)); !_st.ok();) \
    throw AirportFlightException(location, _st, "");

#define AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(expr, location, descriptor, message) \
  for (::arrow::Status _st = ::arrow::internal::GenericToStatus((expr)); !_st.ok();)     \
    throw AirportFlightException(location, descriptor, _st, message);

#define AIRPORT_ARROW_ASSERT_OK_CONTAINER(expr, container, message) \
  AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(expr, (container)->server_location(), (container)->descriptor(), message)

#define AIRPORT_ASSERT_OK_LOCATION_DESCRIPTOR(expr, location, descriptor, message) \
  if (!expr)                                                                       \
  {                                                                                \
    throw AirportFlightException(location, descriptor, #expr, message);            \
  }

#define AIRPORT_ASSERT_OK_CONTAINER(expr, container, message) \
  AIRPORT_ASSERT_OK_LOCATION_DESCRIPTOR(expr, container->server_location(), container->descriptor(), message)

#define AIRPORT_RETURN_IF_LOCATION_DESCRIPTOR(error_prefix, condition, status, location, descriptor, message, extra_message) \
  do                                                                                                                         \
  {                                                                                                                          \
    if (ARROW_PREDICT_FALSE(condition))                                                                                      \
    {                                                                                                                        \
      throw AirportFlightException(location, descriptor, status, message, {{"extra_details", string(extra_message)}});       \
    }                                                                                                                        \
  } while (0)

#define AIRPORT_RETURN_IF_LOCATION(error_prefix, condition, status, location, message, extra_message)      \
  do                                                                                                       \
  {                                                                                                        \
    if (ARROW_PREDICT_FALSE(condition))                                                                    \
    {                                                                                                      \
      throw AirportFlightException(location, status, message, {{"extra_details", string(extra_message)}}); \
    }                                                                                                      \
  } while (0)

#define AIRPORT_ASSIGN_OR_RAISE_IMPL_LOCATION_DESCRIPTOR(result_name, lhs, rexpr, location, descriptor, message)                                           \
  auto &&result_name = (rexpr);                                                                                                                            \
  AIRPORT_RETURN_IF_LOCATION_DESCRIPTOR(error_prefix, !(result_name).ok(), (result_name).status(), location, descriptor, message, ARROW_STRINGIFY(rexpr)); \
  lhs = std::move(result_name).ValueUnsafe();

#define AIRPORT_ASSIGN_OR_RAISE_IMPL_LOCATION(result_name, lhs, rexpr, location, message)                                           \
  auto &&result_name = (rexpr);                                                                                                     \
  AIRPORT_RETURN_IF_LOCATION(error_prefix, !(result_name).ok(), (result_name).status(), location, message, ARROW_STRINGIFY(rexpr)); \
  lhs = std::move(result_name).ValueUnsafe();

#define AIRPORT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(lhs, rexpr, location, descriptor, message) \
  AIRPORT_ASSIGN_OR_RAISE_IMPL_LOCATION_DESCRIPTOR(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr, location, descriptor, message);

#define AIRPORT_ASSIGN_OR_RAISE_CONTAINER(lhs, rexpr, container, message) \
  AIRPORT_ASSIGN_OR_RAISE_IMPL_LOCATION_DESCRIPTOR(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr, (container)->server_location(), (container)->descriptor(), message);

#define AIRPORT_ASSIGN_OR_RAISE_LOCATION(lhs, rexpr, location, message) \
  AIRPORT_ASSIGN_OR_RAISE_IMPL_LOCATION(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr, location, message);

#define AIRPORT_MSGPACK_UNPACK(destination_type, destination_name, source, location, message) \
  destination_type destination_name;                                                          \
  try                                                                                         \
  {                                                                                           \
    msgpack::object_handle oh = msgpack::unpack(                                              \
        reinterpret_cast<const char *>(source.data()),                                        \
        source.size(),                                                                        \
        0);                                                                                   \
    msgpack::object obj = oh.get();                                                           \
    obj.convert(destination_name);                                                            \
  }                                                                                           \
  catch (const std::exception &e)                                                             \
  {                                                                                           \
    throw AirportFlightException(location, message + string(e.what()));                       \
  }

#define AIRPORT_MSGPACK_UNPACK_CONTAINER(destination_type, destination_name, source, container, message) \
  AIRPORT_MSGPACK_UNPACK(destination_type, destination_name, source, container->server_location(), message);

#define AIRPORT_MSGPACK_ACTION_SINGLE_PARAMETER(result_name, action_name, parameters) \
  msgpack::sbuffer parameters_packed_buffer;                                          \
  msgpack::pack(parameters_packed_buffer, parameters);                                \
  arrow::flight::Action result_name{                                                  \
      action_name,                                                                    \
      std::make_shared<arrow::Buffer>(                                                \
          reinterpret_cast<const uint8_t *>(parameters_packed_buffer.data()),         \
          parameters_packed_buffer.size())};
