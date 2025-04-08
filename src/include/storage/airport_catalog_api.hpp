#pragma once

#include "duckdb/common/types.hpp"

#include <arrow/flight/client.h>
#include <curl/curl.h>
#include <msgpack.hpp>
#include "airport_macros.hpp"
#include <arrow/io/memory.h>
#include "airport_location_descriptor.hpp"

namespace duckdb
{
  struct AirportAttachParameters;

  struct AirportSerializedCatalogSchemaRequest
  {
    std::string catalog_name;

    MSGPACK_DEFINE_MAP(catalog_name)
  };

  struct AirportSerializedCompressedContent
  {
    // The uncompressed length of the data.
    uint32_t length;
    // The compressed data using ZStandard.
    std::string data;

    MSGPACK_DEFINE(length, data)
  };

  struct AirportSerializedContentsWithSHA256Hash
  {
    // The SHA256 of the serialized contents.
    // or the external url.
    std::string sha256;

    // The external URL where the contents should be obtained.
    std::optional<std::string> url;

    // The inline serialized contents.
    std::optional<std::string> serialized;

    MSGPACK_DEFINE_MAP(sha256, url, serialized)
  };

  struct AirportSerializedSchema
  {
    // The name of the schema
    std::string schema;
    // The description of the schema
    std::string description;
    // Any tags to apply to the schema.
    std::unordered_map<std::string, std::string> tags;
    // The contents of the schema itself.
    AirportSerializedContentsWithSHA256Hash contents;

    MSGPACK_DEFINE_MAP(schema, description, tags, contents)
  };

  struct AirportGetCatalogVersionResult
  {
    uint64_t catalog_version;
    bool is_fixed;
    MSGPACK_DEFINE(catalog_version, is_fixed)
  };

  struct AirportSerializedCatalogRoot
  {
    // The contents of the catalog itself.
    AirportSerializedContentsWithSHA256Hash contents;
    // A list of schemas.
    std::vector<AirportSerializedSchema> schemas;

    // The version of the catalog returned.
    AirportGetCatalogVersionResult version_info;

    MSGPACK_DEFINE_MAP(contents, schemas, version_info)
  };

  struct AirportSerializedFlightAppMetadata
  {
    // This is the type of item to populate in DuckDB's catalog
    // it can be "table", "scalar_function", "table_function"
    string type;

    // The name of the schema where this item exists.
    string schema;

    // The name of the catalog or database where this item exists.
    string catalog;

    // The name of this item.
    string name;

    // A custom comment for this item.
    string comment;

    // This is the Arrow serialized schema for the input
    // to the function, its not set on tables.

    // In the case of scalar function this is the input schema
    std::optional<string> input_schema;

    // The name of the action passed to the Arrow Flight server
    std::optional<string> action_name;

    // This is the function description for table or scalar functions.
    std::optional<string> description;

    MSGPACK_DEFINE_MAP(
        type, schema,
        catalog, name,
        comment, input_schema,
        action_name, description)
  };

  struct AirportAPIObjectBase : public AirportLocationDescriptor
  {

  public:
    AirportAPIObjectBase(
        const arrow::flight::FlightDescriptor &descriptor,
        const std::shared_ptr<arrow::Schema> &schema,
        const std::string &server_location,
        const std::string &catalog,
        const std::string &schema_name,
        const std::string &name,
        const std::string &comment,
        const std::optional<std::string> &input_schema = std::nullopt)
        : AirportLocationDescriptor(server_location, descriptor),
          schema_(schema),
          catalog_name_(catalog),
          schema_name_(schema_name),
          name_(name),
          comment_(comment)
    {
      if (input_schema.has_value())
      {
        auto &serialized_schema = input_schema.value();

        arrow::io::BufferReader parameter_schema_reader(
            std::make_shared<arrow::Buffer>(serialized_schema));

        arrow::ipc::DictionaryMemo in_memo;
        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
            auto parameter_schema,
            arrow::ipc::ReadSchema(&parameter_schema_reader, &in_memo),
            server_location,
            descriptor,
            "Read serialized input schema");

        input_schema_ = parameter_schema;
      }
      else
      {
        input_schema_ = nullptr;
      }
    }

    AirportAPIObjectBase(
        const arrow::flight::FlightDescriptor &descriptor,
        const std::shared_ptr<arrow::Schema> &schema,
        const std::string &server_location,
        const AirportSerializedFlightAppMetadata &parsed_app_metadata) : AirportAPIObjectBase(descriptor,
                                                                                              schema,
                                                                                              server_location,
                                                                                              parsed_app_metadata.catalog,
                                                                                              parsed_app_metadata.schema,
                                                                                              parsed_app_metadata.name,
                                                                                              parsed_app_metadata.comment,
                                                                                              parsed_app_metadata.input_schema)
    {
    }

    const std::shared_ptr<arrow::Schema> &schema() const
    {
      return schema_;
    }

    const std::string &catalog_name() const
    {
      return catalog_name_;
    }

    const std::string &schema_name() const
    {
      return schema_name_;
    }

    const std::string &name() const
    {
      return name_;
    }

    const std::string &comment() const
    {
      return comment_;
    }

    const std::shared_ptr<arrow::Schema> &input_schema() const
    {
      return input_schema_;
    }

  public:
    static std::shared_ptr<arrow::Schema> GetSchema(
        const std::string &server_location,
        const arrow::flight::FlightInfo &flight_info)
    {
      arrow::ipc::DictionaryMemo dictionary_memo;
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
          auto extracted_schema,
          flight_info.GetSchema(&dictionary_memo),
          server_location,
          flight_info.descriptor(),
          "GetSchema");
      return extracted_schema;
    }

  private:
    std::shared_ptr<arrow::Schema> input_schema_;
    const std::shared_ptr<arrow::Schema> schema_;
    const string catalog_name_;
    const string schema_name_;
    const string name_;
    const string comment_;
  };

  struct AirportAPITable : AirportAPIObjectBase
  {
    AirportAPITable(
        const std::string &server_location,
        const arrow::flight::FlightDescriptor &descriptor,
        const std::shared_ptr<arrow::Schema> &schema,
        const AirportSerializedFlightAppMetadata &parsed_app_metadata)
        : AirportAPIObjectBase(
              descriptor,
              schema,
              server_location,
              parsed_app_metadata)
    {
    }
  };

  struct AirportAPIScalarFunction : AirportAPIObjectBase
  {

    AirportAPIScalarFunction(
        const std::string &server_location,
        const arrow::flight::FlightDescriptor &descriptor,
        const std::shared_ptr<arrow::Schema> &schema,
        const AirportSerializedFlightAppMetadata &parsed_app_metadata)
        : AirportAPIObjectBase(
              descriptor,
              schema,
              server_location,
              parsed_app_metadata),
          description_(parsed_app_metadata.description.value_or(""))
    {

      if (input_schema() == nullptr)
      {
        throw IOException("Function metadata does not have an input_schema defined for function " + parsed_app_metadata.schema + "." + parsed_app_metadata.name);
      }
    }

    const string &description() const
    {
      return description_;
    }

  private:
    const string description_;
  };

  struct AirportAPITableFunction : AirportAPIObjectBase
  {
  private:
    const string description_;
    // The name of the action passed, if there is a single
    // flight that exists it can respond with different outputs
    // based on this name.
    const string action_name_;

    // The location of the flight server that will prduce the data.
    const string location_;

  public:
    AirportAPITableFunction(
        const std::string &server_location,
        const flight::FlightDescriptor &flight_descriptor,
        const std::shared_ptr<arrow::Schema> &schema,
        const AirportSerializedFlightAppMetadata &parsed_app_metadata)
        : AirportAPIObjectBase(
              flight_descriptor,
              schema,
              server_location,
              parsed_app_metadata),
          description_(parsed_app_metadata.description.value_or(""))
    {
      if (input_schema() == nullptr)
      {
        throw IOException("Function metadata does not have an input_schema defined for function " + parsed_app_metadata.schema + "." + parsed_app_metadata.name);
      }
    }

    const string &description() const
    {
      return description_;
    }

    const std::string &action_name() const
    {
      return action_name_;
    }

    const std::string &location() const
    {
      return location_;
    }
  };

  struct AirportAPISchema
  {

    AirportAPISchema(const string &catalog_name,
                     const string &schema_name,
                     const string &comment,
                     const unordered_map<string, string> &tags,
                     const std::shared_ptr<AirportSerializedContentsWithSHA256Hash> &source)
        : catalog_name_(catalog_name),
          schema_name_(schema_name),
          comment_(comment),
          tags_(tags),
          source_(source)
    {
    }

    const string &catalog_name() const
    {
      return catalog_name_;
    }

    const string &schema_name() const
    {
      return schema_name_;
    }

    const string &comment() const
    {
      return comment_;
    }

    const unordered_map<string, string> &tags() const
    {
      return tags_;
    }

    const std::shared_ptr<AirportSerializedContentsWithSHA256Hash> &source() const
    {
      return source_;
    }

  private:
    const string catalog_name_;
    const string schema_name_;
    const string comment_;
    const unordered_map<string, string> tags_;
    const std::shared_ptr<AirportSerializedContentsWithSHA256Hash> source_;
  };

  struct AirportSchemaCollection
  {
    AirportSerializedContentsWithSHA256Hash source;

    vector<AirportAPISchema> schemas;

    AirportGetCatalogVersionResult version_info;
  };

  // A collection of parsed items from a schema's metadata.
  struct AirportSchemaContents
  {
  public:
    vector<AirportAPITable> tables;
    vector<AirportAPIScalarFunction> scalar_functions;
    vector<AirportAPITableFunction> table_functions;
  };

  class AirportAPI
  {
  public:
    static vector<string> GetCatalogs(const string &catalog, AirportAttachParameters credentials);
    static unique_ptr<AirportSchemaContents> GetSchemaItems(CURL *curl,
                                                            const string &catalog,
                                                            const string &schema,
                                                            std::shared_ptr<const AirportSerializedContentsWithSHA256Hash> source,
                                                            const string &cache_base_dir,
                                                            std::shared_ptr<AirportAttachParameters> credentials);
    static unique_ptr<AirportSchemaCollection> GetSchemas(const string &catalog,
                                                          const std::shared_ptr<AirportAttachParameters> &credentials);

    static void PopulateCatalogSchemaCacheFromURLorContent(CURL *curl,
                                                           const AirportSchemaCollection &collection,
                                                           const string &catalog_name,
                                                           const string &baseDir);

    static std::shared_ptr<arrow::flight::FlightClient> FlightClientForLocation(const std::string &location);

    // The the rowid column type, LogicalType::INVALID if none is present.
    static LogicalType GetRowIdType(ClientContext &context,
                                    const std::shared_ptr<arrow::Schema> &schema,
                                    const string &location,
                                    const arrow::flight::FlightDescriptor &descriptor);
  };

} // namespace duckdb
