#pragma once

#include "duckdb/common/types.hpp"

#include <arrow/flight/client.h>
#include <curl/curl.h>
#include <msgpack.hpp>

namespace duckdb
{
  struct AirportCredentials;

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

  struct AirportAPITable
  {
    string location;
    std::shared_ptr<arrow::flight::FlightInfo> flight_info;

    string catalog_name;
    string schema_name;
    string name;
    string comment;

    AirportAPITable(
        const std::string &location,
        std::shared_ptr<arrow::flight::FlightInfo> flightInfo,
        const std::string &catalog,
        const std::string &schema,
        const std::string &tableName,
        const std::string &tableComment)
        : location(location),
          flight_info(flightInfo),
          catalog_name(catalog),
          schema_name(schema),
          name(tableName),
          comment(tableComment) {}
  };

  struct AirportAPIScalarFunction
  {
    string catalog_name;
    string schema_name;
    string name;

    string comment;
    string description;

    string location;
    std::shared_ptr<arrow::flight::FlightInfo> flight_info;
    std::shared_ptr<arrow::Schema> input_schema;
  };

  struct AirportAPITableFunction
  {
    string catalog_name;
    string schema_name;

    // The name of the table function.
    string name;
    string description;
    string comment;

    // The name of the action passed, if there is a single
    // flight that exists it can respond with different outputs
    // based on this name.
    string action_name;

    // The location of the flight server that will prduce the data.
    string location;

    // This is the flight that will be called to satisfy the function.
    std::shared_ptr<arrow::flight::FlightInfo> flight_info;

    // The schema of the input to the function.
    std::shared_ptr<arrow::Schema> input_schema;
  };

  struct AirportAPISchema
  {
    string schema_name;
    string catalog_name;
    string comment;
    unordered_map<string, string> tags;

    AirportSerializedContentsWithSHA256Hash source;
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
    static vector<string> GetCatalogs(const string &catalog, AirportCredentials credentials);
    static unique_ptr<AirportSchemaContents> GetSchemaItems(CURL *curl,
                                                            const string &catalog,
                                                            const string &schema,
                                                            const AirportSerializedContentsWithSHA256Hash &source,
                                                            const string &cache_base_dir,
                                                            shared_ptr<AirportCredentials> credentials);
    static unique_ptr<AirportSchemaCollection> GetSchemas(const string &catalog, shared_ptr<AirportCredentials> credentials);

    static void PopulateCatalogSchemaCacheFromURLorContent(CURL *curl,
                                                           const AirportSchemaCollection &collection,
                                                           const string &catalog_name,
                                                           const string &baseDir);

    static std::shared_ptr<arrow::flight::FlightClient> FlightClientForLocation(const std::string &location);

    // The the rowid column type, LogicalType::INVALID if none is present.
    static LogicalType GetRowIdType(ClientContext &context,
                                    std::shared_ptr<arrow::flight::FlightInfo> flight_info,
                                    const string &location,
                                    const arrow::flight::FlightDescriptor &descriptor);
  };

} // namespace duckdb
