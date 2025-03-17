#include "airport_extension.hpp"

#include <openssl/evp.h>
#include <openssl/sha.h>
#include <iomanip>
#include <random>
#include <string_view>
#include <vector>
#include <curl/curl.h>

#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/buffer.h>
#include <arrow/ipc/api.h>
#include <arrow/io/memory.h>
#include <arrow/c/bridge.h>
#include "storage/airport_catalog_api.hpp"
#include "storage/airport_catalog.hpp"

#include "duckdb/common/file_system.hpp"

#include "airport_macros.hpp"
#include "airport_secrets.hpp"
#include "airport_request_headers.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"

namespace flight = arrow::flight;

namespace duckdb
{

  static constexpr idx_t FILE_FLAGS_READ = idx_t(1 << 0);
  static constexpr idx_t FILE_FLAGS_WRITE = idx_t(1 << 1);
  //  static constexpr idx_t FILE_FLAGS_FILE_CREATE = idx_t(1 << 3);
  static constexpr idx_t FILE_FLAGS_FILE_CREATE_NEW = idx_t(1 << 4);

  static void writeToTempFile(FileSystem &fs, const string &tempFilename, const std::string_view &data)
  {
    auto handle = fs.OpenFile(tempFilename, FILE_FLAGS_WRITE | FILE_FLAGS_FILE_CREATE_NEW);
    if (!handle)
    {
      throw IOException("Airport: Failed to open file for writing: %s", tempFilename.c_str());
    }

    handle->Write((void *)data.data(), data.size());
    handle->Sync();
    handle->Close();
  }

  static string decompressZStandard(const string &source, const int decompressed_size, const string &location)
  {
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(
        auto codec,
        arrow::util::Codec::Create(arrow::Compression::ZSTD),
        location, "");

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto decompressed_data,
                                            ::arrow::AllocateBuffer(decompressed_size),
                                            location,
                                            "");

    auto decompress_result = codec->Decompress(
        source.size(),
        reinterpret_cast<const uint8_t *>(source.data()),
        decompressed_size,
        decompressed_data->mutable_data());

    AIRPORT_ARROW_ASSERT_OK_LOCATION(decompress_result, location, "Failed to decompress the schema data");

    return string(reinterpret_cast<const char *>(decompressed_data->data()), decompressed_size);
  }

  static string generateTempFilename(FileSystem &fs, const string &dir)
  {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, 999999);

    string filename;

    do
    {
      filename = fs.JoinPath(dir, "temp_" + std::to_string(dis(gen)) + ".tmp");
    } while (fs.FileExists(filename));
    return filename;
  }

  static std::string readFromFile(FileSystem &fs, const string &filename)
  {
    auto handle = fs.OpenFile(filename, FILE_FLAGS_READ);
    if (!handle)
    {
      return "";
    }
    auto file_size = handle->GetFileSize();
    string read_buffer = string(file_size, '\0');
    handle->Read((void *)read_buffer.data(), file_size);
    return read_buffer;
  }

  vector<string> AirportAPI::GetCatalogs(const string &catalog, AirportCredentials credentials)
  {
    throw NotImplementedException("AirportAPI::GetCatalogs");
  }

  static std::unordered_map<std::string, std::shared_ptr<flight::FlightClient>> airport_flight_clients_by_location;

  std::shared_ptr<flight::FlightClient> AirportAPI::FlightClientForLocation(const std::string &location)
  {
    auto it = airport_flight_clients_by_location.find(location);
    if (it != airport_flight_clients_by_location.end())
    {
      return it->second; // Return a reference to the object
    }

    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto parsed_location,
                                            flight::Location::Parse(location), location, "");
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto created_flight_client, flight::FlightClient::Connect(parsed_location), location, "");

    airport_flight_clients_by_location[location] = std::move(created_flight_client);

    return airport_flight_clients_by_location[location];
  }

  static size_t GetRequestWriteCallback(void *contents, size_t size, size_t nmemb, void *userp)
  {
    ((std::string *)userp)->append((char *)contents, size * nmemb);
    return size * nmemb;
  }

  static std::string SHA256ForString(const std::string_view &input)
  {
    EVP_MD_CTX *context = EVP_MD_CTX_new();
    const EVP_MD *md = EVP_sha256();

    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int lengthOfHash = 0;

    EVP_DigestInit_ex(context, md, nullptr);
    EVP_DigestUpdate(context, input.data(), input.size());
    EVP_DigestFinal_ex(context, hash, &lengthOfHash);
    EVP_MD_CTX_free(context);

    std::stringstream ss;
    for (unsigned int i = 0; i < lengthOfHash; ++i)
    {
      ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }

    return ss.str();
  }

  static std::string SHA256ForString(const std::string &input)
  {
    EVP_MD_CTX *context = EVP_MD_CTX_new();
    const EVP_MD *md = EVP_sha256();

    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int lengthOfHash = 0;

    EVP_DigestInit_ex(context, md, nullptr);
    EVP_DigestUpdate(context, input.data(), input.size());
    EVP_DigestFinal_ex(context, hash, &lengthOfHash);
    EVP_MD_CTX_free(context);

    std::stringstream ss;
    for (unsigned int i = 0; i < lengthOfHash; ++i)
    {
      ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }

    return ss.str();
  }

  static std::pair<long, std::string> GetRequest(CURL *curl, const string &url, const string expected_sha256)
  {
    CURLcode res;
    string readBuffer;
    long http_code = 0;

    if (curl)
    {
      // Enable HTTP/2
      curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2);
      curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, GetRequestWriteCallback);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
      res = curl_easy_perform(curl);

      if (res != CURLcode::CURLE_OK)
      {
        string error = curl_easy_strerror(res);
        throw IOException("Airport: Curl Request to " + url + " failed with error: " + error);
      }
      // Get the HTTP response code
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

      if (http_code != 200 || expected_sha256.empty())
      {
        return std::make_pair(http_code, readBuffer);
      }

      // Verify that the SHA256 matches the returned data, don't want a server to
      // corrupt the data.
      auto buffer_view = std::string_view(readBuffer.data(), readBuffer.size());
      auto encountered_sha256 = SHA256ForString(buffer_view);

      if (encountered_sha256 != expected_sha256)
      {
        throw IOException("Airport: SHA256 mismatch for URL: " + url);
      }
      return std::make_pair(http_code, readBuffer);
    }
    throw InternalException("Airport: Failed to initialize curl");
  }

  static std::pair<const string, const string> GetCachePath(FileSystem &fs, const string &input, const string &baseDir)
  {
    auto cacheDir = fs.JoinPath(baseDir, "airport_cache");
    if (!fs.DirectoryExists(cacheDir))
    {
      fs.CreateDirectory(cacheDir);
    }

    if (input.size() < 6)
    {
      throw std::invalid_argument("String is too short to contain the SHA256");
    }

    auto subDirName = input.substr(0, 3); // First 3 characters for subdirectory
    auto fileName = input.substr(3);      // Remaining characters for filename

    auto subDir = fs.JoinPath(cacheDir, subDirName);
    if (!fs.DirectoryExists(subDir))
    {
      fs.CreateDirectory(subDir);
    }

    return std::make_pair(subDir, fs.JoinPath(subDir, fileName));
  }

  void
  AirportAPI::PopulateCatalogSchemaCacheFromURLorContent(CURL *curl,
                                                         const AirportSchemaCollection &collection,
                                                         const string &catalog_name,
                                                         const string &baseDir)
  {
    auto fs = FileSystem::CreateLocal();

    if (collection.source.sha256.empty())
    {
      // If the collection doesn't have a SHA256 there isn't anything we can do.
      throw IOException("Catalog " + catalog_name + " has no SHA256 value for its contents");
    }

    // If there is a large amount of data to be populate its useful to determine
    // if the SHA256 value of all schemas has already been populated.
    //
    // So we have a sentinel path that indicates the entire contents of the SHA256
    // has already been written out to the cache.
    auto sentinel_paths = GetCachePath(*fs, collection.source.sha256, baseDir);

    if (fs->FileExists(sentinel_paths.second))
    {
      // All of the schemas for this overall SHA256
      // have been populated so there is nothing to do.
      return;
    }

    // The schema data is serialized using msgpack
    //
    // it can either be retrieved from a URL or provided inline.
    //
    // The schemas are packed like:
    // [
    //   [string, string]
    // ]
    // the first item is the SHA256 of the data, then the actual data
    // is the second item.
    //
    msgpack::object_handle oh;
    if (collection.source.serialized.has_value())
    {
      const string found_sha = SHA256ForString(collection.source.serialized.value());
      if (found_sha != collection.source.sha256)
      {
        throw IOException("Catalog " + catalog_name + " SHA256 Mismatch expected " + collection.source.sha256 + " found " + found_sha);
      }
      auto &data = collection.source.serialized.value();
      oh = msgpack::unpack((const char *)data.data(), data.size(), 0);
    }
    else if (collection.source.url.has_value())
    {
      // How do we know if the URLs haven't already been populated.
      auto get_result = GetRequest(curl, collection.source.url.value(), collection.source.sha256);

      if (get_result.first != 200)
      {
        throw IOException("Catalog " + catalog_name + " failed to retrieve schema collection contents from URL: " + collection.source.url.value());
      }
      oh = msgpack::unpack((const char *)get_result.second.data(), get_result.second.size(), 0);
    }
    else
    {
      throw IOException("Catalog " + catalog_name + " has no serialized or URL contents for its schema collection");
    }

    std::vector<std::vector<std::string>> unpacked_data = oh.get().as<std::vector<std::vector<std::string>>>();

    // Each item contained in the serailized catalog will be a SHA256 then the
    // the actual value.
    //
    // the SHA256 will be used to check, if the data is corrupted.
    // then used as part of the filename to store the data on disk.
    for (auto &item : unpacked_data)
    {
      if (item.size() != 2)
      {
        throw IOException("Catalog schema cache contents had an item where size != 2");
      }

      const string &expected_sha = item[0];
      const string found_sha = SHA256ForString(item[1]);
      if (found_sha != expected_sha)
      {
        auto error_prefix = "Catalog " + catalog_name + " SHA256 Mismatch expected " + expected_sha + " found " + found_sha;

        // There is corruption.
        if (collection.source.url.has_value())
        {
          throw IOException(error_prefix + " from URL: " + collection.source.url.value());
        }
        else
        {
          throw IOException(error_prefix + " from serialized content");
        }
      }

      auto paths = GetCachePath(*fs, item[0], baseDir);
      auto tempFilename = generateTempFilename(*fs, paths.first);

      writeToTempFile(*fs, tempFilename, item[1]);

      // Rename the temporary file to the final filename
      fs->MoveFile(tempFilename, paths.second);
    }

    // There is a bit of a problem here, we could have an infinite set of
    // schema temp files being written, because if a server is dynamically generating
    // its schemas, the SHA256 will always change.
    //
    // Rusty address this later on.

    // Write a file that the cache has been populated with the top level SHA256
    // value, so that we can skip doing this next time all schemas are used.
    writeToTempFile(*fs, sentinel_paths.second, "1");
  }

  // Function to handle caching
  static std::pair<long, std::string> getCachedRequestData(CURL *curl,
                                                           const AirportSerializedContentsWithSHA256Hash &source,
                                                           const string &baseDir)
  {
    if (source.sha256.empty())
    {
      // Can't cache anything since we don't know the expected sha256 value.
      // and the caching is based on the sha256 values.
      //
      // So if there was inline content supplied use that and fake that it was
      // retrieved from a server.
      if (source.serialized.has_value())
      {
        return std::make_pair(200, source.serialized.value());
      }
      else if (source.url.has_value())
      {
        return GetRequest(curl, source.url.value(), source.sha256);
      }
      else
      {
        throw IOException("SHA256 is empty and URL is empty");
      }
    }

    // If the user supplied an inline serialized value, check if the sha256 matches, if so
    // use it otherwise fall abck to the url
    if (source.serialized.has_value())
    {
      if (SHA256ForString(source.serialized.value()) == source.sha256)
      {
        return std::make_pair(200, source.serialized.value());
      }
      if (!source.url.has_value())
      {
        throw IOException("SHA256 mismatch for inline serialized data and URL is empty");
      }
    }

    auto fs = FileSystem::CreateLocal();

    auto paths = GetCachePath(*fs, source.sha256, baseDir);

    // Check if data is in cache
    if (fs->FileExists(paths.second))
    {
      std::string cachedData = readFromFile(*fs, paths.second);
      if (!cachedData.empty())
      {
        // Verify that the SHA256 matches the returned data, don't allow a corrupted filesystem
        // to affect things.
        if (!source.sha256.empty() && SHA256ForString(cachedData) != source.sha256)
        {
          throw IOException("SHA256 mismatch for URL: %s from cached data at %s, check for cache corruption", source.url.value(), paths.second.c_str());
        }
        return std::make_pair(200, cachedData);
      }
    }

    // I know this doesn't work for zero byte cached responses, its okay.

    // Data not in cache, fetch it
    auto get_result = GetRequest(curl, source.url.value(), source.sha256);

    if (get_result.first != 200)
    {
      return get_result;
    }

    // Save the fetched data to a temporary file
    auto tempFilename = generateTempFilename(*fs, paths.first);
    auto content = std::string_view(get_result.second.data(), get_result.second.size());
    writeToTempFile(*fs, tempFilename, content);

    // Rename the temporary file to the final filename
    fs->MoveFile(tempFilename, paths.second);
    return get_result;
  }

  struct SerializedFlightAppMetadata
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

  static std::unique_ptr<SerializedFlightAppMetadata> ParseFlightAppMetadata(const string &app_metadata)
  {
    SerializedFlightAppMetadata app_metadata_obj;
    try
    {
      msgpack::object_handle oh = msgpack::unpack((const char *)app_metadata.data(), app_metadata.size(), 0);
      msgpack::object obj = oh.get();
      obj.convert(app_metadata_obj);
    }
    catch (const std::exception &e)
    {
      throw InvalidInputException("File to parse MsgPack object describing in Arrow Flight app_metadata %s", e.what());
    }
    return std::make_unique<SerializedFlightAppMetadata>(app_metadata_obj);
  }

  void handle_flight_app_metadata(const string &app_metadata,
                                  const string &target_catalog,
                                  const string &target_schema,
                                  const string &location,
                                  const std::shared_ptr<arrow::flight::FlightInfo> &flight_info,
                                  unique_ptr<AirportSchemaContents> &contents)
  {
    auto parsed_app_metadata = ParseFlightAppMetadata(app_metadata);
    if (!(parsed_app_metadata->catalog == target_catalog && parsed_app_metadata->schema == target_schema))
    {
      throw IOException("Mismatch in metadata for catalog " + parsed_app_metadata->catalog + " schema " + parsed_app_metadata->schema + " expected " + target_catalog + " schema " + target_schema);
    }

    if (parsed_app_metadata->type == "table")
    {
      AirportAPITable table(
          location,
          std::move(flight_info),
          parsed_app_metadata->catalog,
          parsed_app_metadata->schema,
          parsed_app_metadata->name,
          parsed_app_metadata->comment);
      contents->tables.emplace_back(table);
    }
    else if (parsed_app_metadata->type == "table_function")
    {
      AirportAPITableFunction function;

      function.location = location;
      function.flight_info = std::move(flight_info);
      function.catalog_name = parsed_app_metadata->catalog;
      function.schema_name = parsed_app_metadata->schema;
      function.name = parsed_app_metadata->name;
      function.comment = parsed_app_metadata->comment;
      function.action_name = parsed_app_metadata->action_name.value_or("");
      function.description = parsed_app_metadata->description.value_or("");

      if (!parsed_app_metadata->input_schema.has_value())
      {
        throw IOException("Table Function metadata does not have an input_schema defined for function " + function.schema_name + "." + function.name);
      }

      auto serialized_schema = parsed_app_metadata->input_schema.value();

      arrow::io::BufferReader parameter_schema_reader(
          std::make_shared<arrow::Buffer>(serialized_schema));

      arrow::ipc::DictionaryMemo in_memo;
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
          auto parameter_schema,
          arrow::ipc::ReadSchema(&parameter_schema_reader, &in_memo),
          location,
          function.flight_info->descriptor(),
          "Read serialized input schema");

      function.input_schema = parameter_schema;

      contents->table_functions.emplace_back(function);
    }
    else if (parsed_app_metadata->type == "scalar_function")
    {
      AirportAPIScalarFunction function;
      function.location = location;
      function.flight_info = std::move(flight_info);

      function.catalog_name = parsed_app_metadata->catalog;
      function.schema_name = parsed_app_metadata->schema;
      function.name = parsed_app_metadata->name;
      function.comment = parsed_app_metadata->comment;
      function.description = parsed_app_metadata->description.value_or("");

      if (!parsed_app_metadata->input_schema.has_value())
      {
        throw IOException("Function metadata does not have an input_schema defined for function " + function.schema_name + "." + function.name);
      }

      auto serialized_schema = parsed_app_metadata->input_schema.value();

      arrow::io::BufferReader parameter_schema_reader(
          std::make_shared<arrow::Buffer>(serialized_schema));

      arrow::ipc::DictionaryMemo in_memo;
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(
          auto parameter_schema,
          arrow::ipc::ReadSchema(&parameter_schema_reader, &in_memo),
          location,
          function.flight_info->descriptor(),
          "Read serialized input schema");

      function.input_schema = parameter_schema;

      contents->scalar_functions.emplace_back(function);
    }
    else
    {
      throw IOException("Unknown object type in app_metadata: " + parsed_app_metadata->type);
    }
  }

  unique_ptr<AirportSchemaContents>
  AirportAPI::GetSchemaItems(CURL *curl,
                             const string &catalog,
                             const string &schema,
                             const AirportSerializedContentsWithSHA256Hash &source,
                             const string &cache_base_dir,
                             shared_ptr<AirportCredentials> credentials)
  {
    auto contents = make_uniq<AirportSchemaContents>();

    // So the value can be provided by an external URL or inline, but either way
    // we must have a SHA256 provided, because if the overall catalog populated the
    // cache at the top level it wrote the data for each schema to a sha256 named file
    // in the on disk cache.
    //
    // this is still a bit messy.
    if (
        source.url.has_value() ||
        source.serialized.has_value() ||
        !source.sha256.empty())
    {
      string url_contents;
      auto get_response = getCachedRequestData(curl, source, cache_base_dir);

      if (get_response.first != 200)
      {
        throw IOException("Failed to get Airport schema contents from URL: %s http response code %ld", source.url.value(), get_response.first);
      }
      url_contents = get_response.second;

      // So this data has this layout

      AirportSerializedCompressedContent compressed_content;
      try
      {
        msgpack::object_handle oh = msgpack::unpack(
            (const char *)url_contents.data(),
            url_contents.size(),
            0);
        msgpack::object obj = oh.get();
        obj.convert(compressed_content);
      }
      catch (const std::exception &e)
      {
        throw AirportFlightException(credentials->location,
                                     "File to parse msgpack encoded object describing Arrow Flight schema data: " + string(e.what()));
      }

      auto decompressed_url_contents = decompressZStandard(compressed_content.data, compressed_content.length, credentials->location);

      msgpack::object_handle oh = msgpack::unpack(
          (const char *)decompressed_url_contents.data(),
          compressed_content.length,
          0);
      std::vector<std::string> unpacked_data = oh.get().as<std::vector<std::string>>();

      for (auto item : unpacked_data)
      {
        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto flight_info, arrow::flight::FlightInfo::Deserialize(item), credentials->location, "");

        // Look in api_metadata for each flight and determine if it should be handled
        // if there is no metadata specified on a flight it is ignored.
        auto app_metadata = flight_info->app_metadata();
        if (!app_metadata.empty())
        {
          handle_flight_app_metadata(app_metadata, catalog, schema, credentials->location, std::move(flight_info), contents);
        }
      }

      return contents;
    }
    else
    {
      // We need to load the contents of the schemas by listing the flights.
      arrow::flight::FlightCallOptions call_options;
      airport_add_standard_headers(call_options, credentials->location);
      call_options.headers.emplace_back("airport-list-flights-filter-catalog", catalog);
      call_options.headers.emplace_back("airport-list-flights-filter-schema", schema);

      airport_add_authorization_header(call_options, credentials->auth_token);

      auto flight_client = FlightClientForLocation(credentials->location);

      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto listing, flight_client->ListFlights(call_options, {credentials->criteria}), credentials->location, "");

      std::shared_ptr<flight::FlightInfo> flight_info;
      AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(flight_info, listing->Next(), credentials->location, "");

      while (flight_info != nullptr)
      {
        // Look in api_metadata for each flight and determine if it should be a table.
        auto app_metadata = flight_info->app_metadata();
        if (!app_metadata.empty())
        {
          handle_flight_app_metadata(app_metadata, catalog, schema, credentials->location, flight_info, contents);
        }
        AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(flight_info, listing->Next(), credentials->location, "");
      }

      return contents;
    }
  }

  LogicalType
  AirportAPI::GetRowIdType(ClientContext &context,
                           std::shared_ptr<arrow::flight::FlightInfo> flight_info,
                           const string &location,
                           const arrow::flight::FlightDescriptor &descriptor)
  {
    std::shared_ptr<arrow::Schema> info_schema;
    arrow::ipc::DictionaryMemo dictionary_memo;
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION_DESCRIPTOR(info_schema,
                                                       flight_info->GetSchema(&dictionary_memo),
                                                       location,
                                                       descriptor,
                                                       "");

    ArrowSchemaWrapper schema_root;
    AIRPORT_ARROW_ASSERT_OK_LOCATION_DESCRIPTOR(ExportSchema(*info_schema,
                                                             &schema_root.arrow_schema),
                                                location,
                                                descriptor,
                                                "ExportSchema");

    for (idx_t col_idx = 0;
         col_idx < (idx_t)schema_root.arrow_schema.n_children; col_idx++)
    {
      auto &schema = *schema_root.arrow_schema.children[col_idx];
      if (!schema.release)
      {
        throw InvalidInputException("airport_take_flight: released schema passed");
      }

      if (schema.metadata != nullptr)
      {
        auto column_metadata = ArrowSchemaMetadata(schema.metadata);
        auto comment = column_metadata.GetOption("is_rowid");
        if (!comment.empty())
        {
          auto arrow_type = ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), schema);
          return arrow_type->GetDuckType();
        }
      }
    }
    return LogicalType::SQLNULL;
  }

  unique_ptr<AirportSchemaCollection>
  AirportAPI::GetSchemas(const string &catalog_name, shared_ptr<AirportCredentials> credentials)
  {
    auto result = make_uniq<AirportSchemaCollection>();
    arrow::flight::FlightCallOptions call_options;
    airport_add_standard_headers(call_options, credentials->location);
    airport_add_authorization_header(call_options, credentials->auth_token);

    call_options.headers.emplace_back("airport-action-name", "list_schemas");

    auto flight_client = FlightClientForLocation(credentials->location);

    AirportSerializedCatalogSchemaRequest catalog_request = {catalog_name};
    std::stringstream packed_buffer;
    msgpack::pack(packed_buffer, catalog_request);
    arrow::flight::Action action{"list_schemas", arrow::Buffer::FromString(packed_buffer.str())};

    std::unique_ptr<arrow::flight::ResultStream> action_results;
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(action_results,
                                            flight_client->DoAction(call_options, action),
                                            credentials->location,
                                            "");

    // There is a single item returned which is the compressed schema data.
    AIRPORT_FLIGHT_ASSIGN_OR_RAISE_LOCATION(auto msgpack_serialized_response, action_results->Next(), credentials->location, "");

    if (msgpack_serialized_response == nullptr)
    {
      throw AirportFlightException(credentials->location, "Failed to obtain schema data from Arrow Flight server via DoAction()");
    }

    AirportSerializedCompressedContent compressed_content;
    try
    {
      auto &body_buffer = msgpack_serialized_response.get()->body;

      msgpack::object_handle oh = msgpack::unpack(
          (const char *)body_buffer->data(),
          body_buffer->size(),
          0);
      msgpack::object obj = oh.get();
      obj.convert(compressed_content);
    }
    catch (const std::exception &e)
    {
      throw AirportFlightException(credentials->location, "File to parse msgpack encoded object describing Arrow Flight schema data: " + string(e.what()));
    }

    auto decompressed_schema_data = decompressZStandard(compressed_content.data, compressed_content.length, credentials->location);

    AirportSerializedCatalogRoot catalog_root;
    try
    {
      msgpack::object_handle oh = msgpack::unpack((const char *)decompressed_schema_data.data(), decompressed_schema_data.size(), 0);
      msgpack::object obj = oh.get();
      obj.convert(catalog_root);
    }
    catch (const std::exception &e)
    {
      throw InvalidInputException("Failed to parse MsgPack object describing catalog root %s", e.what());
    }

    result->source = catalog_root.contents;
    result->version_info = catalog_root.version_info;

    for (auto &schema : catalog_root.schemas)
    {
      AirportAPISchema schema_result;
      schema_result.schema_name = schema.schema;
      schema_result.catalog_name = catalog_name;
      schema_result.comment = schema.description;
      schema_result.tags = schema.tags;

      schema_result.source = schema.contents;
      result->schemas.emplace_back(schema_result);
    }

    AIRPORT_ARROW_ASSERT_OK_LOCATION(action_results->Drain(), credentials->location, "");

    return result;
  }

} // namespace duckdb
