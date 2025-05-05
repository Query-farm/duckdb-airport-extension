#define DUCKDB_EXTENSION_MAIN

#include "airport_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"

#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "storage/airport_catalog.hpp"
#include "storage/airport_transaction_manager.hpp"
#include "airport_secrets.hpp"
#include "airport_optimizer.hpp"
#include "airport_scalar_function.hpp"
#include <curl/curl.h>

namespace duckdb
{

    static unique_ptr<BaseSecret> CreateAirportSecretFunction(ClientContext &, CreateSecretInput &input)
    {
        // apply any overridden settings
        vector<string> prefix_paths;

        auto scope = input.scope;
        if (scope.empty())
        {
            throw InternalException("No scope set Airport create secret (should start with grpc://): '%s'", input.type);
        }

        auto result = make_uniq<KeyValueSecret>(scope, "airport", "config", input.name);
        for (const auto &named_param : input.options)
        {
            auto lower_name = StringUtil::Lower(named_param.first);

            if (lower_name == "auth_token")
            {
                result->secret_map["auth_token"] = named_param.second.ToString();
            }
            else
            {
                throw InternalException("Unknown named parameter passed to CreateAirportSecretFunction: " + lower_name);
            }
        }

        //! Set redact keys
        result->redact_keys = {"auth_token"};

        return result;
    }

    static void SetAirportSecretParameters(CreateSecretFunction &function)
    {
        function.named_parameters["auth_token"] = LogicalType::VARCHAR;
    }

    static unique_ptr<Catalog> AirportCatalogAttach(StorageExtensionInfo *storage_info, ClientContext &context,
                                                    AttachedDatabase &db, const string &name, AttachInfo &info,
                                                    AccessMode access_mode)
    {
        string secret_name;
        string auth_token;
        string location;

        // check if we have a secret provided
        for (auto &entry : info.options)
        {
            auto lower_name = StringUtil::Lower(entry.first);
            if (lower_name == "type")
            {
                continue;
            }
            else if (lower_name == "secret")
            {
                secret_name = entry.second.ToString();
            }
            else if (lower_name == "auth_token")
            {
                auth_token = entry.second.ToString();
            }
            else if (lower_name == "location")
            {
                location = entry.second.ToString();
            }
            else
            {
                throw BinderException("Unrecognized option for Airport ATTACH: %s", entry.first);
            }
        }

        auth_token = AirportAuthTokenForLocation(context, location, secret_name, auth_token);

        if (location.empty())
        {
            throw BinderException("No location provided for Airport ATTACH.");
        }

        return make_uniq<AirportCatalog>(db, info.path, access_mode, AirportAttachParameters(location, auth_token, secret_name, ""));
    }

    static unique_ptr<TransactionManager> CreateTransactionManager(StorageExtensionInfo *storage_info, AttachedDatabase &db,
                                                                   Catalog &catalog)
    {
        auto &airport_catalog = catalog.Cast<AirportCatalog>();
        return make_uniq<AirportTransactionManager>(db, airport_catalog);
    }

    class AirportCatalogStorageExtension : public StorageExtension
    {
    public:
        AirportCatalogStorageExtension()
        {
            attach = AirportCatalogAttach;
            create_transaction_manager = CreateTransactionManager;
        }
    };

    inline void get_user_agent(DataChunk &args, ExpressionState &state, Vector &result)
    {
        D_ASSERT(args.ColumnCount() == 0);
        Value val(airport_user_agent());
        result.Reference(val);
    }

    void AirportAddUserAgentFunction(DatabaseInstance &instance)
    {
        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                "airport_user_agent",
                {},
                LogicalType::VARCHAR,
                get_user_agent));
    }

    static void RegisterTableMacro(DatabaseInstance &db, const string &name, const string &query,
                                   const vector<string> &params, const child_list_t<Value> &named_params)
    {
        Parser parser;
        parser.ParseQuery(query);
        const auto &stmt = parser.statements.back();
        auto &node = stmt->Cast<SelectStatement>().node;

        auto func = make_uniq<TableMacroFunction>(std::move(node));
        for (auto &param : params)
        {
            func->parameters.push_back(make_uniq<ColumnRefExpression>(param));
        }

        for (auto &param : named_params)
        {
            func->default_parameters[param.first] = make_uniq<ConstantExpression>(param.second);
        }

        CreateMacroInfo info(CatalogType::TABLE_MACRO_ENTRY);
        info.schema = DEFAULT_SCHEMA;
        info.name = name;
        info.temporary = true;
        info.internal = true;
        info.macros.push_back(std::move(func));

        ExtensionUtil::RegisterFunction(db, info);
    }

    static void AirportAddListDatabasesMacro(DatabaseInstance &instance)
    {
        child_list_t<Value> named_params = {
            //            {"auth_token", Value()},
            //            {"secret", Value()},
            // {"headers", Value()},
        };

        RegisterTableMacro(
            instance,
            "airport_databases",
            "select * from airport_take_flight(server_location, ['__databases'])",
            //            "select * from airport_take_flight(server_location, ['__databases'], auth_token=auth_token, secret=secret, headers=headers)",
            {"server_location"},
            named_params);
    }

    static void LoadInternal(DatabaseInstance &instance)
    {
        curl_global_init(CURL_GLOBAL_DEFAULT);

        AirportAddListFlightsFunction(instance);
        AirportAddTakeFlightFunction(instance);
        AirportAddUserAgentFunction(instance);
        AirportAddActionFlightFunction(instance);

        // So to create a new macro for airport_list_databases
        // that calls airport_take_flight with a fixed flight descriptor
        // of PATH /__databases

        AirportAddListDatabasesMacro(instance);

        SecretType secret_type;
        secret_type.name = "airport";
        secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
        secret_type.default_provider = "config";

        ExtensionUtil::RegisterSecretType(instance, secret_type);

        CreateSecretFunction airport_secret_function = {"airport", "config", CreateAirportSecretFunction};
        SetAirportSecretParameters(airport_secret_function);
        ExtensionUtil::RegisterFunction(instance, airport_secret_function);

        auto &config = DBConfig::GetConfig(instance);
        config.storage_extensions["airport"] = make_uniq<AirportCatalogStorageExtension>();

        OptimizerExtension airport_optimizer;
        airport_optimizer.optimize_function = AirportOptimizer::Optimize;
        config.optimizer_extensions.push_back(std::move(airport_optimizer));
    }

    void AirportExtension::Load(DuckDB &db)
    {
        LoadInternal(*db.instance);
    }
    std::string AirportExtension::Name()
    {
        return "airport";
    }

    std::string AirportExtension::Version() const
    {
#ifdef EXT_VERSION_airport
        return EXT_VERSION_airport;
#else
        return "";
#endif
    }

} // namespace duckdb

extern "C"
{
    DUCKDB_EXTENSION_API void airport_init(duckdb::DatabaseInstance &db)
    {
        duckdb::DuckDB db_wrapper(db);
        db_wrapper.LoadExtension<duckdb::AirportExtension>();
    }

    DUCKDB_EXTENSION_API const char *airport_version()
    {
        return duckdb::DuckDB::LibraryVersion();
    }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
