#define DUCKDB_EXTENSION_MAIN

#include "duckfs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <filesystem>

namespace fs = std::__fs::filesystem;

namespace duckdb {

    inline void DuckfsScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
        auto &name_vector = args.data[0];
        UnaryExecutor::Execute<string_t, string_t>(
                name_vector, result, args.size(),
                [&](string_t name) {
                    return StringVector::AddString(result, "Duckfs " + name.GetString() + " 🐥");;
                });
    }


    static void PrintWorkingDirectoryFun(DataChunk &input, ExpressionState &state, Vector &result) {
        // get the current working directory
        auto cwd = fs::current_path();
        auto val = Value(cwd.string());
        result.Reference(val);
    }

    struct ListDirFunctionData final: FunctionData {
        string path;

        explicit ListDirFunctionData(string path) : path(path) {}

        unique_ptr<FunctionData> Copy() const override {
            return make_uniq<ListDirFunctionData>(path);
        }

        bool Equals(const FunctionData &other) const override {
            return path == other.Cast<ListDirFunctionData>().path;
        }
    };

    struct ListDirState final : GlobalTableFunctionState {
        ListDirState() : run(false) {};
        std::atomic_bool run;

        static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
            return make_uniq<ListDirState>();
        }
    };

    static void ListDirFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {

        // get the args
        auto &function_data = data_p.bind_data->Cast<ListDirFunctionData>();
        auto path = function_data.path;

        auto &state = data_p.global_state->Cast<ListDirState>();
        if (state.run.exchange(true)) {
            return;
        }

        // list the files in the directory
        idx_t count = 0;
        for (const auto &entry: fs::directory_iterator(path)) {
            output.data[0].SetValue(count, Value(entry.path().string()));
            count++;
        }

        output.SetCardinality(count);
    }

    static unique_ptr<FunctionData> ListDirBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
        names.emplace_back("path");
        return_types.emplace_back(LogicalType::VARCHAR);

        // if no arguments are provided, use the current working directory
        string directory = ".";
        if (!input.inputs.empty()) {
            directory = input.inputs[0].GetValue<string>();
        }

        auto data = make_uniq<ListDirFunctionData>(directory);
        return std::move(data);
    }

    static void LoadInternal(DatabaseInstance &instance) {

        // Register scalar functions
        auto duckfs_scalar_function = ScalarFunction("duckfs", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                                     DuckfsScalarFun);
        ExtensionUtil::RegisterFunction(instance, duckfs_scalar_function);

        auto duckfs_pwd_function = ScalarFunction("pwd", {}, LogicalType::VARCHAR, PrintWorkingDirectoryFun);
        ExtensionUtil::RegisterFunction(instance, duckfs_pwd_function);

        // Register table functions
        TableFunctionSet list_dir_set("ls");

        TableFunction list_dir_default({}, ListDirFun, ListDirBind, ListDirState::Init);
        list_dir_set.AddFunction(list_dir_default);

        TableFunction list_dir({LogicalType::VARCHAR}, ListDirFun, ListDirBind, ListDirState::Init);
        list_dir_set.AddFunction(list_dir);


        ExtensionUtil::RegisterFunction(instance, list_dir_set);


    }

    void DuckfsExtension::Load(DuckDB &db) {
        LoadInternal(*db.instance);
    }

    std::string DuckfsExtension::Name() {
        return "duckfs";
    }

    std::string DuckfsExtension::Version() const {
#ifdef EXT_VERSION_DUCKFS
        return EXT_VERSION_DUCKFS;
#else
        return "";
#endif
    }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void duckfs_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::DuckfsExtension>();
}

DUCKDB_EXTENSION_API const char *duckfs_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
