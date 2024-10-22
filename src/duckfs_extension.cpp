#define DUCKDB_EXTENSION_MAIN

#include "duckfs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <chrono>       // for std::chrono::duration_cast
#include <ctime>        // for std::time_t
#include <iomanip>      // for std::fixed and std::setprecision

#include "third_party/filesystem.hpp"

#include "table_functions/list_dir.hpp"
#include "table_functions/change_dir.hpp"

namespace fs = ghc::filesystem;

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

    std::string HumanReadableSize(uint64_t size) {
        constexpr uint64_t KB = 1024;
        constexpr uint64_t MB = KB * 1024;
        constexpr uint64_t GB = MB * 1024;

        std::ostringstream oss;
        if (size >= GB) {
            oss << std::fixed << std::setprecision(2) << (double) size / GB << " GB";
        } else if (size >= MB) {
            oss << std::fixed << std::setprecision(2) << (double) size / MB << " MB";
        } else if (size >= KB) {
            oss << std::fixed << std::setprecision(2) << (double) size / KB << " KB";
        } else {
            oss << size << " B";
        }
        return oss.str();
    }


    static void HumanReadableSizeScalarFun(DataChunk &input, ExpressionState &state, Vector &result) {

        auto &size_vector = input.data[0];
        UnaryExecutor::Execute<int64_t, string_t>(
                size_vector, result, input.size(),
                [&](int64_t size) {
                    return StringVector::AddString(result, HumanReadableSize(size));
                });
    }


    string PragmaChangeDir(ClientContext &context, const FunctionParameters &parameters) {
        return StringUtil::Format("SELECT * FROM cd(%s);",
                                  KeywordHelper::WriteQuoted(parameters.values[0].ToString(), '\''));
    }

    static void LoadInternal(DatabaseInstance &instance) {

        // Register scalar functions
        auto duckfs_scalar_function = ScalarFunction("duckfs", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                                     DuckfsScalarFun);
        ExtensionUtil::RegisterFunction(instance, duckfs_scalar_function);

        auto duckfs_pwd_function = ScalarFunction("pwd", {}, LogicalType::VARCHAR, PrintWorkingDirectoryFun);
        ExtensionUtil::RegisterFunction(instance, duckfs_pwd_function);

        auto duckfs_human_readable_size_function = ScalarFunction("hsize", {LogicalType::BIGINT},
                                                                  LogicalType::VARCHAR, HumanReadableSizeScalarFun);
        ExtensionUtil::RegisterFunction(instance, duckfs_human_readable_size_function);

        // Register table functions
        TableFunctionSet list_dir_set("ls");

        TableFunction list_dir_default({}, ListDirFun, ListDirBind, ListDirState::Init);
        list_dir_set.AddFunction(list_dir_default);

        TableFunction list_dir({LogicalType::VARCHAR}, ListDirFun, ListDirBind, ListDirState::Init);
        list_dir_set.AddFunction(list_dir);

        ExtensionUtil::RegisterFunction(instance, list_dir_set);

        TableFunction change_dir("cd", {LogicalType::VARCHAR}, ChangeDirFun, ChangeDirBind, ChangeDirState::Init);
        ExtensionUtil::RegisterFunction(instance, change_dir);

        // Pragma functions

        PragmaFunction cd = PragmaFunction::PragmaCall("cd", PragmaChangeDir, {LogicalType::VARCHAR});
        ExtensionUtil::RegisterFunction(instance, cd);
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
