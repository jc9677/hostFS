#pragma once


#include "duckfs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <chrono>      // for std::chrono::duration_cast
#include <ctime>  // for std::time_t

#include <iomanip>    // for std::fixed and std::setprecision

namespace fs = ghc::filesystem;


namespace duckdb {
    struct ListDirFunctionData final : FunctionData {
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
            // File path as string
            string path_str = entry.path().string();
            output.data[0].SetValue(count, Value(path_str));

            // File size in bytes (0 for directories or if unknown)
            auto file_size = entry.is_regular_file() ? fs::file_size(entry.path()) : 0;
            output.data[1].SetValue(count, Value::BIGINT(file_size));

            // File type: file, directory, or symlink
            string file_type;
            if (entry.is_regular_file()) {
                file_type = "file";
            } else if (entry.is_directory()) {
                file_type = "directory";
            } else if (entry.is_symlink()) {
                file_type = "symlink";
            } else {
                file_type = "other";
            }
            output.data[2].SetValue(count, Value(file_type));

            // Last modified time
            auto last_modified_time_point = fs::last_write_time(entry.path());
            auto last_modified_int = std::chrono::duration_cast<std::chrono::seconds>(last_modified_time_point.time_since_epoch()).count();
            timestamp_t timestamp = Timestamp::FromEpochSeconds(last_modified_int);
            output.data[3].SetValue(count, Value::TIMESTAMP(timestamp));

            count++;
        }

        output.SetCardinality(count);
    }

    static unique_ptr<FunctionData> ListDirBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
        names.emplace_back("path");
        return_types.emplace_back(LogicalType::VARCHAR);

        names.emplace_back("size");
        return_types.emplace_back(LogicalType::BIGINT);

        names.emplace_back("file_type");
        return_types.emplace_back(LogicalType::VARCHAR);

        names.emplace_back("last_modified");
        return_types.emplace_back(LogicalType::TIMESTAMP);

        // if no arguments are provided, use the current working directory
        string directory = ".";
        if (!input.inputs.empty()) {
            directory = input.inputs[0].GetValue<string>();
        }

        auto data = make_uniq<ListDirFunctionData>(directory);
        return std::move(data);
    }
}