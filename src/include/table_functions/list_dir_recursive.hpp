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
#include <utility>

namespace fs = ghc::filesystem;

namespace duckdb {


    struct ListDirRecursiveFunctionData final : FunctionData {

        string directory;
        int depth; // -1 for infinite depth, 0 for no recursion

        explicit ListDirRecursiveFunctionData(string directory, int depth) : directory(std::move(directory)), depth(depth) {}

        unique_ptr<FunctionData> Copy() const override {
            return make_uniq<ListDirRecursiveFunctionData>(directory, depth);
        }

        bool Equals(const FunctionData &other) const override {
            return directory == other.Cast<ListDirRecursiveFunctionData>().directory &&
                   depth == other.Cast<ListDirRecursiveFunctionData>().depth;

        }
    };

    struct ListDirRecursiveState final : GlobalTableFunctionState {
        ListDirRecursiveState() : gathered_paths(false), paths(), current_idx(0) {}

        std::atomic_bool gathered_paths;
        std::vector<std::string> paths;
        idx_t current_idx = 0;

        static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
            return make_uniq<ListDirRecursiveState>();
        }
    };

    static unique_ptr<FunctionData> ListDirRecursiveBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
        names.emplace_back("path");
        return_types.emplace_back(LogicalType::VARCHAR);

        // if no arguments are provided, use the current working directory
        string directory = ".";
        int depth = -1;
        if (input.inputs.size() == 1) {
            directory = input.inputs[0].GetValue<string>();
        } else if (input.inputs.size() == 2) {
            directory = input.inputs[0].GetValue<string>();
            depth = input.inputs[1].GetValue<int>();
        }

        auto data = make_uniq<ListDirRecursiveFunctionData>(directory, depth);
        return std::move(data);
    }

    static unique_ptr<FunctionData> ListDirBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
        names.emplace_back("path");
        return_types.emplace_back(LogicalType::VARCHAR);

        // if no arguments are provided, use the current working directory
        string directory = ".";
        int depth = 0;
        if (input.inputs.size() == 1) {
            directory = input.inputs[0].GetValue<string>();
        }

        auto data = make_uniq<ListDirRecursiveFunctionData>(directory, depth);
        return std::move(data);
    }

    void ListDirectoryRecursive(const std::string &directory, std::vector<std::string> &paths, int current_depth = 0, int max_depth = -1) {
        // Stop recursion if max_depth is reached and not -1
        if (max_depth != -1 && current_depth > max_depth) {
            return;
        }

        for (const auto &entry : fs::directory_iterator(directory)) {
            // Add the current path to the vector
            paths.push_back(entry.path().string());

            // Recursively go into directories if applicable
            if (entry.is_directory()) {
                ListDirectoryRecursive(entry.path().string(), paths, current_depth + 1, max_depth);
            }
        }
    }

    static void ListDirRecursiveFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {

        // get the args
        auto &function_data = data_p.bind_data->Cast<ListDirRecursiveFunctionData>();
        auto directory = function_data.directory;
        auto depth = function_data.depth;

        auto &state = data_p.global_state->Cast<ListDirRecursiveState>();

        // gather the paths if not already done
        if (!state.gathered_paths.exchange(true)) {
            // first call, gather the paths
            ListDirectoryRecursive(directory, state.paths, 0, depth);
        }

        // we can max return STANDARD_VECTOR_SIZE paths at a time
        idx_t total_count = state.paths.size();
        idx_t remaining_count = total_count - state.current_idx;
        idx_t count = std::min((idx_t)STANDARD_VECTOR_SIZE, remaining_count);

        // set up the chunk
        output.SetCardinality(count);

        // set the paths in the chunk
        for (idx_t index = 0; index < count; index++) {
            string path_str = state.paths[state.current_idx + index];
            output.data[0].SetValue(index, Value(path_str));
        }

        // update the current index
        state.current_idx += count;
    }

}