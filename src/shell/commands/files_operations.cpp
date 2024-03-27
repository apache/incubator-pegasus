// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <fmt/core.h>
#include <fmt/format.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/iterator.h>
#include <rocksdb/metadata.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/table_properties.h>
#include <rocksdb/threadpool.h>
#include <stdio.h>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/meta_store.h"
#include "base/pegasus_key_schema.h"
#include "base/value_schema_manager.h"
#include "client/partition_resolver.h"
#include "client/replication_ddl_client.h"
#include "common/gpid.h"
#include "common/json_helper.h"
#include "common/replication_common.h"
#include "dsn.layer2_types.h"
#include "pegasus_value_schema.h"
#include "replica/replica_stub.h"
#include "replica/replication_app_base.h"
#include "shell/argh.h"
#include "shell/command_executor.h"
#include "shell/command_helper.h"
#include "shell/commands.h"
#include "utils/blob.h"
#include "utils/errors.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/load_dump_object.h"
#include "utils/output_utils.h"

const std::string update_info_file_help =
    fmt::format("Update the content of the info file (e.g., {}, {})",
                replica_init_info::kInitInfo,
                replica_app_info::kAppInfo);

enum class FieldType
{
    kBool,
    kInt32,
    kInt64,
    kString,
    kStringMap,
};

// TODO(yingchun): update
void interactive_update_field(
    const std::map<std::string, std::tuple<FieldType, void *>> &obj_fields)
{
    // Prepare the valid fields.
    // TODO(yingchun): avoid to contrcut the string every time.
    std::set<std::string> valid_fields;
    for (const auto & [ key, _ ] : obj_fields) {
        valid_fields.insert(key);
    }
    auto const fields_str = fmt::format("{}", fmt::join(valid_fields, ", "));

    // Get field.
    fmt::print(std::cout, "Which field do you want to update? [{}]: ", fields_str);
    std::string field;
    std::cin >> field;

    // Get value.
    fmt::print(std::cout, "What value do you want to update it to? ");
    std::string value;
    std::cin >> value;
    std::transform(field.begin(), field.end(), field.begin(), ::tolower);

    // Update value.
    if (valid_fields.count(field) == 0) {
        fmt::print(stderr, "invalid field '{}', should be one of '{}'\n", field, fields_str);
        return;
    }

    for (const auto & [ key, type_and_addr ] : obj_fields) {
        if (field != key) {
            continue;
        }

        // For each type.
        switch (std::get<0>(type_and_addr)) {
        case FieldType::kBool: {
            bool v;
            if (!dsn::buf2bool(value, v)) {
                fmt::print(stderr, "value '{}' should be a valid boolean\n", value);
                break;
            }
            *(static_cast<bool *>(std::get<1>(type_and_addr))) = v;
            break;
        }
        case FieldType::kInt32: {
            int32_t v;
            if (!dsn::buf2int32(value, v)) {
                fmt::print(stderr, "value '{}' should be a valid int32\n", value);
                break;
            }
            *(static_cast<int32_t *>(std::get<1>(type_and_addr))) = v;
            break;
        }
        case FieldType::kInt64: {
            int64_t v;
            if (!dsn::buf2int64(value, v)) {
                fmt::print(stderr, "value '{}' should be a valid int64\n", value);
                break;
            }
            *(static_cast<int64_t *>(std::get<1>(type_and_addr))) = v;
            break;
        }
        case FieldType::kString:
            *(static_cast<std::string *>(std::get<1>(type_and_addr))) = value;
            break;
        case FieldType::kStringMap: {
            // key1=value1,key2=value2...
            std::map<std::string, std::string> envs;
            if (!dsn::utils::parse_kv_map(value.c_str(), envs, ',', '=')) {
                fmt::print(stderr, "invalid envs: {}\n", value);
                break;
            }
            *(static_cast<std::map<std::string, std::string> *>(std::get<1>(type_and_addr))) = envs;
            break;
        }
        default:
            CHECK(false, "");
        }

        // There must match some field, always break the for-loop here.
        break;
    }
}

// Return true if continue to update the object, otherwise return false.
bool interactive_update_object(
    const std::map<std::string, std::tuple<FieldType, void *>> &obj_fields)
{
    std::cout << "Do you want to update the file? [y/n]: ";
    char c;
    std::cin >> c;
    switch (::tolower(c)) {
    case 'n':
        return false;
    case 'y':
        interactive_update_field(obj_fields);
        return true;
    default:
        fmt::print(stderr, "invalid input '{}', should be 'y' or 'n'\n", c);
        return true;
    }
}

template <class T>
void update_object(T &obj, const std::map<std::string, std::tuple<FieldType, void *>> &obj_fields)
{
    do {
        // Preview the object content.
        const auto bb = obj.serialize();
        fmt::print(stdout, "the file content is:\n{}\n", bb);
        if (!interactive_update_object(obj_fields)) {
            break;
        }
    } while (true);
}

bool update_info_file(command_executor *e, shell_context *sc, arguments args)
{
    // 1. Parse parameters.
    argh::parser cmd(args.argc, args.argv);
    RETURN_FALSE_IF_NOT(cmd.pos_args().size() != 1,
                        "invalid command, should be in the form of '<file_path>'");

    const auto file_path = cmd(1).str();
    if (!dsn::utils::filesystem::file_exists(file_path)) {
        fmt::print(stderr, "file '{}' is not exist\n", file_path);
        return false;
    }

    // TODO(yingchun): auto backup file
    const auto s1 = file_path.substr(file_path.length() - replica_init_info::kInitInfo.length());
    fmt::print(stdout, "s1: '{}'\n", s1);
    if (s1 == replica_init_info::kInitInfo) {
        replica_init_info rii;
        auto err = dsn::utils::load_rjobj_from_file(file_path, &rii);
        if (err != dsn::ERR_OK) {
            fmt::print(stderr, "load file '{}' failed, error={}\n", file_path, err);
            return false;
        }

        update_object(
            rii,
            {
                {"init_ballot", {FieldType::kInt64, &rii.init_ballot}},
                {"init_durable_decree", {FieldType::kInt64, &rii.init_durable_decree}},
                {"init_offset_in_shared_log", {FieldType::kInt64, &rii.init_offset_in_shared_log}},
                {"init_offset_in_private_log",
                 {FieldType::kInt64, &rii.init_offset_in_private_log}},
            });

        err = dsn::utils::dump_rjobj_to_file(rii, file_path);
        if (err != dsn::ERR_OK) {
            fmt::print(stderr, "write file '{}' failed, error={}\n", file_path, err);
            // TODO(yingchun): recover the file
            return false;
        }

        return true;
    }

    const auto s2 = file_path.substr(file_path.length() - replica_app_info::kAppInfo.length());
    fmt::print(stdout, "s2: '{}'\n", s2);
    if (s2 == replica_app_info::kAppInfo) {
        dsn::app_info ai;
        dsn::replication::replica_app_info rai(&ai);
        auto err = rai.load(file_path);
        if (err != dsn::ERR_OK) {
            fmt::print(stderr, "load file '{}' failed, error={}\n", file_path, err);
            return false;
        }

        // TODO(yingchun): different version has different fields.
        update_object(rai,
                      {
                          {"status", {FieldType::kInt32, &ai.status}},
                          {"app_type", {FieldType::kString, &ai.app_type}},
                          {"app_name", {FieldType::kString, &ai.app_name}},
                          {"app_id", {FieldType::kInt32, &ai.app_id}},
                          {"partition_count", {FieldType::kInt32, &ai.partition_count}},
                          {"envs", {FieldType::kStringMap, &ai.envs}},
                          {"is_stateful", {FieldType::kBool, &ai.is_stateful}},
                          {"max_replica_count", {FieldType::kInt32, &ai.max_replica_count}},
                          {"expire_second", {FieldType::kInt64, &ai.expire_second}},
                          {"create_second", {FieldType::kInt64, &ai.create_second}},
                          {"drop_second", {FieldType::kInt64, &ai.drop_second}},
                          {"duplicating", {FieldType::kBool, &ai.duplicating}},
                          {"init_partition_count", {FieldType::kInt32, &ai.init_partition_count}},
                          {"is_bulk_loading", {FieldType::kBool, &ai.is_bulk_loading}},
                      });

        replica_app_info new_rai(&ai);
        err = new_rai.store(file_path);
        if (dsn_unlikely(err != dsn::ERR_OK)) {
            fmt::print(stderr, "write file '{}' failed, error={}\n", file_path, err);
            // TODO(yingchun): recover the file
            return false;
        }

        return true;
    }

    return true;
}
