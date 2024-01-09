/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <fmt/core.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <rocksdb/env.h>
#include <rocksdb/slice.h>
#include <initializer_list>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "common/json_helper.h"
#include "utils/blob.h"
#include "utils/defer.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace utils {

// Write 'data' to path 'file_path', while 'type' decide whether to encrypt the data.
template <class T>
error_code write_data_to_file(const std::string &file_path, const T &data, const FileDataType &type)
{
    const std::string tmp_file_path = fmt::format("{}.tmp", file_path);
    auto cleanup = defer([tmp_file_path]() { filesystem::remove_path(tmp_file_path); });
    LOG_AND_RETURN_CODE_NOT_RDB_OK(
        ERROR,
        rocksdb::WriteStringToFile(PegasusEnv(type),
                                   rocksdb::Slice(data.data(), data.length()),
                                   tmp_file_path,
                                   /* should_sync */ true),
        ERR_FILE_OPERATION_FAILED,
        "write file '{}' failed",
        tmp_file_path);
    LOG_AND_RETURN_NOT_TRUE(ERROR,
                            filesystem::rename_path(tmp_file_path, file_path),
                            ERR_FILE_OPERATION_FAILED,
                            "move file from '{}' to '{}' failed",
                            tmp_file_path,
                            file_path);
    return ERR_OK;
}

// Load a object from the file in 'file_path' to 'obj', while 'type' decide whether the data is
// encrypted, the file content must be in JSON format.
// The object T must use the marco DEFINE_JSON_SERIALIZATION(...) when defining, which implement
// the RapidJson APIs.
template <class T>
error_code load_rjobj_from_file(const std::string &file_path, T *obj)
{
    return load_rjobj_from_file(file_path, FileDataType::kSensitive, obj);
}

template <class T>
error_code load_rjobj_from_file(const std::string &file_path, FileDataType type, T *obj)
{
    LOG_AND_RETURN_NOT_TRUE(ERROR,
                            filesystem::path_exists(file_path),
                            ERR_PATH_NOT_FOUND,
                            "file '{}' not exist",
                            file_path);
    std::string data;
    LOG_AND_RETURN_CODE_NOT_RDB_OK(ERROR,
                                   rocksdb::ReadFileToString(PegasusEnv(type), file_path, &data),
                                   ERR_FILE_OPERATION_FAILED,
                                   "read file '{}' failed",
                                   file_path);
    LOG_AND_RETURN_NOT_TRUE(
        ERROR,
        json::json_forwarder<T>::decode(blob::create_from_bytes(std::move(data)), *obj),
        ERR_CORRUPTION,
        "decode JSON from file '{}' failed",
        file_path);
    return ERR_OK;
}

// Dump the object to the file in 'file_path' according to 'obj', while 'type' decide whether the
// data is encrypted, the file content will be in JSON
// format.
// The object T must use the marco DEFINE_JSON_SERIALIZATION(...) when defining, which implement
// the RapidJson APIs.
template <class T>
error_code dump_rjobj_to_file(const T &obj, const std::string &file_path)
{
    return dump_rjobj_to_file(obj, FileDataType::kSensitive, file_path);
}

template <class T>
error_code dump_rjobj_to_file(const T &obj, FileDataType type, const std::string &file_path)
{
    const auto data = json::json_forwarder<T>::encode(obj);
    LOG_AND_RETURN_NOT_OK(
        ERROR, write_data_to_file(file_path, data, type), "dump content to '{}' failed", file_path);
    return ERR_OK;
}

// Similar to load_rjobj_from_file, but the object T must use the
// marco NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(...) when defining,
// which implement the NlohmannJson APIs.
template <class T>
error_code load_njobj_from_file(const std::string &file_path, T *obj)
{
    return load_njobj_from_file(file_path, FileDataType::kSensitive, obj);
}

template <class T>
error_code load_njobj_from_file(const std::string &file_path, FileDataType type, T *obj)
{
    LOG_AND_RETURN_NOT_TRUE(ERROR,
                            filesystem::path_exists(file_path),
                            ERR_PATH_NOT_FOUND,
                            "file '{}' not exist",
                            file_path);
    std::string data;
    LOG_AND_RETURN_CODE_NOT_RDB_OK(ERROR,
                                   rocksdb::ReadFileToString(PegasusEnv(type), file_path, &data),
                                   ERR_FILE_OPERATION_FAILED,
                                   "read file '{}' failed",
                                   file_path);
    try {
        nlohmann::json::parse(data).get_to(*obj);
    } catch (nlohmann::json::exception &exp) {
        LOG_WARNING("decode JSON from file '{}' failed, exception = {}, data = [{}]",
                    file_path,
                    exp.what(),
                    data);
        return ERR_CORRUPTION;
    }
    return ERR_OK;
}

// Similar to dump_rjobj_to_file, but the object T must use the
// marco NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(...) when defining,
// which implement the NlohmannJson APIs.
template <class T>
error_code dump_njobj_to_file(const T &obj, const std::string &file_path)
{
    return dump_njobj_to_file(obj, FileDataType::kSensitive, file_path);
}

template <class T>
error_code dump_njobj_to_file(const T &obj, FileDataType type, const std::string &file_path)
{
    const auto data = nlohmann::json(obj).dump();
    LOG_AND_RETURN_NOT_OK(
        ERROR, write_data_to_file(file_path, data, type), "dump content to '{}' failed", file_path);
    return ERR_OK;
}

} // namespace utils
} // namespace dsn
