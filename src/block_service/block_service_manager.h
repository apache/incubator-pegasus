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

#pragma once

#include <stdint.h>
#include <map>
#include <memory>
#include <string>

#include "utils/error_code.h"
#include "utils/singleton.h"
#include "utils/zlocks.h"

namespace dsn {
namespace dist {
namespace block_service {
class block_filesystem;

// a singleton for rDSN service_engine to register all blocks, this should be called only once
class block_service_registry : public utils::singleton<block_service_registry>
{
private:
    block_service_registry();
    ~block_service_registry() = default;

    friend class utils::singleton<block_service_registry>;
};

// this should be shared within a service node
// we can't make the block_service_manager shared among service nodes because of rDSN's
// share-nothing archiecture among different apps
class block_service_manager
{
public:
    block_service_manager();
    ~block_service_manager();
    block_filesystem *get_or_create_block_filesystem(const std::string &provider);

    // download files from remote file system
    // \return  ERR_FILE_OPERATION_FAILED: local file system error
    // \return  ERR_FS_INTERNAL: remote file system error
    // \return  ERR_CORRUPTION: file not exist or damaged
    // \return  ERR_PATH_ALREADY_EXIST: local file exist
    // if download file succeed, download_err = ERR_OK and set download_file_size
    //
    // TODO(wutao1): create block_filesystem_wrapper instead.
    // NOTE: This function is not responsible for the correctness of the downloaded file.
    // The file may be half-downloaded or corrupted due to disk failure.
    // The users can compare checksums, and retry download if validation failed.
    error_code download_file(const std::string &remote_dir,
                             const std::string &local_dir,
                             const std::string &file_name,
                             block_filesystem *fs,
                             /*out*/ uint64_t &download_file_size,
                             /*out*/ std::string &download_file_md5);

    error_code download_file(const std::string &remote_dir,
                             const std::string &local_dir,
                             const std::string &file_name,
                             block_filesystem *fs,
                             /*out*/ uint64_t &download_file_size);

private:
    block_service_registry &_registry_holder;

    mutable zrwlock_nr _fs_lock;
    std::map<std::string, std::unique_ptr<block_filesystem>> _fs_map;

    friend class block_service_manager_mock;
};

} // namespace block_service
} // namespace dist
} // namespace dsn
