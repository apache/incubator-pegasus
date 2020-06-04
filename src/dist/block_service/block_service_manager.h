// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/dist/block_service.h>
#include <dsn/utility/singleton_store.h>
#include <dsn/tool-api/zlocks.h>

namespace dsn {
namespace dist {
namespace block_service {

// a singleton for rDSN service_engine to register all blocks, this should be called only once
class block_service_registry : public utils::singleton<block_service_registry>
{
public:
    block_service_registry();
};

// this should be shared within a service node
// we can't make the block_service_manager shared among service nodes because of rDSN's
// share-nothing archiecture among different apps
class block_service_manager
{
public:
    block_service_manager();
    block_filesystem *get_block_filesystem(const std::string &provider);

    // download files from remote file system
    // \return  ERR_FILE_OPERATION_FAILED: local file system error
    // \return  ERR_FS_INTERNAL: remote file system error
    // \return  ERR_CORRUPTION: file not exist or damaged
    // if download file succeed, download_err = ERR_OK and set download_file_size
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
