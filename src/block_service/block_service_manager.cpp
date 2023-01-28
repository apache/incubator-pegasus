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

#include "block_service_manager.h"
#include "block_service/fds/fds_service.h"
#include "block_service/hdfs/hdfs_service.h"
#include "block_service/local/local_service.h"

#include "utils/fmt_logging.h"
#include "utils/factory_store.h"
#include "utils/filesystem.h"

namespace dsn {
namespace dist {
namespace block_service {

block_service_registry::block_service_registry()
{
    CHECK(utils::factory_store<block_filesystem>::register_factory(
              "fds_service", block_filesystem::create<fds_service>, PROVIDER_TYPE_MAIN),
          "register fds_service failed");

    CHECK(utils::factory_store<block_filesystem>::register_factory(
              "hdfs_service", block_filesystem::create<hdfs_service>, PROVIDER_TYPE_MAIN),
          "register hdfs_service failed");

    CHECK(utils::factory_store<block_filesystem>::register_factory(
              "local_service", block_filesystem::create<local_service>, PROVIDER_TYPE_MAIN),
          "register local_service failed");
}

block_service_manager::block_service_manager()
    : // we got a instance of block_service_registry each time we create a block_service_manger
      // to make sure that the filesystem providers are registered
      _registry_holder(block_service_registry::instance())
{
}

block_service_manager::~block_service_manager()
{
    LOG_INFO("close block service manager.");
    zauto_write_lock l(_fs_lock);
    _fs_map.clear();
}

block_filesystem *block_service_manager::get_or_create_block_filesystem(const std::string &provider)
{
    zauto_write_lock l(_fs_lock);
    auto iter = _fs_map.find(provider);
    if (iter != _fs_map.end()) {
        return iter->second.get();
    }

    const char *provider_type = dsn_config_get_value_string(
        (std::string("block_service.") + provider).c_str(), "type", "", "block service type");

    block_filesystem *fs =
        utils::factory_store<block_filesystem>::create(provider_type, PROVIDER_TYPE_MAIN);
    if (fs == nullptr) {
        LOG_ERROR("acquire block filesystem failed, provider = {}, provider_type = {}",
                  provider,
                  std::string(provider_type));
        return nullptr;
    }

    const char *arguments = dsn_config_get_value_string(
        (std::string("block_service.") + provider).c_str(), "args", "", "args for block_service");

    std::vector<std::string> args;
    utils::split_args(arguments, args);
    dsn::error_code err = fs->initialize(args);

    if (dsn::ERR_OK == err) {
        LOG_INFO("create block filesystem ok for provider {}", provider);
        _fs_map.emplace(provider, std::unique_ptr<block_filesystem>(fs));
    } else {
        LOG_ERROR("create block file system err {} for provider {}",
                  std::string(err.to_string()),
                  provider);
        delete fs;
        fs = nullptr;
    }
    return fs;
}

static create_file_response create_block_file_sync(const std::string &remote_file_path,
                                                   bool ignore_meta,
                                                   block_filesystem *fs,
                                                   task_tracker *tracker)
{
    create_file_response ret;
    fs->create_file(create_file_request{remote_file_path, ignore_meta},
                    TASK_CODE_EXEC_INLINED,
                    [&ret](const create_file_response &resp) { ret = resp; },
                    tracker);
    tracker->wait_outstanding_tasks();
    return ret;
}

static download_response
download_block_file_sync(const std::string &local_file_path, block_file *bf, task_tracker *tracker)
{
    download_response ret;
    bf->download(download_request{local_file_path, 0, -1},
                 TASK_CODE_EXEC_INLINED,
                 [&ret](const download_response &resp) { ret = resp; },
                 tracker);
    tracker->wait_outstanding_tasks();
    return ret;
}

error_code block_service_manager::download_file(const std::string &remote_dir,
                                                const std::string &local_dir,
                                                const std::string &file_name,
                                                block_filesystem *fs,
                                                /*out*/ uint64_t &download_file_size)
{
    std::string md5;
    return download_file(remote_dir, local_dir, file_name, fs, download_file_size, md5);
}

// ThreadPool: THREAD_POOL_REPLICATION, THREAD_POOL_DEFAULT
error_code block_service_manager::download_file(const std::string &remote_dir,
                                                const std::string &local_dir,
                                                const std::string &file_name,
                                                block_filesystem *fs,
                                                /*out*/ uint64_t &download_file_size,
                                                /*out*/ std::string &download_file_md5)
{
    // local file exists
    const std::string local_file_name = utils::filesystem::path_combine(local_dir, file_name);
    if (utils::filesystem::file_exists(local_file_name)) {
        LOG_INFO("local file({}) exists", local_file_name);
        return ERR_PATH_ALREADY_EXIST;
    }

    task_tracker tracker;

    // Create a block_file object.
    const std::string remote_file_name = utils::filesystem::path_combine(remote_dir, file_name);
    auto create_resp =
        create_block_file_sync(remote_file_name, false /*ignore file meta*/, fs, &tracker);
    error_code err = create_resp.err;
    if (err != ERR_OK) {
        LOG_ERROR("create file({}) failed with error({})", remote_file_name, err.to_string());
        return err;
    }
    block_file_ptr bf = create_resp.file_handle;

    download_response resp = download_block_file_sync(local_file_name, bf.get(), &tracker);
    if (resp.err != ERR_OK) {
        // during bulk load process, ERR_OBJECT_NOT_FOUND will be considered as a recoverable
        // error, however, if file damaged on remote file provider, bulk load should stop,
        // return ERR_CORRUPTION instead
        if (resp.err == ERR_OBJECT_NOT_FOUND) {
            LOG_ERROR("download file({}) failed, file on remote file provider is damaged",
                      local_file_name);
            return ERR_CORRUPTION;
        }
        return resp.err;
    }

    LOG_INFO("download file({}) succeed, file_size = {}, md5 = {}",
             local_file_name.c_str(),
             resp.downloaded_size,
             resp.file_md5);
    download_file_size = resp.downloaded_size;
    download_file_md5 = resp.file_md5;
    return ERR_OK;
}

} // namespace block_service
} // namespace dist
} // namespace dsn
