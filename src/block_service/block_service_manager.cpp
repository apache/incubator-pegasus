// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "block_service_manager.h"
#include "block_service/fds/fds_service.h"
#include "block_service/local/local_service.h"

#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/factory_store.h>
#include <dsn/utility/filesystem.h>

namespace dsn {
namespace dist {
namespace block_service {

block_service_registry::block_service_registry()
{
    bool ans;
    ans = utils::factory_store<block_filesystem>::register_factory(
        "fds_service", block_filesystem::create<fds_service>, PROVIDER_TYPE_MAIN);
    dassert(ans, "register fds_service failed");

    ans = utils::factory_store<block_filesystem>::register_factory(
        "local_service", block_filesystem::create<local_service>, PROVIDER_TYPE_MAIN);
    dassert(ans, "register local_service failed");
}

block_service_manager::block_service_manager()
    : // we got a instance of block_service_registry each time we create a block_service_manger
      // to make sure that the filesystem providers are registered
      _registry_holder(block_service_registry::instance())
{
}

block_filesystem *block_service_manager::get_block_filesystem(const std::string &provider)
{
    {
        zauto_read_lock l(_fs_lock);
        auto iter = _fs_map.find(provider);
        if (iter != _fs_map.end())
            return iter->second.get();
    }

    {
        zauto_write_lock l(_fs_lock);
        auto iter = _fs_map.find(provider);
        if (iter != _fs_map.end())
            return iter->second.get();

        const char *provider_type = dsn_config_get_value_string(
            (std::string("block_service.") + provider).c_str(), "type", "", "block service type");

        block_filesystem *fs =
            utils::factory_store<block_filesystem>::create(provider_type, PROVIDER_TYPE_MAIN);
        if (fs == nullptr) {
            derror("acquire block filesystem failed, provider = %s, provider_type = %s",
                   provider.c_str(),
                   provider_type);
            return nullptr;
        }

        const char *arguments =
            dsn_config_get_value_string((std::string("block_service.") + provider).c_str(),
                                        "args",
                                        "",
                                        "args for block_service");

        std::vector<std::string> args;
        utils::split_args(arguments, args);
        dsn::error_code err = fs->initialize(args);

        if (dsn::ERR_OK == err) {
            ddebug("create block filesystem ok for provider(%s)", provider.c_str());
            _fs_map.emplace(provider, std::unique_ptr<block_filesystem>(fs));
            return fs;
        } else {
            derror("create block file system err(%s) for provider(%s)",
                   err.to_string(),
                   provider.c_str());
            delete fs;
            return nullptr;
        }
    }
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

// ThreadPool: THREAD_POOL_REPLICATION, THREAD_POOL_REPLICATION_LONG
error_code block_service_manager::download_file(const std::string &remote_dir,
                                                const std::string &local_dir,
                                                const std::string &file_name,
                                                block_filesystem *fs,
                                                /*out*/ uint64_t &download_file_size)
{
    // local file exists
    const std::string local_file_name = utils::filesystem::path_combine(local_dir, file_name);
    if (utils::filesystem::file_exists(local_file_name)) {
        ddebug_f("local file({}) has been downloaded", local_file_name);
        return ERR_OK;
    }

    task_tracker tracker;

    // Create a block_file object.
    const std::string remote_file_name = utils::filesystem::path_combine(remote_dir, file_name);
    auto create_resp =
        create_block_file_sync(remote_file_name, false /*ignore file meta*/, fs, &tracker);
    error_code err = create_resp.err;
    if (err != ERR_OK) {
        derror_f("create file({}) failed with error({})", remote_file_name, err.to_string());
        return err;
    }
    block_file_ptr bf = create_resp.file_handle;

    download_response resp = download_block_file_sync(local_file_name, bf.get(), &tracker);
    if (resp.err != ERR_OK) {
        // during bulk load process, ERR_OBJECT_NOT_FOUND will be considered as a recoverable
        // error, however, if file damaged on remote file provider, bulk load should stop,
        // return ERR_CORRUPTION instead
        if (resp.err == ERR_OBJECT_NOT_FOUND) {
            derror_f("download file({}) failed, file on remote file provider is damaged",
                     local_file_name);
            return ERR_CORRUPTION;
        }
        return resp.err;
    }

    ddebug_f(
        "download file({}) succeed, file_size = {}", local_file_name.c_str(), resp.downloaded_size);
    download_file_size = resp.downloaded_size;
    return ERR_OK;
}

} // namespace block_service
} // namespace dist
} // namespace dsn
