// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "block_service_manager.h"
#include "fds/fds_service.h"
#include "local/local_service.h"

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

// ThreadPool: THREAD_POOL_REPLICATION, THREAD_POOL_REPLICATION_LONG
error_code block_service_manager::download_file(const std::string &remote_dir,
                                                const std::string &local_dir,
                                                const std::string &file_name,
                                                block_filesystem *fs,
                                                /*out*/ uint64_t &download_file_size)
{
    error_code download_err = ERR_OK;
    task_tracker tracker;

    auto download_file_callback_func = [this, &download_err, &download_file_size](
        const download_response &resp, block_file_ptr bf, const std::string &local_file_name) {
        if (resp.err != ERR_OK) {
            // during bulk load process, ERR_OBJECT_NOT_FOUND will be considered as a recoverable
            // error, however, if file damaged on remote file provider, bulk load should stop,
            // return ERR_CORRUPTION instead
            if (resp.err == ERR_OBJECT_NOT_FOUND) {
                derror_f("download file({}) failed, file on remote file provider is damaged",
                         local_file_name);
                download_err = ERR_CORRUPTION;
            } else {
                download_err = resp.err;
            }
            return;
        }

        if (resp.downloaded_size != bf->get_size()) {
            derror_f(
                "size not match while downloading file({}), file_size({}) vs downloaded_size({})",
                bf->file_name(),
                bf->get_size(),
                resp.downloaded_size);
            download_err = ERR_CORRUPTION;
            return;
        }

        std::string current_md5;
        error_code e = utils::filesystem::md5sum(local_file_name, current_md5);
        if (e != ERR_OK) {
            derror_f("calculate file({}) md5 failed", local_file_name);
            download_err = e;
            return;
        }
        if (current_md5 != bf->get_md5sum()) {
            derror_f("local file({}) is different from remote file({}), download failed, md5: "
                     "local({}) VS remote({})",
                     local_file_name,
                     bf->file_name(),
                     current_md5,
                     bf->get_md5sum());
            download_err = ERR_CORRUPTION;
            return;
        }
        ddebug_f("download file({}) succeed, file_size = {}",
                 local_file_name.c_str(),
                 resp.downloaded_size);
        download_err = ERR_OK;
        download_file_size = resp.downloaded_size;
    };

    auto create_file_cb = [this,
                           &local_dir,
                           &download_err,
                           &download_file_size,
                           &download_file_callback_func,
                           &tracker](const create_file_response &resp, const std::string &fname) {
        if (resp.err != ERR_OK) {
            derror_f("create file({}) failed with error({})", fname, resp.err.to_string());
            download_err = resp.err;
            return;
        }

        block_file *bf = resp.file_handle.get();
        if (bf->get_md5sum().empty()) {
            derror_f("file({}) doesn't exist on remote file provider", bf->file_name());
            download_err = ERR_CORRUPTION;
            return;
        }

        const std::string &local_file_name = utils::filesystem::path_combine(local_dir, fname);
        // local file exists
        if (utils::filesystem::file_exists(local_file_name)) {
            std::string current_md5;
            error_code e = utils::filesystem::md5sum(local_file_name, current_md5);
            if (e != ERR_OK || current_md5 != bf->get_md5sum()) {
                if (e != ERR_OK) {
                    dwarn_f("calculate file({}) md5 failed, should remove and redownload it",
                            local_file_name);
                } else {
                    dwarn_f("local file({}) is different from remote file({}), md5: local({}) VS "
                            "remote({}), should remove and redownload it",
                            local_file_name,
                            bf->file_name(),
                            current_md5,
                            bf->get_md5sum());
                }
                if (!utils::filesystem::remove_path(local_file_name)) {
                    derror_f("failed to remove file({})", local_file_name);
                    download_err = e;
                    return;
                }
            } else {
                download_err = ERR_OK;
                download_file_size = bf->get_size();
                ddebug_f("local file({}) has been downloaded, file size = {}",
                         local_file_name,
                         download_file_size);
                return;
            }
        }

        // download or redownload file
        bf->download(download_request{local_file_name, 0, -1},
                     TASK_CODE_EXEC_INLINED,
                     std::bind(download_file_callback_func,
                               std::placeholders::_1,
                               resp.file_handle,
                               local_file_name),
                     &tracker);
    };

    const std::string remote_file_name = utils::filesystem::path_combine(remote_dir, file_name);
    fs->create_file(create_file_request{remote_file_name, false},
                    TASK_CODE_EXEC_INLINED,
                    std::bind(create_file_cb, std::placeholders::_1, file_name),
                    &tracker);
    tracker.wait_outstanding_tasks();
    return download_err;
}

} // namespace block_service
} // namespace dist
} // namespace dsn
