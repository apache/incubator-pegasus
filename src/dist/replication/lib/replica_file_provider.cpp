// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "replica.h"

#include <fstream>

#include <dsn/dist/block_service.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/filesystem.h>

namespace dsn {
namespace replication {

// ThreadPool: THREAD_POOL_REPLICATION, THREAD_POOL_REPLICATION_LONG
error_code replica::do_download(const std::string &remote_dir,
                                const std::string &local_dir,
                                const std::string &file_name,
                                dist::block_service::block_filesystem *fs,
                                /*out*/ uint64_t &download_file_size)
{
    error_code download_err = ERR_OK;
    task_tracker tracker;

    auto download_file_callback_func = [this, &download_err, &download_file_size](
        const dist::block_service::download_response &resp,
        dist::block_service::block_file_ptr bf,
        const std::string &local_file_name) {
        if (resp.err != ERR_OK) {
            // during bulk load process, ERR_OBJECT_NOT_FOUND will be considered as a recoverable
            // error, however, if file damaged on remote file provider, bulk load should stop,
            // return ERR_CORRUPTION instead
            if (resp.err == ERR_OBJECT_NOT_FOUND) {
                derror_replica("download file({}) failed, file on remote file provider is damaged",
                               local_file_name);
                download_err = ERR_CORRUPTION;
            } else {
                download_err = resp.err;
            }
            return;
        }

        if (resp.downloaded_size != bf->get_size()) {
            derror_replica(
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
            derror_replica("calculate file({}) md5 failed", local_file_name);
            download_err = e;
            return;
        }
        if (current_md5 != bf->get_md5sum()) {
            derror_replica(
                "local file({}) is different from remote file({}), download failed, md5: "
                "local({}) VS remote({})",
                local_file_name,
                bf->file_name(),
                current_md5,
                bf->get_md5sum());
            download_err = ERR_CORRUPTION;
            return;
        }
        ddebug_replica("download file({}) succeed, file_size = {}",
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
                           &tracker](const dist::block_service::create_file_response &resp,
                                     const std::string &fname) {
        if (resp.err != ERR_OK) {
            derror_replica("create file({}) failed with error({})", fname, resp.err.to_string());
            download_err = resp.err;
            return;
        }

        dist::block_service::block_file *bf = resp.file_handle.get();
        if (bf->get_md5sum().empty()) {
            derror_replica("file({}) doesn't exist on remote file provider", bf->file_name());
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
                    dwarn_replica("calculate file({}) md5 failed, should remove and redownload it",
                                  local_file_name);
                } else {
                    dwarn_replica(
                        "local file({}) is different from remote file({}), md5: local({}) VS "
                        "remote({}), should remove and redownload it",
                        local_file_name,
                        bf->file_name(),
                        current_md5,
                        bf->get_md5sum());
                }
                if (!utils::filesystem::remove_path(local_file_name)) {
                    derror_replica("failed to remove file({})", local_file_name);
                    download_err = e;
                    return;
                }
            } else {
                download_err = ERR_OK;
                download_file_size = bf->get_size();
                ddebug_replica("local file({}) has been downloaded, file size = {}",
                               local_file_name,
                               download_file_size);
                return;
            }
        }

        // download or redownload file
        bf->download(dist::block_service::download_request{local_file_name, 0, -1},
                     TASK_CODE_EXEC_INLINED,
                     std::bind(download_file_callback_func,
                               std::placeholders::_1,
                               resp.file_handle,
                               local_file_name),
                     &tracker);
    };

    const std::string remote_file_name = utils::filesystem::path_combine(remote_dir, file_name);
    fs->create_file(dist::block_service::create_file_request{remote_file_name, false},
                    TASK_CODE_EXEC_INLINED,
                    std::bind(create_file_cb, std::placeholders::_1, file_name),
                    &tracker);
    tracker.wait_outstanding_tasks();
    return download_err;
}

} // namespace replication
} // namespace dsn
