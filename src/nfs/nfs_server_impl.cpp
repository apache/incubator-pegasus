/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include "nfs/nfs_server_impl.h"

#include <fcntl.h>
#include <sys/stat.h>

#include <cstdlib>

#include "aio/disk_engine.h"
#include "runtime/task/async_calls.h"
#include "utils/filesystem.h"
#include "utils/safe_strerror_posix.h"
#include "utils/string_conv.h"

namespace dsn {
namespace service {

DSN_DEFINE_uint32(
    nfs,
    max_send_rate_megabytes_per_disk,
    0,
    "max rate per disk of send to remote node(MB/s)，zero means disable rate limiter");
DSN_TAG_VARIABLE(max_send_rate_megabytes_per_disk, FT_MUTABLE);

DSN_DECLARE_int32(file_close_timer_interval_ms_on_server);
DSN_DECLARE_int32(file_close_expire_time_ms);

nfs_service_impl::nfs_service_impl() : ::dsn::serverlet<nfs_service_impl>("nfs")
{
    _file_close_timer = ::dsn::tasking::enqueue_timer(
        LPC_NFS_FILE_CLOSE_TIMER,
        &_tracker,
        [this] { close_file(); },
        std::chrono::milliseconds(FLAGS_file_close_timer_interval_ms_on_server));

    _recent_copy_data_size.init_app_counter("eon.nfs_server",
                                            "recent_copy_data_size",
                                            COUNTER_TYPE_VOLATILE_NUMBER,
                                            "nfs server copy data size in the recent period");
    _recent_copy_fail_count.init_app_counter(
        "eon.nfs_server",
        "recent_copy_fail_count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "nfs server copy fail count count in the recent period");

    _send_token_buckets = std::make_unique<dsn::utils::token_buckets>();
    register_cli_commands();
}

void nfs_service_impl::on_copy(const ::dsn::service::copy_request &request,
                               ::dsn::rpc_replier<::dsn::service::copy_response> &reply)
{
    std::string file_path =
        dsn::utils::filesystem::path_combine(request.source_dir, request.file_name);
    disk_file *dfile = nullptr;

    {
        zauto_lock l(_handles_map_lock);
        auto it = _handles_map.find(file_path); // find file handle cache first

        if (it == _handles_map.end()) {
            dfile = file::open(file_path.c_str(), O_RDONLY | O_BINARY, 0);
            if (dfile != nullptr) {
                auto fh = std::make_shared<file_handle_info_on_server>();
                fh->file_handle = dfile;
                fh->file_access_count = 1;
                fh->last_access_time = dsn_now_ms();
                _handles_map.insert(std::make_pair(file_path, std::move(fh)));
            }
        } else {
            dfile = it->second->file_handle;
            it->second->file_access_count++;
            it->second->last_access_time = dsn_now_ms();
        }
    }

    LOG_DEBUG(
        "nfs: copy file {} [{}, {}]", file_path, request.offset, request.offset + request.size);

    if (dfile == nullptr) {
        LOG_ERROR("[nfs_service] open file {} failed", file_path);
        ::dsn::service::copy_response resp;
        resp.error = ERR_OBJECT_NOT_FOUND;
        reply(resp);
        return;
    }

    std::shared_ptr<callback_para> cp = std::make_shared<callback_para>(std::move(reply));
    cp->bb = blob(dsn::utils::make_shared_array<char>(request.size), request.size);
    cp->dst_dir = request.dst_dir;
    cp->source_disk_tag = request.source_disk_tag;
    cp->file_path = std::move(file_path);
    cp->offset = request.offset;
    cp->size = request.size;

    auto buffer_save = cp->bb.buffer().get();

    file::read(
        dfile,
        buffer_save,
        request.size,
        request.offset,
        LPC_NFS_READ,
        &_tracker,
        [this, cp](error_code err, size_t sz) mutable { internal_read_callback(err, sz, *cp); });
}

void nfs_service_impl::internal_read_callback(error_code err, size_t sz, callback_para &cp)
{
    if (FLAGS_max_send_rate_megabytes_per_disk > 0) {
        _send_token_buckets->get_token_bucket(cp.source_disk_tag)
            ->consumeWithBorrowAndWait(sz,
                                       FLAGS_max_send_rate_megabytes_per_disk << 20,
                                       1.5 * (FLAGS_max_send_rate_megabytes_per_disk << 20));
    }

    {
        zauto_lock l(_handles_map_lock);
        auto it = _handles_map.find(cp.file_path);

        if (it != _handles_map.end()) {
            it->second->file_access_count--;
        }
    }

    if (err != ERR_OK) {
        LOG_ERROR("[nfs_service] read file {} failed, err = {}", cp.file_path, err);
        _recent_copy_fail_count->increment();
    } else {
        _recent_copy_data_size->add(sz);
    }

    ::dsn::service::copy_response resp;
    resp.error = err;
    resp.file_content = std::move(cp.bb);
    resp.offset = cp.offset;
    resp.size = cp.size;

    cp.replier(resp);
}

// RPC_NFS_NEW_NFS_GET_FILE_SIZE
void nfs_service_impl::on_get_file_size(
    const ::dsn::service::get_file_size_request &request,
    ::dsn::rpc_replier<::dsn::service::get_file_size_response> &reply)
{
    get_file_size_response resp;
    error_code err = ERR_OK;
    std::vector<std::string> file_list;
    std::string folder = request.source_dir;
    if (request.file_list.size() == 0) // return all file size in the destination file folder
    {
        if (!dsn::utils::filesystem::directory_exists(folder)) {
            LOG_ERROR("[nfs_service] directory {} not exist", folder);
            err = ERR_OBJECT_NOT_FOUND;
        } else {
            if (!dsn::utils::filesystem::get_subfiles(folder, file_list, true)) {
                LOG_ERROR("[nfs_service] get subfiles of directory {} failed", folder);
                err = ERR_FILE_OPERATION_FAILED;
            } else {
                for (auto &fpath : file_list) {
                    // TODO: using uint64 instead as file ma
                    // Done
                    int64_t sz;
                    if (!dsn::utils::filesystem::file_size(fpath, sz)) {
                        LOG_ERROR("[nfs_service] get size of file {} failed", fpath);
                        err = ERR_FILE_OPERATION_FAILED;
                        break;
                    }

                    resp.size_list.push_back((uint64_t)sz);
                    resp.file_list.push_back(
                        fpath.substr(request.source_dir.length(), fpath.length() - 1));
                }
                file_list.clear();
            }
        }
    } else // return file size in the request file folder
    {
        for (size_t i = 0; i < request.file_list.size(); i++) {
            std::string file_path =
                dsn::utils::filesystem::path_combine(folder, request.file_list[i]);

            struct stat st;
            if (0 != ::stat(file_path.c_str(), &st)) {
                LOG_ERROR("[nfs_service] get stat of file {} failed, err = {}",
                          file_path,
                          dsn::utils::safe_strerror(errno));
                err = ERR_OBJECT_NOT_FOUND;
                break;
            }

            // TODO: using int64 instead as file may exceed the size of 32bit
            // Done
            uint64_t size = st.st_size;

            resp.size_list.push_back(size);
            resp.file_list.push_back((folder + request.file_list[i])
                                         .substr(request.source_dir.length(),
                                                 (folder + request.file_list[i]).length() - 1));
        }
    }

    resp.error = err;
    reply(resp);
}

void nfs_service_impl::close_file() // release out-of-date file handle
{
    zauto_lock l(_handles_map_lock);

    for (auto it = _handles_map.begin(); it != _handles_map.end();) {
        auto fptr = it->second;

        // not used and expired
        if (fptr->file_access_count == 0 &&
            dsn_now_ms() - fptr->last_access_time > (uint64_t)FLAGS_file_close_expire_time_ms) {
            LOG_DEBUG("nfs: close file handle {}", it->first);
            it = _handles_map.erase(it);
        } else
            it++;
    }
}

// TODO(jiashuo1):  just for compatibility with scripts, such as
// https://github.com/apache/incubator-pegasus/blob/v2.3/scripts/pegasus_offline_node_list.sh
void nfs_service_impl::register_cli_commands()
{
    static std::once_flag flag;
    std::call_once(flag, [&]() {
        _nfs_max_send_rate_megabytes_cmd = dsn::command_manager::instance().register_command(
            {"nfs.max_send_rate_megabytes_per_disk"},
            "nfs.max_send_rate_megabytes_per_disk [num]",
            "control the max rate(MB/s) for one disk to send file to remote node",
            [](const std::vector<std::string> &args) {
                std::string result("OK");

                if (args.empty()) {
                    return std::to_string(FLAGS_max_send_rate_megabytes_per_disk);
                }

                int32_t max_send_rate_megabytes = 0;
                if (!dsn::buf2int32(args[0], max_send_rate_megabytes) ||
                    max_send_rate_megabytes <= 0) {
                    return std::string("ERR: invalid arguments");
                }

                FLAGS_max_send_rate_megabytes_per_disk = max_send_rate_megabytes;
                return result;
            });
    });
}

} // namespace service
} // namespace dsn
