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

#include <algorithm>
#include <fstream>

#include "utils/fmt_logging.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task.h"
#include "runtime/task/task_tracker.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/safe_strerror_posix.h"
#include "utils/TokenBucket.h"
#include "utils/utils.h"

#include "hdfs_service.h"
#include "block_service/directio_writable_file.h"

namespace dsn {
namespace dist {
namespace block_service {

DEFINE_TASK_CODE(LPC_HDFS_SERVICE_CALL, TASK_PRIORITY_COMMON, THREAD_POOL_BLOCK_SERVICE)

DSN_DEFINE_uint64("replication",
                  hdfs_read_batch_size_bytes,
                  64 << 20,
                  "hdfs read batch size, the default value is 64MB");
DSN_TAG_VARIABLE(hdfs_read_batch_size_bytes, FT_MUTABLE);

DSN_DEFINE_uint32("replication", hdfs_read_limit_rate_mb_per_sec, 200, "hdfs read limit(MB/s)");
DSN_TAG_VARIABLE(hdfs_read_limit_rate_mb_per_sec, FT_MUTABLE);

DSN_DEFINE_uint32("replication", hdfs_write_limit_rate_mb_per_sec, 200, "hdfs write limit(MB/s)");
DSN_TAG_VARIABLE(hdfs_write_limit_rate_mb_per_sec, FT_MUTABLE);

DSN_DEFINE_uint64("replication",
                  hdfs_write_batch_size_bytes,
                  64 << 20,
                  "hdfs write batch size, the default value is 64MB");
DSN_TAG_VARIABLE(hdfs_write_batch_size_bytes, FT_MUTABLE);

DSN_DECLARE_bool(enable_direct_io);

hdfs_service::hdfs_service()
{
    _read_token_bucket.reset(new folly::DynamicTokenBucket());
    _write_token_bucket.reset(new folly::DynamicTokenBucket());
}

hdfs_service::~hdfs_service()
{
    // We should not call hdfsDisconnect() here if jvm has exited.
    // And there is no simple, safe way to call hdfsDisconnect()
    // when process terminates (the proper solution is likely to create a
    // signal handler to detect when the process is killed, but we would still
    // leak when pegasus crashes).
    //
    // close();
}

error_code hdfs_service::initialize(const std::vector<std::string> &args)
{
    if (args.size() < 1) {
        return ERR_INVALID_PARAMETERS;
    }
    // Name_node and root_path should be set in args of block_service configuration.
    // If no path was configured, just use "/" as default root path.
    _hdfs_name_node = args[0];
    _hdfs_path = args.size() >= 2 ? args[1] : "/";
    LOG_INFO_F("hdfs backup root path is initialized to {}.", _hdfs_path);

    return create_fs();
}

error_code hdfs_service::create_fs()
{
    hdfsBuilder *builder = hdfsNewBuilder();
    if (!builder) {
        LOG_ERROR_F("Fail to create an HDFS builder, error: {}.", utils::safe_strerror(errno));
        return ERR_FS_INTERNAL;
    }
    hdfsBuilderSetNameNode(builder, _hdfs_name_node.c_str());
    _fs = hdfsBuilderConnect(builder);
    if (!_fs) {
        LOG_ERROR_F("Fail to connect hdfs name node {}, error: {}.",
                    _hdfs_name_node,
                    utils::safe_strerror(errno));
        return ERR_FS_INTERNAL;
    }
    LOG_INFO_F("Succeed to connect hdfs name node {}.", _hdfs_name_node);
    return ERR_OK;
}

void hdfs_service::close()
{
    // This method should be carefully called.
    // Calls to hdfsDisconnect() by individual threads would terminate
    // all other connections handed out via hdfsConnect() to the same URI.
    LOG_INFO("Try to disconnect hdfs.");
    int result = hdfsDisconnect(_fs);
    if (result == -1) {
        LOG_ERROR_F("Fail to disconnect from the hdfs file system, error: {}.",
                    utils::safe_strerror(errno));
    }
    // Even if there is an error, the resources associated with the hdfsFS will be freed.
    _fs = nullptr;
}

std::string hdfs_service::get_hdfs_entry_name(const std::string &hdfs_path)
{
    // get exact file name from an hdfs path.
    int pos = hdfs_path.find_last_of("/");
    return hdfs_path.substr(pos + 1);
}

dsn::task_ptr hdfs_service::list_dir(const ls_request &req,
                                     dsn::task_code code,
                                     const ls_callback &cb,
                                     dsn::task_tracker *tracker = nullptr)
{
    ls_future_ptr tsk(new ls_future(code, cb, 0));
    tsk->set_tracker(tracker);

    auto list_dir_background = [this, req, tsk]() {
        std::string path = dsn::utils::filesystem::path_combine(_hdfs_path, req.dir_name);
        ls_response resp;

        if (hdfsExists(_fs, path.c_str()) == -1) {
            LOG_ERROR_F("HDFS list directory failed: path {} not found.", path);
            resp.err = ERR_OBJECT_NOT_FOUND;
            tsk->enqueue_with(resp);
            return;
        }

        hdfsFileInfo *dir_info = hdfsGetPathInfo(_fs, path.c_str());
        if (dir_info == nullptr) {
            LOG_ERROR_F("HDFS get path {} failed.", path);
            resp.err = ERR_FS_INTERNAL;
            tsk->enqueue_with(resp);
            return;
        }

        if (dir_info->mKind == kObjectKindFile) {
            LOG_ERROR_F("HDFS list directory failed, {} is not a directory", path);
            resp.err = ERR_INVALID_PARAMETERS;
        } else {
            int entries = 0;
            hdfsFileInfo *info = hdfsListDirectory(_fs, path.c_str(), &entries);
            if (info == nullptr) {
                LOG_ERROR_F("HDFS list directory {} failed.", path);
                resp.err = ERR_FS_INTERNAL;
            } else {
                for (int i = 0; i < entries; i++) {
                    ls_entry tentry;
                    tentry.entry_name = get_hdfs_entry_name(std::string(info[i].mName));
                    tentry.is_directory = (info[i].mKind == kObjectKindDirectory);
                    resp.entries->emplace_back(tentry);
                }
                hdfsFreeFileInfo(info, entries);
                resp.err = ERR_OK;
            }
        }
        hdfsFreeFileInfo(dir_info, 1);
        tsk->enqueue_with(resp);
    };

    dsn::tasking::enqueue(LPC_HDFS_SERVICE_CALL, tracker, list_dir_background);
    return tsk;
}

dsn::task_ptr hdfs_service::create_file(const create_file_request &req,
                                        dsn::task_code code,
                                        const create_file_callback &cb,
                                        dsn::task_tracker *tracker = nullptr)
{
    create_file_future_ptr tsk(new create_file_future(code, cb, 0));
    tsk->set_tracker(tracker);
    std::string hdfs_file = dsn::utils::filesystem::path_combine(_hdfs_path, req.file_name);

    if (req.ignore_metadata) {
        create_file_response resp;
        resp.err = ERR_OK;
        resp.file_handle = new hdfs_file_object(this, hdfs_file);
        tsk->enqueue_with(resp);
        return tsk;
    }

    auto create_file_in_background = [this, req, hdfs_file, tsk]() {
        create_file_response resp;
        dsn::ref_ptr<hdfs_file_object> f = new hdfs_file_object(this, hdfs_file);
        resp.err = f->get_file_meta();
        if (resp.err == ERR_OK || resp.err == ERR_OBJECT_NOT_FOUND) {
            // Just to create a hdfs_file_object locally. The file may not appear on HDFS
            // immediately after this call.
            resp.err = ERR_OK;
            resp.file_handle = f;
            LOG_INFO_F("create remote file {} succeed", hdfs_file);
        }
        tsk->enqueue_with(resp);
    };

    dsn::tasking::enqueue(LPC_HDFS_SERVICE_CALL, tracker, create_file_in_background);
    return tsk;
}

dsn::task_ptr hdfs_service::remove_path(const remove_path_request &req,
                                        dsn::task_code code,
                                        const remove_path_callback &cb,
                                        dsn::task_tracker *tracker)
{
    remove_path_future_ptr tsk(new remove_path_future(code, cb, 0));
    tsk->set_tracker(tracker);

    auto remove_path_background = [this, req, tsk]() {
        std::string path = dsn::utils::filesystem::path_combine(_hdfs_path, req.path);
        remove_path_response resp;

        // Check if path exists.
        if (hdfsExists(_fs, path.c_str()) == -1) {
            LOG_ERROR_F("HDFS remove_path failed: path {} not found.", path);
            resp.err = ERR_OBJECT_NOT_FOUND;
            tsk->enqueue_with(resp);
            return;
        }

        int entries = 0;
        hdfsFileInfo *info = hdfsListDirectory(_fs, path.c_str(), &entries);
        hdfsFreeFileInfo(info, entries);
        if (entries > 0 && !req.recursive) {
            LOG_ERROR_F("HDFS remove_path failed: directory {} is not empty.", path);
            resp.err = ERR_DIR_NOT_EMPTY;
            tsk->enqueue_with(resp);
            return;
        }

        // Remove directory now.
        if (hdfsDelete(_fs, path.c_str(), req.recursive) == -1) {
            LOG_ERROR_F("HDFS remove_path {} failed.", path);
            resp.err = ERR_FS_INTERNAL;
        } else {
            resp.err = ERR_OK;
        }
        tsk->enqueue_with(resp);
    };

    dsn::tasking::enqueue(LPC_HDFS_SERVICE_CALL, tracker, remove_path_background);
    return tsk;
}

hdfs_file_object::hdfs_file_object(hdfs_service *s, const std::string &name)
    : block_file(name), _service(s), _md5sum(""), _size(0), _has_meta_synced(false)
{
}

error_code hdfs_file_object::get_file_meta()
{
    if (hdfsExists(_service->get_fs(), file_name().c_str()) == -1) {
        LOG_WARNING_F("HDFS file {} does not exist.", file_name());
        return ERR_OBJECT_NOT_FOUND;
    }
    hdfsFileInfo *info = hdfsGetPathInfo(_service->get_fs(), file_name().c_str());
    if (info == nullptr) {
        LOG_ERROR_F("HDFS get file info failed, file: {}.", file_name());
        return ERR_FS_INTERNAL;
    }
    _size = info->mSize;
    _has_meta_synced = true;
    hdfsFreeFileInfo(info, 1);
    return ERR_OK;
}

hdfs_file_object::~hdfs_file_object() {}

error_code hdfs_file_object::write_data_in_batches(const char *data,
                                                   const uint64_t data_size,
                                                   uint64_t &written_size)
{
    written_size = 0;
    hdfsFile write_file =
        hdfsOpenFile(_service->get_fs(), file_name().c_str(), O_WRONLY | O_CREAT, 0, 0, 0);
    if (!write_file) {
        LOG_ERROR_F("Failed to open hdfs file {} for writting, error: {}.",
                    file_name(),
                    utils::safe_strerror(errno));
        return ERR_FS_INTERNAL;
    }
    uint64_t cur_pos = 0;
    uint64_t write_len = 0;
    while (cur_pos < data_size) {
        write_len = std::min(data_size - cur_pos, FLAGS_hdfs_write_batch_size_bytes);
        const uint64_t rate = FLAGS_hdfs_write_limit_rate_mb_per_sec << 20;
        const uint64_t burst_size = std::max(2 * rate, write_len);
        _service->_write_token_bucket->consumeWithBorrowAndWait(write_len, rate, burst_size);

        tSize num_written_bytes = hdfsWrite(_service->get_fs(),
                                            write_file,
                                            (void *)(data + cur_pos),
                                            static_cast<tSize>(write_len));
        if (num_written_bytes == -1) {
            LOG_ERROR_F("Failed to write hdfs file {}, error: {}.",
                        file_name(),
                        utils::safe_strerror(errno));
            hdfsCloseFile(_service->get_fs(), write_file);
            return ERR_FS_INTERNAL;
        }
        cur_pos += num_written_bytes;
    }
    if (hdfsHFlush(_service->get_fs(), write_file) != 0) {
        LOG_ERROR_F(
            "Failed to flush hdfs file {}, error: {}.", file_name(), utils::safe_strerror(errno));
        hdfsCloseFile(_service->get_fs(), write_file);
        return ERR_FS_INTERNAL;
    }
    written_size = cur_pos;
    if (hdfsCloseFile(_service->get_fs(), write_file) != 0) {
        LOG_ERROR_F(
            "Failed to close hdfs file {}, error: {}", file_name(), utils::safe_strerror(errno));
        return ERR_FS_INTERNAL;
    }

    LOG_INFO("start to synchronize meta data after successfully wrote data to hdfs");
    return get_file_meta();
}

dsn::task_ptr hdfs_file_object::write(const write_request &req,
                                      dsn::task_code code,
                                      const write_callback &cb,
                                      dsn::task_tracker *tracker = nullptr)
{
    add_ref();
    write_future_ptr tsk(new write_future(code, cb, 0));
    tsk->set_tracker(tracker);
    auto write_background = [this, req, tsk]() {
        write_response resp;
        resp.err = write_data_in_batches(req.buffer.data(), req.buffer.length(), resp.written_size);
        tsk->enqueue_with(resp);
        release_ref();
    };
    dsn::tasking::enqueue(LPC_HDFS_SERVICE_CALL, tracker, std::move(write_background));
    return tsk;
}

dsn::task_ptr hdfs_file_object::upload(const upload_request &req,
                                       dsn::task_code code,
                                       const upload_callback &cb,
                                       dsn::task_tracker *tracker = nullptr)
{
    upload_future_ptr t(new upload_future(code, cb, 0));
    t->set_tracker(tracker);

    add_ref();
    auto upload_background = [this, req, t]() {
        upload_response resp;
        resp.uploaded_size = 0;
        std::ifstream is(req.input_local_name, std::ios::binary | std::ios::in);
        if (is.is_open()) {
            int64_t file_sz = 0;
            dsn::utils::filesystem::file_size(req.input_local_name, file_sz);
            std::unique_ptr<char[]> buffer(new char[file_sz]);
            is.read(buffer.get(), file_sz);
            is.close();
            resp.err = write_data_in_batches(buffer.get(), file_sz, resp.uploaded_size);
        } else {
            LOG_ERROR_F(
                "HDFS upload failed: open local file {} failed when upload to {}, error: {}",
                req.input_local_name,
                file_name(),
                utils::safe_strerror(errno));
            resp.err = dsn::ERR_FILE_OPERATION_FAILED;
        }
        t->enqueue_with(resp);
        release_ref();
    };

    dsn::tasking::enqueue(LPC_HDFS_SERVICE_CALL, tracker, upload_background);
    return t;
}

error_code hdfs_file_object::read_data_in_batches(uint64_t start_pos,
                                                  int64_t length,
                                                  std::string &read_buffer,
                                                  size_t &read_length)
{
    // get file meta if it is not synchronized.
    if (!_has_meta_synced) {
        error_code err = get_file_meta();
        if (err != ERR_OK) {
            LOG_ERROR_F("Failed to read remote file {}", file_name());
            return err;
        }
    }

    hdfsFile read_file = hdfsOpenFile(_service->get_fs(), file_name().c_str(), O_RDONLY, 0, 0, 0);
    if (!read_file) {
        LOG_ERROR_F("Failed to open hdfs file {} for reading, error: {}.",
                    file_name(),
                    utils::safe_strerror(errno));
        return ERR_FS_INTERNAL;
    }
    std::unique_ptr<char[]> raw_buf(new char[_size]);
    char *dst_buf = raw_buf.get();

    // if length = -1, we should read the whole file.
    uint64_t data_length = (length == -1 ? _size : length);
    uint64_t cur_pos = start_pos;
    uint64_t read_size = 0;
    bool read_success = true;
    while (cur_pos < start_pos + data_length) {
        const uint64_t rate = FLAGS_hdfs_read_limit_rate_mb_per_sec << 20;
        read_size = std::min(start_pos + data_length - cur_pos, FLAGS_hdfs_read_batch_size_bytes);
        // burst size should not be less than consume size
        const uint64_t burst_size = std::max(2 * rate, read_size);
        _service->_read_token_bucket->consumeWithBorrowAndWait(read_size, rate, burst_size);

        tSize num_read_bytes = hdfsPread(_service->get_fs(),
                                         read_file,
                                         static_cast<tOffset>(cur_pos),
                                         (void *)dst_buf,
                                         static_cast<tSize>(read_size));
        if (num_read_bytes > 0) {
            cur_pos += num_read_bytes;
            dst_buf += num_read_bytes;
        } else if (num_read_bytes == -1) {
            LOG_ERROR_F("Failed to read hdfs file {}, error: {}.",
                        file_name(),
                        utils::safe_strerror(errno));
            read_success = false;
            break;
        }
    }
    if (hdfsCloseFile(_service->get_fs(), read_file) != 0) {
        LOG_ERROR_F(
            "Failed to close hdfs file {}, error: {}.", file_name(), utils::safe_strerror(errno));
        return ERR_FS_INTERNAL;
    }
    if (read_success) {
        read_length = cur_pos - start_pos;
        read_buffer = std::string(raw_buf.get(), dst_buf - raw_buf.get());
        return ERR_OK;
    }
    return ERR_FS_INTERNAL;
}

dsn::task_ptr hdfs_file_object::read(const read_request &req,
                                     dsn::task_code code,
                                     const read_callback &cb,
                                     dsn::task_tracker *tracker = nullptr)
{
    read_future_ptr tsk(new read_future(code, cb, 0));
    tsk->set_tracker(tracker);

    add_ref();
    auto read_func = [this, req, tsk]() {
        size_t read_length = 0;
        read_response resp;
        std::string read_buffer;
        resp.err =
            read_data_in_batches(req.remote_pos, req.remote_length, read_buffer, read_length);
        if (resp.err == ERR_OK) {
            resp.buffer = blob::create_from_bytes(std::move(read_buffer));
        }
        tsk->enqueue_with(resp);
        release_ref();
    };

    dsn::tasking::enqueue(LPC_HDFS_SERVICE_CALL, tracker, std::move(read_func));
    return tsk;
}

dsn::task_ptr hdfs_file_object::download(const download_request &req,
                                         dsn::task_code code,
                                         const download_callback &cb,
                                         dsn::task_tracker *tracker = nullptr)
{
    download_future_ptr t(new download_future(code, cb, 0));
    t->set_tracker(tracker);

    add_ref();
    auto download_background = [this, req, t]() {
        download_response resp;
        resp.downloaded_size = 0;
        std::string read_buffer;
        size_t read_length = 0;
        resp.err =
            read_data_in_batches(req.remote_pos, req.remote_length, read_buffer, read_length);
        if (resp.err == ERR_OK) {
            bool write_succ = false;
            if (FLAGS_enable_direct_io) {
                auto dio_file = std::make_unique<direct_io_writable_file>(req.output_local_name);
                do {
                    if (!dio_file->initialize()) {
                        break;
                    }
                    bool wr_ret = dio_file->write(read_buffer.c_str(), read_length);
                    if (!wr_ret) {
                        break;
                    }
                    if (dio_file->finalize()) {
                        resp.downloaded_size = read_length;
                        resp.file_md5 = utils::string_md5(read_buffer.c_str(), read_length);
                        write_succ = true;
                    }
                } while (0);
            } else {
                std::ofstream out(req.output_local_name,
                                  std::ios::binary | std::ios::out | std::ios::trunc);
                if (out.is_open()) {
                    out.write(read_buffer.c_str(), read_length);
                    out.close();
                    resp.downloaded_size = read_length;
                    resp.file_md5 = utils::string_md5(read_buffer.c_str(), read_length);
                    write_succ = true;
                }
            }
            if (!write_succ) {
                LOG_ERROR_F("HDFS download failed: fail to open localfile {} when download {}, "
                            "error: {}",
                            req.output_local_name,
                            file_name(),
                            utils::safe_strerror(errno));
                resp.err = ERR_FILE_OPERATION_FAILED;
                resp.downloaded_size = 0;
            }
        }
        t->enqueue_with(resp);
        release_ref();
    };

    dsn::tasking::enqueue(LPC_HDFS_SERVICE_CALL, tracker, download_background);
    return t;
}
} // namespace block_service
} // namespace dist
} // namespace dsn
