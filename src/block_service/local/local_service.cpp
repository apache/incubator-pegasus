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

#include <rocksdb/env.h>
#include <memory>
#include <set>
#include <type_traits>
#include <utility>

#include <string_view>
#include "local_service.h"
#include "nlohmann/json.hpp"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "task/async_calls.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/load_dump_object.h"
#include "utils/strings.h"

DSN_DECLARE_bool(enable_direct_io);

namespace dsn {
class task_tracker;
} // namespace dsn

static const int max_length = 2048; // max data length read from file each time

namespace dsn {
namespace dist {
namespace block_service {

DEFINE_TASK_CODE(LPC_LOCAL_SERVICE_CALL, TASK_PRIORITY_COMMON, THREAD_POOL_BLOCK_SERVICE)

std::string local_service::get_metafile(const std::string &filepath)
{
    std::string dir_part = utils::filesystem::remove_file_name(filepath);
    std::string base_part = utils::filesystem::get_file_name(filepath);

    return utils::filesystem::path_combine(dir_part, std::string(".") + base_part + ".meta");
}

local_service::local_service() {}

local_service::local_service(const std::string &root) : _root(root) {}

local_service::~local_service() {}

error_code local_service::initialize(const std::vector<std::string> &args)
{
    if (args.size() > 0 && _root.empty())
        _root = args[0];

    if (_root.empty()) {
        LOG_INFO("initialize local block service succeed with empty root");
    } else {
        if (::dsn::utils::filesystem::directory_exists(_root)) {
            LOG_WARNING("old local block service root dir has already exist, path({})", _root);
        } else {
            CHECK(::dsn::utils::filesystem::create_directory(_root),
                  "local block service create directory({}) fail",
                  _root);
        }
        LOG_INFO("local block service initialize succeed with root({})", _root);
    }
    return ERR_OK;
}

dsn::task_ptr local_service::list_dir(const ls_request &req,
                                      dsn::task_code code,
                                      const ls_callback &callback,
                                      task_tracker *tracker)
{
    ls_future_ptr tsk(new ls_future(code, callback, 0));
    tsk->set_tracker(tracker);

    // process
    auto list_dir_background = [this, req, tsk]() {
        std::string dir_path = ::dsn::utils::filesystem::path_combine(_root, req.dir_name);
        std::vector<std::string> children;

        ls_response resp;
        resp.err = ERR_OK;

        if (::dsn::utils::filesystem::file_exists(dir_path)) {
            LOG_INFO("list_dir: invalid parameter({})", dir_path);
            resp.err = ERR_INVALID_PARAMETERS;
        } else if (!::dsn::utils::filesystem::directory_exists(dir_path)) {
            LOG_INFO("directory does not exist, dir = {}", dir_path);
            resp.err = ERR_OBJECT_NOT_FOUND;
        } else {
            if (!::dsn::utils::filesystem::get_subfiles(dir_path, children, false)) {
                LOG_ERROR("get files under directory: {} fail", dir_path);
                resp.err = ERR_FS_INTERNAL;
                children.clear();
            } else {
                ls_entry tentry;
                tentry.is_directory = false;

                std::set<std::string> file_matcher;
                for (const std::string &file : children) {
                    file_matcher.insert(utils::filesystem::get_file_name(file));
                }
                for (const auto &file : file_matcher) {
                    if (file_matcher.find(get_metafile(file)) != file_matcher.end()) {
                        tentry.entry_name = file;
                        resp.entries->emplace_back(tentry);
                    }
                }
            }

            children.clear();
            if (!::dsn::utils::filesystem::get_subdirectories(dir_path, children, false)) {
                LOG_ERROR("get subpaths under directory: {} fail", dir_path);
                resp.err = ERR_FS_INTERNAL;
                children.clear();
            } else {
                ls_entry tentry;
                tentry.is_directory = true;

                for (const auto &dir : children) {
                    tentry.entry_name = ::dsn::utils::filesystem::get_file_name(dir);
                    resp.entries->emplace_back(tentry);
                }
            }
        }
        tsk->enqueue_with(std::move(resp));
    };

    tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(list_dir_background));
    return tsk;
}

dsn::task_ptr local_service::create_file(const create_file_request &req,
                                         dsn::task_code code,
                                         const create_file_callback &cb,
                                         task_tracker *tracker)
{
    create_file_future_ptr tsk(new create_file_future(code, cb, 0));
    tsk->set_tracker(tracker);

    if (req.ignore_metadata) {
        create_file_response resp;
        resp.err = ERR_OK;
        resp.file_handle =
            new local_file_object(::dsn::utils::filesystem::path_combine(_root, req.file_name));
        tsk->enqueue_with(resp);
        return tsk;
    }

    auto create_file_background = [this, req, tsk]() {
        std::string file_path = utils::filesystem::path_combine(_root, req.file_name);
        std::string meta_file_path =
            utils::filesystem::path_combine(_root, get_metafile(req.file_name));
        create_file_response resp;
        resp.err = ERR_OK;

        dsn::ref_ptr<local_file_object> f = new local_file_object(file_path);
        if (utils::filesystem::file_exists(file_path) &&
            utils::filesystem::file_exists(meta_file_path)) {

            LOG_INFO("file({}) already exist", file_path);
            resp.err = f->load_metadata();
        }

        if (ERR_OK == resp.err)
            resp.file_handle = f;

        tsk->enqueue_with(resp);
    };

    tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(create_file_background));
    return tsk;
}

dsn::task_ptr local_service::remove_path(const remove_path_request &req,
                                         dsn::task_code code,
                                         const remove_path_callback &cb,
                                         task_tracker *tracker)
{
    remove_path_future_ptr tsk(new remove_path_future(code, cb, 0));
    tsk->set_tracker(tracker);

    auto remove_path_background = [this, req, tsk]() {
        remove_path_response resp;
        resp.err = ERR_OK;

        bool do_remove = true;

        std::string full_path = utils::filesystem::path_combine(_root, req.path);
        if (utils::filesystem::directory_exists(full_path)) {
            auto res = utils::filesystem::is_directory_empty(full_path);
            if (res.first == ERR_OK) {
                // directory is not empty & recursive = false
                if (!res.second && !req.recursive) {
                    resp.err = ERR_DIR_NOT_EMPTY;
                    do_remove = false;
                }
            } else {
                resp.err = ERR_FS_INTERNAL;
                do_remove = false;
            }
        } else if (!utils::filesystem::file_exists(full_path)) {
            resp.err = ERR_OBJECT_NOT_FOUND;
            do_remove = false;
        }

        if (do_remove) {
            if (!utils::filesystem::remove_path(full_path)) {
                resp.err = ERR_FS_INTERNAL;
            }
        }

        tsk->enqueue_with(resp);
    };

    tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(remove_path_background));
    return tsk;
}

// local_file_object
local_file_object::local_file_object(const std::string &name)
    : block_file(name), _size(0), _md5_value(""), _has_meta_synced(false)
{
}

local_file_object::~local_file_object() {}

const std::string &local_file_object::get_md5sum() { return _md5_value; }

uint64_t local_file_object::get_size() { return _size; }

error_code local_file_object::load_metadata()
{
    if (_has_meta_synced) {
        return ERR_OK;
    }

    file_metadata fmd;
    std::string filepath = local_service::get_metafile(file_name());
    auto ec = dsn::utils::load_njobj_from_file(filepath, &fmd);
    if (ec != ERR_OK) {
        LOG_WARNING("load metadata file '{}' failed", filepath);
        return ERR_FS_INTERNAL;
    }
    _size = fmd.size;
    _md5_value = fmd.md5;
    _has_meta_synced = true;
    return ERR_OK;
}

dsn::task_ptr local_file_object::write(const write_request &req,
                                       dsn::task_code code,
                                       const write_callback &cb,
                                       task_tracker *tracker)
{
    add_ref();

    write_future_ptr tsk(new write_future(code, cb, 0));
    tsk->set_tracker(tracker);

    FAIL_POINT_INJECT_F("mock_local_service_write_failed", [=](std::string_view) {
        auto write_failed = [=]() {
            write_response resp;
            resp.err = ERR_FS_INTERNAL;
            tsk->enqueue_with(resp);
            release_ref();
        };
        dsn::tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(write_failed));
        return tsk;
    });

    auto write_background = [this, req, tsk]() {
        write_response resp;
        resp.err = ERR_OK;
        if (!::dsn::utils::filesystem::file_exists(file_name())) {
            if (!::dsn::utils::filesystem::create_file(file_name())) {
                resp.err = ERR_FS_INTERNAL;
            }
        }

        if (resp.err == ERR_OK) {
            LOG_INFO("start write file, file = {}", file_name());

            do {
                auto s = rocksdb::WriteStringToFile(
                    dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive),
                    rocksdb::Slice(req.buffer.data(), req.buffer.length()),
                    file_name(),
                    /* should_sync */ true);
                if (!s.ok()) {
                    LOG_WARNING("write file '{}' failed, err = {}", file_name(), s.ToString());
                    resp.err = ERR_FS_INTERNAL;
                    break;
                }

                resp.written_size = req.buffer.length();

                // Currently we calc the meta data from source data, which save the io bandwidth
                // a lot, but it is somewhat not correct.
                _size = resp.written_size;
                _md5_value = utils::string_md5(req.buffer.data(), req.buffer.length());
                auto err = dsn::utils::dump_njobj_to_file(file_metadata(_size, _md5_value),
                                                          local_service::get_metafile(file_name()));
                if (err != ERR_OK) {
                    LOG_ERROR("file_metadata write failed");
                    resp.err = ERR_FS_INTERNAL;
                    break;
                }
                _has_meta_synced = true;
            } while (false);
        }
        tsk->enqueue_with(resp);
        release_ref();
    };
    ::dsn::tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(write_background));
    return tsk;
}

dsn::task_ptr local_file_object::read(const read_request &req,
                                      dsn::task_code code,
                                      const read_callback &cb,
                                      task_tracker *tracker)
{
    add_ref();

    read_future_ptr tsk(new read_future(code, cb, 0));
    tsk->set_tracker(tracker);

    auto read_func = [this, req, tsk]() {
        read_response resp;
        do {
            if (!utils::filesystem::file_exists(file_name()) ||
                !utils::filesystem::file_exists(local_service::get_metafile(file_name()))) {
                LOG_WARNING("data file '{}' or metadata file '{}' not exist",
                            file_name(),
                            local_service::get_metafile(file_name()));
                resp.err = ERR_OBJECT_NOT_FOUND;
                break;
            }

            resp.err = load_metadata();
            if (resp.err != ERR_OK) {
                LOG_WARNING("load metadata of {} failed", file_name());
                break;
            }

            int64_t file_sz = _size;
            int64_t total_sz = 0;
            if (req.remote_length == -1 || req.remote_length + req.remote_pos > file_sz) {
                total_sz = file_sz - req.remote_pos;
            } else {
                total_sz = req.remote_length;
            }

            LOG_INFO("start to read file '{}', offset = {}, size = {}",
                     file_name(),
                     req.remote_pos,
                     total_sz);
            rocksdb::EnvOptions env_options;
            env_options.use_direct_reads = FLAGS_enable_direct_io;
            std::unique_ptr<rocksdb::SequentialFile> sfile;
            auto s = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive)
                         ->NewSequentialFile(file_name(), &sfile, env_options);
            if (!s.ok()) {
                LOG_WARNING("open file '{}' failed, err = {}", file_name(), s.ToString());
                resp.err = ERR_FS_INTERNAL;
                break;
            }

            s = sfile->Skip(req.remote_pos);
            if (!s.ok()) {
                LOG_WARNING(
                    "skip '{}' for {} failed, err = {}", file_name(), req.remote_pos, s.ToString());
                resp.err = ERR_FS_INTERNAL;
                break;
            }

            rocksdb::Slice result;
            std::string buf;
            buf.resize(total_sz + 1);
            s = sfile->Read(total_sz, &result, buf.data());
            if (!s.ok()) {
                LOG_WARNING("read file '{}' failed, err = {}", file_name(), s.ToString());
                resp.err = ERR_FS_INTERNAL;
                break;
            }

            buf[result.size()] = 0;
            resp.buffer = blob::create_from_bytes(std::move(buf));
        } while (false);
        tsk->enqueue_with(resp);
        release_ref();
    };

    dsn::tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(read_func));
    return tsk;
}

dsn::task_ptr local_file_object::upload(const upload_request &req,
                                        dsn::task_code code,
                                        const upload_callback &cb,
                                        task_tracker *tracker)
{
    add_ref();
    upload_future_ptr tsk(new upload_future(code, cb, 0));
    tsk->set_tracker(tracker);
    auto upload_file_func = [this, req, tsk]() {
        LOG_INFO("start to upload from '{}' to '{}'", req.input_local_name, file_name());

        upload_response resp;
        do {
            // Create the directory.
            std::string path = dsn::utils::filesystem::remove_file_name(file_name());
            if (!dsn::utils::filesystem::create_directory(path)) {
                LOG_WARNING("create directory '{}' failed", path);
                resp.err = ERR_FILE_OPERATION_FAILED;
                break;
            }

            uint64_t file_size;
            auto s = dsn::utils::copy_file(req.input_local_name, file_name(), &file_size);
            if (!s.ok()) {
                LOG_WARNING("upload from '{}' to '{}' failed, err = {}",
                            req.input_local_name,
                            file_name(),
                            s.ToString());
                resp.err = ERR_FILE_OPERATION_FAILED;
                break;
            }
            LOG_INFO("finish to upload from '{}' to '{}', size = {}",
                     req.input_local_name,
                     file_name(),
                     file_size);

            resp.uploaded_size = file_size;
            _size = file_size;
            auto res = utils::filesystem::md5sum(file_name(), _md5_value);
            if (res != dsn::ERR_OK) {
                LOG_WARNING("calculate md5sum for '{}' failed", file_name());
                resp.err = ERR_FS_INTERNAL;
                break;
            }

            auto err = dsn::utils::dump_njobj_to_file(file_metadata(_size, _md5_value),
                                                      local_service::get_metafile(file_name()));
            if (err != ERR_OK) {
                LOG_ERROR("file_metadata write failed");
                resp.err = ERR_FS_INTERNAL;
                break;
            }
            _has_meta_synced = true;
        } while (false);
        tsk->enqueue_with(resp);
        release_ref();
    };
    ::dsn::tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(upload_file_func));

    return tsk;
}

dsn::task_ptr local_file_object::download(const download_request &req,
                                          dsn::task_code code,
                                          const download_callback &cb,
                                          task_tracker *tracker)
{
    // download the whole file
    add_ref();
    download_future_ptr tsk(new download_future(code, cb, 0));
    tsk->set_tracker(tracker);
    auto download_file_func = [this, req, tsk]() {
        download_response resp;
        resp.err = ERR_OK;
        std::string target_file = req.output_local_name;

        do {
            if (target_file.empty()) {
                LOG_WARNING("download {} failed, because target name({}) is invalid",
                            file_name(),
                            target_file);
                resp.err = ERR_INVALID_PARAMETERS;
                break;
            }

            if (!_has_meta_synced) {
                if (!utils::filesystem::file_exists(file_name()) ||
                    !utils::filesystem::file_exists(local_service::get_metafile(file_name()))) {
                    LOG_WARNING("file '{}' or metadata file '{}' not found",
                                file_name(),
                                local_service::get_metafile(file_name()));
                    resp.err = ERR_OBJECT_NOT_FOUND;
                    break;
                }
            }

            LOG_INFO("start to download from '{}' to '{}'", file_name(), target_file);

            // Create the directory.
            std::string path = dsn::utils::filesystem::remove_file_name(file_name());
            if (!dsn::utils::filesystem::create_directory(path)) {
                LOG_WARNING("create directory '{}' failed", path);
                resp.err = ERR_FILE_OPERATION_FAILED;
                break;
            }

            uint64_t file_size;
            auto s = dsn::utils::copy_file(file_name(), target_file, &file_size);
            if (!s.ok()) {
                LOG_WARNING("download from '{}' to '{}' failed, err = {}",
                            file_name(),
                            target_file,
                            s.ToString());
                resp.err = ERR_FILE_OPERATION_FAILED;
                break;
            }

            auto res = utils::filesystem::md5sum(target_file, _md5_value);
            if (res != dsn::ERR_OK) {
                LOG_WARNING("calculate md5sum for {} failed", target_file);
                resp.err = ERR_FILE_OPERATION_FAILED;
                break;
            }

            LOG_INFO("finish download file '{}', size = {}", target_file, file_size);
            resp.downloaded_size = file_size;
            resp.file_md5 = _md5_value;
            _size = file_size;
            _has_meta_synced = true;
        } while (false);
        tsk->enqueue_with(resp);
        release_ref();
    };
    ::dsn::tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(download_file_func));

    return tsk;
}
} // namespace block_service
} // namespace dist
} // namespace dsn
