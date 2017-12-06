#include <memory>

#include <dsn/utility/filesystem.h>
#include <dsn/utility/error_code.h>
#include "local_service.h"

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "block.service.local"

static const int max_length = 2048; // max data length read from file each time

namespace dsn {
namespace dist {
namespace block_service {

DEFINE_THREAD_POOL_CODE(THREAD_POOL_LOCAL_SERVICE)
DEFINE_TASK_CODE(LPC_LOCAL_SERVICE_CALL, TASK_PRIORITY_COMMON, THREAD_POOL_LOCAL_SERVICE)

local_service::local_service() {}

local_service::local_service(const std::string &root) : _root(root) {}

local_service::~local_service()
{
    // do nothing
}

error_code local_service::initialize(const std::vector<std::string> &args)
{
    if (_root.empty()) {
        ddebug("initialize local block service succeed with empty root");
    } else {
        if (::dsn::utils::filesystem::directory_exists(_root)) {
            dwarn("old local block service root dir has already exist, path(%s)", _root.c_str());
        } else {
            if (!::dsn::utils::filesystem::create_directory(_root)) {
                dassert(false, "local block service create directory(%s) fail", _root.c_str());
                return ERR_FS_INTERNAL;
            }
        }
    }
    ddebug("local block service initialize succeed");
    return ERR_OK;
}

dsn::task_ptr local_service::list_dir(const ls_request &req,
                                      dsn::task_code code,
                                      const ls_callback &callback,
                                      clientlet *tracker)
{
    task_ptr tsk = tasking::create_late_task(code, std::move(callback), 0, tracker);
    // process
    auto list_dir_background = [this, req, tsk]() {
        std::string dir_path = ::dsn::utils::filesystem::path_combine(_root, req.dir_name);
        std::vector<std::string> children;

        ls_response resp;
        resp.err = ERR_OK;

        if (::dsn::utils::filesystem::file_exists(dir_path)) {
            ddebug("list_dir: invalid parameter(%s)", dir_path.c_str());
            resp.err = ERR_INVALID_PARAMETERS;
        } else if (!::dsn::utils::filesystem::directory_exists(dir_path)) {
            ddebug("directory does not exist, dir = %s", dir_path.c_str());
            resp.err = ERR_OBJECT_NOT_FOUND;
        } else {
            if (!::dsn::utils::filesystem::get_subfiles(dir_path, children, false)) {
                derror("get files under directory: %s fail", dir_path.c_str());
                resp.err = ERR_FS_INTERNAL;
                children.clear();
            } else {
                ls_entry tentry;
                tentry.is_directory = false;

                for (const auto &file : children) {
                    tentry.entry_name = ::dsn::utils::filesystem::get_file_name(file);
                    resp.entries->emplace_back(tentry);
                }
            }

            children.clear();
            if (!::dsn::utils::filesystem::get_subdirectories(dir_path, children, false)) {
                derror("get subpaths under directory: %s fail", dir_path.c_str());
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
        call_safe_late_task(tsk, resp);
    };

    tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(list_dir_background));
    return tsk;
}

dsn::task_ptr local_service::create_file(const create_file_request &req,
                                         dsn::task_code code,
                                         const create_file_callback &cb,
                                         clientlet *tracker)
{
    task_ptr tsk = tasking::create_late_task(code, cb, 0, tracker);
    if (req.ignore_metadata) {
        create_file_response resp;
        resp.err = ERR_OK;
        resp.file_handle = new local_file_object(
            this, ::dsn::utils::filesystem::path_combine(_root, req.file_name));
        call_safe_late_task(tsk, resp);
        return tsk;
    }

    auto create_file_background = [this, req, tsk]() {
        std::string file_path = ::dsn::utils::filesystem::path_combine(_root, req.file_name);
        create_file_response resp;
        resp.err = ERR_OK;

        if (::dsn::utils::filesystem::file_exists(file_path)) {
            ddebug("file: %s already exist", file_path.c_str());
            resp.file_handle = new local_file_object(this, file_path);
        } else {
            ddebug("start create file, file = %s", file_path.c_str());
            if (!::dsn::utils::filesystem::create_file(file_path)) {
                derror("create file: %s fail", file_path.c_str());
                resp.err = ERR_FS_INTERNAL;
            } else {
                resp.file_handle = new local_file_object(this, file_path);
                ddebug("create file succeed, file = %s", resp.file_handle->file_name().c_str());
            }
        }

        call_safe_late_task(tsk, resp);
    };

    tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(create_file_background));
    return tsk;
}

dsn::task_ptr local_service::delete_file(const delete_file_request &req,
                                         dsn::task_code code,
                                         const delete_file_callback &cb,
                                         clientlet *tracker)
{
    task_ptr tsk = tasking::create_late_task(code, cb, 0, tracker);

    auto delete_file_background = [this, req, tsk]() {
        delete_file_response resp;
        resp.err = ERR_OK;
        ddebug("delete file(%s)", req.file_name.c_str());
        std::string file = ::dsn::utils::filesystem::path_combine(_root, req.file_name);

        if (::dsn::utils::filesystem::file_exists(file)) {
            if (!::dsn::utils::filesystem::remove_path(file)) {
                resp.err = ERR_FS_INTERNAL;
            }
        } else {
            resp.err = ERR_OBJECT_NOT_FOUND;
        }

        call_safe_late_task(tsk, resp);
    };

    tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(delete_file_background));
    return tsk;
}

dsn::task_ptr local_service::exist(const exist_request &req,
                                   dsn::task_code code,
                                   const exist_callback &cb,
                                   clientlet *tracker)
{
    task_ptr tsk = tasking::create_late_task(code, cb, 0, tracker);

    auto exist_background = [this, req, tsk]() {
        exist_response resp;
        if (utils::filesystem::path_exists(req.path)) {
            resp.err = ERR_OK;
        } else {
            resp.err = ERR_OBJECT_NOT_FOUND;
        }
        call_safe_late_task(tsk, resp);
    };

    tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(exist_background));
    return tsk;
}

dsn::task_ptr local_service::remove_path(const remove_path_request &req,
                                         dsn::task_code code,
                                         const remove_path_callback &cb,
                                         clientlet *tracker)
{
    task_ptr tsk = tasking::create_late_task(code, cb, 0, tracker);

    auto remove_path_background = [this, req, tsk]() {
        remove_path_response resp;
        resp.err = ERR_OK;

        bool is_need_truly_remove = true;

        if (utils::filesystem::directory_exists(req.path)) {
            auto res = utils::filesystem::is_directory_empty(req.path);
            if (res.first == ERR_OK) {
                // directory is not empty & recursive = false
                if (!res.second && !req.recursive) {
                    resp.err = ERR_DIR_NOT_EMPTY;
                    is_need_truly_remove = false;
                }
            } else {
                resp.err = ERR_FS_INTERNAL;
                is_need_truly_remove = false;
            }
        } else if (!utils::filesystem::file_exists(req.path)) {
            resp.err = ERR_OBJECT_NOT_FOUND;
            is_need_truly_remove = false;
        }

        if (is_need_truly_remove) {
            if (!utils::filesystem::remove_path(req.path)) {
                resp.err = ERR_FS_INTERNAL;
            }
        }

        call_safe_late_task(tsk, resp);
    };

    tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(remove_path_background));
    return tsk;
}

// local_file_object
local_file_object::local_file_object(local_service *local_svc, const std::string &name)
    : block_file(name), _local_service(local_svc), _md5_value()
{
    _md5_value = compute_md5();
}

local_file_object::~local_file_object()
{
    // do nothing
}

const std::string &local_file_object::get_md5sum() { return _md5_value; }

uint64_t local_file_object::get_size()
{
    if (!::dsn::utils::filesystem::file_exists(file_name())) {
        return 0;
    } else {
        int64_t size = 0;
        ::dsn::utils::filesystem::file_size(file_name(), size);
        return static_cast<uint64_t>(size);
    }
}

dsn::task_ptr local_file_object::write(const write_request &req,
                                       dsn::task_code code,
                                       const write_callback &cb,
                                       clientlet *tracker)
{
    add_ref();

    task_ptr tsk = tasking::create_late_task(code, cb, 0, tracker);

    auto write_background = [this, req, tsk]() {
        write_response resp;
        resp.err = ERR_OK;
        if (!::dsn::utils::filesystem::file_exists(file_name())) {
            if (!::dsn::utils::filesystem::create_file(file_name())) {
                resp.err = ERR_FS_INTERNAL;
            }
        }

        if (resp.err == ERR_OK) {
            ddebug("start write file, file = %s", file_name().c_str());

            std::ofstream fout(file_name(), std::ifstream::out | std::ifstream::trunc);
            if (!fout.is_open()) {
                resp.err = ERR_FS_INTERNAL;
            } else {
                fout.write(req.buffer.data(), req.buffer.length());
                resp.written_size = req.buffer.length();
                fout.close();
                _md5_value = compute_md5();
            }
        }
        call_safe_late_task(tsk, resp);
        release_ref();
    };
    ::dsn::tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(write_background));
    return tsk;
}

dsn::task_ptr local_file_object::read(const read_request &req,
                                      dsn::task_code code,
                                      const read_callback &cb,
                                      clientlet *tracker)
{
    add_ref();

    task_ptr tsk = tasking::create_late_task(code, cb, 0, tracker);

    auto read_func = [this, req, tsk]() {
        read_response resp;
        resp.err = ERR_OK;
        if (!::dsn::utils::filesystem::file_exists(file_name())) {
            resp.err = ERR_OBJECT_NOT_FOUND;
        } else {
            ddebug("start read file, file = %s", file_name().c_str());
            // just read the whole file
            int64_t file_sz = 0;
            if (!::dsn::utils::filesystem::file_size(file_name(), file_sz)) {
                dassert(false, "get file size failed, file = %s", file_name().c_str());
                resp.err = ERR_FILE_OPERATION_FAILED;
            }
            int64_t total_sz = 0;
            if (req.remote_length == -1 || req.remote_length > file_sz) {
                total_sz = file_sz;
            } else {
                total_sz = req.remote_length;
            }

            ddebug("read file(%s), size = %ld", file_name().c_str(), total_sz);
            std::shared_ptr<char> buf = std::shared_ptr<char>(new char[total_sz + 1]);
            std::ifstream fin(file_name(), std::ifstream::in);
            if (!fin.is_open()) {
                resp.err = ERR_FS_INTERNAL;
            } else {
                fin.seekg(static_cast<int64_t>(req.remote_pos), fin.beg);
                fin.read(buf.get(), total_sz);
                buf.get()[fin.gcount()] = '\0';
                ddebug("read func, read value = %s", buf.get());
                resp.buffer.assign(std::move(buf), 0, fin.gcount());
            }
            fin.close();
        }

        call_safe_late_task(tsk, resp);
        release_ref();
    };
    ::dsn::tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(read_func));
    return tsk;
}

dsn::task_ptr local_file_object::upload(const upload_request &req,
                                        dsn::task_code code,
                                        const upload_callback &cb,
                                        clientlet *tracker)
{
    add_ref();
    task_ptr tsk = tasking::create_late_task(code, cb, 0, tracker);
    auto upload_file_func = [this, req, tsk]() {
        upload_response resp;
        resp.err = ERR_OK;
        if (!::dsn::utils::filesystem::file_exists(file_name())) {
            if (!::dsn::utils::filesystem::create_file(file_name())) {
                resp.err = ERR_FS_INTERNAL;
            }
        }
        if (resp.err == ERR_OK) {
            ddebug("start upload file, src = %s, des = %s",
                   req.input_local_name.c_str(),
                   file_name().c_str());
            std::ifstream fin(req.input_local_name, std::ifstream::in);
            std::ofstream fout(file_name(), std::ifstream::out | std::ifstream::trunc);
            if (!fin.is_open() || !fout.is_open()) {
                if (fin) {
                    resp.err = ERR_FS_INTERNAL;
                    fin.close();
                }
                if (fout) {
                    resp.err = ERR_FILE_OPERATION_FAILED;
                    fout.close();
                }
            }
            if (resp.err == ERR_OK) {
                ddebug("start to transfer the file, src_file = %s, des_file = %s",
                       req.input_local_name.c_str(),
                       file_name().c_str());
                int64_t total_sz = 0;
                char buf[max_length] = {'\0'};
                while (!fin.eof()) {
                    fin.read(buf, max_length);
                    total_sz += fin.gcount();
                    fout.write(buf, fin.gcount());
                }
                ddebug("finish upload file, file = %s, total_size = %d",
                       file_name().c_str(),
                       total_sz);
                fout.close();
                fin.close();
                resp.uploaded_size = static_cast<uint64_t>(total_sz);
                _md5_value = compute_md5();
            }
        }
        call_safe_late_task(tsk, resp);
        ddebug("%s: start to release_ref in upload file", file_name().c_str());
        release_ref();
    };
    // push this task to thread_pool, make it work
    ::dsn::tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(upload_file_func));

    return tsk;
}

dsn::task_ptr local_file_object::download(const download_request &req,
                                          dsn::task_code code,
                                          const download_callback &cb,
                                          clientlet *tracker)
{
    // download the whole file
    add_ref();
    task_ptr tsk = tasking::create_late_task(code, cb, 0, tracker);
    auto download_file_func = [this, req, tsk]() {
        download_response resp;
        resp.err = ERR_OK;
        std::string target_file = req.output_local_name;
        if (target_file.empty()) {
            derror("%s: download file failed, because output file is invalid", file_name().c_str());
            resp.err = ERR_INVALID_PARAMETERS;
        }

        std::ifstream fin(file_name(), std::ifstream::in);
        std::ofstream fout(target_file, std::ifstream::out | std::ifstream::trunc);
        if (!fin.is_open() || !fout.is_open()) {
            if (fin) {
                resp.err = ERR_FILE_OPERATION_FAILED;
                fin.close();
            }
            if (fout) {
                resp.err = ERR_FS_INTERNAL;
                fout.close();
            }
        }
        if (resp.err == ERR_OK) {
            ddebug("start to transfer the file, src_file = %s, des_file = %s",
                   file_name().c_str(),
                   target_file.c_str());
            int64_t total_sz = 0;
            char buf[max_length] = {'\0'};
            while (!fin.eof()) {
                fin.read(buf, max_length);
                total_sz += fin.gcount();
                fout.write(buf, fin.gcount());
            }
            ddebug(
                "finish download file, file = %s, total_size = %d", target_file.c_str(), total_sz);
            fout.close();
            fin.close();
            resp.downloaded_size = static_cast<uint64_t>(total_sz);
        }

        call_safe_late_task(tsk, resp);
        release_ref();
    };
    ::dsn::tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(download_file_func));

    return tsk;
}

std::string local_file_object::compute_md5()
{
    std::string result;
    if (::dsn::utils::filesystem::file_exists(file_name())) {
        auto err = ::dsn::utils::filesystem::md5sum(file_name(), result);
        dassert(err == ERR_OK, "local file object calculate md5 failed");
    }
    return result;
}
}
}
}
