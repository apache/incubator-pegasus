#include <memory>

#include <dsn/utility/filesystem.h>
#include <dsn/utility/error_code.h>
#include <dsn/utility/defer.h>
#include <dsn/utility/utils.h>
#include <dsn/utility/strings.h>
#include <dsn/utility/safe_strerror_posix.h>

#include <dsn/cpp/json_helper.h>
#include <dsn/tool-api/task_tracker.h>
#include "local_service.h"

static const int max_length = 2048; // max data length read from file each time

namespace dsn {
namespace dist {
namespace block_service {

DEFINE_THREAD_POOL_CODE(THREAD_POOL_LOCAL_SERVICE)
DEFINE_TASK_CODE(LPC_LOCAL_SERVICE_CALL, TASK_PRIORITY_COMMON, THREAD_POOL_LOCAL_SERVICE)

struct file_metadata
{
    uint64_t size;
    std::string md5;
    DEFINE_JSON_SERIALIZATION(size, md5)
};

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
        ddebug("local block service initialize succeed with root(%s)", _root.c_str());
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

            dinfo("file(%s) already exist", file_path.c_str());
            resp.err = f->load_metadata();
        }

        if (ERR_OK == resp.err)
            resp.file_handle = f;

        tsk->enqueue_with(resp);
    };

    tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(create_file_background));
    return tsk;
}

dsn::task_ptr local_service::delete_file(const delete_file_request &req,
                                         dsn::task_code code,
                                         const delete_file_callback &cb,
                                         task_tracker *tracker)
{
    delete_file_future_ptr tsk(new delete_file_future(code, cb, 0));
    tsk->set_tracker(tracker);

    auto delete_file_background = [this, req, tsk]() {
        delete_file_response resp;
        resp.err = ERR_OK;
        dinfo("delete file(%s)", req.file_name.c_str());

        // delete the meta data file.
        std::string meta_file = utils::filesystem::path_combine(_root, get_metafile(req.file_name));
        if (utils::filesystem::file_exists(meta_file)) {
            if (!utils::filesystem::remove_path(meta_file)) {
                resp.err = ERR_FS_INTERNAL;
            }
        }

        // if "delete meta data file ok" or "meta data file not found", then delete the real file
        if (resp.err == ERR_OK) {
            std::string file = utils::filesystem::path_combine(_root, req.file_name);
            if (::dsn::utils::filesystem::file_exists(file)) {
                if (!::dsn::utils::filesystem::remove_path(file)) {
                    resp.err = ERR_FS_INTERNAL;
                }
            } else {
                resp.err = ERR_OBJECT_NOT_FOUND;
            }
        }

        tsk->enqueue_with(resp);
    };

    tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(delete_file_background));
    return tsk;
}

dsn::task_ptr local_service::exist(const exist_request &req,
                                   dsn::task_code code,
                                   const exist_callback &cb,
                                   task_tracker *tracker)
{
    exist_future_ptr tsk(new exist_future(code, cb, 0));
    tsk->set_tracker(tracker);
    auto exist_background = [this, req, tsk]() {
        exist_response resp;
        std::string meta_file = utils::filesystem::path_combine(_root, get_metafile(req.path));
        if (utils::filesystem::path_exists(meta_file)) {
            resp.err = ERR_OK;
        } else {
            resp.err = ERR_OBJECT_NOT_FOUND;
        }
        tsk->enqueue_with(resp);
    };

    tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(exist_background));
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
    if (_has_meta_synced)
        return ERR_OK;

    std::string metadata_path = local_service::get_metafile(file_name());
    std::ifstream is(metadata_path, std::ios::in);
    if (!is.is_open()) {
        dwarn("load meta data from %s failed, err = %s", utils::safe_strerror(errno).c_str());
        return ERR_FS_INTERNAL;
    }
    auto cleanup = dsn::defer([&is]() { is.close(); });

    std::string data;
    is >> data;

    file_metadata meta;
    bool ans = dsn::json::json_forwarder<file_metadata>::decode(
        dsn::blob(data.c_str(), 0, data.size()), meta);
    if (!ans) {
        dwarn("decode meta data from json(%s) failed", data.c_str());
        return ERR_FS_INTERNAL;
    }
    _size = meta.size;
    _md5_value = meta.md5;
    _has_meta_synced = true;
    return ERR_OK;
}

error_code local_file_object::store_metadata()
{
    file_metadata meta;
    meta.md5 = _md5_value;
    meta.size = _size;

    std::string metadata_path = local_service::get_metafile(file_name());
    std::ofstream os(metadata_path, std::ios::out | std::ios::trunc);
    if (!os.is_open()) {
        dwarn("store to metadata file %s failed, err=%s",
              metadata_path.c_str(),
              utils::safe_strerror(errno).c_str());
        return ERR_FS_INTERNAL;
    }
    auto cleanup = dsn::defer([&os]() { os.close(); });
    dsn::json::json_forwarder<file_metadata>::encode(os, meta);

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

    auto write_background = [this, req, tsk]() {
        write_response resp;
        resp.err = ERR_OK;
        if (!::dsn::utils::filesystem::file_exists(file_name())) {
            if (!::dsn::utils::filesystem::create_file(file_name())) {
                resp.err = ERR_FS_INTERNAL;
            }
        }

        if (resp.err == ERR_OK) {
            dinfo("start write file, file = %s", file_name().c_str());

            std::ofstream fout(file_name(), std::ifstream::out | std::ifstream::trunc);
            if (!fout.is_open()) {
                resp.err = ERR_FS_INTERNAL;
            } else {
                fout.write(req.buffer.data(), req.buffer.length());
                resp.written_size = req.buffer.length();
                fout.close();

                // Currently we calc the meta data from source data, which save the io bandwidth
                // a lot, but it is somewhat not correct.
                _size = resp.written_size;
                _md5_value = utils::string_md5(req.buffer.data(), req.buffer.length());
                _has_meta_synced = true;

                store_metadata();
            }
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
        resp.err = ERR_OK;
        if (!utils::filesystem::file_exists(file_name()) ||
            !utils::filesystem::file_exists(local_service::get_metafile(file_name()))) {
            resp.err = ERR_OBJECT_NOT_FOUND;
        } else {
            if ((resp.err = load_metadata()) != ERR_OK) {
                dwarn("load meta data of %s failed", file_name().c_str());
            } else {
                int64_t file_sz = _size;
                int64_t total_sz = 0;
                if (req.remote_length == -1 || req.remote_length + req.remote_pos > file_sz) {
                    total_sz = file_sz - req.remote_pos;
                } else {
                    total_sz = req.remote_length;
                }

                dinfo("read file(%s), size = %ld", file_name().c_str(), total_sz);
                std::shared_ptr<char> buf = std::shared_ptr<char>(new char[total_sz + 1]);
                std::ifstream fin(file_name(), std::ifstream::in);
                if (!fin.is_open()) {
                    resp.err = ERR_FS_INTERNAL;
                } else {
                    fin.seekg(static_cast<int64_t>(req.remote_pos), fin.beg);
                    fin.read(buf.get(), total_sz);
                    buf.get()[fin.gcount()] = '\0';
                    resp.buffer.assign(std::move(buf), 0, fin.gcount());
                }
                fin.close();
            }
        }

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
        upload_response resp;
        resp.err = ERR_OK;
        std::ifstream fin(req.input_local_name, std::ios_base::in);
        if (!fin.is_open()) {
            dwarn("open source file %s for read failed, err(%s)",
                  req.input_local_name.c_str(),
                  utils::safe_strerror(errno).c_str());
            resp.err = ERR_FILE_OPERATION_FAILED;
        }

        utils::filesystem::create_file(file_name());
        std::ofstream fout(file_name(), std::ios_base::out | std::ios_base::trunc);
        if (!fout.is_open()) {
            dwarn("open target file %s for write failed, err(%s)",
                  file_name().c_str(),
                  utils::safe_strerror(errno).c_str());
            resp.err = ERR_FS_INTERNAL;
        }

        if (resp.err == ERR_OK) {
            dinfo("start to transfer from src_file(%s) to des_file(%s)",
                  req.input_local_name.c_str(),
                  file_name().c_str());
            int64_t total_sz = 0;
            char buf[max_length] = {'\0'};
            while (!fin.eof()) {
                fin.read(buf, max_length);
                total_sz += fin.gcount();
                fout.write(buf, fin.gcount());
            }
            dinfo("finish upload file, file = %s, total_size = %d", file_name().c_str(), total_sz);
            fout.close();
            fin.close();

            resp.uploaded_size = static_cast<uint64_t>(total_sz);

            // calc the md5sum by source file for simplicity
            _size = total_sz;
            error_code res = utils::filesystem::md5sum(req.input_local_name, _md5_value);
            if (res == dsn::ERR_OK) {
                _has_meta_synced = true;
                store_metadata();
            } else {
                resp.err = ERR_FS_INTERNAL;
            }
        } else {
            if (fin.is_open())
                fin.close();
            if (fout.is_open())
                fout.close();
        }

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
        if (target_file.empty()) {
            derror("download %s failed, because target name(%s) is invalid",
                   file_name().c_str(),
                   target_file.c_str());
            resp.err = ERR_INVALID_PARAMETERS;
        }

        if (resp.err == ERR_OK && !_has_meta_synced) {
            if (!utils::filesystem::file_exists(file_name()) ||
                !utils::filesystem::file_exists(local_service::get_metafile(file_name()))) {
                resp.err = ERR_OBJECT_NOT_FOUND;
            }
        }

        if (resp.err == ERR_OK) {
            std::ifstream fin(file_name(), std::ifstream::in);
            if (!fin.is_open()) {
                derror("open block file(%s) failed, err(%s)",
                       file_name().c_str(),
                       utils::safe_strerror(errno).c_str());
                resp.err = ERR_FS_INTERNAL;
            }

            std::ofstream fout(target_file, std::ios_base::out | std::ios_base::trunc);
            if (!fout.is_open()) {
                if (fin.is_open())
                    fin.close();
                derror("open target file(%s) failed, err(%s)",
                       target_file.c_str(),
                       utils::safe_strerror(errno).c_str());
                resp.err = ERR_FILE_OPERATION_FAILED;
            }

            if (resp.err == ERR_OK) {
                dinfo("start to transfer, src_file(%s), des_file(%s)",
                      file_name().c_str(),
                      target_file.c_str());
                int64_t total_sz = 0;
                char buf[max_length] = {'\0'};
                while (!fin.eof()) {
                    fin.read(buf, max_length);
                    total_sz += fin.gcount();
                    fout.write(buf, fin.gcount());
                }
                dinfo("finish download file(%s), total_size = %d", target_file.c_str(), total_sz);
                fout.close();
                fin.close();
                resp.downloaded_size = static_cast<uint64_t>(total_sz);

                _size = total_sz;
                if ((resp.err = utils::filesystem::md5sum(target_file, _md5_value)) != ERR_OK) {
                    dwarn("download %s failed when calculate the md5sum of %s",
                          file_name().c_str(),
                          target_file.c_str());
                } else {
                    _has_meta_synced = true;
                }
            }
        }

        tsk->enqueue_with(resp);
        release_ref();
    };
    ::dsn::tasking::enqueue(LPC_LOCAL_SERVICE_CALL, nullptr, std::move(download_file_func));

    return tsk;
}
}
}
}
