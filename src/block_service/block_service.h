/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include "runtime/task/task_code.h"
#include "runtime/task/task_tracker.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "runtime/rpc/rpc_address.h"
#include "common/replication_other_types.h"
#include "common/replication.codes.h"
#include <functional>

namespace dsn {

namespace dist {
namespace block_service {

DEFINE_THREAD_POOL_CODE(THREAD_POOL_BLOCK_SERVICE)

class block_file;
typedef dsn::ref_ptr<block_file> block_file_ptr;

/**
 * @brief The ls_request struct, use to list all the files and directories under the dir_name
 * dir_name: a valid absolute path string, which "/" as splitter.We don't support relative path
 */
struct ls_request
{
    std::string dir_name;
};

/**
 * @brief The ls_entry struct
 *  entry_name: an entry name which doesn't contain the directory preceding
 */
struct ls_entry
{
    std::string entry_name;
    bool is_directory;
};

/**
 * @brief The ls_response struct
 *  err: ERR_OK means the ls request is responsed succeed. Then
 *          user can view all entries by entries.
 *       ERR_OBJECT_NOT_FOUND: can't find the dir by ls_request.dir_name
 *       ERR_INVALID_PARAMETERS: the ls_request.dir_name is not a dir
 *       ERR_TIMEOUT: request timeout
 *       ERR_FS_INTERNAL: an internal error occured in the service implementation
 *          which we can't handle
 */
struct ls_response
{
    dsn::error_code err;
    // use shared_ptr to avoid extra memory copy
    std::shared_ptr<std::vector<ls_entry>> entries;
    ls_response() : entries(std::make_shared<std::vector<ls_entry>>()) {}
};
typedef std::function<void(const ls_response &)> ls_callback;
typedef future_task<ls_response> ls_future;
typedef dsn::ref_ptr<ls_future> ls_future_ptr;

/**
 * @brief The create_file_request struct, used to create a block_file_ptr
 *  file_name: a valid absolute path string, which "/" as splitter.
 *             We don't support relative path.
 *  ignore_metadata: With the flag set, implementation is not necessary to pre-fetch the
 *                   metadata(size, md5sum..) when create the handle. This is useful in
 *                   cases that users don't care the current content of the file, and they
 *                   simply need the handle and do operations. An implementation can do
 *                   some optimization according to this options.
 */
struct create_file_request
{
    std::string file_name;
    bool ignore_metadata;
};

/**
 * @brief The create_file_response struct
 *  err: ERR_OK: the file handle is successfully created.
 *       ERR_TIMEOUT: request timeout
 *       ERR_FS_INTERNAL: an internal error occured in the service implementation
 *          which we can't handle
 *  file_handle: the file_handle will not be null if err is ERR_OK.
 *               user can read/write the file by the handle, and get the metata(size, md5..).
 *               please ref {@link #block_file::get_size}, {@link #block_file::get_md5sum} for
 *               the return value of metadata
 */
struct create_file_response
{
    dsn::error_code err;
    block_file_ptr file_handle;
};
typedef std::function<void(const create_file_response &)> create_file_callback;
typedef future_task<create_file_response> create_file_future;
typedef dsn::ref_ptr<create_file_future> create_file_future_ptr;

/**
 * @brief The remove_path_request struct
 *  path: a valid absolute path string, which point to file or directory, which "/" as splitter
 *  recursive: if path point to a non-empty directory, and if recursive = true, then all the
 *             files or dirs under the path will be removed; if recursive = false, then the
 *             non-empty directory will not be removed.
 *             if path point to an empty directory or file, just remove the path.
 */
struct remove_path_request
{
    std::string path;
    bool recursive;
};

/**
 * @brief The remove_path_response struct
 *  err:  ERR_OK: request succeed, and the remove path succeed
 *        ERR_OBJECT_NOT_FOUND: request succeed, but the path do not exist
 *        ERR_TIMEOUT: request timeout
 *        ERR_DIR_NOT_EMPTY: the directory is non-empty, can't be removed
 *        ERR_FS_INTERNAL:  remove directory failed, need check it again
 */
struct remove_path_response
{
    dsn::error_code err;
};
typedef std::function<void(const remove_path_response &)> remove_path_callback;
typedef future_task<remove_path_response> remove_path_future;
typedef dsn::ref_ptr<remove_path_future> remove_path_future_ptr;

/**
 * @brief The read_request struct
 *  remote_pos: where of the file to start read
 *  remote_length: the amount of bytes to read.
 *                 if set -1, means read to the end of the file
 */
struct read_request
{
    uint64_t remote_pos;
    int64_t remote_length;
};

/**
 * @brief The read_response struct
 *  err: ERR_OK: read succeed
 *       ERR_OBJECT_NOT_FOUND: try to read an non-exist file.
 *          this happens when try to read a file handle which
 *          doesn't have a coressponding remote file
 *       ERR_TIMEOUT: request timeout
 *       ERR_FS_INTERNAL: an internal error occured in the service implementation
 *          which we can't handle
 *  buffer: the read data. The implementation can choose to return partially read data when
 *          error occured, or discard them and only return an empty buffer. But implementation
 *          should never return ERR_OK if partitial data got, otherwize the user can't tell
 *          whether partital data is transfered or reach the end of file with remote_length == -1.
 *          If ERR_OK returned but only partitial data got, means reach the end of file.
 */
struct read_response
{
    dsn::error_code err;
    dsn::blob buffer;
};
typedef std::function<void(const read_response &)> read_callback;
typedef future_task<read_response> read_future;
typedef dsn::ref_ptr<read_future> read_future_ptr;

/**
 * @brief The write_request struct
 *  buffer: the new content of the file. Returns ERR_OK if and only if all the data in buffer
 *          has been written into the file.
 *  Notice: we don't have the insert/append semantic for file, only truncate.
 */
struct write_request
{
    dsn::blob buffer;
};

/**
 * @brief The write_response struct
 *  err: ERR_OK: write succeed
 *       ERR_TIMEOUT: request timeout
 *       ERR_FS_INTERNAL: an internal error occured in the service implementation
 *          which we can't handle
 *  written_size: amount of bytes have been written.
 *
 * Notice: user can call get_size/get_md5sum to get the metadata of the file
 */
struct write_response
{
    dsn::error_code err;
    uint64_t written_size;
};
typedef std::function<void(const write_response &)> write_callback;
typedef future_task<write_response> write_future;
typedef dsn::ref_ptr<write_future> write_future_ptr;

/**
 * @brief The upload_request struct
 *  input_local_name: a local filesystem path, you can use a relative or absolute path.
 */
struct upload_request
{
    std::string input_local_name;
};

/**
 * @brief The upload_response struct
 *  similar to write_response with more errors in err:
 *     ERR_FILE_OPERATION_FAILED: open the local file for read failed.
 *
 * Notice: user can call get_size/get_md5sum to get the metadata of the file
 */
struct upload_response
{
    dsn::error_code err;
    uint64_t uploaded_size;
};
typedef std::function<void(const upload_response &)> upload_callback;
typedef future_task<upload_response> upload_future;
typedef dsn::ref_ptr<upload_future> upload_future_ptr;

/**
 * @brief The download_request struct
 *  output_local_file: a local filesystem path, you can use a relative or absolute path.
 */
struct download_request
{
    std::string output_local_name;
    uint64_t remote_pos;
    int64_t remote_length;
};
/**
 * @brief The download_response struct
 * similar to read_response. With more errors in err:
 *    ERR_FILE_OPERATION_FAILED: open output_local_name for write failed.
 *    if try to download a non-exist file and with an invalid output_local_name,
 *    it's up to implementation to return which error.
 */
struct download_response
{
    dsn::error_code err;
    uint64_t downloaded_size;
    std::string file_md5;
};
typedef std::function<void(const download_response &)> download_callback;
typedef future_task<download_response> download_future;
typedef dsn::ref_ptr<download_future> download_future_ptr;

class block_filesystem
{
public:
    template <typename T>
    static block_filesystem *create()
    {
        return new T();
    }

    typedef block_filesystem *(*factory)();
    block_filesystem() {}

    /**
     * @brief initialize
     * @param args, the implemented related parameter in initializing
     *        should be represented as strings and passed by args
     * @return ERR_OK if initialized succeed. If failed, return with the failed error.
     */
    virtual error_code initialize(const std::vector<std::string> &args) = 0;

    /**
     * @brief list_dir
     * @param req, ref {@link #ls_request}
     * @param code, a task_code, describe how the callback executed
     * @param callback, called when get the list result
     * @param tracker
     * @return a task which represent the async operation
     */
    virtual dsn::task_ptr list_dir(const ls_request &req,
                                   dsn::task_code code,
                                   const ls_callback &callback,
                                   dsn::task_tracker *tracker = nullptr) = 0;

    /**
     * @brief create_file
     * @param req, ref {@link #create_file_request}
     * @param code, a task_code, describe how the callback executed
     * @param callback, called when get the list result
     * @param tracker
     * @return a task which represent the async operation
     */
    virtual dsn::task_ptr create_file(const create_file_request &req,
                                      dsn::task_code code,
                                      const create_file_callback &cb,
                                      dsn::task_tracker *tracker = nullptr) = 0;

    /**
     * @brief remove_path
     * @param req, ref {@link #remove_path_request}
     * @param code, a task_code, describe how the callback executed
     * @param callback, called when get the list result
     * @param tracker
     * @return a task which represent the async operation
     */
    virtual dsn::task_ptr remove_path(const remove_path_request &req,
                                      dsn::task_code code,
                                      const remove_path_callback &cb,
                                      dsn::task_tracker *tracker = nullptr) = 0;

    virtual bool is_root_path_set() const { return false; }

    virtual ~block_filesystem() {}
};

class block_file : public dsn::ref_counter
{
public:
    block_file(const std::string &name) : _name(name) {}
    virtual ~block_file() {}
    const std::string &file_name() const { return _name; }

    /**
     * @brief get_size
     *    this api should never block, implementation should
     *    fetch the size in {@link block_filesystem::create_file}.
     *    if the block_file is created by "ignore_metadata" {@link create_file_request},
     *    should return 0
     * @return the file_size. If the file doesn't exist, should return 0
     */
    virtual uint64_t get_size() = 0;

    /**
     * @brief get_md5sum
     *    this api should never block, implementation should
     *    fetch the md5sum in {@link block_filesystem::create_file}.
     *    if the block_file is created by "ignore_metadata" {@link create_file_request},
     *    should return ""
     * @return the md5 value. If the file doesn't exist, should return "".
     *    NOTICE: if an existing file is empty(size == 0), the returning value is not "".
     *    user can use this feature to check if a file exist.
     */
    virtual const std::string &get_md5sum() = 0;

    /**
     * @brief write
     * @param req, ref {@link #write_request}
     * @param code, a task_code, describe how the callback executed
     * @param callback, called when get the list result
     * @param tracker
     * @return a task which represent the async operation
     */
    virtual dsn::task_ptr write(const write_request &req,
                                dsn::task_code code,
                                const write_callback &cb,
                                dsn::task_tracker *tracker = nullptr) = 0;

    /**
     * @brief read
     * @param req, ref {@link #read_request}
     * @param code, a task_code, describe how the callback executed
     * @param callback, called when get the list result
     * @param tracker
     * @return a task which represent the async operation
     */
    virtual dsn::task_ptr read(const read_request &req,
                               dsn::task_code code,
                               const read_callback &cb,
                               dsn::task_tracker *tracker = nullptr) = 0;

    /**
     * @brief upload
     * @param req, ref {@link #upload_request}
     * @param code, a task_code, describe how the callback executed
     * @param callback, called when get the list result
     * @param tracker
     * @return a task which represent the async operation
     */
    virtual dsn::task_ptr upload(const upload_request &req,
                                 dsn::task_code code,
                                 const upload_callback &cb,
                                 dsn::task_tracker *tracker = nullptr) = 0;

    /**
     * @brief download
     * @param req, ref {@link #download_request}
     * @param code, a task_code, describe how the callback executed
     * @param callback, called when get the list result
     * @param tracker
     * @return a task which represent the async operation
     */
    virtual dsn::task_ptr download(const download_request &req,
                                   dsn::task_code code,
                                   const download_callback &cb,
                                   dsn::task_tracker *tracker = nullptr) = 0;

protected:
    std::string _name;
};
}
}
}
