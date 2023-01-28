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

#include "block_service/fds/fds_service.h"

#include <cstring>
#include <fstream>
#include <memory>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/scoped_ptr.hpp>
#include <fds_client_configuration.h>
#include <galaxy_fds_client.h>
#include <galaxy_fds_client_exception.h>
#include <model/delete_multi_objects_result.h>
#include <model/fds_object.h>
#include <model/fds_object_metadata.h>
#include <model/fds_object_listing.h>
#include <model/fds_object_summary.h>
#include <Poco/Net/HTTPResponse.h>

#include "utils/defer.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/safe_strerror_posix.h"
#include "utils/string_conv.h"
#include "utils/TokenBucket.h"

namespace dsn {
namespace dist {
namespace block_service {

DSN_DEFINE_uint32(replication, fds_write_limit_rate, 100, "write rate limit of fds(MB/s)");
DSN_TAG_VARIABLE(fds_write_limit_rate, FT_MUTABLE);

DSN_DEFINE_uint32(replication, fds_write_burst_size, 500, "write burst size of fds(MB)");
DSN_TAG_VARIABLE(fds_write_burst_size, FT_MUTABLE);

DSN_DEFINE_uint32(replication, fds_read_limit_rate, 100, "read rate limit of fds(MB/s)");
DSN_TAG_VARIABLE(fds_read_limit_rate, FT_MUTABLE);

DSN_DEFINE_uint32(replication, fds_read_batch_size, 100, "read batch size of fds(MB)");
DSN_TAG_VARIABLE(fds_read_batch_size, FT_MUTABLE);

class utils
{
public:
    /*
     * read data from inputstream 'istr' then write to outputstream 'ostr' piece by piece until
     * reach EOF, the size of each piece is specified by 'piece_size'
     */
    static size_t copy_stream(std::istream &istr, std::ostream &ostr, size_t piece_size);
    /*
     * normalize a absolute path to a valid fds object path:
     * 1. the output_path shouldn't start with /
     * 2. the path shoudn't contain "." or ".." or "//"
     * 3. if the path is a dir, the path should ends with "/"
     * 4. if the path is root("/"), then return an empty string
     */
    static std::string path_to_fds(const std::string &input, bool is_dir);
    /*
     * normalize a fds object path to a absolute path:
     * 1. the output_path starts with /
     * 2. all the postfix / are removed if the path marks a dir
     */
    static std::string path_from_fds(const std::string &input, bool is_dir);
};

/*static*/
size_t utils::copy_stream(std::istream &is, std::ostream &os, size_t piece_size)
{
    std::unique_ptr<char[]> buffer(new char[piece_size]);
    size_t length = 0;
    is.read(buffer.get(), piece_size);
    size_t got_length = is.gcount();
    while (got_length > 0) {
        length += got_length;
        os.write(buffer.get(), got_length);
        if (is && os) {
            is.read(buffer.get(), piece_size);
            got_length = is.gcount();
        } else
            got_length = 0;
    }
    return length;
}

/*static*/
std::string utils::path_to_fds(const std::string &input, bool is_dir)
{
    // TODO: handle the "." and ".." and "//"
    if (input.size() < 1 || input == "/")
        return std::string();
    std::string result;
    if (input.front() == '/')
        result = input.substr(1);
    else
        result = input;

    if (is_dir)
        result.push_back('/');
    return result;
}

/*static*/
std::string utils::path_from_fds(const std::string &input, bool /*is_dir*/)
{
    std::string result = input;
    if (!input.empty() && input.back() == '/')
        result.pop_back();
    return result;
}

DEFINE_TASK_CODE(LPC_FDS_CALL, TASK_PRIORITY_COMMON, THREAD_POOL_BLOCK_SERVICE)

const std::string fds_service::FILE_LENGTH_CUSTOM_KEY = "x-xiaomi-meta-content-length";
const std::string fds_service::FILE_MD5_KEY = "content-md5";

fds_service::fds_service()
{
    _write_token_bucket.reset(new folly::DynamicTokenBucket());
    _read_token_bucket.reset(new folly::DynamicTokenBucket());
}

fds_service::~fds_service() {}

/**
 * @brief fds_service::initialize
 * @param args: {httpServer, accessKey, secretKey, bucket}
 * @return
 */
error_code fds_service::initialize(const std::vector<std::string> &args)
{
    galaxy::fds::FDSClientConfiguration config;
    config.enableHttps(true);
    config.setEndpoint(args[0]);
    const std::string &access_key = args[1];
    const std::string &secret_key = args[2];

    _client.reset(new galaxy::fds::GalaxyFDSClient(access_key, secret_key, config));
    _bucket_name = args[3];
    return dsn::ERR_OK;
}

#define FDS_EXCEPTION_HANDLE(ERR_REFERENCE, OPERATION, INPUT_PARAMETER)                            \
    catch (const Poco::TimeoutException &ex)                                                       \
    {                                                                                              \
        LOG_ERROR("fds {} timeout: parameter({}), code({}), msg({})",                              \
                  OPERATION,                                                                       \
                  INPUT_PARAMETER,                                                                 \
                  ex.code(),                                                                       \
                  ex.message());                                                                   \
        ERR_REFERENCE = ERR_TIMEOUT;                                                               \
    }                                                                                              \
    catch (const Poco::Exception &ex)                                                              \
    {                                                                                              \
        LOG_ERROR("fds {} get poco exception: parameter({}), code({}), msg({}), what({})",         \
                  OPERATION,                                                                       \
                  INPUT_PARAMETER,                                                                 \
                  ex.code(),                                                                       \
                  ex.message(),                                                                    \
                  ex.what());                                                                      \
        ERR_REFERENCE = ERR_FS_INTERNAL;                                                           \
    }                                                                                              \
    catch (...)                                                                                    \
    {                                                                                              \
        LOG_ERROR("fds {} get unknown exception: parameter({})", OPERATION, INPUT_PARAMETER);      \
        ERR_REFERENCE = ERR_FS_INTERNAL;                                                           \
    }

dsn::task_ptr fds_service::list_dir(const ls_request &req,
                                    dsn::task_code code,
                                    const ls_callback &callback,
                                    dsn::task_tracker *tracker = nullptr)
{
    ls_future_ptr t(new ls_future(code, callback, 0));
    t->set_tracker(tracker);

    auto list_dir_in_background = [this, req, t]() {
        ls_response resp;
        std::string fds_path = utils::path_to_fds(req.dir_name, true);
        try {
            std::shared_ptr<galaxy::fds::FDSObjectListing> result =
                _client->listObjects(_bucket_name, fds_path);

            while (true) {
                const std::vector<galaxy::fds::FDSObjectSummary> &objs = result->objectSummaries();
                const std::vector<std::string> &common_prefix = result->commonPrefixes();
                resp.err = dsn::ERR_OK;

                // fds listing's objects are with full-path, we must extract the postfix to emulate
                // the filesystem structure
                for (const galaxy::fds::FDSObjectSummary &obj : objs) {
                    CHECK(fds_path.empty() || boost::starts_with(obj.objectName(), fds_path),
                          "invalid path({}) in parent({})",
                          obj.objectName(),
                          fds_path);
                    resp.entries->push_back(
                        {utils::path_from_fds(obj.objectName().substr(fds_path.size()), false),
                         false});
                }
                for (const std::string &s : common_prefix) {
                    CHECK(fds_path.empty() || boost::starts_with(s, fds_path),
                          "invalid path({}) in parent({})",
                          s,
                          fds_path);
                    resp.entries->push_back(
                        {utils::path_from_fds(s.substr(fds_path.size()), true), true});
                }

                // list result may be paged
                if (result->truncated()) {
                    auto res_temp = _client->listNextBatchOfObjects(*result);
                    result.swap(res_temp);
                } else {
                    break;
                }
            }
        } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
            LOG_ERROR("fds listObjects failed: parameter({}), code({}), msg({})",
                      req.dir_name,
                      ex.code(),
                      ex.what());
            resp.err = ERR_FS_INTERNAL;
        }
        FDS_EXCEPTION_HANDLE(resp.err, "listObject", req.dir_name.c_str())

        if (resp.err == dsn::ERR_OK && resp.entries->empty()) {
            try {
                if (_client->doesObjectExist(_bucket_name,
                                             utils::path_to_fds(req.dir_name, false))) {
                    LOG_ERROR("fds list_dir failed: path not dir, parameter({})", req.dir_name);
                    resp.err = ERR_INVALID_PARAMETERS;
                } else {
                    LOG_ERROR("fds list_dir failed: path not found, parameter({})", req.dir_name);
                    resp.err = ERR_OBJECT_NOT_FOUND;
                }
            } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
                LOG_ERROR("fds doesObjectExist failed: parameter({}), code({}), msg({})",
                          req.dir_name,
                          ex.code(),
                          ex.what());
                resp.err = ERR_FS_INTERNAL;
            }
            FDS_EXCEPTION_HANDLE(resp.err, "doesObjectExist", req.dir_name.c_str())
        }

        t->enqueue_with(resp);
    };

    dsn::tasking::enqueue(LPC_FDS_CALL, nullptr, list_dir_in_background);
    return t;
}

dsn::task_ptr fds_service::create_file(const create_file_request &req,
                                       dsn::task_code code,
                                       const create_file_callback &cb,
                                       dsn::task_tracker *tracker = nullptr)
{
    create_file_future_ptr t(new create_file_future(code, cb, 0));
    t->set_tracker(tracker);
    if (req.ignore_metadata) {
        create_file_response resp;
        resp.err = dsn::ERR_OK;
        resp.file_handle =
            new fds_file_object(this, req.file_name, utils::path_to_fds(req.file_name, false));
        t->enqueue_with(resp);
        return t;
    }

    auto create_file_in_background = [this, req, t]() {
        create_file_response resp;
        resp.err = ERR_IO_PENDING;
        std::string fds_path = utils::path_to_fds(req.file_name, false);

        dsn::ref_ptr<fds_file_object> f = new fds_file_object(this, req.file_name, fds_path);
        resp.err = f->get_file_meta();
        if (resp.err == ERR_OK || resp.err == ERR_OBJECT_NOT_FOUND) {
            resp.err = ERR_OK;
            resp.file_handle = f;
        }

        t->enqueue_with(resp);
    };

    dsn::tasking::enqueue(LPC_FDS_CALL, nullptr, create_file_in_background);
    return t;
}

dsn::task_ptr fds_service::remove_path(const remove_path_request &req,
                                       dsn::task_code code,
                                       const remove_path_callback &cb,
                                       dsn::task_tracker *tracker)
{
    remove_path_future_ptr callback(new remove_path_future(code, cb, 0));
    callback->set_tracker(tracker);
    auto remove_path_background = [this, req, callback]() {
        remove_path_response resp;
        resp.err = ERR_OK;
        std::string fds_path = utils::path_to_fds(req.path, true);
        bool should_remove_path = false;

        try {
            std::shared_ptr<galaxy::fds::FDSObjectListing> result =
                _client->listObjects(_bucket_name, fds_path);
            while (result->objectSummaries().size() <= 0 && result->commonPrefixes().size() <= 0 &&
                   result->truncated()) {
                result = _client->listNextBatchOfObjects(*result);
            }
            const std::vector<galaxy::fds::FDSObjectSummary> &objs = result->objectSummaries();
            const std::vector<std::string> &common_prefix = result->commonPrefixes();

            if (!objs.empty() || !common_prefix.empty()) {
                // path is non-empty directory
                if (req.recursive) {
                    should_remove_path = true;
                } else {
                    LOG_ERROR("fds remove_path failed: dir not empty, parameter({})", req.path);
                    resp.err = ERR_DIR_NOT_EMPTY;
                }
            } else {
                if (_client->doesObjectExist(_bucket_name, utils::path_to_fds(req.path, false))) {
                    should_remove_path = true;
                } else {
                    LOG_ERROR("fds remove_path failed: path not found, parameter({})", req.path);
                    resp.err = ERR_OBJECT_NOT_FOUND;
                }
            }
        } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
            LOG_ERROR("fds remove_path failed: parameter({}), code({}), msg({})",
                      req.path,
                      ex.code(),
                      ex.what());
            resp.err = ERR_FS_INTERNAL;
        }
        FDS_EXCEPTION_HANDLE(resp.err, "remove_path", req.path.c_str());

        if (resp.err == ERR_OK && should_remove_path) {
            fds_path = utils::path_to_fds(req.path, false);
            try {
                auto deleting = _client->deleteObjects(_bucket_name, fds_path, false);
                if (deleting->countFailedObjects() <= 0) {
                    resp.err = ERR_OK;
                } else {
                    LOG_ERROR("fds remove_path failed: countFailedObjects = {}, parameter({})",
                              deleting->countFailedObjects(),
                              req.path);
                    resp.err = ERR_FS_INTERNAL;
                }
            } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
                LOG_ERROR("fds remove_path failed: parameter({}), code({}), msg({})",
                          req.path,
                          ex.code(),
                          ex.what());
                resp.err = ERR_FS_INTERNAL;
            }
            FDS_EXCEPTION_HANDLE(resp.err, "remove_path", req.path.c_str());
        }

        callback->enqueue_with(resp);
        return;
    };

    dsn::tasking::enqueue(LPC_FDS_CALL, nullptr, remove_path_background);
    return callback;
}

fds_file_object::fds_file_object(fds_service *s,
                                 const std::string &name,
                                 const std::string &fds_path)
    : block_file(name),
      _service(s),
      _fds_path(fds_path),
      _md5sum(""),
      _size(0),
      _has_meta_synced(false)
{
}

fds_file_object::~fds_file_object() {}

error_code fds_file_object::get_file_meta()
{
    error_code err = ERR_OK;
    galaxy::fds::GalaxyFDSClient *c = _service->get_client();
    try {
        auto meta = c->getObjectMetadata(_service->get_bucket_name(), _fds_path)->metadata();

        // get file length
        auto iter = meta.find(fds_service::FILE_LENGTH_CUSTOM_KEY);
        CHECK(iter != meta.end(),
              "can't find {} in object({})'s metadata",
              fds_service::FILE_LENGTH_CUSTOM_KEY,
              _fds_path);
        bool valid = dsn::buf2uint64(iter->second, _size);
        CHECK(valid, "error to get file size");

        // get md5 key
        iter = meta.find(fds_service::FILE_MD5_KEY);
        CHECK(iter != meta.end(),
              "can't find {} in object({})'s metadata",
              fds_service::FILE_MD5_KEY,
              _fds_path);
        _md5sum = iter->second;

        _has_meta_synced = true;
    } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
        if (ex.code() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND) {
            err = ERR_OBJECT_NOT_FOUND;
        } else {
            LOG_ERROR("fds getObjectMetadata failed: parameter({}), code({}), msg({})",
                      _name.c_str(),
                      ex.code(),
                      ex.what());
            err = ERR_FS_INTERNAL;
        }
    }
    FDS_EXCEPTION_HANDLE(err, "getObjectMetadata", _fds_path.c_str());
    return err;
}

error_code fds_file_object::get_content_in_batches(uint64_t start,
                                                   int64_t length,
                                                   /*out*/ std::ostream &os,
                                                   /*out*/ uint64_t &transfered_bytes)
{
    error_code err = ERR_OK;
    transfered_bytes = 0;

    // get file meta if it is not synced
    if (!_has_meta_synced) {
        err = get_file_meta();
        if (ERR_OK != err) {
            return err;
        }
    }

    // if length = -1, it means we should transfer the whole file
    uint64_t to_transfer_bytes = (length == -1 ? _size : length);

    uint64_t pos = start;
    uint64_t once_transfered_bytes = 0;
    while (pos < start + to_transfer_bytes) {
        const uint64_t BATCH_SIZE = FLAGS_fds_read_batch_size << 20;
        uint64_t batch_size = std::min(BATCH_SIZE, start + to_transfer_bytes - pos);

        // burst size should not be less than consume size
        const uint64_t rate = FLAGS_fds_read_limit_rate << 20;
        _service->_read_token_bucket->consumeWithBorrowAndWait(
            batch_size, rate, std::max(2 * rate, batch_size));

        err = get_content(pos, batch_size, os, once_transfered_bytes);
        transfered_bytes += once_transfered_bytes;
        if (err != ERR_OK || once_transfered_bytes < batch_size) {
            return err;
        }
        pos += batch_size;
    }

    return ERR_OK;
}

error_code fds_file_object::get_content(uint64_t pos,
                                        uint64_t length,
                                        /*out*/ std::ostream &os,
                                        /*out*/ uint64_t &transfered_bytes)
{
    error_code err = ERR_OK;
    transfered_bytes = 0;
    while (true) {
        // if we have download enough or we have reach the end
        if (transfered_bytes >= length || transfered_bytes + pos >= _size) {
            return ERR_OK;
        }

        try {
            galaxy::fds::GalaxyFDSClient *c = _service->get_client();
            std::shared_ptr<galaxy::fds::FDSObject> obj;
            obj = c->getObject(_service->get_bucket_name(),
                               _fds_path,
                               pos + transfered_bytes,
                               length - transfered_bytes);
            LOG_DEBUG("get object from fds succeed, remote_file({})", _fds_path);
            std::istream &is = obj->objectContent();
            transfered_bytes += utils::copy_stream(is, os, PIECE_SIZE);
            err = ERR_OK;
        } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
            LOG_ERROR("fds getObject error: remote_file({}), code({}), msg({})",
                      file_name(),
                      ex.code(),
                      ex.what());
            if (ex.code() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND) {
                _has_meta_synced = true;
                _md5sum = "";
                _size = 0;
                err = ERR_OBJECT_NOT_FOUND;
            } else {
                err = ERR_FS_INTERNAL;
            }
        }
        FDS_EXCEPTION_HANDLE(err, "getObject", file_name().c_str())

        if (err != ERR_OK) {
            return err;
        }
    }
}

error_code fds_file_object::put_content(/*in-out*/ std::istream &is,
                                        int64_t to_transfer_bytes,
                                        uint64_t &transfered_bytes)
{
    error_code err = ERR_OK;
    transfered_bytes = 0;
    galaxy::fds::GalaxyFDSClient *c = _service->get_client();

    // get tokens from token bucket
    if (!_service->_write_token_bucket->consumeWithBorrowAndWait(to_transfer_bytes,
                                                                 FLAGS_fds_write_limit_rate << 20,
                                                                 FLAGS_fds_write_burst_size
                                                                     << 20)) {
        LOG_INFO("the transfer count({}B) is greater than burst size({}MB), so it is rejected by "
                 "token bucket",
                 to_transfer_bytes,
                 FLAGS_fds_write_burst_size);
        return ERR_BUSY;
    }

    try {
        c->putObject(_service->get_bucket_name(), _fds_path, is, galaxy::fds::FDSObjectMetadata());
    } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
        LOG_ERROR("fds putObject error: remote_file({}), code({}), msg({})",
                  file_name(),
                  ex.code(),
                  ex.what());
        err = ERR_FS_INTERNAL;
    }
    FDS_EXCEPTION_HANDLE(err, "putObject", file_name().c_str())

    if (err != ERR_OK) {
        return err;
    }

    LOG_INFO("start to synchronize meta data after successfully wrote data to fds");
    err = get_file_meta();
    if (err == ERR_OK) {
        transfered_bytes = _size;
    }
    return err;
}

dsn::task_ptr fds_file_object::write(const write_request &req,
                                     dsn::task_code code,
                                     const write_callback &cb,
                                     dsn::task_tracker *tracker = nullptr)
{
    write_future_ptr t(new write_future(code, cb, 0));
    t->set_tracker(tracker);

    add_ref();
    auto write_in_background = [this, req, t]() {
        write_response resp;
        std::istringstream is;
        is.str(std::string(req.buffer.data(), req.buffer.length()));
        resp.err = put_content(is, req.buffer.length(), resp.written_size);

        t->enqueue_with(resp);
        release_ref();
    };

    dsn::tasking::enqueue(LPC_FDS_CALL, nullptr, write_in_background);
    return t;
}

// TODO: handle the localfile path
dsn::task_ptr fds_file_object::upload(const upload_request &req,
                                      dsn::task_code code,
                                      const upload_callback &cb,
                                      dsn::task_tracker *tracker = nullptr)
{
    upload_future_ptr t(new upload_future(code, cb, 0));
    t->set_tracker(tracker);

    add_ref();
    auto upload_background = [this, req, t]() {
        const std::string &local_file = req.input_local_name;
        // get file size
        int64_t file_sz = 0;
        dsn::utils::filesystem::file_size(local_file, file_sz);

        upload_response resp;
        // TODO: we can cache the whole file in buffer, then upload the buffer rather than the
        // ifstream, because if ifstream read file beyond 60s, fds-server will reset the session,
        // then upload will fail with error broken-pipe
        std::ifstream is(local_file, std::ios::binary | std::ios::in);

        if (!is.is_open()) {
            LOG_ERROR("fds upload failed: open local file({}) failed when upload to({}), error({})",
                      local_file,
                      file_name(),
                      ::dsn::utils::safe_strerror(errno));
            resp.err = dsn::ERR_FILE_OPERATION_FAILED;
        } else {
            resp.err = put_content(is, file_sz, resp.uploaded_size);
            is.close();
        }

        t->enqueue_with(resp);
        release_ref();
    };

    dsn::tasking::enqueue(LPC_FDS_CALL, nullptr, upload_background);
    return t;
}

dsn::task_ptr fds_file_object::read(const read_request &req,
                                    dsn::task_code code,
                                    const read_callback &cb,
                                    dsn::task_tracker *tracker = nullptr)
{
    read_future_ptr t(new read_future(code, cb, 0));
    t->set_tracker(tracker);

    add_ref();
    auto read_in_background = [this, req, t]() {
        read_response resp;
        std::ostringstream os;
        uint64_t transferd_size;
        resp.err = get_content_in_batches(req.remote_pos, req.remote_length, os, transferd_size);
        if (os.tellp() > 0) {
            std::string *output = new std::string();
            *output = os.str();
            std::shared_ptr<char> ptr((char *)output->c_str(), [output](char *) { delete output; });
            resp.buffer.assign(std::move(ptr), 0, output->length());
        }
        t->enqueue_with(resp);
        release_ref();
    };

    dsn::tasking::enqueue(LPC_FDS_CALL, nullptr, read_in_background);
    return t;
}

// TODO: handle the localfile path
dsn::task_ptr fds_file_object::download(const download_request &req,
                                        dsn::task_code code,
                                        const download_callback &cb,
                                        dsn::task_tracker *tracker = nullptr)
{
    download_future_ptr t(new download_future(code, cb, 0));
    t->set_tracker(tracker);
    download_response resp;

    std::shared_ptr<std::ofstream> handle(new std::ofstream(
        req.output_local_name, std::ios::binary | std::ios::out | std::ios::trunc));
    if (!handle->is_open()) {
        LOG_ERROR("fds download failed: fail to open localfile({}) when download({}), error({})",
                  req.output_local_name,
                  _fds_path,
                  ::dsn::utils::safe_strerror(errno));
        resp.err = ERR_FILE_OPERATION_FAILED;
        resp.downloaded_size = 0;
        t->enqueue_with(resp);
        return t;
    }

    add_ref();
    auto download_background = [this, req, handle, t]() {
        download_response resp;
        uint64_t transfered_size;
        resp.err =
            get_content_in_batches(req.remote_pos, req.remote_length, *handle, transfered_size);
        resp.downloaded_size = 0;
        if (resp.err == ERR_OK && handle->tellp() != -1) {
            resp.downloaded_size = handle->tellp();
        }
        handle->close();
        if (resp.err != ERR_OK && dsn::utils::filesystem::file_exists(req.output_local_name)) {
            LOG_ERROR("fail to download file {} from fds, remove localfile {}",
                      _fds_path,
                      req.output_local_name);
            dsn::utils::filesystem::remove_path(req.output_local_name);
        } else if ((resp.err = dsn::utils::filesystem::md5sum(req.output_local_name,
                                                              resp.file_md5)) != ERR_OK) {
            LOG_ERROR("download failed when calculate the md5sum of local file {}",
                      req.output_local_name);
        }
        t->enqueue_with(resp);
        release_ref();
    };

    dsn::tasking::enqueue(LPC_FDS_CALL, nullptr, download_background);
    return t;
}
} // namespace block_service
} // namespace dist
} // namespace dsn
