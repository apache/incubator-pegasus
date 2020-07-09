#include "fds_service.h"

#include <galaxy_fds_client.h>
#include <fds_client_configuration.h>
#include <galaxy_fds_client_exception.h>
#include <model/fds_object_metadata.h>
#include <model/fds_object.h>
#include <model/fds_object_summary.h>
#include <model/fds_object_listing.h>
#include <model/delete_multi_objects_result.h>
#include <dsn/utility/error_code.h>
#include <Poco/Net/HTTPResponse.h>

#include <boost/scoped_ptr.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <memory>
#include <fstream>
#include <string.h>
#include <dsn/utility/defer.h>
#include <dsn/utility/filesystem.h>
#include <dsn/utility/TokenBucket.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/flags.h>

namespace dsn {
namespace dist {
namespace block_service {

DSN_DEFINE_uint64("pegasus.server",
                  rocksdb_target_file_size_base,
                  64 << 20,
                  "rocksdb options.target_file_size_base");

DSN_DEFINE_uint64("pegasus.server",
                  rocksdb_write_buffer_size,
                  64 << 20,
                  "rocksdb options.write_buffer_size");

DSN_DEFINE_uint32("replication", fds_write_limit_rate, 100, "rate limit of fds(MB/s)");

DSN_DEFINE_uint32("replication", fds_read_limit_rate, 100, "rate limit of fds(MB/s)");

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

DEFINE_THREAD_POOL_CODE(THREAD_POOL_FDS_SERVICE)
DEFINE_TASK_CODE(LPC_FDS_CALL, TASK_PRIORITY_COMMON, THREAD_POOL_FDS_SERVICE)

const std::string fds_service::FILE_LENGTH_CUSTOM_KEY = "x-xiaomi-meta-content-length";
const std::string fds_service::FILE_MD5_KEY = "content-md5";

fds_service::fds_service()
{
    /// In normal scenario, the sst file size of level 0 is write_buffer_size * [0.75, 1.25]
    /// And in BULK_LOAD scenario, it is 4 * write_buffer_size * [0.75, 1.25].
    /// In rdsn, we can't get the scenario, so if we take BULK_LOAD scenario into consideration,
    /// we must set max_sst_file_size to 4 * write_buffer_size * [0.75, 1.25], which is too big.
    /// So in this implementation, we don't take BULK_LOAD scenario into consideration.
    uint64_t max_sst_file_size = std::max(FLAGS_rocksdb_target_file_size_base,
                                          (uint64_t)1.25 * FLAGS_rocksdb_write_buffer_size);

    /// For write operation, we can't send a file in batches. Because putContent interface of fds
    /// will overwrite what was sent before for the same file. So we must send a file as a whole.
    /// If file size > burst size, the file will be rejected by the token bucket.
    ///  Here we set burst_size = max_sst_file_size + 3MB, a litte greater than max_sst_file_size
    uint32_t burst_size =
        std::max(2.0 * (FLAGS_fds_write_limit_rate << 20), max_sst_file_size + 3e6);
    _write_token_bucket.reset(new folly::TokenBucket(FLAGS_fds_write_limit_rate << 20, burst_size));
    burst_size = 2 * FLAGS_fds_read_limit_rate << 20;
    _read_token_bucket.reset(new folly::TokenBucket(FLAGS_fds_read_limit_rate << 20, burst_size));
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
        derror("fds %s timeout: parameter(%s), code(%d), msg(%s)",                                 \
               OPERATION,                                                                          \
               INPUT_PARAMETER,                                                                    \
               ex.code(),                                                                          \
               ex.message().c_str());                                                              \
        ERR_REFERENCE = ERR_TIMEOUT;                                                               \
    }                                                                                              \
    catch (const Poco::Exception &ex)                                                              \
    {                                                                                              \
        derror("fds %s get poco exception: parameter(%s), code(%d), msg(%s), what(%s)",            \
               OPERATION,                                                                          \
               INPUT_PARAMETER,                                                                    \
               ex.code(),                                                                          \
               ex.message().c_str(),                                                               \
               ex.what());                                                                         \
        ERR_REFERENCE = ERR_FS_INTERNAL;                                                           \
    }                                                                                              \
    catch (...)                                                                                    \
    {                                                                                              \
        derror("fds %s get unknown exception: parameter(%s)", OPERATION, INPUT_PARAMETER);         \
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
                    dassert(fds_path.empty() || boost::starts_with(obj.objectName(), fds_path),
                            "invalid path(%s) in parent(%s)",
                            obj.objectName().c_str(),
                            fds_path.c_str());
                    resp.entries->push_back(
                        {utils::path_from_fds(obj.objectName().substr(fds_path.size()), false),
                         false});
                }
                for (const std::string &s : common_prefix) {
                    dassert(fds_path.empty() || boost::starts_with(s, fds_path),
                            "invalid path(%s) in parent(%s)",
                            s.c_str(),
                            fds_path.c_str());
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
            derror("fds listObjects failed: parameter(%s), code(%d), msg(%s)",
                   req.dir_name.c_str(),
                   ex.code(),
                   ex.what());
            resp.err = ERR_FS_INTERNAL;
        }
        FDS_EXCEPTION_HANDLE(resp.err, "listObject", req.dir_name.c_str())

        if (resp.err == dsn::ERR_OK && resp.entries->empty()) {
            try {
                if (_client->doesObjectExist(_bucket_name,
                                             utils::path_to_fds(req.dir_name, false))) {
                    derror("fds list_dir failed: path not dir, parameter(%s)",
                           req.dir_name.c_str());
                    resp.err = ERR_INVALID_PARAMETERS;
                } else {
                    derror("fds list_dir failed: path not found, parameter(%s)",
                           req.dir_name.c_str());
                    resp.err = ERR_OBJECT_NOT_FOUND;
                }
            } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
                derror("fds doesObjectExist failed: parameter(%s), code(%d), msg(%s)",
                       req.dir_name.c_str(),
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

// TODO(zhaoliwei) refactor these code, because there have same code in get_file_meta()
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
    } else {
        auto create_file_in_background = [this, req, t]() {
            create_file_response resp;
            resp.err = ERR_IO_PENDING;
            std::string fds_path = utils::path_to_fds(req.file_name, false);
            try {
                std::shared_ptr<galaxy::fds::FDSObjectMetadata> metadata =
                    _client->getObjectMetadata(_bucket_name, fds_path);
                // if we get the object metadata succeed, we expect to get the content-md5 and the
                // content-length
                const std::map<std::string, std::string> &meta_map = metadata->metadata();
                auto iter = meta_map.find(FILE_MD5_KEY);
                dassert(iter != meta_map.end(),
                        "can't find %s in object(%s)'s metadata",
                        FILE_MD5_KEY.c_str(),
                        fds_path.c_str());
                const std::string &md5 = iter->second;

                // in a head http-request, the file length is in the x-xiaomi-meta-content-length
                // while in a get http-request, the file length is in the contentLength
                iter = meta_map.find(FILE_LENGTH_CUSTOM_KEY);
                dassert(iter != meta_map.end(),
                        "can't find %s in object(%s)'s metadata",
                        FILE_LENGTH_CUSTOM_KEY.c_str(),
                        fds_path.c_str());
                uint64_t size = (uint64_t)atol(iter->second.c_str());
                resp.err = dsn::ERR_OK;
                resp.file_handle = new fds_file_object(this, req.file_name, fds_path, md5, size);
            } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
                if (ex.code() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND) {
                    resp.err = dsn::ERR_OK;
                    resp.file_handle = new fds_file_object(this, req.file_name, fds_path, "", 0);
                } else {
                    derror("fds getObjectMetadata failed: parameter(%s), code(%d), msg(%s)",
                           req.file_name.c_str(),
                           ex.code(),
                           ex.what());
                    resp.err = ERR_FS_INTERNAL;
                }
            }
            FDS_EXCEPTION_HANDLE(resp.err, "getObjectMetadata", req.file_name.c_str());

            t->enqueue_with(resp);
        };

        dsn::tasking::enqueue(LPC_FDS_CALL, nullptr, create_file_in_background);
        return t;
    }
}

dsn::task_ptr fds_service::delete_file(const delete_file_request &req,
                                       task_code code,
                                       const delete_file_callback &cb,
                                       dsn::task_tracker *tracker)
{
    delete_file_future_ptr t(new delete_file_future(code, cb, 0));
    t->set_tracker(tracker);
    auto delete_file_in_background = [this, req, t]() {
        std::string fds_path = utils::path_to_fds(req.file_name, false);
        delete_file_response resp;
        try {
            _client->deleteObject(_bucket_name, fds_path, false);
            resp.err = ERR_OK;
        } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
            if (ex.code() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND) {
                derror("fds deleteObject failed: file not found, parameter(%s)",
                       req.file_name.c_str());
                resp.err = ERR_OBJECT_NOT_FOUND;
            } else {
                derror("fds deleteObject failed: parameter(%s), code(%d), msg(%s)",
                       req.file_name.c_str(),
                       ex.code(),
                       ex.what());
                resp.err = ERR_FS_INTERNAL;
            }
        }
        FDS_EXCEPTION_HANDLE(resp.err, "deleteObject", req.file_name.c_str());
        t->enqueue_with(resp);
    };

    dsn::tasking::enqueue(LPC_FDS_CALL, nullptr, delete_file_in_background);
    return t;
}

// FDS don't have the concept of directory, so if no file(req.path/***/file) exist
// then the path isn't exist, otherwise we think path is exist
//
// Attention： using listObjects to implement, if req.path is an non-empty dir, and these is many
// file under req.path, then this function may consume much time
//
dsn::task_ptr fds_service::exist(const exist_request &req,
                                 dsn::task_code code,
                                 const exist_callback &cb,
                                 dsn::task_tracker *tracker)
{
    exist_future_ptr callback(new exist_future(code, cb, 0));
    callback->set_tracker(tracker);
    auto exist_in_background = [this, req, callback]() {
        exist_response resp;
        std::string fds_path = utils::path_to_fds(req.path, true);
        try {
            std::shared_ptr<galaxy::fds::FDSObjectListing> result =
                _client->listObjects(_bucket_name, fds_path);
            const std::vector<galaxy::fds::FDSObjectSummary> &objs = result->objectSummaries();
            const std::vector<std::string> &common_prefix = result->commonPrefixes();

            if (!objs.empty() || !common_prefix.empty()) {
                // path is a non-empty directory
                resp.err = ERR_OK;
            } else {
                if (_client->doesObjectExist(_bucket_name, utils::path_to_fds(req.path, false))) {
                    resp.err = ERR_OK;
                } else {
                    derror("fds exist failed: path not found, parameter(%s)", req.path.c_str());
                    resp.err = ERR_OBJECT_NOT_FOUND;
                }
            }
        } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
            derror("fds exist failed: parameter(%s), code(%d), msg(%s)",
                   req.path.c_str(),
                   ex.code(),
                   ex.what());
            resp.err = ERR_FS_INTERNAL;
        }
        FDS_EXCEPTION_HANDLE(resp.err, "exist", req.path.c_str());
        callback->enqueue_with(resp);
    };
    tasking::enqueue(LPC_FDS_CALL, nullptr, std::move(exist_in_background));
    return callback;
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
                    derror("fds remove_path failed: dir not empty, parameter(%s)",
                           req.path.c_str());
                    resp.err = ERR_DIR_NOT_EMPTY;
                }
            } else {
                if (_client->doesObjectExist(_bucket_name, utils::path_to_fds(req.path, false))) {
                    should_remove_path = true;
                } else {
                    derror("fds remove_path failed: path not found, parameter(%s)",
                           req.path.c_str());
                    resp.err = ERR_OBJECT_NOT_FOUND;
                }
            }
        } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
            derror("fds remove_path failed: parameter(%s), code(%d), msg(%s)",
                   req.path.c_str(),
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
                    derror("fds remove_path failed: countFailedObjects = %d, parameter(%s)",
                           deleting->countFailedObjects(),
                           req.path.c_str());
                    resp.err = ERR_FS_INTERNAL;
                }
            } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
                derror("fds remove_path failed: parameter(%s), code(%d), msg(%s)",
                       req.path.c_str(),
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
      _md5sum(),
      _size(0),
      _has_meta_synced(false)
{
}

fds_file_object::fds_file_object(fds_service *s,
                                 const std::string &name,
                                 const std::string &fds_path,
                                 const std::string &md5,
                                 uint64_t size)
    : block_file(name),
      _service(s),
      _fds_path(fds_path),
      _md5sum(md5),
      _size(size),
      _has_meta_synced(true)
{
}

fds_file_object::~fds_file_object() {}

error_code fds_file_object::get_file_meta()
{
    galaxy::fds::GalaxyFDSClient *c = _service->get_client();
    try {
        auto meta = c->getObjectMetadata(_service->get_bucket_name(), _fds_path)->metadata();

        // get file length
        auto iter = meta.find(fds_service::FILE_LENGTH_CUSTOM_KEY);
        dassert_f(iter != meta.end(),
                  "can't find {} in object({})'s metadata",
                  fds_service::FILE_LENGTH_CUSTOM_KEY.c_str(),
                  _fds_path.c_str());
        bool valid = dsn::buf2uint64(iter->second, _size);
        dassert_f(valid, "error to get file size");

        // get md5 key
        iter = meta.find(fds_service::FILE_MD5_KEY);
        dassert_f(iter != meta.end(),
                  "can't find {} in object({})'s metadata",
                  fds_service::FILE_MD5_KEY.c_str(),
                  _fds_path.c_str());
        _md5sum = iter->second;

        _has_meta_synced = true;
        return ERR_OK;
    } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
        if (ex.code() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND) {
            return ERR_OBJECT_NOT_FOUND;
        } else {
            derror_f("fds getObjectMetadata failed: parameter({}), code({}), msg({})",
                     _name.c_str(),
                     ex.code(),
                     ex.what());
            return ERR_FS_INTERNAL;
        }
    }
}

error_code fds_file_object::get_content_in_batches(uint64_t start,
                                                   int64_t length,
                                                   /*out*/ std::ostream &os,
                                                   /*out*/ uint64_t &transfered_bytes)
{
    // the max batch size is 1MB
    const uint64_t BATCH_MAX = 1 << 20;
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
        uint64_t batch_len = std::min(BATCH_MAX, start + to_transfer_bytes - pos);
        // get tokens from token bucket
        _service->_read_token_bucket->consumeWithBorrowAndWait(batch_len);

        err = get_content(pos, batch_len, os, once_transfered_bytes);
        transfered_bytes += once_transfered_bytes;
        if (err != ERR_OK || once_transfered_bytes < batch_len) {
            return err;
        }
        pos += batch_len;
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
            dinfo("get object from fds succeed, remote_file(%s)", _fds_path.c_str());
            std::istream &is = obj->objectContent();
            transfered_bytes += utils::copy_stream(is, os, PIECE_SIZE);
            err = ERR_OK;
        } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
            derror("fds getObject error: remote_file(%s), code(%d), msg(%s)",
                   file_name().c_str(),
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
    if (!_service->_write_token_bucket->consumeWithBorrowAndWait(to_transfer_bytes)) {
        ddebug_f("the transfer count({}) is greater than burst size({}), so it is rejected by "
                 "token bucket",
                 to_transfer_bytes,
                 _service->_write_token_bucket->burst());
        return ERR_BUSY;
    }

    try {
        c->putObject(_service->get_bucket_name(), _fds_path, is, galaxy::fds::FDSObjectMetadata());
    } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
        derror("fds putObject error: remote_file(%s), code(%d), msg(%s)",
               file_name().c_str(),
               ex.code(),
               ex.what());
        err = ERR_FS_INTERNAL;
    }
    FDS_EXCEPTION_HANDLE(err, "putObject", file_name().c_str())

    if (err != ERR_OK) {
        return err;
    }

    try {
        // Get Object meta data
        std::shared_ptr<galaxy::fds::FDSObjectMetadata> metadata =
            c->getObjectMetadata(_service->get_bucket_name(), _fds_path);
        const std::map<std::string, std::string> metaMap = metadata->metadata();

        auto iter = metaMap.find(fds_service::FILE_MD5_KEY);
        dassert(iter != metaMap.end(),
                "can't find %s in object(%s)'s metadata",
                fds_service::FILE_MD5_KEY.c_str(),
                _fds_path.c_str());
        _md5sum = iter->second;

        // in a head http-request, the file length is in the x-xiaomi-meta-content-length
        // while in a get http-request, the file length is in the contentLength
        iter = metaMap.find(fds_service::FILE_LENGTH_CUSTOM_KEY);
        dassert(iter != metaMap.end(),
                "can't find %s in object(%s) metadata",
                fds_service::FILE_LENGTH_CUSTOM_KEY.c_str(),
                _fds_path.c_str());
        _size = (uint64_t)atoll(iter->second.c_str());
        transfered_bytes = _size;
    } catch (const galaxy::fds::GalaxyFDSClientException &ex) {
        derror("fds getObjectMetadata after put failed: remote_file(%s), code(%d), msg(%s)",
               file_name().c_str(),
               ex.code(),
               ex.what());
        err = ERR_FS_INTERNAL;
    }
    FDS_EXCEPTION_HANDLE(err, "getObjectMetadata_after_put", file_name().c_str())
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
            char buffer[256];
            char *ptr = strerror_r(errno, buffer, 256);
            derror("fds upload failed: open local file(%s) failed when upload to(%s), error(%s)",
                   local_file.c_str(),
                   file_name().c_str(),
                   ptr);
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
    read_response resp;
    if (_has_meta_synced && _md5sum.empty()) {
        derror("fds read failed: meta not synced or md5sum empty when read (%s)",
               _fds_path.c_str());
        resp.err = dsn::ERR_OBJECT_NOT_FOUND;
        t->enqueue_with(resp);
        return t;
    }

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
    if (_has_meta_synced && _md5sum.empty()) {
        derror("fds download failed: meta not synced or md5sum empty when download (%s)",
               _fds_path.c_str());
        resp.err = dsn::ERR_OBJECT_NOT_FOUND;
        resp.downloaded_size = 0;
        t->enqueue_with(resp);
        return t;
    }

    std::shared_ptr<std::ofstream> handle(new std::ofstream(
        req.output_local_name, std::ios::binary | std::ios::out | std::ios::trunc));
    if (!handle->is_open()) {
        char buffer[512];
        char *ptr = strerror_r(errno, buffer, 512);
        derror("fds download failed: fail to open localfile(%s) when download(%s), error(%s)",
               req.output_local_name.c_str(),
               _fds_path.c_str(),
               ptr);
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
        if (handle->tellp() != -1)
            resp.downloaded_size = handle->tellp();
        handle->close();
        t->enqueue_with(resp);
        release_ref();
    };

    dsn::tasking::enqueue(LPC_FDS_CALL, nullptr, download_background);
    return t;
}
} // namespace block_service
} // namespace dist
} // namespace dsn
