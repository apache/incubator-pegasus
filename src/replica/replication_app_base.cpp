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

#include "replica.h"
#include "mutation.h"
#include "common/bulk_load_common.h"
#include "common/duplication_common.h"
#include "utils/latency_tracer.h"
#include "utils/fmt_logging.h"
#include "replica/replication_app_base.h"
#include "utils/defer.h"
#include "utils/factory_store.h"
#include "utils/filesystem.h"
#include "utils/crc.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include <fstream>
#include <sstream>
#include <memory>
#include "utils/fail_point.h"
#include "common/replica_envs.h"

namespace dsn {
namespace replication {

const std::string replica_init_info::kInitInfo = ".init-info";

DEFINE_TASK_CODE_AIO(LPC_AIO_INFO_WRITE, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

namespace {
error_code write_blob_to_file(const std::string &file, const blob &data)
{
    std::string tmp_file = file + ".tmp";
    disk_file *hfile = file::open(tmp_file.c_str(), O_WRONLY | O_CREAT | O_BINARY | O_TRUNC, 0666);
    ERR_LOG_AND_RETURN_NOT_TRUE(hfile, ERR_FILE_OPERATION_FAILED, "open file {} failed", tmp_file);
    auto cleanup = defer([tmp_file]() { utils::filesystem::remove_path(tmp_file); });

    error_code err;
    size_t sz = 0;
    task_tracker tracker;
    aio_task_ptr tsk = file::write(hfile,
                                   data.data(),
                                   data.length(),
                                   0,
                                   LPC_AIO_INFO_WRITE,
                                   &tracker,
                                   [&err, &sz](error_code e, size_t s) {
                                       err = e;
                                       sz = s;
                                   },
                                   0);
    CHECK_NOTNULL(tsk, "create file::write task failed");
    tracker.wait_outstanding_tasks();
    file::flush(hfile);
    file::close(hfile);
    ERR_LOG_AND_RETURN_NOT_OK(err, "write file {} failed", tmp_file);
    CHECK_EQ(data.length(), sz);
    // TODO(yingchun): need fsync too？
    ERR_LOG_AND_RETURN_NOT_TRUE(utils::filesystem::rename_path(tmp_file, file),
                                ERR_FILE_OPERATION_FAILED,
                                "move file from {} to {} failed",
                                tmp_file,
                                file);

    return ERR_OK;
}
} // namespace

error_code replica_init_info::load(const std::string &dir)
{
    std::string info_path = utils::filesystem::path_combine(dir, kInitInfo);
    CHECK(utils::filesystem::path_exists(info_path), "file({}) not exist", info_path);
    ERR_LOG_AND_RETURN_NOT_OK(
        load_json(info_path), "load replica_init_info from {} failed", info_path);
    LOG_INFO_F("load replica_init_info from {} succeed: {}", info_path, to_string());
    return ERR_OK;
}

error_code replica_init_info::store(const std::string &dir)
{
    uint64_t start = dsn_now_ns();
    std::string info_path = utils::filesystem::path_combine(dir, kInitInfo);
    ERR_LOG_AND_RETURN_NOT_OK(store_json(info_path),
                              "store replica_init_info to {} failed, time_used_ns = {}",
                              info_path,
                              dsn_now_ns() - start);
    LOG_INFO_F("store replica_init_info to {} succeed, time_used_ns = {}: {}",
               info_path,
               dsn_now_ns() - start,
               to_string());
    return ERR_OK;
}

error_code replica_init_info::load_json(const std::string &file)
{
    std::ifstream is(file, std::ios::binary);
    ERR_LOG_AND_RETURN_NOT_TRUE(
        is.is_open(), ERR_FILE_OPERATION_FAILED, "open file {} failed", file);

    int64_t sz = 0;
    ERR_LOG_AND_RETURN_NOT_TRUE(utils::filesystem::file_size(std::string(file), sz),
                                ERR_FILE_OPERATION_FAILED,
                                "get file size of {} failed",
                                file);

    std::shared_ptr<char> buffer(utils::make_shared_array<char>(sz));
    is.read((char *)buffer.get(), sz);
    ERR_LOG_AND_RETURN_NOT_TRUE(!is.bad(), ERR_FILE_OPERATION_FAILED, "read file {} failed", file);
    is.close();

    ERR_LOG_AND_RETURN_NOT_TRUE(
        json::json_forwarder<replica_init_info>::decode(blob(buffer, sz), *this),
        ERR_FILE_OPERATION_FAILED,
        "decode json from file {} failed",
        file);

    return ERR_OK;
}

error_code replica_init_info::store_json(const std::string &file)
{
    return write_blob_to_file(file, json::json_forwarder<replica_init_info>::encode(*this));
}

std::string replica_init_info::to_string()
{
    // TODO(yingchun): use fmt instead
    std::ostringstream oss;
    oss << "init_ballot = " << init_ballot << ", init_durable_decree = " << init_durable_decree
        << ", init_offset_in_shared_log = " << init_offset_in_shared_log
        << ", init_offset_in_private_log = " << init_offset_in_private_log;
    return oss.str();
}

error_code replica_app_info::load(const std::string &file)
{
    std::ifstream is(file, std::ios::binary);
    ERR_LOG_AND_RETURN_NOT_TRUE(
        is.is_open(), ERR_FILE_OPERATION_FAILED, "open file {} failed", file);

    int64_t sz = 0;
    ERR_LOG_AND_RETURN_NOT_TRUE(utils::filesystem::file_size(std::string(file), sz),
                                ERR_FILE_OPERATION_FAILED,
                                "get file size of {} failed",
                                file);

    std::shared_ptr<char> buffer(utils::make_shared_array<char>(sz));
    is.read((char *)buffer.get(), sz);
    is.close();

    binary_reader reader(blob(buffer, sz));
    int magic;
    unmarshall(reader, magic, DSF_THRIFT_BINARY);

    ERR_LOG_AND_RETURN_NOT_TRUE(
        magic == 0xdeadbeef, ERR_INVALID_DATA, "data in file {} is invalid (magic)", file);

    unmarshall(reader, *_app, DSF_THRIFT_JSON);
    return ERR_OK;
}

error_code replica_app_info::store(const std::string &file)
{
    binary_writer writer;
    int magic = 0xdeadbeef;

    marshall(writer, magic, DSF_THRIFT_BINARY);
    if (_app->envs.empty()) {
        marshall(writer, *_app, DSF_THRIFT_JSON);
    } else {
        // for most envs, do not persistent them to app info file
        // ROCKSDB_ALLOW_INGEST_BEHIND should be persistent
        app_info tmp = *_app;
        tmp.envs.clear();
        const auto &iter = _app->envs.find(replica_envs::ROCKSDB_ALLOW_INGEST_BEHIND);
        if (iter != _app->envs.end()) {
            tmp.envs[replica_envs::ROCKSDB_ALLOW_INGEST_BEHIND] = iter->second;
        }
        marshall(writer, tmp, DSF_THRIFT_JSON);
    }

    return write_blob_to_file(file, writer.get_buffer());
}

/*static*/
void replication_app_base::register_storage_engine(const std::string &name, factory f)
{
    utils::factory_store<replication_app_base>::register_factory(
        name.c_str(), f, PROVIDER_TYPE_MAIN);
}
/*static*/
replication_app_base *replication_app_base::new_storage_instance(const std::string &name,
                                                                 replica *r)
{
    return utils::factory_store<replication_app_base>::create(name.c_str(), PROVIDER_TYPE_MAIN, r);
}

replication_app_base::replication_app_base(replica *replica) : replica_base(replica)
{
    _dir_data = utils::filesystem::path_combine(replica->dir(), "data");
    _dir_learn = utils::filesystem::path_combine(replica->dir(), "learn");
    _dir_backup = utils::filesystem::path_combine(replica->dir(), "backup");
    _dir_bulk_load = utils::filesystem::path_combine(replica->dir(),
                                                     bulk_load_constant::BULK_LOAD_LOCAL_ROOT_DIR);
    _dir_duplication = utils::filesystem::path_combine(
        replica->dir(), duplication_constants::kDuplicationCheckpointRootDir);
    _last_committed_decree = 0;
    _replica = replica;
}

bool replication_app_base::is_primary() const
{
    return _replica->status() == partition_status::PS_PRIMARY;
}

bool replication_app_base::is_duplication_master() const
{
    return _replica->is_duplication_master();
}

bool replication_app_base::is_duplication_follower() const
{
    return _replica->is_duplication_follower();
}

const ballot &replication_app_base::get_ballot() const { return _replica->get_ballot(); }

error_code replication_app_base::open_internal(replica *r)
{
    ERR_LOG_AND_RETURN_NOT_TRUE(utils::filesystem::directory_exists(_dir_data),
                                ERR_FILE_OPERATION_FAILED,
                                "[{}]: replica data dir {} does not exist",
                                r->name(),
                                _dir_data);

    ERR_LOG_AND_RETURN_NOT_OK(open(), "[{}]: open replica app failed", r->name());

    _last_committed_decree = last_durable_decree();

    auto err = _info.load(r->dir());
    ERR_LOG_AND_RETURN_NOT_OK(err, "[{}]: load replica_init_info failed", r->name());

    ERR_LOG_AND_RETURN_NOT_TRUE(err != ERR_OK || last_durable_decree() >= _info.init_durable_decree,
                                ERR_INCOMPLETE_DATA,
                                "[{}]: replica data is not complete coz "
                                "last_durable_decree({}) < init_durable_decree({})",
                                r->name(),
                                last_durable_decree(),
                                _info.init_durable_decree);

    return ERR_OK;
}

error_code replication_app_base::open_new_internal(replica *r,
                                                   int64_t shared_log_start,
                                                   int64_t private_log_start)
{
    CHECK(utils::filesystem::remove_path(_dir_data), "remove data dir {} failed", _dir_data);
    CHECK(utils::filesystem::create_directory(_dir_data), "create data dir {} failed", _dir_data);
    ERR_LOG_AND_RETURN_NOT_TRUE(utils::filesystem::directory_exists(_dir_data),
                                ERR_FILE_OPERATION_FAILED,
                                "[{}]: create replica data dir {} failed",
                                r->name(),
                                _dir_data);

    ERR_LOG_AND_RETURN_NOT_OK(open(), "[{}]: open replica app failed", r->name());
    _last_committed_decree = last_durable_decree();
    ERR_LOG_AND_RETURN_NOT_OK(update_init_info(_replica, shared_log_start, private_log_start, 0),
                              "[{}]: open replica app failed",
                              r->name());
    return ERR_OK;
}

error_code replication_app_base::open()
{
    const app_info *info = get_app_info();
    int argc = 1;
    argc += (2 * info->envs.size());
    // check whether replica have some extra envs that meta don't known
    const std::map<std::string, std::string> &extra_envs = _replica->get_replica_extra_envs();
    argc += (2 * extra_envs.size());

    std::unique_ptr<char *[]> argvs = make_unique<char *[]>(argc);
    char **argv = argvs.get();
    CHECK_NOTNULL(argv, "");
    int idx = 0;
    argv[idx++] = (char *)(info->app_name.c_str());
    if (argc > 1) {
        for (auto &kv : info->envs) {
            argv[idx++] = (char *)(kv.first.c_str());
            argv[idx++] = (char *)(kv.second.c_str());
        }

        // combine extra envs
        for (auto &kv : extra_envs) {
            argv[idx++] = (char *)(kv.first.c_str());
            argv[idx++] = (char *)(kv.second.c_str());
        }
    }
    CHECK_EQ(argc, idx);

    return start(argc, argv);
}

error_code replication_app_base::close(bool clear_state)
{
    ERR_LOG_AND_RETURN_NOT_OK(stop(clear_state), "[{}]: stop storage failed", replica_name());

    _last_committed_decree.store(0);

    return ERR_OK;
}

error_code replication_app_base::apply_checkpoint(chkpt_apply_mode mode, const learn_state &state)
{
    int64_t current_commit_decree = last_committed_decree();
    error_code err = storage_apply_checkpoint(mode, state);
    if (ERR_OK == err && state.to_decree_included > current_commit_decree) {
        _last_committed_decree.store(state.to_decree_included);
    }
    return err;
}

int replication_app_base::on_batched_write_requests(int64_t decree,
                                                    uint64_t timestamp,
                                                    message_ex **requests,
                                                    int request_length)
{
    int storage_error = 0;
    for (int i = 0; i < request_length; ++i) {
        // TODO(yingchun): better to return error_code
        int e = on_request(requests[i]);
        if (e != 0) {
            LOG_ERROR_PREFIX("got storage error when handler request({})",
                             requests[i]->header->rpc_name);
            storage_error = e;
        }
    }
    return storage_error;
}

error_code replication_app_base::apply_mutation(const mutation *mu)
{
    FAIL_POINT_INJECT_F("replication_app_base_apply_mutation", [](string_view) { return ERR_OK; });

    CHECK_EQ_PREFIX(mu->data.header.decree, last_committed_decree() + 1);
    CHECK_EQ_PREFIX(mu->data.updates.size(), mu->client_requests.size());
    CHECK_GT_PREFIX(mu->data.updates.size(), 0);

    if (_replica->status() == partition_status::PS_PRIMARY) {
        ADD_POINT(mu->_tracer);
    }

    bool has_ingestion_request = false;
    int request_count = static_cast<int>(mu->client_requests.size());
    message_ex **batched_requests = (message_ex **)alloca(sizeof(message_ex *) * request_count);
    message_ex **faked_requests = (message_ex **)alloca(sizeof(message_ex *) * request_count);
    int batched_count = 0; // write-empties are not included.
    int faked_count = 0;
    for (int i = 0; i < request_count; i++) {
        const mutation_update &update = mu->data.updates[i];
        message_ex *req = mu->client_requests[i];
        LOG_DEBUG_PREFIX("mutation {} #{}: dispatch rpc call {}", mu->name(), i, update.code);
        if (update.code != RPC_REPLICATION_WRITE_EMPTY) {
            if (req == nullptr) {
                req = message_ex::create_received_request(
                    update.code,
                    (dsn_msg_serialize_format)update.serialization_type,
                    (void *)update.data.data(),
                    update.data.length());
                faked_requests[faked_count++] = req;
            }

            batched_requests[batched_count++] = req;
            if (update.code == apps::RPC_RRDB_RRDB_BULK_LOAD) {
                has_ingestion_request = true;
            }
        }
    }

    int perror = on_batched_write_requests(
        mu->data.header.decree, mu->data.header.timestamp, batched_requests, batched_count);

    // release faked requests
    for (int i = 0; i < faked_count; i++) {
        faked_requests[i]->release_ref();
    }

    if (perror != 0) {
        LOG_ERROR_PREFIX("mutation {}: get internal error {}", mu->name(), perror);
        // for normal write requests, if got rocksdb error, this replica will be set error and evoke
        // learn for ingestion requests, should not do as normal write requests, there are two
        // reasons:
        // 1. all ingestion errors should be handled by meta server in function
        // `on_partition_ingestion_reply`, rocksdb error will be returned to meta server in
        // structure `ingestion_response`, not in this function
        // 2. if replica apply ingestion mutation during learn, it may got error from rocksdb,
        // because the external sst files may not exist, in this case, we won't consider it as an
        // error
        if (!has_ingestion_request) {
            return ERR_LOCAL_APP_FAILURE;
        }
    }

    ++_last_committed_decree;

    if (_replica->verbose_commit_log()) {
        auto status = _replica->status();
        const char *str;
        switch (status) {
        case partition_status::PS_INACTIVE:
            str = "I";
            break;
        case partition_status::PS_PRIMARY:
            str = "P";
            break;
        case partition_status::PS_SECONDARY:
            str = "S";
            break;
        case partition_status::PS_POTENTIAL_SECONDARY:
            str = "PS";
            break;
        default:
            CHECK_PREFIX_MSG(false, "status = {}", enum_to_string(status));
            __builtin_unreachable();
        }
        LOG_INFO_PREFIX(
            "mutation {} committed on {}, batched_count = {}", mu->name(), str, batched_count);
    }

    _replica->update_commit_qps(batched_count);

    return ERR_OK;
}

error_code replication_app_base::update_init_info(replica *r,
                                                  int64_t shared_log_offset,
                                                  int64_t private_log_offset,
                                                  int64_t durable_decree)
{
    _info.crc = 0;
    _info.magic = 0xdeadbeef;
    _info.init_ballot = r->get_ballot();
    _info.init_durable_decree = durable_decree;
    _info.init_offset_in_shared_log = shared_log_offset;
    _info.init_offset_in_private_log = private_log_offset;

    ERR_LOG_AND_RETURN_NOT_OK(
        _info.store(r->dir()), "[{}]: store replica_init_info failed", r->name());

    return ERR_OK;
}

error_code replication_app_base::update_init_info_ballot_and_decree(replica *r)
{
    return update_init_info(r,
                            _info.init_offset_in_shared_log,
                            _info.init_offset_in_private_log,
                            r->last_durable_decree());
}

const app_info *replication_app_base::get_app_info() const { return _replica->get_app_info(); }

} // namespace replication
} // namespace dsn
