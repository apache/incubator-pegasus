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

#include <fmt/core.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "common/bulk_load_common.h"
#include "common/duplication_common.h"
#include "common/replica_envs.h"
#include "common/replication.codes.h"
#include "common/replication_enums.h"
#include "consensus_types.h"
#include "dsn.layer2_types.h"
#include "mutation.h"
#include "replica.h"
#include "replica/replication_app_base.h"
#include "rpc/rpc_message.h"
#include "rpc/serialization.h"
#include "task/task_code.h"
#include "task/task_spec.h"
#include "utils/alloc.h"
#include "utils/autoref_ptr.h"
#include "utils/binary_reader.h"
#include "utils/binary_writer.h"
#include "utils/blob.h"
#include "utils/env.h"
#include "utils/factory_store.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/latency_tracer.h"
#include "utils/load_dump_object.h"

METRIC_DEFINE_counter(replica,
                      committed_requests,
                      dsn::metric_unit::kRequests,
                      "The number of committed requests");

namespace dsn {

namespace replication {

const std::string replica_app_info::kAppInfo = ".app-info";
const std::string replica_init_info::kInitInfo = ".init-info";
const std::string kms_info::kKmsInfo = ".kms-info";

std::string replica_init_info::to_string() const
{
    return fmt::format(
        "init_ballot = {}, init_durable_decree = {}, init_offset_in_private_log = {}",
        init_ballot,
        init_durable_decree,
        init_offset_in_private_log);
}

error_code replica_app_info::load(const std::string &fname)
{
    std::string data;
    auto s = rocksdb::ReadFileToString(
        dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive), fname, &data);
    LOG_AND_RETURN_NOT_TRUE(ERROR, s.ok(), ERR_FILE_OPERATION_FAILED, "read file {} failed", fname);
    binary_reader reader(blob::create_from_bytes(std::move(data)));
    int magic = 0;
    unmarshall(reader, magic, DSF_THRIFT_BINARY);
    LOG_AND_RETURN_NOT_TRUE(
        ERROR, magic == 0xdeadbeef, ERR_INVALID_DATA, "data in file {} is invalid (magic)", fname);
    unmarshall(reader, *_app, DSF_THRIFT_JSON);
    return ERR_OK;
}

error_code replica_app_info::store(const std::string &fname)
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

    return dsn::utils::write_data_to_file(
        fname, writer.get_buffer(), dsn::utils::FileDataType::kSensitive);
}

const std::string replication_app_base::kDataDir = "data";
const std::string replication_app_base::kRdbDir = "rdb";

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

replication_app_base::replication_app_base(replica *replica)
    : replica_base(replica), METRIC_VAR_INIT_replica(committed_requests)
{
    _dir_data = utils::filesystem::path_combine(replica->dir(), kDataDir);
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
    LOG_AND_RETURN_NOT_TRUE(ERROR_PREFIX,
                            utils::filesystem::directory_exists(_dir_data),
                            ERR_FILE_OPERATION_FAILED,
                            "replica data dir {} does not exist",
                            _dir_data);

    LOG_AND_RETURN_NOT_OK(ERROR_PREFIX, open(), "open replica app failed");

    _last_committed_decree = last_durable_decree();

    auto err = dsn::utils::load_rjobj_from_file(
        utils::filesystem::path_combine(r->dir(), replica_init_info::kInitInfo), &_info);
    LOG_AND_RETURN_NOT_OK(ERROR_PREFIX, err, "load replica_init_info failed");

    LOG_AND_RETURN_NOT_TRUE(ERROR_PREFIX,
                            err != ERR_OK || last_durable_decree() >= _info.init_durable_decree,
                            ERR_INCOMPLETE_DATA,
                            "replica data is not complete coz "
                            "last_durable_decree({}) < init_durable_decree({})",
                            last_durable_decree(),
                            _info.init_durable_decree);

    return ERR_OK;
}

error_code replication_app_base::open_new_internal(replica *r, int64_t private_log_start)
{
    CHECK(utils::filesystem::remove_path(_dir_data), "remove data dir {} failed", _dir_data);
    CHECK(utils::filesystem::create_directory(_dir_data), "create data dir {} failed", _dir_data);
    LOG_AND_RETURN_NOT_TRUE(ERROR_PREFIX,
                            utils::filesystem::directory_exists(_dir_data),
                            ERR_FILE_OPERATION_FAILED,
                            "create replica data dir {} failed",
                            _dir_data);

    LOG_AND_RETURN_NOT_OK(ERROR_PREFIX, open(), "open replica app failed");
    _last_committed_decree = last_durable_decree();
    LOG_AND_RETURN_NOT_OK(
        ERROR_PREFIX, update_init_info(_replica, private_log_start, 0), "open replica app failed");
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

    std::unique_ptr<char *[]> argvs = std::make_unique<char *[]>(argc);
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
    LOG_AND_RETURN_NOT_OK(ERROR_PREFIX, stop(clear_state), "stop storage failed");

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
                                                    int request_length,
                                                    message_ex *original_request)
{
    int storage_error = rocksdb::Status::kOk;
    for (int i = 0; i < request_length; ++i) {
        int e = on_request(requests[i]);
        if (e != rocksdb::Status::kOk) {
            LOG_ERROR_PREFIX("got storage engine error when handler request({})",
                             requests[i]->header->rpc_name);
            storage_error = e;
        }
    }
    return storage_error;
}

error_code replication_app_base::apply_mutation(const mutation *mu)
{
    FAIL_POINT_INJECT_F("replication_app_base_apply_mutation",
                        [](std::string_view) { return ERR_OK; });

    CHECK_EQ_PREFIX(mu->data.header.decree, last_committed_decree() + 1);
    CHECK_EQ_PREFIX(mu->data.updates.size(), mu->client_requests.size());
    CHECK_GT_PREFIX(mu->data.updates.size(), 0);

    if (_replica->status() == partition_status::PS_PRIMARY) {
        ADD_POINT(mu->_tracer);
    }

    bool has_ingestion_request = false;
    const int request_count = static_cast<int>(mu->client_requests.size());
    auto **batched_requests = ALLOC_STACK(message_ex *, request_count);
    auto **faked_requests = ALLOC_STACK(message_ex *, request_count);
    int batched_count = 0; // write-empties are not included.
    int faked_count = 0;
    for (int i = 0; i < request_count; ++i) {
        const mutation_update &update = mu->data.updates[i];
        LOG_DEBUG_PREFIX("mutation {} #{}: dispatch rpc call {}", mu->name(), i, update.code);
        if (update.code == RPC_REPLICATION_WRITE_EMPTY) {
            continue;
        }

        message_ex *req = mu->client_requests[i];
        if (req == nullptr) {
            req = message_ex::create_received_request(
                update.code,
                static_cast<dsn_msg_serialize_format>(update.serialization_type),
                update.data.data(),
                update.data.length());
            faked_requests[faked_count++] = req;
        }

        batched_requests[batched_count++] = req;
        if (update.code == apps::RPC_RRDB_RRDB_BULK_LOAD) {
            has_ingestion_request = true;
        }
    }

    const int storage_error = on_batched_write_requests(mu->data.header.decree,
                                                        mu->data.header.timestamp,
                                                        batched_requests,
                                                        batched_count,
                                                        mu->original_request);

    // release faked requests
    for (int i = 0; i < faked_count; ++i) {
        faked_requests[i]->release_ref();
    }

    if (storage_error != rocksdb::Status::kOk) {
        LOG_ERROR_PREFIX("mutation {}: get internal error {}", mu->name(), storage_error);
        // For normal write requests, if got rocksdb error, this replica will be set error and evoke
        // learn.
        // For ingestion requests, should not do as normal write requests, there are two reasons:
        //   1. All ingestion errors should be handled by meta server in function
        //      `on_partition_ingestion_reply`, rocksdb error will be returned to meta server in
        //      structure `ingestion_response`, not in this function.
        //   2. If replica apply ingestion mutation during learn, it may get error from rocksdb,
        //      because the external sst files may not exist, in this case, we won't consider it as
        //      an error.
        if (!has_ingestion_request) {
            switch (storage_error) {
            // TODO(yingchun): Now only kCorruption and kIOError are dealt, consider to deal with
            //  more storage engine errors.
            case rocksdb::Status::kCorruption:
                return ERR_RDB_CORRUPTION;
            case rocksdb::Status::kIOError:
                return ERR_DISK_IO_ERROR;
            default:
                return ERR_LOCAL_APP_FAILURE;
            }
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

    METRIC_VAR_INCREMENT_BY(committed_requests, batched_count);

    return ERR_OK;
}

error_code replication_app_base::update_init_info(replica *r,
                                                  int64_t private_log_offset,
                                                  int64_t durable_decree)
{
    _info.crc = 0;
    _info.magic = 0xdeadbeef;
    _info.init_ballot = r->get_ballot();
    _info.init_durable_decree = durable_decree;
    _info.init_offset_in_private_log = private_log_offset;

    LOG_AND_RETURN_NOT_OK(
        ERROR_PREFIX,
        utils::dump_rjobj_to_file(
            _info, utils::filesystem::path_combine(r->dir(), replica_init_info::kInitInfo)),
        "store replica_init_info failed");

    return ERR_OK;
}

error_code replication_app_base::update_init_info_ballot_and_decree(replica *r)
{
    return update_init_info(r, _info.init_offset_in_private_log, r->last_durable_decree());
}

const app_info *replication_app_base::get_app_info() const { return _replica->get_app_info(); }

} // namespace replication
} // namespace dsn
