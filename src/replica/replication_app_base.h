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

#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <string>

#include "bulk_load_types.h"
#include "common/json_helper.h"
#include "common/replication_other_types.h"
#include "metadata_types.h"
#include "replica/replica_base.h"
#include "replica_admin_types.h"
#include "utils/error_code.h"
#include "utils/fmt_utils.h"
#include "utils/metrics.h"
#include "utils/ports.h"

namespace dsn {
class app_info;
class blob;
class message_ex;

namespace replication {
class learn_state;
class mutation;
class replica;

class replica_init_info
{
public:
    int32_t magic = 0;
    int32_t crc = 0;
    ballot init_ballot = 0;
    decree init_durable_decree = 0;
    int64_t init_offset_in_shared_log = 0; // Deprecated since Pegasus 2.6.0, but leave it to keep
                                           // compatible readability to read replica_init_info from
                                           // older Pegasus version.
    int64_t init_offset_in_private_log = 0;
    DEFINE_JSON_SERIALIZATION(init_ballot,
                              init_durable_decree,
                              init_offset_in_shared_log,
                              init_offset_in_private_log)

    static const std::string kInitInfo;

private:
    std::string to_string() const;
};

class replica_app_info
{
public:
    static const std::string kAppInfo;

private:
    app_info *_app;

public:
    replica_app_info(app_info *app) { _app = app; }
    error_code load(const std::string &fname);
    error_code store(const std::string &fname);
};

// This class stores and loads EEK, IV, and KV from KMS as a JSON file.
// To get the decrypted key, should POST EEK, IV, and KV to KMS.
struct kms_info
{
    std::string encrypted_key;         // a.k.a encrypted encryption key
    std::string initialization_vector; // a.k.a initialization vector
    std::string key_version;           // a.k.a key version
    DEFINE_JSON_SERIALIZATION(encrypted_key, initialization_vector, key_version)
    static const std::string kKmsInfo; // json file name

    kms_info(const std::string &e_key = "",
             const std::string &i = "",
             const std::string &k_version = "")
        : encrypted_key(e_key), initialization_vector(i), key_version(k_version)
    {
    }
};

/// The store engine interface of Pegasus.
/// Inherited by pegasus::pegasus_server_impl
/// Inherited by apps::rrdb_service
class replication_app_base : public replica_base
{
public:
    enum chkpt_apply_mode
    {
        copy,
        learn
    };

    template <typename T>
    static replication_app_base *create(replica *r)
    {
        return new T(r);
    }
    typedef replication_app_base *factory(replica *r);
    static void register_storage_engine(const std::string &name, factory f);
    static replication_app_base *new_storage_instance(const std::string &name, replica *r);
    static const std::string kDataDir;
    static const std::string kRdbDir;

    virtual ~replication_app_base() = default;

    [[nodiscard]] bool is_primary() const;

    // Whether this replica is duplicating as master.
    [[nodiscard]] virtual bool is_duplication_master() const;
    // Whether this replica is duplicating as follower.
    [[nodiscard]] virtual bool is_duplication_follower() const;

    [[nodiscard]] const ballot &get_ballot() const;

    //
    // Open the app.
    //
    error_code open();

    //
    // Close the app.
    // If `clear_state' is true, means clear the app state after close it.
    //
    // Must be thread safe.
    //
    error_code close(bool clear_state);

    error_code apply_checkpoint(chkpt_apply_mode mode, const learn_state &state);

    // Return code:
    //   - ERR_OK: everything is OK.
    //   - ERR_RDB_CORRUPTION: encountered some unrecoverable data errors, i.e. kCorruption from
    //     storage engine.
    //   - ERR_LOCAL_APP_FAILURE: other type of errors.
    error_code apply_mutation(const mutation *mu) WARN_UNUSED_RESULT;

    // methods need to implement on storage engine side
    virtual error_code start(int argc, char **argv) = 0;
    virtual error_code stop(bool clear_state) = 0;
    //
    // synchonously checkpoint, and update last_durable_decree internally.
    // which stops replication writes to the app concurrently.
    //
    // Postconditions:
    // * last_committed_decree() == last_durable_decree()
    //
    virtual error_code sync_checkpoint() = 0;
    //
    // asynchonously checkpoint, which will not stall the normal write ops.
    // replication layer will check last_durable_decree() later.
    //
    // Must be thread safe.
    //
    // It is not always necessary for the apps to implement this method,
    // but if it is implemented, the checkpoint logic in replication will be much simpler.
    //
    virtual error_code async_checkpoint(bool flush_memtable) = 0;
    //
    // prepare an app-specific learning request (on learner, to be sent to learnee
    // and used by method get_checkpoint), so that the learning process is more efficient
    //
    virtual error_code prepare_get_checkpoint(/*out*/ blob &learn_req) = 0;
    //
    // Learn [start, infinite) from remote replicas (learner)
    //
    // Must be thread safe.
    //
    // The learned checkpoint can be a complete checkpoint (0, infinite), or a delta checkpoint
    // [start, infinite), depending on the capability of the underlying implementation.
    //
    // Note the files in learn_state are copied from dir /replica@remote/data to dir
    // /replica@local/learn,
    // so when apply the learned file state, make sure using learn_dir() instead of data_dir() to
    // get the
    // full path of the files.
    //
    virtual error_code get_checkpoint(int64_t learn_start,
                                      const blob &learn_request,
                                      /*out*/ learn_state &state) = 0;
    //
    // [DSN_CHKPT_LEARN]
    // after learn the state from learner, apply the learned state to the local app
    //
    // Or,
    //
    // [DSN_CHKPT_COPY]
    // when an app only implement synchonous checkpoint, the primary replica
    // needs to copy checkpoint from secondaries instead of
    // doing checkpointing by itself, in order to not stall the normal
    // write operations.
    //
    // Postconditions:
    // * if mode == DSN_CHKPT_COPY, after apply_checkpoint() succeed:
    //   last_durable_decree() == state.to_decree_included
    // * if mode == DSN_CHKPT_LEARN, after apply_checkpoint() succeed:
    //   last_committed_decree() == last_durable_decree() == state.to_decree_included
    //
    virtual error_code storage_apply_checkpoint(chkpt_apply_mode mode,
                                                const learn_state &state) = 0;
    //
    // copy the latest checkpoint to checkpoint_dir, and the decree of the checkpoint
    // copied will be assigned to checkpoint_decree if checkpoint_decree not null
    //
    // must be thread safe
    //
    virtual error_code copy_checkpoint_to_dir(const char *checkpoint_dir,
                                              /*output*/ int64_t *last_decree,
                                              bool flush_memtable = false) = 0;

    //
    // Query methods.
    //

    // Get the decree of the last flushed mutation. -1 means failed to get.
    virtual replication::decree last_flushed_decree() const = 0;

    // Get the decree of the last created checkpoint.
    virtual replication::decree last_durable_decree() const = 0;

    // The return type is generated by storage engine, e.g. rocksdb::Status::Code, 0 always mean OK.
    virtual int on_request(message_ex *request) WARN_UNUSED_RESULT = 0;

    // Make an atomic request received from the client idempotent. Only called by primary replicas.
    //
    // Current implementation for atomic requests (incr, check_and_set and check_and_mutate) is
    // not idempotent. This function is used to translate them into requests like single put
    // which is naturally idempotent.
    //
    // For the other requests which must be idempotent such as single put/remove or non-batch
    // writes, this function would do nothing.
    //
    // Parameters:
    // - request: the original request received from a client.
    // - new_request: as the output parameter pointing to the resulting idempotent request if the
    // original request is atomic, otherwise keeping unchanged.
    //
    // Return:
    // - for an idempotent requess always return rocksdb::Status::kOk .
    // - for an atomic request, return rocksdb::Status::kOk if succeed in making it idempotent;
    // otherwise, return error code (rocksdb::Status::Code).
    virtual int make_idempotent(dsn::message_ex *request, dsn::message_ex **new_request) = 0;

    // Apply batched write requests from a mutation. This is a virtual function, and base class
    // provide a naive implementation that just call on_request for each request. Storage engine
    // may override this function to get better performance.
    //
    // Parameters:
    //  - decree: the decree of the mutation which these requests are batched into.
    //  - timestamp: an incremental timestamp generated for this batch of requests.
    //  - requests: the requests to be applied.
    //  - request_length: the number of the requests.
    //  - original_request: the original request received from the client. Must be an atomic
    //  request (i.e. incr, check_and_set and check_and_mutate) if non-null, and another
    //  parameter `requests` must hold the idempotent request translated from it. Used to
    //  reply to the client.
    //
    // Return rocksdb::Status::kOk or some error code (rocksdb::Status::Code) if these requests
    // failed to be applied by storage engine.
    virtual int on_batched_write_requests(int64_t decree,
                                          uint64_t timestamp,
                                          message_ex **requests,
                                          int request_length,
                                          message_ex *original_request);

    // Query compact state.
    [[nodiscard]] virtual std::string query_compact_state() const = 0;

    // update app envs.
    virtual void update_app_envs(const std::map<std::string, std::string> &envs) = 0;

    // query app envs.
    virtual void query_app_envs(/*out*/ std::map<std::string, std::string> &envs) = 0;

    // `partition_version` is used to guarantee data consistency during partition split.
    // In normal cases, partition_version = partition_count-1, when this replica rejects read
    // and write request, partition_version = -1.
    //
    // Thread-safe.
    virtual void set_partition_version(int32_t partition_version){};

    // dump the write request some info to string, it may need overload
    virtual std::string dump_write_request(message_ex *request) { return "write request"; };

    virtual void set_ingestion_status(ingestion_status::type status) {}

    virtual ingestion_status::type get_ingestion_status() { return ingestion_status::IS_INVALID; }

    virtual void on_detect_hotkey(const detect_hotkey_request &req,
                                  /*out*/ detect_hotkey_response &resp)
    {
        resp.err = ERR_OBJECT_NOT_FOUND;
        resp.__set_err_hint("on_detect_hotkey implementation not found");
    }

    // query pegasus data version
    virtual uint32_t query_data_version() const = 0;

    virtual manual_compaction_status::type query_compact_status() const = 0;

public:
    //
    // utility functions to be used by app
    //
    const std::string &data_dir() const { return _dir_data; }
    const std::string &learn_dir() const { return _dir_learn; }
    const std::string &backup_dir() const { return _dir_backup; }
    const std::string &bulk_load_dir() const { return _dir_bulk_load; }
    const std::string &duplication_dir() const { return _dir_duplication; }
    const app_info *get_app_info() const;
    replication::decree last_committed_decree() const { return _last_committed_decree.load(); }

private:
    // routines for replica internal usage
    friend class replica;
    friend class replica_stub;
    friend class mock_replica;
    friend class replica_disk_migrator;

    error_code open_internal(replica *r);
    error_code open_new_internal(replica *r, int64_t private_log_start);

    const replica_init_info &init_info() const { return _info; }
    error_code update_init_info(replica *r, int64_t private_log_offset, int64_t durable_decree);
    error_code update_init_info_ballot_and_decree(replica *r);

protected:
    std::string _dir_data;        // ${replica_dir}/data
    std::string _dir_learn;       // ${replica_dir}/learn
    std::string _dir_backup;      // ${replica_dir}/backup
    std::string _dir_bulk_load;   // ${replica_dir}/bulk_load
    std::string _dir_duplication; // ${replica_dir}/duplication
    replica *_replica;
    std::atomic<int64_t> _last_committed_decree;
    replica_init_info _info;

    explicit replication_app_base(replication::replica *replica);

private:
    METRIC_VAR_DECLARE_counter(committed_requests);
};
USER_DEFINED_ENUM_FORMATTER(replication_app_base::chkpt_apply_mode)
} // namespace replication
} // namespace dsn
