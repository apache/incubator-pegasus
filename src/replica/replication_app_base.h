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

#include <rocksdb/env.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <stdint.h>
#include <string.h>
#include <atomic>
#include <map>
#include <string>

#include "bulk_load_types.h"
#include "common/json_helper.h"
#include "common/replication_other_types.h"
#include "metadata_types.h"
#include "replica/replica_base.h"
#include "replica_admin_types.h"
#include "utils/defer.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/fmt_utils.h"
#include "utils/ports.h"

namespace dsn {
class app_info;
class blob;
class message_ex;

namespace replication {

class learn_state;
class mutation;
class replica;

namespace {
template <class T>
error_code write_blob_to_file(const std::string &fname,
                              const T &data,
                              const dsn::utils::FileDataType &fileDataType)
{
    std::string tmp_fname = fname + ".tmp";
    auto cleanup = defer([tmp_fname]() { utils::filesystem::remove_path(tmp_fname); });
    auto s = rocksdb::WriteStringToFile(dsn::utils::PegasusEnv(fileDataType),
                                        rocksdb::Slice(data.data(), data.length()),
                                        tmp_fname,
                                        /* should_sync */ true);
    LOG_AND_RETURN_NOT_TRUE(
        ERROR, s.ok(), ERR_FILE_OPERATION_FAILED, "write file {} failed", tmp_fname);
    LOG_AND_RETURN_NOT_TRUE(ERROR,
                            utils::filesystem::rename_path(tmp_fname, fname),
                            ERR_FILE_OPERATION_FAILED,
                            "move file from {} to {} failed",
                            tmp_fname,
                            fname);
    return ERR_OK;
}
} // namespace

class replica_init_info
{
public:
    int32_t magic;
    int32_t crc;
    ballot init_ballot;
    decree init_durable_decree;
    int64_t init_offset_in_shared_log;
    int64_t init_offset_in_private_log;
    DEFINE_JSON_SERIALIZATION(init_ballot,
                              init_durable_decree,
                              init_offset_in_shared_log,
                              init_offset_in_private_log)

    static const std::string kInitInfo;

public:
    replica_init_info() { memset((void *)this, 0, sizeof(*this)); }
    error_code load(const std::string &dir) WARN_UNUSED_RESULT;
    error_code store(const std::string &dir);
    std::string to_string();

private:
    error_code load_json(const std::string &fname);
    error_code store_json(const std::string &fname);
};

class replica_app_info
{
private:
    app_info *_app;

public:
    replica_app_info(app_info *app) { _app = app; }
    error_code load(const std::string &fname);
    error_code store(const std::string &fname);
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

    virtual ~replication_app_base() {}

    bool is_primary() const;

    // Whether this replica is duplicating as master.
    virtual bool is_duplication_master() const;
    // Whether this replica is duplicating as follower.
    virtual bool is_duplication_follower() const;

    const ballot &get_ballot() const;

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
    virtual replication::decree last_durable_decree() const = 0;
    // The return type is generated by storage engine, e.g. rocksdb::Status::Code, 0 always mean OK.
    virtual int on_request(message_ex *request) WARN_UNUSED_RESULT = 0;

    //
    // Parameters:
    //  - timestamp: an incremental timestamp generated for this batch of requests.
    //
    // The base class gives a naive implementation that just call on_request
    // repeatedly. Storage engine may override this function to get better performance.
    //
    // The return type is generated by storage engine, e.g. rocksdb::Status::Code, 0 always mean OK.
    virtual int on_batched_write_requests(int64_t decree,
                                          uint64_t timestamp,
                                          message_ex **requests,
                                          int request_length);

    // query compact state.
    virtual std::string query_compact_state() const = 0;

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
    error_code open_new_internal(replica *r, int64_t shared_log_start, int64_t private_log_start);

    const replica_init_info &init_info() const { return _info; }
    error_code update_init_info(replica *r,
                                int64_t shared_log_offset,
                                int64_t private_log_offset,
                                int64_t durable_decree);
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
};
USER_DEFINED_ENUM_FORMATTER(replication_app_base::chkpt_apply_mode)
} // namespace replication
} // namespace dsn
