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

#include "replica/replication_app_base.h"
#include "replica/duplication/mutation_duplicator.h"
#include "utils/fmt_logging.h"
#include "utils/filesystem.h"

#include "replica/replica.h"
#include "replica/replica_stub.h"
#include "replica/backup/cold_backup_context.h"

namespace dsn {
namespace replication {

DSN_DECLARE_int32(log_private_file_size_mb);

class mock_replication_app_base : public replication_app_base
{
public:
    explicit mock_replication_app_base(replica *replica) : replication_app_base(replica) {}

    error_code start(int, char **) override { return ERR_NOT_IMPLEMENTED; }
    error_code stop(bool) override { return ERR_NOT_IMPLEMENTED; }
    error_code sync_checkpoint() override { return ERR_OK; }
    error_code async_checkpoint(bool) override
    {
        _last_durable_decree = _expect_last_durable_decree;
        return ERR_OK;
    }
    error_code prepare_get_checkpoint(blob &) override { return ERR_NOT_IMPLEMENTED; }
    error_code get_checkpoint(int64_t learn_start,
                              const dsn::blob &learn_request,
                              dsn::replication::learn_state &state) override
    {
        state.to_decree_included = last_durable_decree();
        return ERR_OK;
    }
    error_code storage_apply_checkpoint(chkpt_apply_mode, const learn_state &) override
    {
        return ERR_OK;
    }
    error_code copy_checkpoint_to_dir(const char *checkpoint_dir,
                                      /*output*/ int64_t *last_decree,
                                      bool flush_memtable = false) override
    {
        if (last_decree != nullptr) {
            *last_decree = _decree;
        }

        utils::filesystem::create_file(fmt::format("{}/checkpoint.file", checkpoint_dir));
        return ERR_OK;
    }
    int on_request(message_ex *request) override { return 0; }
    std::string query_compact_state() const { return ""; };

    // we mock the followings
    void update_app_envs(const std::map<std::string, std::string> &envs) override { _envs = envs; }
    void query_app_envs(std::map<std::string, std::string> &out) override { out = _envs; }
    decree last_durable_decree() const override { return _last_durable_decree; }

    // TODO(heyuchen): implement this function in further pull request
    void set_partition_version(int32_t partition_version) override {}

    void set_ingestion_status(ingestion_status::type status) { _ingestion_status = status; }
    ingestion_status::type get_ingestion_status() override { return _ingestion_status; }

    uint32_t query_data_version() const { return 1; }

    manual_compaction_status::type query_compact_status() const
    {
        return manual_compaction_status::IDLE;
    }

    void set_last_durable_decree(decree d) { _last_durable_decree = d; }

    void set_expect_last_durable_decree(decree d) { _expect_last_durable_decree = d; }

private:
    std::map<std::string, std::string> _envs;
    decree _decree = 5;
    ingestion_status::type _ingestion_status;
    decree _last_durable_decree{0};
    decree _expect_last_durable_decree{0};
};

class mock_replica : public replica
{
public:
    mock_replica(replica_stub *stub,
                 gpid gpid,
                 const app_info &app,
                 const char *dir,
                 bool need_restore = false,
                 bool is_duplication_follower = false)
        : replica(stub, gpid, app, dir, need_restore, is_duplication_follower)
    {
        _app = make_unique<replication::mock_replication_app_base>(this);
    }

    void register_service()
    {
        _app->register_storage_engine("replica",
                                      replication_app_base::create<mock_replication_app_base>);
    }

    ~mock_replica() override
    {
        _config.status = partition_status::PS_INACTIVE;
        _tracker.wait_outstanding_tasks();
        _app.reset(nullptr);
    }

    void init_private_log(const std::string &log_dir)
    {
        utils::filesystem::remove_path(log_dir);

        _private_log =
            new mutation_log_private(log_dir, FLAGS_log_private_file_size_mb, get_gpid(), this);

        error_code err =
            _private_log->open(nullptr, [this](error_code err) { CHECK_EQ_PREFIX(err, ERR_OK); });
        CHECK_EQ_PREFIX(err, ERR_OK);
    }

    void init_private_log(mutation_log_ptr log) { _private_log = std::move(log); }

    replica_duplicator_manager &get_replica_duplicator_manager() { return *_duplication_mgr; }

    void as_primary() { _config.status = partition_status::PS_PRIMARY; }

    void as_secondary() { _config.status = partition_status::PS_SECONDARY; }

    void mock_max_gced_decree(decree d) { _max_gced_decree = d; }

    decree max_gced_decree_no_lock() const override
    {
        if (_max_gced_decree == (invalid_decree - 1)) {
            // if the value is not fake, return the real value from replica.
            return replica::max_gced_decree_no_lock();
        }
        return _max_gced_decree;
    }
    /// helper functions
    void set_replica_config(replica_configuration &config) { _config = config; }
    void set_partition_status(partition_status::type status) { _config.status = status; }
    void set_last_committed_decree(decree d) { _prepare_list->reset(d); }
    prepare_list *get_plist() const { return _prepare_list.get(); }
    void prepare_list_truncate(decree d) { _prepare_list->truncate(d); }
    void prepare_list_commit_hard(decree d) { _prepare_list->commit(d, COMMIT_TO_DECREE_HARD); }
    decree get_app_last_committed_decree() { return _app->last_committed_decree(); }
    void set_app_last_committed_decree(decree d) { _app->_last_committed_decree = d; }
    void set_primary_partition_configuration(partition_configuration &pconfig)
    {
        _primary_states.membership = pconfig;
    }
    partition_bulk_load_state get_secondary_bulk_load_state(const rpc_address &node)
    {
        return _primary_states.secondary_bulk_load_states[node];
    }
    void set_secondary_bulk_load_state(const rpc_address &node,
                                       const partition_bulk_load_state &state)
    {
        _primary_states.secondary_bulk_load_states[node] = state;
    }
    void set_is_empty_prepare_sent(bool flag)
    {
        _primary_states.ingestion_is_empty_prepare_sent = flag;
    }
    bool is_ingestion() { return _is_bulk_load_ingestion; }
    void set_is_ingestion(bool flag) { _is_bulk_load_ingestion = flag; }
    void set_ingestion_status(ingestion_status::type status) { _app->set_ingestion_status(status); }
    ingestion_status::type get_ingestion_status() { return _app->get_ingestion_status(); }
    bool is_primary_bulk_load_states_cleaned()
    {
        return (!_primary_states.ingestion_is_empty_prepare_sent &&
                _primary_states.secondary_bulk_load_states.size() == 0);
    }

    // mock cold backup related function.
    void generate_backup_checkpoint(cold_backup_context_ptr backup_context) override
    {
        if (backup_context->status() != ColdBackupCheckpointing) {
            LOG_INFO("{}: ignore generating backup checkpoint because backup_status = {}",
                     backup_context->name,
                     cold_backup_status_to_string(backup_context->status()));
            backup_context->ignore_checkpoint();
            return;
        }
        backup_context->complete_checkpoint();
    }

    void update_last_durable_decree(decree decree)
    {
        dynamic_cast<mock_replication_app_base *>(_app.get())->set_last_durable_decree(decree);
    }

    void update_expect_last_durable_decree(decree decree)
    {
        dynamic_cast<mock_replication_app_base *>(_app.get())
            ->set_expect_last_durable_decree(decree);
    }

private:
    decree _max_gced_decree{invalid_decree - 1};
};
typedef dsn::ref_ptr<mock_replica> mock_replica_ptr;

inline std::unique_ptr<mock_replica> create_mock_replica(replica_stub *stub,
                                                         int appid = 1,
                                                         int partition_index = 1,
                                                         const char *dir = "./")
{
    gpid gpid(appid, partition_index);
    app_info app_info;
    app_info.app_type = "replica";
    app_info.app_name = "temp";

    return make_unique<mock_replica>(stub, gpid, app_info, dir);
}

class mock_replica_stub : public replica_stub
{
public:
    mock_replica_stub() = default;

    ~mock_replica_stub() override = default;

    void add_replica(replica *r) { _replicas[r->get_gpid()] = replica_ptr(r); }

    mock_replica *add_primary_replica(int appid, int part_index = 1)
    {
        auto r = add_non_primary_replica(appid, part_index);
        r->as_primary();
        return r;
    }

    mock_replica *add_non_primary_replica(int appid, int part_index = 1)
    {
        auto r = create_mock_replica(this, appid, part_index).release();
        add_replica(r);
        mock_replicas[gpid(appid, part_index)] = r;
        return r;
    }

    mock_replica *find_replica(int appid, int part_index = 1)
    {
        return mock_replicas[gpid(appid, part_index)];
    }

    void set_state_connected() { _state = replica_node_state::NS_Connected; }

    rpc_address get_meta_server_address() const override { return rpc_address("127.0.0.2", 12321); }

    std::map<gpid, mock_replica *> mock_replicas;

    /// helper functions
    mock_replica_ptr
    generate_replica_ptr(const app_info &info,
                         gpid pid,
                         partition_status::type status = partition_status::PS_INACTIVE,
                         ballot b = 5,
                         bool need_restore = false,
                         bool is_duplication_follower = false)
    {
        replica_configuration config;
        config.ballot = b;
        config.pid = pid;
        config.status = status;

        mock_replica_ptr rep =
            new mock_replica(this, pid, info, "./", need_restore, is_duplication_follower);
        rep->set_replica_config(config);
        _replicas[pid] = rep;

        return rep;
    }

    replica *generate_replica(const app_info &info,
                              gpid pid,
                              partition_status::type status = partition_status::PS_INACTIVE,
                              ballot b = 5,
                              bool need_restore = false,
                              bool is_duplication_follower = false)
    {
        replica_configuration config;
        config.ballot = b;
        config.pid = pid;
        config.status = status;

        auto data_dirs = std::vector<std::string>{"./"};
        auto data_dirs_tag = std::vector<std::string>{"tag"};
        initialize_fs_manager(data_dirs, data_dirs_tag);
        auto *rep = new mock_replica(this, pid, info, "./", need_restore, is_duplication_follower);
        rep->set_replica_config(config);
        return rep;
    }

    void generate_replicas_base_dir_nodes_for_app(app_info mock_app,
                                                  int primary_count_for_disk = 1,
                                                  int secondary_count_for_disk = 2)
    {
        const auto &dir_nodes = _fs_manager._dir_nodes;
        for (auto &dir_node : dir_nodes) {
            const auto &replica_iter = dir_node->holding_replicas.find(mock_app.app_id);
            if (replica_iter == dir_node->holding_replicas.end()) {
                continue;
            }
            const std::set<gpid> &pids = replica_iter->second;
            int primary_count = primary_count_for_disk;
            int secondary_count = secondary_count_for_disk;
            for (const gpid &pid : pids) {
                // generate primary replica and secondary replica.
                if (primary_count-- > 0) {
                    add_replica(generate_replica_ptr(
                        mock_app, pid, partition_status::PS_PRIMARY, mock_app.app_id));
                } else if (secondary_count-- > 0) {
                    add_replica(generate_replica_ptr(
                        mock_app, pid, partition_status::PS_SECONDARY, mock_app.app_id));
                }
            }
        }
    }

    void set_log(mutation_log_ptr log) { _log = log; }

    int32_t get_bulk_load_downloading_count() const { return _bulk_load_downloading_count.load(); }
    void set_bulk_load_downloading_count(int32_t count)
    {
        _bulk_load_downloading_count.store(count);
    }

    void set_rpc_address(const rpc_address &address) { _primary_address = address; }
};

class mock_log_file : public log_file
{
public:
    mock_log_file(const std::string path, int index)
        : log_file(path.c_str(), nullptr, index, 0, false)
    {
    }

    void set_file_size(int size) { _end_offset = _start_offset + size; }
};
typedef dsn::ref_ptr<mock_log_file> mock_log_file_ptr;

class mock_mutation_log_private : public mutation_log_private
{
public:
    mock_mutation_log_private(dsn::gpid pid, dsn::replication::replica *r)
        : mutation_log_private("", 10, pid, r)
    {
    }

    dsn::task_ptr append(dsn::replication::mutation_ptr &mu,
                         dsn::task_code callback_code,
                         dsn::task_tracker *tracker,
                         dsn::aio_handler &&callback,
                         int hash = 0,
                         int64_t *pending_size = nullptr) override
    {
        _mu_list.push_back(mu);
        return nullptr;
    }

    void get_in_memory_mutations(decree start_decree,
                                 ballot start_ballot,
                                 std::vector<mutation_ptr> &mutation_list) const override
    {
        for (auto &mu : _mu_list) {
            ballot current_ballot =
                (start_ballot == invalid_ballot) ? invalid_ballot : mu->get_ballot();
            if ((mu->get_decree() >= start_decree && start_ballot == current_ballot) ||
                current_ballot > start_ballot) {
                mutation_list.push_back(mu);
            }
        }
    }

    static error_code replay(std::vector<std::string> &log_files,
                             replay_callback callback,
                             /*out*/ int64_t &end_offset)
    {
        return dsn::ERR_OK;
    }

    void add_log_file(dsn::replication::log_file_ptr lf) { _log_files[lf->index()] = lf; }

private:
    std::vector<dsn::replication::mutation_ptr> _mu_list;
};
typedef dsn::ref_ptr<mock_mutation_log_private> mock_mutation_log_private_ptr;

class mock_mutation_log_shared : public mutation_log_shared
{
public:
    mock_mutation_log_shared(const std::string &dir) : mutation_log_shared(dir, 1000, false) {}

    ::dsn::task_ptr append(mutation_ptr &mu,
                           dsn::task_code callback_code,
                           dsn::task_tracker *tracker,
                           aio_handler &&callback,
                           int hash = 0,
                           int64_t *pending_size = nullptr)
    {
        _mu_list.push_back(mu);
        return nullptr;
    }

    void flush() {}
    void flush_once() {}

private:
    std::vector<dsn::replication::mutation_ptr> _mu_list;
};
typedef dsn::ref_ptr<mock_mutation_log_shared> mock_mutation_log_shared_ptr;

struct mock_mutation_duplicator : public mutation_duplicator
{
    explicit mock_mutation_duplicator(replica_base *r) : mutation_duplicator(r) {}

    void duplicate(mutation_tuple_set mut, callback cb) override { _func(mut, cb); }

    typedef std::function<void(mutation_tuple_set, callback)> duplicate_function;
    static void mock(duplicate_function hook) { _func = std::move(hook); }
    static duplicate_function _func;
};

} // namespace replication
} // namespace dsn
