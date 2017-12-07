#include <boost/lexical_cast.hpp>

#include "replica.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include "replication_app_base.h"
#include "../client_lib/block_service_manager.h"

namespace dsn {
namespace replication {

void replica::on_cold_backup(const backup_request &request, /*out*/ backup_response &response)
{
    check_hashed_access();

    const std::string &policy_name = request.policy.policy_name;
    auto backup_id = request.backup_id;
    cold_backup_context_ptr new_context(
        new cold_backup_context(this, request, _options->max_concurrent_uploading_file_count));

    if (status() == partition_status::type::PS_PRIMARY ||
        status() == partition_status::type::PS_SECONDARY) {
        cold_backup_context_ptr backup_context = nullptr;
        auto find = _cold_backup_contexts.find(policy_name);
        if (find != _cold_backup_contexts.end()) {
            backup_context = find->second;
        } else {
            /// TODO: policy may change provider
            dist::block_service::block_filesystem *block_service =
                _stub->_block_service_manager.get_block_filesystem(
                    request.policy.backup_provider_type);
            if (block_service == nullptr) {
                derror("%s: create cold backup block service failed, provider_type = %s, response "
                       "ERR_INVALID_PARAMETERS",
                       new_context->name,
                       request.policy.backup_provider_type.c_str());
                response.err = ERR_INVALID_PARAMETERS;
                return;
            }
            auto r = _cold_backup_contexts.insert(std::make_pair(policy_name, new_context));
            dassert(r.second, "");
            backup_context = r.first->second;
            backup_context->block_service = block_service;
            backup_context->backup_root = _options->cold_backup_root;
        }

        dassert(backup_context != nullptr, "");
        dassert(backup_context->request.policy.policy_name == policy_name,
                "%s VS %s",
                backup_context->request.policy.policy_name.c_str(),
                policy_name.c_str());
        cold_backup_status backup_status = backup_context->status();

        if (backup_context->request.backup_id < backup_id || backup_status == ColdBackupCanceled) {
            // clear obsoleted backup firstly
            /// TODO: clear dir
            ddebug("%s: clear obsoleted cold backup, old_backup_id = %" PRId64
                   ", old_backup_status = %s",
                   new_context->name,
                   backup_context->request.backup_id,
                   cold_backup_status_to_string(backup_status));
            backup_context->cancel();
            _cold_backup_contexts.erase(policy_name);
            on_cold_backup(request, response);
            return;
        }

        if (backup_context->request.backup_id > backup_id) {
            // backup_id is outdated
            derror("%s: request outdated cold backup, current_backup_id = %" PRId64
                   ", response ERR_VERSION_OUTDATED",
                   new_context->name,
                   backup_context->request.backup_id);
            response.err = ERR_VERSION_OUTDATED;
            return;
        }

        // for secondary, request is already filtered by primary, so if
        //      request is repeated, so generate_backup_checkpoint is already running, we do
        //      nothing;
        //      request is new, we should call generate_backup_checkpoint;

        // TODO: if secondary's status have changed, how to process the _cold_backup_state,
        // and how to process the backup_status, cancel/pause
        if (status() == partition_status::PS_SECONDARY) {
            if (backup_status == ColdBackupInvalid) {
                // new backup_request, should set status to ColdBackupChecked to allow secondary
                // can start to checkpoint
                backup_context->start_check();
                backup_context->complete_check(false);
                if (backup_context->start_checkpoint()) {
                    _stub->_counter_cold_backup_recent_start_count.increment();
                    tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP, this, [this, backup_context]() {
                        generate_backup_checkpoint(backup_context);
                    });
                }
            }
            return;
        }

        send_backup_request_to_secondary(request);

        if (backup_status == ColdBackupChecking || backup_status == ColdBackupCheckpointing ||
            backup_status == ColdBackupUploading) {
            // do nothing
            ddebug("%s: backup is busy, status = %s, progress = %d, response ERR_BUSY",
                   backup_context->name,
                   cold_backup_status_to_string(backup_status),
                   backup_context->progress());
            response.err = ERR_BUSY;
        } else if (backup_status == ColdBackupInvalid && backup_context->start_check()) {
            _stub->_counter_cold_backup_recent_start_count.increment();
            ddebug("%s: start checking backup on remote, response ERR_BUSY", backup_context->name);
            tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP, nullptr, [backup_context]() {
                backup_context->check_backup_on_remote();
            });
            response.err = ERR_BUSY;
        } else if (backup_status == ColdBackupChecked && backup_context->start_checkpoint()) {
            // start generating checkpoint
            ddebug("%s: start generating checkpoint, response ERR_BUSY", backup_context->name);
            tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP, this, [this, backup_context]() {
                generate_backup_checkpoint(backup_context);
            });
            response.err = ERR_BUSY;
        } else if ((backup_status == ColdBackupCheckpointed || backup_status == ColdBackupPaused) &&
                   backup_context->start_upload()) {
            // start uploading checkpoint
            ddebug("%s: start uploading checkpoint, response ERR_BUSY", backup_context->name);
            tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP, nullptr, [backup_context]() {
                backup_context->upload_checkpoint_to_remote();
            });
            response.err = ERR_BUSY;
        } else if (backup_status == ColdBackupFailed) {
            derror("%s: upload checkpoint failed, reason = %s, response ERR_LOCAL_APP_FAILURE",
                   backup_context->name,
                   backup_context->reason());
            response.err = ERR_LOCAL_APP_FAILURE;
            backup_context->cancel();
            _cold_backup_contexts.erase(policy_name);
        } else if (backup_status == ColdBackupCompleted) {
            ddebug("%s: upload checkpoint completed, response ERR_OK", backup_context->name);
            response.err = ERR_OK;
        } else {
            dwarn(
                "%s: unhandled case, handle_status = %s, real_time_status = %s, response ERR_BUSY",
                backup_context->name,
                cold_backup_status_to_string(backup_status),
                cold_backup_status_to_string(backup_context->status()));
            response.err = ERR_BUSY;
        }

        response.progress = backup_context->progress();
        ddebug("%s: backup progress is %d", backup_context->name, response.progress);
    } else {
        derror(
            "%s: invalid state for cold backup, partition_status = %s, response ERR_INVALID_STATE",
            new_context->name,
            enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
    }
}

void replica::send_backup_request_to_secondary(const backup_request &request)
{
    for (const auto &target_address : _primary_states.membership.secondaries) {
        // primary will send backup_request to secondary periodically
        // so, we shouldn't handler the response
        rpc::call_one_way_typed(
            target_address, RPC_COLD_BACKUP, request, gpid_to_thread_hash(get_gpid()));
    }
}

// backup/backup.<policy_name>.<backup_id>.<decree>.<timestamp>
static std::string backup_get_dir_name(const std::string &policy_name,
                                       int64_t backup_id,
                                       int64_t decree,
                                       int64_t timestamp)
{
    char buffer[256];
    sprintf(buffer,
            "backup.%s.%" PRId64 ".%" PRId64 ".%" PRId64 "",
            policy_name.c_str(),
            backup_id,
            decree,
            timestamp);
    return std::string(buffer);
}

static bool backup_parse_dir_name(const char *name,
                                  std::string &policy_name,
                                  int64_t &backup_id,
                                  int64_t &decree,
                                  int64_t &timestamp)
{
    std::vector<std::string> strs;
    ::dsn::utils::split_args(name, strs, '.');
    if (strs.size() < 5) {
        return false;
    } else {
        policy_name = strs[1];
        backup_id = boost::lexical_cast<int64_t>(strs[2]);
        decree = boost::lexical_cast<int64_t>(strs[3]);
        timestamp = boost::lexical_cast<int64_t>(strs[4]);
        return (std::string(name) ==
                backup_get_dir_name(policy_name, backup_id, decree, timestamp));
    }
}

// backup/backup.<policy_name>.<backup_id>.<decree>.<timestamp>.tmp.<timestamp>
static std::string backup_get_temp_dir_name(const std::string &policy_name,
                                            int64_t backup_id,
                                            int64_t decree,
                                            int64_t timestamp,
                                            int64_t temp_timestamp)
{
    char buffer[256];
    sprintf(buffer,
            "backup.%s.%" PRId64 ".%" PRId64 ".%" PRId64 ".tmp.%" PRId64,
            policy_name.c_str(),
            backup_id,
            decree,
            timestamp,
            temp_timestamp);
    return std::string(buffer);
}

static bool backup_parse_temp_dir_name(const char *name,
                                       std::string &policy_name,
                                       int64_t &backup_id,
                                       int64_t &decree,
                                       int64_t &timestamp,
                                       int64_t &temp_timestamp)
{
    std::vector<std::string> strs;
    ::dsn::utils::split_args(name, strs, '.');
    if (strs.size() < 7) {
        return false;
    } else {
        policy_name = strs[1];
        backup_id = boost::lexical_cast<int64_t>(strs[2]);
        decree = boost::lexical_cast<int64_t>(strs[3]);
        timestamp = boost::lexical_cast<int64_t>(strs[4]);
        temp_timestamp = boost::lexical_cast<int64_t>(strs[6]);
        return (
            std::string(name) ==
            backup_get_temp_dir_name(policy_name, backup_id, decree, timestamp, temp_timestamp));
    }
}

// data/checkpoint.<decree>
static std::string checkpoint_get_dir_name(int64_t decree)
{
    char buffer[256];
    sprintf(buffer, "checkpoint.%" PRId64 "", decree);
    return std::string(buffer);
}

static bool checkpoint_parse_dir_name(const char *name, int64_t &decree)
{
    return 1 == sscanf(name, "checkpoint.%" PRId64 "", &decree) &&
           std::string(name) == checkpoint_get_dir_name(decree);
}

struct backup_dir_info
{
    std::string checkpoint_dir; // full path
    std::string policy_name;
    int64_t backup_id;
    int64_t decree;
    int64_t timestamp;
    int64_t temp_timestamp;
    backup_dir_info() : backup_id(0), decree(0), timestamp(0), temp_timestamp(0) {}
};

// run in REPLICATION_LONG thread
// Effection:
// - may ignore_checkpoint() if in invalid status
// - may fail_checkpoint() if some error occurs
// - may complete_checkpoint() and schedule on_cold_backup() if backup checkpoint dir is already
// exist
// - may schedule trigger_async_checkpoint_for_backup() if backup checkpoint dir is not exist

// FIX: if replica is secondary call generate_backup_checkpoint, then it's be upgrade to primary
// and then call generate_backup_checkpoint again, this will may be generate two backup_checkpoint
// how to solve this, problem
void replica::generate_backup_checkpoint(cold_backup_context_ptr backup_context)
{
    if (backup_context->status() != ColdBackupCheckpointing) {
        ddebug("%s: ignore generating backup checkpoint because backup_status = %s",
               backup_context->name,
               cold_backup_status_to_string(backup_context->status()));
        backup_context->ignore_checkpoint();
        return;
    }

    // prepare back dir
    auto backup_dir = _app->backup_dir();
    if (!dsn::utils::filesystem::directory_exists(backup_dir) &&
        !dsn::utils::filesystem::create_directory(backup_dir)) {
        derror("%s: create backup dir %s failed", backup_context->name, backup_dir.c_str());
        backup_context->fail_checkpoint("create backup dir failed");
        return;
    }

    // list sub dirs of back/
    std::vector<std::string> sub_dirs;
    if (!dsn::utils::filesystem::get_subdirectories(backup_dir, sub_dirs, false)) {
        derror(
            "%s: list sub dirs of backup dir %s failed", backup_context->name, backup_dir.c_str());
        backup_context->fail_checkpoint("list sub dirs of backup dir failed");
        return;
    }

    // parse sub dirs of back/
    std::vector<backup_dir_info> backup_checkpoint_dirs;
    std::vector<backup_dir_info> backup_checkpoint_temp_dirs;
    for (std::string &d : sub_dirs) {
        std::string d1 = d.substr(backup_dir.size() + 1);
        backup_dir_info info;
        if (backup_parse_dir_name(
                d1.c_str(), info.policy_name, info.backup_id, info.decree, info.timestamp)) {
            info.checkpoint_dir = d;
            backup_checkpoint_dirs.push_back(std::move(info));
        } else if (backup_parse_temp_dir_name(d1.c_str(),
                                              info.policy_name,
                                              info.backup_id,
                                              info.decree,
                                              info.timestamp,
                                              info.temp_timestamp)) {
            info.checkpoint_dir = d;
            backup_checkpoint_temp_dirs.push_back(std::move(info));
        } else {
            dwarn("%s: unrecognized dir %s", backup_context->name, d.c_str());
        }
    }

    // find proper dir
    backup_dir_info checkpoint_info;
    for (backup_dir_info &b : backup_checkpoint_dirs) {
        if (b.policy_name == backup_context->request.policy.policy_name &&
            b.backup_id == backup_context->request.backup_id) {
            checkpoint_info = b;
            break;
        }
    }

    if (!checkpoint_info.checkpoint_dir.empty()) {
        // list sub files of backup checkpoint dir
        std::vector<std::string> sub_files;
        if (!dsn::utils::filesystem::get_subfiles(
                checkpoint_info.checkpoint_dir, sub_files, false)) {
            derror("%s: list sub files of backup checkpoint dir %s failed",
                   backup_context->name,
                   checkpoint_info.checkpoint_dir.c_str());
            backup_context->fail_checkpoint("list sub files of backup checkpoint dir failed");
            return;
        }

        std::vector<std::string> backup_checkpoint_files;
        std::vector<int64_t> backup_checkpoint_file_sizes;
        int64_t backup_checkpoint_file_total_size = 0;
        for (std::string &f : sub_files) {
            int64_t file_size = 0;
            if (!::dsn::utils::filesystem::file_size(f, file_size)) {
                derror("%s: get file size of %s failed", backup_context->name, f.c_str());
                backup_context->fail_checkpoint("get file size failed");
                return;
            }
            std::string f1 = f.substr(checkpoint_info.checkpoint_dir.size() + 1);
            backup_checkpoint_files.push_back(f1);
            backup_checkpoint_file_sizes.push_back(file_size);
            backup_checkpoint_file_total_size += file_size;
        }

        backup_context->checkpoint_decree = checkpoint_info.decree;
        backup_context->checkpoint_timestamp = checkpoint_info.timestamp;
        backup_context->checkpoint_dir = checkpoint_info.checkpoint_dir;
        backup_context->checkpoint_files = std::move(backup_checkpoint_files);
        backup_context->checkpoint_file_sizes = std::move(backup_checkpoint_file_sizes);
        backup_context->checkpoint_file_total_size = backup_checkpoint_file_total_size;
        backup_context->complete_checkpoint();

        ddebug(
            "%s: backup checkpoint aleady exist, dir = %s, file_count = %d, total_size = %" PRId64,
            backup_context->name,
            backup_context->checkpoint_dir.c_str(),
            (int)backup_checkpoint_files.size(),
            backup_checkpoint_file_total_size);
        // TODO: in primary, this will make the request send to secondary again
        tasking::enqueue(LPC_REPLICATION_COLD_BACKUP,
                         this,
                         [this, backup_context]() {
                             backup_response response;
                             on_cold_backup(backup_context->request, response);
                         },
                         gpid_to_thread_hash(get_gpid()));
    } else {
        ddebug("%s: backup checkpoint not exist, start to trigger async checkpoint",
               backup_context->name);
        tasking::enqueue(
            LPC_REPLICATION_COLD_BACKUP,
            this,
            [this, backup_context]() { trigger_async_checkpoint_for_backup(backup_context); },
            gpid_to_thread_hash(get_gpid()));
    }

    // clear old dirs
    for (backup_dir_info &b : backup_checkpoint_dirs) {
        ddebug("%s: found old backup dir(%s), backup_id(%" PRId64 ")",
               backup_context->name,
               b.checkpoint_dir.c_str(),
               b.backup_id);
        if (b.policy_name == backup_context->request.policy.policy_name &&
            b.backup_id < backup_context->request.backup_id) {
            if (dsn::utils::filesystem::remove_path(b.checkpoint_dir))
                ddebug("%s: removed old checkpoint dir %s",
                       backup_context->name,
                       b.checkpoint_dir.c_str());
            else
                dwarn("%s: remove old checkpoint dir %s failed",
                      backup_context->name,
                      b.checkpoint_dir.c_str());
        }
    }

    for (backup_dir_info &b : backup_checkpoint_temp_dirs) {
        ddebug("%s: found old backup dir(%s), backup_id(%" PRId64 ")",
               backup_context->name,
               b.checkpoint_dir.c_str(),
               b.backup_id);
        if (b.policy_name == backup_context->request.policy.policy_name &&
            b.backup_id < backup_context->request.backup_id) {
            if (dsn::utils::filesystem::remove_path(b.checkpoint_dir))
                ddebug("%s: removed old checkpoint temp dir %s",
                       backup_context->name,
                       b.checkpoint_dir.c_str());
            else
                dwarn("%s: remove old checkpoint temp dir %s failed",
                      backup_context->name,
                      b.checkpoint_dir.c_str());
        }
    }
}

// run in REPLICATION thread
// Effection:
// - may ignore_checkpoint() if in invalid status
// - may fail_checkpoint() if some error occurs
// - may trigger async checkpoint and invoke wait_async_checkpoint_for_backup()
void replica::trigger_async_checkpoint_for_backup(cold_backup_context_ptr backup_context)
{
    check_hashed_access();

    if (backup_context->status() != ColdBackupCheckpointing) {
        ddebug("%s: ignore triggering async checkpoint because backup_status = %s",
               backup_context->name,
               cold_backup_status_to_string(backup_context->status()));
        backup_context->ignore_checkpoint();
        return;
    }

    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY) {
        ddebug("%s: ignore triggering async checkpoint because partition_status = %s",
               backup_context->name,
               enum_to_string(status()));
        backup_context->ignore_checkpoint();
        return;
    }

    // after triggering init_checkpoint, we just wait until it finish
    if (backup_context->checkpoint_decree > 0) {
        char time_buf[20];
        dsn::utils::time_ms_to_date_time(backup_context->checkpoint_timestamp, time_buf, 20);
        ddebug("%s: do not trigger async checkpoint because it is already triggered, "
               "checkpoint_decree = %" PRId64 ", checkpoint_timestamp = %" PRId64 " (%s)",
               backup_context->name,
               backup_context->checkpoint_decree,
               backup_context->checkpoint_timestamp,
               time_buf);
    } else {
        backup_context->checkpoint_decree = last_committed_decree();
        backup_context->checkpoint_timestamp = dsn_now_ms();
        char time_buf[20];
        dsn::utils::time_ms_to_date_time(backup_context->checkpoint_timestamp, time_buf, 20);
        ddebug("%s: trigger async checkpoint, "
               "checkpoint_decree = %" PRId64 ", checkpoint_timestamp = %" PRId64 " (%s)",
               backup_context->name,
               backup_context->checkpoint_decree,
               backup_context->checkpoint_timestamp,
               time_buf);
        init_checkpoint(true);
    }

    wait_async_checkpoint_for_backup(backup_context);
}

// run in REPLICATION thread
// Effection:
// - may ignore_checkpoint() if in invalid status
// - may delay some time and schedule trigger_async_checkpoint_for_backup() if async checkpoint not
// completed
// - may schedule local_copy_backup_checkpoint if async checkpoint completed
void replica::wait_async_checkpoint_for_backup(cold_backup_context_ptr backup_context)
{
    check_hashed_access();

    if (backup_context->status() != ColdBackupCheckpointing) {
        ddebug("%s: ignore waiting async checkpoint because backup_status = %s",
               backup_context->name,
               cold_backup_status_to_string(backup_context->status()));
        backup_context->ignore_checkpoint();
        return;
    }

    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY) {
        ddebug("%s: ignore waiting async checkpoint because partition_status = %s",
               backup_context->name,
               enum_to_string(status()));
        backup_context->ignore_checkpoint();
        return;
    }

    decree du = last_durable_decree();
    if (du < backup_context->checkpoint_decree) {
        ddebug("%s: async checkpoint not done, we just wait it done, "
               "last_durable_decree = %" PRId64 ", backup_checkpoint_decree = %" PRId64,
               backup_context->name,
               du,
               backup_context->checkpoint_decree);
        tasking::enqueue(
            LPC_REPLICATION_COLD_BACKUP,
            this,
            [this, backup_context]() { trigger_async_checkpoint_for_backup(backup_context); },
            gpid_to_thread_hash(get_gpid()),
            std::chrono::seconds(10));
    } else {
        ddebug("%s: async checkpoint done, last_durable_decree = %" PRId64
               ", backup_context->checkpoint_decree = %" PRId64,
               backup_context->name,
               du,
               backup_context->checkpoint_decree);
        tasking::enqueue(LPC_BACKGROUND_COLD_BACKUP, this, [this, backup_context]() {
            local_copy_backup_checkpoint(backup_context);
        });
    }
}

class dir_auto_deleter
{
public:
    dir_auto_deleter(const std::string &dir) : _dir(dir) {}
    ~dir_auto_deleter()
    {
        if (::dsn::utils::filesystem::directory_exists(_dir))
            ::dsn::utils::filesystem::remove_path(_dir);
    }

private:
    std::string _dir;
};

// run in REPLICATION_LONG thread
// Effection:
// - may ignore_checkpoint() if in invalid status
// - may fail_checkpoint() if some error occurs
// - may complete_checkpoint() and schedule on_cold_backup() if checkpoint dir is successfully
// copied
// - may delay some time and schedule generate_backup_checkpoint() if no proper checkpoint exist for
// backup
void replica::local_copy_backup_checkpoint(cold_backup_context_ptr backup_context)
{
    if (backup_context->status() != ColdBackupCheckpointing) {
        ddebug("%s: ignore generating backup checkpoint because backup_status = %s",
               backup_context->name,
               cold_backup_status_to_string(backup_context->status()));
        backup_context->ignore_checkpoint();
        return;
    }

    // prepare backup checkpoint variables
    std::string backup_checkpoint_dir = ::dsn::utils::filesystem::path_combine(
        _app->backup_dir(),
        backup_get_dir_name(backup_context->request.policy.policy_name,
                            backup_context->request.backup_id,
                            backup_context->checkpoint_decree,
                            backup_context->checkpoint_timestamp));
    std::vector<std::string> backup_checkpoint_files;
    std::vector<int64_t> backup_checkpoint_file_sizes;
    int64_t backup_checkpoint_file_total_size = 0;

    // create temp dir
    std::string backup_checkpoint_temp_dir = ::dsn::utils::filesystem::path_combine(
        _app->backup_dir(),
        backup_get_temp_dir_name(backup_context->request.policy.policy_name,
                                 backup_context->request.backup_id,
                                 backup_context->checkpoint_decree,
                                 backup_context->checkpoint_timestamp,
                                 dsn_now_ms()));
    if (!::dsn::utils::filesystem::create_directory(backup_checkpoint_temp_dir)) {
        derror("%s: create temp dir %s failed",
               backup_context->name,
               backup_checkpoint_temp_dir.c_str());
        backup_context->fail_checkpoint("create temp dir failed");
        return;
    }
    dir_auto_deleter temp_dir_deleter(backup_checkpoint_temp_dir);

    dassert(backup_context->checkpoint_timestamp > 0, "");
    if (backup_context->checkpoint_decree > 0) {
        // list checkpoint dirs under data/
        auto data_dir = _app->data_dir();
        std::vector<std::string> sub_dirs;
        if (!::dsn::utils::filesystem::get_subdirectories(data_dir, sub_dirs, false)) {
            derror("%s: list sub directories of data dir %s failed",
                   backup_context->name,
                   data_dir.c_str());
            backup_context->fail_checkpoint("list sub files of data dir failed");
            return;
        }

        // parse dirs
        std::vector<int64_t> checkpoint_decrees;
        for (std::string &d : sub_dirs) {
            std::string d1 = d.substr(data_dir.size() + 1);
            int64_t ci;
            if (checkpoint_parse_dir_name(d1.c_str(), ci)) {
                checkpoint_decrees.push_back(ci);
            }
        }
        std::sort(checkpoint_decrees.begin(), checkpoint_decrees.end());

        // find proper dir (minimal decree which is no less than checkpoint_decree)
        int64_t proper_decree = 0;
        for (int64_t d : checkpoint_decrees) {
            if (d >= backup_context->checkpoint_decree) {
                proper_decree = d;
                break;
            }
        }

        // no proper dir
        if (proper_decree == 0) {
            ddebug("%s: no proper checkpoint exist for backup, retry to generate again after 30 "
                   "seconds, "
                   "checkpoint_decree = %" PRId64 ", checkpoint_timestamp = %" PRId64,
                   backup_context->name,
                   backup_context->checkpoint_decree,
                   backup_context->checkpoint_timestamp);
            tasking::enqueue(
                LPC_BACKGROUND_COLD_BACKUP,
                this,
                [this, backup_context]() { generate_backup_checkpoint(backup_context); },
                0,
                std::chrono::seconds(30));
            return;
        }

        // list sub files in checkpoint dir
        std::string checkpoint_dir = ::dsn::utils::filesystem::path_combine(
            data_dir, checkpoint_get_dir_name(proper_decree));
        std::vector<std::string> sub_files;
        if (!::dsn::utils::filesystem::get_subfiles(checkpoint_dir, sub_files, false)) {
            derror("%s: list sub files of checkpoint dir %s failed",
                   backup_context->name,
                   checkpoint_dir.c_str());
            backup_context->fail_checkpoint("list sub files of checkpoint dir failed");
            return;
        }
        std::sort(sub_files.begin(), sub_files.end());

        // get file sizes in checkpoint dir
        std::vector<int64_t> sub_file_sizes;
        for (std::string &f : sub_files) {
            int64_t file_size = 0;
            if (!::dsn::utils::filesystem::file_size(f, file_size)) {
                derror("%s: get file size of %s failed", backup_context->name, f.c_str());
                backup_context->fail_checkpoint("get file size failed");
                return;
            }
            sub_file_sizes.push_back(file_size);
        }

        // hard link files
        for (std::string &from_file : sub_files) {
            std::string f = from_file.substr(checkpoint_dir.size() + 1);
            std::string to_file =
                ::dsn::utils::filesystem::path_combine(backup_checkpoint_temp_dir, f);
            if (!::dsn::utils::filesystem::link_file(from_file, to_file)) {
                derror("%s: link file from %s to %s failed",
                       backup_context->name,
                       from_file.c_str(),
                       to_file.c_str());
                backup_context->fail_checkpoint("link file failed");
                return;
            }
        }

        // list sub files in temp dir
        std::vector<std::string> temp_sub_files;
        if (!::dsn::utils::filesystem::get_subfiles(
                backup_checkpoint_temp_dir, temp_sub_files, false)) {
            derror("%s: list sub files of temp dir %s failed",
                   backup_context->name,
                   backup_checkpoint_temp_dir.c_str());
            backup_context->fail_checkpoint("list sub files of temp dir failed");
            return;
        }
        std::sort(temp_sub_files.begin(), temp_sub_files.end());

        // get file sizes in temp dir
        std::vector<int64_t> temp_sub_file_sizes;
        for (std::string &f : temp_sub_files) {
            int64_t file_size = 0;
            if (!::dsn::utils::filesystem::file_size(f, file_size)) {
                derror("%s: get file size of %s failed", backup_context->name, f.c_str());
                backup_context->fail_checkpoint("get file size failed");
                return;
            }
            temp_sub_file_sizes.push_back(file_size);
        }

        // check consistency
        if (temp_sub_files.size() != sub_files.size()) {
            derror("%s: check consistency of temp dir %s with checkpoint dir %s failed, "
                   "temp_dir_file_count = %d, checkpoint_dir_file_count = %d",
                   backup_context->name,
                   backup_checkpoint_temp_dir.c_str(),
                   checkpoint_dir.c_str(),
                   (int)temp_sub_files.size(),
                   (int)sub_files.size());
            backup_context->fail_checkpoint("check consistency of temp dir failed");
            return;
        }
        for (int i = 0; i < sub_files.size(); ++i) {
            std::string temp_file = temp_sub_files[i].substr(backup_checkpoint_temp_dir.size() + 1);
            std::string checkpoint_file = sub_files[i].substr(checkpoint_dir.size() + 1);
            if (temp_file != checkpoint_file || temp_sub_file_sizes[i] != sub_file_sizes[i]) {
                derror("%s: check consistency of temp dir %s with checkpoint dir %s failed, "
                       "temp_file_name = %s, temp_file_size = %" PRId64 ", "
                       "checkpoint_file_name = %s, checkpoint_file_size = %" PRId64 "",
                       backup_context->name,
                       backup_checkpoint_temp_dir.c_str(),
                       checkpoint_dir.c_str(),
                       temp_file.c_str(),
                       temp_sub_file_sizes[i],
                       checkpoint_file.c_str(),
                       sub_file_sizes[i]);
                backup_context->fail_checkpoint("check consistency of temp dir failed");
                return;
            }
            backup_checkpoint_files.push_back(temp_file);
            backup_checkpoint_file_sizes.push_back(temp_sub_file_sizes[i]);
            backup_checkpoint_file_total_size += temp_sub_file_sizes[i];
        }

        // list sub files in checkpoint dir again
        std::vector<std::string> sub_files_2;
        if (!::dsn::utils::filesystem::get_subfiles(checkpoint_dir, sub_files_2, false)) {
            derror("%s: list sub files of checkpoint dir %s failed",
                   backup_context->name,
                   checkpoint_dir.c_str());
            backup_context->fail_checkpoint("list sub files of checkpoint dir failed");
            return;
        }
        std::sort(sub_files_2.begin(), sub_files_2.end());

        // compare sub_files and sub_files_2, if not equal, means the checkpoint dir is under
        // deletion,
        // so it is possiable that the copied temp dir is not complete, we should discard it and
        // retry later.
        if (sub_files != sub_files_2) {
            derror("%s: checkpoint dir %s may be under deletion, retry to generate again after 30 "
                   "seconds, "
                   "checkpoint_decree = %" PRId64 ", checkpoint_timestamp = %" PRId64,
                   backup_context->name,
                   checkpoint_dir.c_str(),
                   backup_context->checkpoint_decree,
                   backup_context->checkpoint_timestamp);
            tasking::enqueue(
                LPC_BACKGROUND_COLD_BACKUP,
                this,
                [this, backup_context]() { generate_backup_checkpoint(backup_context); },
                0,
                std::chrono::seconds(30));
            return;
        }
    }

    if (!::dsn::utils::filesystem::rename_path(backup_checkpoint_temp_dir, backup_checkpoint_dir)) {
        derror("%s: rename temp directory %s to %s failed",
               backup_context->name,
               backup_checkpoint_temp_dir.c_str(),
               backup_checkpoint_dir.c_str());
        backup_context->fail_checkpoint("rename temp directory failed");
        return;
    }

    ddebug(
        "%s: generate backup checkpoint succeed, dir = %s, file_count = %d, total_size = %" PRId64,
        backup_context->name,
        backup_checkpoint_dir.c_str(),
        (int)backup_checkpoint_files.size(),
        backup_checkpoint_file_total_size);
    backup_context->checkpoint_dir = backup_checkpoint_dir;
    backup_context->checkpoint_files = std::move(backup_checkpoint_files);
    backup_context->checkpoint_file_sizes = std::move(backup_checkpoint_file_sizes);
    backup_context->checkpoint_file_total_size = backup_checkpoint_file_total_size;
    backup_context->complete_checkpoint();
    tasking::enqueue(LPC_REPLICATION_COLD_BACKUP,
                     this,
                     [this, backup_context]() {
                         backup_response response;
                         on_cold_backup(backup_context->request, response);
                     },
                     gpid_to_thread_hash(get_gpid()));
}

void replica::set_backup_context_cancel()
{
    for (auto &pair : _cold_backup_contexts) {
        pair.second->cancel();
        ddebug("%s: cancel backup progress, backup_request = %s",
               name(),
               boost::lexical_cast<std::string>(pair.second->request).c_str());
    }
}

void replica::set_backup_context_pause()
{
    for (auto &pair : _cold_backup_contexts) {
        pair.second->pause_upload();
        ddebug("%s: pause backup progress, backup_request = %s",
               name(),
               boost::lexical_cast<std::string>(pair.second->request).c_str());
    }
}

void replica::clear_cold_backup_state()
{
    ddebug("%s: clear cold_backup_states", name());
    _cold_backup_contexts.clear();
}

void replica::collect_backup_info()
{
    uint64_t cold_backup_running_count = 0;
    uint64_t cold_backup_max_duration_time_ms = 0;
    uint64_t cold_backup_max_upload_file_size = 0;
    if (!_cold_backup_contexts.empty()) {
        for (const auto &p : _cold_backup_contexts) {
            const cold_backup_context_ptr &backup_context = p.second;
            cold_backup_status backup_status = backup_context->status();
            if (status() == partition_status::type::PS_PRIMARY) {
                if (backup_status != ColdBackupInvalid && backup_status != ColdBackupCompleted &&
                    backup_status != ColdBackupCanceled && backup_status != ColdBackupFailed) {
                    cold_backup_running_count++;
                }
            } else if (status() == partition_status::type::PS_SECONDARY) {
                if (backup_status != ColdBackupInvalid && backup_status != ColdBackupFailed &&
                    backup_status != ColdBackupCanceled &&
                    backup_status != ColdBackupCheckpointed) {
                    // secondary end backup with status ColdBackupCheckpointed
                    cold_backup_running_count++;
                }
            }

            if (backup_status == ColdBackupUploading) {
                cold_backup_max_duration_time_ms =
                    std::max(cold_backup_max_duration_time_ms,
                             (dsn_now_ms() - backup_context->get_start_time_ms()));
                cold_backup_max_upload_file_size = std::max(cold_backup_max_upload_file_size,
                                                            backup_context->get_upload_file_size());
            }
        }
    }
    _cold_backup_running_count.store(cold_backup_running_count);
    _cold_backup_max_duration_time_ms.store(cold_backup_max_duration_time_ms);
    _cold_backup_max_upload_file_size.store(cold_backup_max_upload_file_size);
}
}
} // namespace
