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

// IWYU pragma: no_include <boost/detail/basic_pointerbuf.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <fmt/core.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <set>
#include <sstream> // IWYU pragma: keep
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>

#include "common/duplication_common.h"
#include "common/json_helper.h"
#include "common/replica_envs.h"
#include "common/replication.codes.h"
#include "common/replication_common.h"
#include "common/replication_enums.h"
#include "common/replication_other_types.h"
#include "dsn.layer2_types.h"
#include "dump_file.h"
#include "meta/app_env_validator.h"
#include "meta/meta_data.h"
#include "meta/meta_service.h"
#include "meta/meta_state_service.h"
#include "meta/partition_guardian.h"
#include "meta/table_metrics.h"
#include "meta_admin_types.h"
#include "meta_bulk_load_service.h"
#include "metadata_types.h"
#include "replica_admin_types.h"
#include "rpc/dns_resolver.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"
#include "rpc/rpc_message.h"
#include "rpc/serialization.h"
#include "runtime/api_layer1.h"
#include "security/access_controller.h"
#include "server_load_balancer.h"
#include "server_state.h"
#include "task/async_calls.h"
#include "task/task.h"
#include "task/task_spec.h"
#include "utils/autoref_ptr.h"
#include "utils/binary_reader.h"
#include "utils/binary_writer.h"
#include "utils/blob.h"
#include "utils/command_manager.h"
#include "utils/config_api.h"
#include "utils/errors.h"
#include "utils/fail_point.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/metrics.h"
#include "utils/string_conv.h"
#include "utils/strings.h"
#include "utils/utils.h"

DSN_DEFINE_bool(meta_server,
                add_secondary_enable_flow_control,
                false,
                "enable flow control for add secondary proposal");
DSN_DEFINE_int32(meta_server,
                 max_allowed_replica_count,
                 5,
                 "max replica count allowed for any app of a cluster");
DSN_TAG_VARIABLE(max_allowed_replica_count, FT_MUTABLE);
DSN_DEFINE_validator(max_allowed_replica_count, [](int32_t allowed_replica_count) -> bool {
    return allowed_replica_count > 0;
});

DSN_DEFINE_int32(meta_server,
                 min_allowed_replica_count,
                 1,
                 "min replica count allowed for any app of a cluster");
DSN_TAG_VARIABLE(min_allowed_replica_count, FT_MUTABLE);
DSN_DEFINE_validator(min_allowed_replica_count, [](int32_t allowed_replica_count) -> bool {
    return allowed_replica_count > 0;
});

DSN_DEFINE_group_validator(min_max_allowed_replica_count, [](std::string &message) -> bool {
    if (FLAGS_min_allowed_replica_count > FLAGS_max_allowed_replica_count) {
        message = fmt::format("meta_server.min_allowed_replica_count({}) should be <= "
                              "meta_server.max_allowed_replica_count({})",
                              FLAGS_min_allowed_replica_count,
                              FLAGS_max_allowed_replica_count);
        return false;
    }
    return true;
});

DSN_DEFINE_int32(meta_server,
                 hold_seconds_for_dropped_app,
                 604800,
                 "Default time in seconds to reserve the data of deleted tables");
DSN_DEFINE_int32(meta_server,
                 add_secondary_max_count_for_one_node,
                 10,
                 "add secondary max count for one node when flow control enabled");

DSN_DECLARE_bool(recover_from_replica_server);

namespace dsn::replication {

// Reply to the client with specified response.
#define REPLY_TO_CLIENT(msg, response)                                                             \
    _meta_svc->reply_data(msg, response);                                                          \
    msg->release_ref()

// Reply to the client with specified response, and return from current function.
#define REPLY_TO_CLIENT_AND_RETURN(msg, response)                                                  \
    REPLY_TO_CLIENT(msg, response);                                                                \
    return

static const char *lock_state = "lock";
static const char *unlock_state = "unlock";

server_state::server_state()
    : _meta_svc(nullptr),
      _add_secondary_enable_flow_control(false),
      _add_secondary_max_count_for_one_node(0)
{
}

server_state::~server_state() { _tracker.cancel_outstanding_tasks(); }

void server_state::register_cli_commands()
{
    _cmds.emplace_back(dsn::command_manager::instance().register_single_command(
        "meta.dump",
        "Dump app_states of meta server to a local file",
        "<target_file>",
        [this](const std::vector<std::string> &args) {
            if (args.size() != 1) {
                return ERR_INVALID_PARAMETERS.to_string();
            }

            return dump_from_remote_storage(args[0].c_str(), false).to_string();
        }));

    _cmds.emplace_back(dsn::command_manager::instance().register_bool_command(
        _add_secondary_enable_flow_control,
        "meta.lb.add_secondary_enable_flow_control",
        "control whether enable add secondary flow control"));

    _cmds.emplace_back(dsn::command_manager::instance().register_int_command(
        _add_secondary_max_count_for_one_node,
        FLAGS_add_secondary_max_count_for_one_node,
        "meta.lb.add_secondary_max_count_for_one_node",
        "control the max count to add secondary for one node"));
}

void server_state::initialize(meta_service *meta_svc, const std::string &apps_root)
{
    _meta_svc = meta_svc;
    _apps_root = apps_root;
    _add_secondary_enable_flow_control = FLAGS_add_secondary_enable_flow_control;
    _add_secondary_max_count_for_one_node = FLAGS_add_secondary_max_count_for_one_node;
}

bool server_state::spin_wait_staging(int timeout_seconds)
{
    while ((timeout_seconds == -1 || timeout_seconds > 0)) {
        int c = 0;
        {
            zauto_read_lock l(_lock);
            c = count_staging_app();
        }
        if (c == 0) {
            return true;
        }
        LOG_INFO("there are {} apps still in staging, just wait...", c);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (timeout_seconds > 0) {
            --timeout_seconds;
        }
    }
    return false;
}

int server_state::count_staging_app()
{
    int ans = 0;
    for (const auto &app_kv : _all_apps) {
        if (app_kv.second->status == app_status::AS_CREATING ||
            app_kv.second->status == app_status::AS_DROPPING ||
            app_kv.second->status == app_status::AS_RECALLING)
            ++ans;
    }
    return ans;
}

// Create a new variable of `configuration_create_app_response` and assign it with specified
// error code.
#define INIT_CREATE_APP_RESPONSE_WITH_ERR(response, err_code)                                      \
    configuration_create_app_response response;                                                    \
    response.err = err_code

// Create a new variable of `configuration_create_app_response` and assign it with ERR_OK
// and table id.
#define INIT_CREATE_APP_RESPONSE_WITH_OK(response, app_id)                                         \
    configuration_create_app_response response;                                                    \
    response.err = dsn::ERR_OK;                                                                    \
    response.appid = app_id;

// Reply to the client with a newly created failed `configuration_create_app_response` and return
// from current function.
#define FAIL_CREATE_APP_RESPONSE(msg, response, err_code)                                          \
    INIT_CREATE_APP_RESPONSE_WITH_ERR(response, err_code);                                         \
    REPLY_TO_CLIENT_AND_RETURN(msg, response)

// Reply to the client with a newly created successful `configuration_create_app_response` and
// return from current function.
#define SUCC_CREATE_APP_RESPONSE(msg, response, app_id)                                            \
    INIT_CREATE_APP_RESPONSE_WITH_OK(response, app_id);                                            \
    REPLY_TO_CLIENT_AND_RETURN(msg, response)

void server_state::transition_staging_state(std::shared_ptr<app_state> &app)
{
#define send_response(meta, msg, response_data)                                                    \
    do {                                                                                           \
        if (msg != nullptr) {                                                                      \
            meta->reply_data(msg, response_data);                                                  \
            msg->release_ref();                                                                    \
            msg = nullptr;                                                                         \
        }                                                                                          \
    } while (0)

    app_status::type old_status = app->status;
    if (app->status == app_status::AS_CREATING) {
        app->status = app_status::AS_AVAILABLE;
        INIT_CREATE_APP_RESPONSE_WITH_ERR(resp, dsn::ERR_OK);
        resp.appid = app->app_id;
        send_response(_meta_svc, app->helpers->pending_response, resp);
    } else if (app->status == app_status::AS_DROPPING) {
        app->status = app_status::AS_DROPPED;
        configuration_drop_app_response resp;
        resp.err = dsn::ERR_OK;
        send_response(_meta_svc, app->helpers->pending_response, resp);
    } else if (app->status == app_status::AS_RECALLING) {
        app->status = app_status::AS_AVAILABLE;
        configuration_recall_app_response resp;
        resp.err = dsn::ERR_OK;
        resp.info = *app;
        send_response(_meta_svc, app->helpers->pending_response, resp);
    } else {
        CHECK(false,
              "app({}) not in staging state({})",
              app->get_logname(),
              enum_to_string(app->status));
    }

    LOG_INFO("app({}) transfer from {} to {}",
             app->get_logname(),
             enum_to_string(old_status),
             enum_to_string(app->status));
#undef send_response
}

void server_state::process_one_partition(std::shared_ptr<app_state> &app)
{
    int ans = --app->helpers->partitions_in_progress;
    if (ans > 0) {
        LOG_DEBUG("app({}) in status {}, can't transfer to stable state as some partition is in "
                  "progressing",
                  app->get_logname(),
                  enum_to_string(app->status));
        return;
    } else if (ans == 0) {
        transition_staging_state(app);
    } else {
        CHECK(false, "partitions in progress({}) shouldn't be negetive", ans);
    }
}

error_code server_state::dump_app_states(const char *local_path,
                                         const std::function<app_state *()> &iterator)
{
    std::shared_ptr<dump_file> file = dump_file::open_file(local_path, true);
    if (file == nullptr) {
        LOG_ERROR("open file failed, file({})", local_path);
        return ERR_FILE_OPERATION_FAILED;
    }

    file->append_buffer("binary", 6);
    app_state *app;
    while ((app = iterator()) != nullptr) {
        CHECK(app->status == app_status::AS_AVAILABLE || app->status == app_status::AS_DROPPED,
              "invalid app status");
        binary_writer writer;
        dsn::marshall(writer, *app, DSF_THRIFT_BINARY);
        file->append_buffer(writer.get_buffer());
        for (const auto &pc : app->pcs) {
            binary_writer pc_writer;
            dsn::marshall(pc_writer, pc, DSF_THRIFT_BINARY);
            file->append_buffer(pc_writer.get_buffer());
        }
    }
    return ERR_OK;
}

error_code server_state::dump_from_remote_storage(const char *local_path, bool sync_immediately)
{
    error_code ec;

    if (sync_immediately) {
        ec = sync_apps_from_remote_storage();
        if (ec == ERR_OBJECT_NOT_FOUND) {
            LOG_INFO("remote storage is empty, just stop the dump");
            return ERR_OK;
        } else if (ec != ERR_OK) {
            LOG_ERROR("sync from remote storage failed, err({})", ec);
            return ec;
        } else {
            spin_wait_staging();
        }
        auto iter_begin = _all_apps.begin();
        auto iter_end = _all_apps.end();
        return dump_app_states(local_path, [&iter_begin, &iter_end]() -> app_state * {
            if (iter_begin == iter_end)
                return nullptr;
            app_state *result = iter_begin->second.get();
            ++iter_begin;
            return result;
        });
    } else {
        std::vector<app_state> snapshots;
        {
            zauto_read_lock l(_lock);
            if (count_staging_app() != 0) {
                LOG_INFO("there are apps in staging, skip this dump");
                return ERR_INVALID_STATE;
            }
            snapshots.reserve(_all_apps.size());
            for (auto &app_pair : _all_apps)
                snapshots.push_back(*(app_pair.second));
        }
        auto iter_begin = snapshots.begin(), iter_end = snapshots.end();
        return dump_app_states(local_path, [&iter_begin, &iter_end]() -> app_state * {
            if (iter_begin == iter_end)
                return nullptr;
            app_state *result = &(*iter_begin);
            ++iter_begin;
            return result;
        });
    }
}

error_code server_state::restore_from_local_storage(const char *local_path)
{
    error_code ec;

    std::shared_ptr<dump_file> file = dump_file::open_file(local_path, false);
    if (file == nullptr) {
        LOG_ERROR("open file failed, file({})", local_path);
        return ERR_FILE_OPERATION_FAILED;
    }

    blob data;
    CHECK_EQ_MSG(file->read_next_buffer(data), 1, "read format header failed");
    _all_apps.clear();

    CHECK_TRUE(utils::mequals(data.data(), "binary", 6));
    while (true) {
        int ans = file->read_next_buffer(data);
        CHECK_NE_MSG(ans, -1, "read file failed");
        if (ans == 0) // file end
            break;

        app_info info;
        binary_reader reader(data);
        unmarshall(reader, info, DSF_THRIFT_BINARY);
        std::shared_ptr<app_state> app = app_state::create(info);
        _all_apps.emplace(app->app_id, app);

        for (unsigned int i = 0; i != app->partition_count; ++i) {
            ans = file->read_next_buffer(data);
            binary_reader pc_reader(data);
            CHECK_EQ_MSG(ans, 1, "unexpect read buffer");
            unmarshall(pc_reader, app->pcs[i], DSF_THRIFT_BINARY);
            CHECK_EQ_MSG(app->pcs[i].pid.get_partition_index(),
                         i,
                         "uncorrect partition data, gpid({}.{}), appname({})",
                         app->app_id,
                         i,
                         app->app_name);
        }
    }

    for (auto &iter : _all_apps) {
        if (iter.second->status == app_status::AS_AVAILABLE)
            iter.second->status = app_status::AS_CREATING;
        else {
            CHECK_EQ(iter.second->status, app_status::AS_DROPPED);
            iter.second->status = app_status::AS_DROPPING;
        }
    }
    ec = sync_apps_to_remote_storage();
    if (ec != ERR_OK) {
        _all_apps.clear();
        return ec;
    }
    return ERR_OK;
}

error_code server_state::initialize_default_apps()
{
    std::vector<std::string> sections;
    dsn_config_get_all_sections(sections);
    LOG_INFO("start to do initialize");

    app_info default_app;
    for (const auto &section : sections) {
        // Match a section prefixed by "meta_server.apps" or equal to "replication.app"
        // TODO(yingchun): Move "replication.app" out of the loop, and define it as flags, then we
        // can get it by HTTP.
        if (section.find("meta_server.apps") == 0 || section == "replication.app") {
            default_app.status = app_status::AS_CREATING;
            default_app.app_id = _all_apps.size() + 1;

            // TODO(yingchun): the old configuration launch methods should be kept to launch repeat
            //  configs.
            default_app.app_name =
                dsn_config_get_value_string(section.c_str(), "app_name", "", "Table name");
            if (default_app.app_name.length() == 0) {
                LOG_WARNING("'[{}] app_name' not specified, ignore this section", section);
                continue;
            }

            default_app.app_type = dsn_config_get_value_string(
                section.c_str(),
                "app_type",
                "",
                "The storage engine type, 'pegasus' represents the storage engine based on "
                "Rocksdb. Currently, only 'pegasus' is available");
            default_app.partition_count =
                (int)dsn_config_get_value_uint64(section.c_str(),
                                                 "partition_count",
                                                 1,
                                                 "Partition count, i.e., the shards of the table");
            // TODO(yingchun): always true, remove it.
            default_app.is_stateful = dsn_config_get_value_bool(
                section.c_str(),
                "stateful",
                true,
                "Whether this is a stateful table, it must be true if 'app_type = pegasus'");
            default_app.max_replica_count =
                (int)dsn_config_get_value_uint64(section.c_str(),
                                                 "max_replica_count",
                                                 3,
                                                 "The maximum replica count of each partition");
            default_app.create_second = dsn_now_ms() / 1000;
            std::string envs_str = dsn_config_get_value_string(
                section.c_str(), "envs", "", "Table environment variables");
            bool parse = dsn::utils::parse_kv_map(envs_str.c_str(), default_app.envs, ',', '=');

            CHECK_GT_MSG(
                default_app.app_type.length(), 0, "'[{}] app_type' not specified", section);
            CHECK_GT(default_app.partition_count, 0);
            CHECK(parse, "'[{}] envs' is invalid, envs = {}", section, envs_str);

            std::shared_ptr<app_state> app = app_state::create(default_app);
            _all_apps.emplace(app->app_id, app);
        }
    }

    error_code err = sync_apps_to_remote_storage();
    if (err != ERR_OK) {
        _all_apps.clear();
        return err;
    }
    return ERR_OK;
}

// caller should ensure all apps are in staging: creating, dropping
error_code server_state::sync_apps_to_remote_storage()
{
    _exist_apps.clear();
    _table_metric_entities.clear_entities();
    for (auto &kv_pair : _all_apps) {
        if (kv_pair.second->status == app_status::AS_CREATING) {
            CHECK(_exist_apps.find(kv_pair.second->app_name) == _exist_apps.end(),
                  "invalid app name, name = {}",
                  kv_pair.second->app_name);
            _exist_apps.emplace(kv_pair.second->app_name, kv_pair.second);
            _table_metric_entities.create_entity(kv_pair.first, kv_pair.second->partition_count);
        }
    }

    // create cluster_root/apps node
    std::string &apps_path = _apps_root;
    error_code err;
    dist::meta_state_service *storage = _meta_svc->get_remote_storage();

    auto t = storage->create_node(
        apps_path,
        LPC_META_CALLBACK,
        [&err](error_code ec) { err = ec; },
        blob(lock_state, 0, strlen(lock_state)));
    t->wait();

    if (err != ERR_NODE_ALREADY_EXIST && err != ERR_OK) {
        LOG_ERROR("create root node /apps in meta store failed, err = {}", err);
        return err;
    } else {
        LOG_INFO("set {} to lock state in remote storage", _apps_root);
    }

    err = ERR_OK;
    dsn::task_tracker tracker;
    for (auto &kv : _all_apps) {
        std::shared_ptr<app_state> &app = kv.second;
        std::string path = get_app_path(*app);

        CHECK(app->status == app_status::AS_CREATING || app->status == app_status::AS_DROPPING,
              "invalid app status");
        blob value = app->to_json(app_status::AS_CREATING == app->status ? app_status::AS_AVAILABLE
                                                                         : app_status::AS_DROPPED);
        storage->create_node(
            path,
            LPC_META_CALLBACK,
            [&err, path](error_code ec) {
                if (ec != ERR_OK && ec != ERR_NODE_ALREADY_EXIST) {
                    LOG_WARNING("create app node failed, path({}) reason({})", path, ec);
                    err = ec;
                } else {
                    LOG_INFO("create app node {} ok", path);
                }
            },
            value,
            &tracker);
    }
    tracker.wait_outstanding_tasks();

    if (err != ERR_OK) {
        _exist_apps.clear();
        _table_metric_entities.clear_entities();
        return err;
    }
    for (auto &kv : _all_apps) {
        std::shared_ptr<app_state> &app = kv.second;
        for (unsigned int i = 0; i != app->partition_count; ++i) {
            task_ptr init_callback = tasking::create_task(
                LPC_META_STATE_HIGH, &tracker, [] {}, sStateHash);
            init_app_partition_node(app, i, init_callback);
        }
    }

    tracker.wait_outstanding_tasks();
    t = _meta_svc->get_remote_storage()->set_data(_apps_root,
                                                  blob(unlock_state, 0, strlen(unlock_state)),
                                                  LPC_META_STATE_HIGH,
                                                  [&err](dsn::error_code e) { err = e; });
    t->wait();
    if (dsn::ERR_OK == err) {
        LOG_INFO("set {} to unlock state in remote storage", _apps_root);
        return err;
    } else {
        LOG_ERROR("set {} to unlock state in remote storage failed, reason({})", _apps_root, err);
        return err;
    }
}

dsn::error_code server_state::sync_apps_from_remote_storage()
{
    dsn::error_code err;
    dsn::task_tracker tracker;

    dist::meta_state_service *storage = _meta_svc->get_remote_storage();
    auto sync_partition = [this, storage, &err, &tracker](std::shared_ptr<app_state> &app,
                                                          int partition_id,
                                                          const std::string &partition_path) {
        storage->get_data(
            partition_path,
            LPC_META_CALLBACK,
            [this, app, partition_id, partition_path, &err](error_code ec,
                                                            const blob &value) mutable {
                if (ec == ERR_OK) {
                    partition_configuration pc;
                    // TODO(yingchun): when upgrade from old version, check if the fields will be
                    //  filled.
                    // TODO(yingchun): check if the fields will be set after decoding.
                    pc.__isset.hp_secondaries = true;
                    pc.__isset.hp_last_drops = true;
                    pc.__isset.hp_primary = true;
                    dsn::json::json_forwarder<partition_configuration>::decode(value, pc);

                    CHECK(pc.pid.get_app_id() == app->app_id &&
                              pc.pid.get_partition_index() == partition_id,
                          "invalid partition config");
                    {
                        zauto_write_lock l(_lock);
                        app->pcs[partition_id] = pc;
                        CHECK(pc.__isset.hp_last_drops, "");
                        for (const auto &last_drop : pc.hp_last_drops) {
                            app->helpers->contexts[partition_id].record_drop_history(last_drop);
                        }

                        if (app->status == app_status::AS_CREATING &&
                            (pc.partition_flags & pc_flags::dropped) != 0) {
                            recall_partition(app, partition_id);
                        } else if (app->status == app_status::AS_DROPPING &&
                                   (pc.partition_flags & pc_flags::dropped) == 0) {
                            drop_partition(app, partition_id);
                        } else
                            process_one_partition(app);
                        // check consistency between app bulk_loading flag and app bulk load dir
                        if (app->helpers->partitions_in_progress.load() == 0 &&
                            app->status == app_status::AS_AVAILABLE &&
                            _meta_svc->get_bulk_load_service()) {
                            bool is_bulk_loading = app->is_bulk_loading;
                            _meta_svc->get_bulk_load_service()->check_app_bulk_load_states(
                                std::move(app), is_bulk_loading);
                        }
                    }
                } else if (ec == ERR_OBJECT_NOT_FOUND) {
                    auto init_partition_count = app->init_partition_count > 0
                                                    ? app->init_partition_count
                                                    : app->partition_count;
                    if (partition_id < init_partition_count) {
                        LOG_WARNING(
                            "partition node {} not exist on remote storage, may half create before",
                            partition_path);
                        init_app_partition_node(app, partition_id, nullptr);
                    } else if (partition_id >= app->partition_count / 2) {
                        LOG_WARNING(
                            "partition node {} not exist on remote storage, may half split before",
                            partition_path);
                        zauto_write_lock l(_lock);
                        app->helpers->split_states.status[partition_id - app->partition_count / 2] =
                            split_status::SPLITTING;
                        app->helpers->split_states.splitting_count++;
                        app->pcs[partition_id].ballot = invalid_ballot;
                        app->pcs[partition_id].pid = gpid(app->app_id, partition_id);
                        process_one_partition(app);
                    }

                } else {
                    LOG_ERROR("get partition node failed, reason({})", ec);
                    err = ec;
                }
            },
            &tracker);
    };

    auto sync_app = [&](const std::string &app_path) {
        storage->get_data(
            app_path,
            LPC_META_CALLBACK,
            [this, app_path, &err, &sync_partition](error_code ec, const blob &value) {
                if (ec == ERR_OK) {
                    app_info info;
                    CHECK(dsn::json::json_forwarder<app_info>::decode(value, info),
                          "invalid json data");
                    std::shared_ptr<app_state> app = app_state::create(info);
                    {
                        zauto_write_lock l(_lock);
                        _all_apps.emplace(app->app_id, app);
                        if (app->status == app_status::AS_AVAILABLE) {
                            app->status = app_status::AS_CREATING;
                            _exist_apps.emplace(app->app_name, app);
                            _table_metric_entities.create_entity(app->app_id, app->partition_count);
                        } else if (app->status == app_status::AS_DROPPED) {
                            app->status = app_status::AS_DROPPING;
                        } else {
                            CHECK(false,
                                  "invalid status({}) for app({}) in remote storage",
                                  enum_to_string(app->status),
                                  app->get_logname());
                        }
                    }
                    app->helpers->split_states.splitting_count = 0;
                    for (int i = 0; i < app->partition_count; i++) {
                        std::string partition_path =
                            app_path + "/" + boost::lexical_cast<std::string>(i);
                        sync_partition(app, i, partition_path);
                    }
                } else {
                    LOG_ERROR("get app info from meta state service failed, path = {}, err = {}",
                              app_path,
                              ec);
                    err = ec;
                }
            },
            &tracker);
    };

    _all_apps.clear();
    _exist_apps.clear();
    _table_metric_entities.clear_entities();

    std::string transaction_state;
    storage
        ->get_data(_apps_root,
                   LPC_META_CALLBACK,
                   [&err, &transaction_state](error_code ec, const blob &value) {
                       err = ec;
                       if (ec == dsn::ERR_OK) {
                           transaction_state.assign(value.data(), value.length());
                       }
                   })
        ->wait();

    if (ERR_OBJECT_NOT_FOUND == err)
        return err;
    CHECK_EQ_MSG(ERR_OK, err, "can't handle this error");
    CHECK(transaction_state == std::string(unlock_state) || transaction_state.empty(),
          "invalid transaction state({})",
          transaction_state);

    storage->get_children(
        _apps_root,
        LPC_META_CALLBACK,
        [&](error_code ec, const std::vector<std::string> &apps) {
            if (ec == ERR_OK) {
                for (const auto &appid_str : apps) {
                    sync_app(_apps_root + "/" + appid_str);
                }
            } else {
                LOG_ERROR("get app list from meta state service failed, path = {}, err = {}",
                          _apps_root,
                          ec);
                err = ec;
            }
        },
        &tracker);
    tracker.wait_outstanding_tasks();
    if (err == ERR_OK) {
        return _all_apps.empty() ? ERR_OBJECT_NOT_FOUND : ERR_OK;
    }
    return err;
}

void server_state::initialize_node_state()
{
    zauto_write_lock l(_lock);
    for (auto &app_pair : _all_apps) {
        app_state &app = *(app_pair.second);
        for (const auto &pc : app.pcs) {
            if (pc.hp_primary) {
                node_state *ns = get_node_state(_nodes, pc.hp_primary, true);
                ns->put_partition(pc.pid, true);
            }

            for (const auto &secondary : pc.hp_secondaries) {
                CHECK(secondary, "invalid secondary: {}", secondary);
                node_state *ns = get_node_state(_nodes, secondary, true);
                ns->put_partition(pc.pid, false);
            }
        }
    }
    for (auto &node : _nodes) {
        node.second.set_alive(true);
    }
    for (auto &app_pair : _all_apps) {
        app_state &app = *(app_pair.second);
        for (const auto &pc : app.pcs) {
            check_consistency(pc.pid);
        }
    }
}

error_code server_state::initialize_data_structure()
{
    error_code err = sync_apps_from_remote_storage();
    if (err == ERR_OBJECT_NOT_FOUND) {
        if (FLAGS_recover_from_replica_server) {
            return ERR_OBJECT_NOT_FOUND;
        } else {
            LOG_INFO("can't find apps from remote storage, start to initialize default apps");
            err = initialize_default_apps();
        }
    } else if (err == ERR_OK) {
        if (FLAGS_recover_from_replica_server) {
            CHECK(false,
                  "find apps from remote storage, but "
                  "[meta_server].recover_from_replica_server = true");
        } else {
            LOG_INFO(
                "sync apps from remote storage ok, get {} apps, init the node state accordingly",
                _all_apps.size());
            initialize_node_state();
        }
    }
    return err;
}

void server_state::set_config_change_subscriber_for_test(config_change_subscriber subscriber)
{
    _config_change_subscriber = subscriber;
}

void server_state::set_replica_migration_subscriber_for_test(
    replica_migration_subscriber subscriber)
{
    _replica_migration_subscriber = subscriber;
}

// partition server => meta server
// this is done in meta_state_thread_pool
void server_state::on_config_sync(configuration_query_by_node_rpc rpc)
{
    configuration_query_by_node_response &response = rpc.response();
    const configuration_query_by_node_request &request = rpc.request();

    bool reject_this_request = false;
    response.__isset.gc_replicas = false;

    host_port node;
    GET_HOST_PORT(request, node, node);
    LOG_INFO("got config sync request from {}, stored_replicas_count({})",
             node,
             request.stored_replicas.size());

    {
        zauto_read_lock l(_lock);

        // sync the partitions to the replica server
        node_state *ns = get_node_state(_nodes, node, false);
        if (ns == nullptr) {
            LOG_INFO("node({}) not found in meta server", node);
            response.err = ERR_OBJECT_NOT_FOUND;
        } else {
            response.err = ERR_OK;
            unsigned int i = 0;
            response.partitions.resize(ns->partition_count());
            ns->for_each_partition([&, this](const gpid &pid) {
                std::shared_ptr<app_state> app = get_app(pid.get_app_id());
                CHECK(app, "invalid app_id, app_id = {}", pid.get_app_id());
                config_context &cc = app->helpers->contexts[pid.get_partition_index()];

                // config sync need the newest data to keep the perfect FD,
                // so if the syncing config is related to the node, we may need to reject this
                // request
                if (cc.stage == config_status::pending_remote_sync) {
                    configuration_update_request *pending_request = cc.pending_sync_request.get();
                    // when register child partition, stage is config_status::pending_remote_sync,
                    // but cc.pending_sync_request is not set, see more in function
                    // 'register_child_on_meta'
                    if (pending_request == nullptr) {
                        return false;
                    }

                    host_port target;
                    GET_HOST_PORT(*pending_request, node, target);
                    if (target == node) {
                        return false;
                    }
                }

                response.partitions[i].info = *app;
                response.partitions[i].config = app->pcs[pid.get_partition_index()];
                response.partitions[i].host_node = request.node;
                // set meta_split_status
                const split_state &app_split_states = app->helpers->split_states;
                if (app->splitting()) {
                    auto iter = app_split_states.status.find(pid.get_partition_index());
                    if (iter != app_split_states.status.end()) {
                        response.partitions[i].__set_meta_split_status(iter->second);
                    }
                }
                ++i;
                return true;
            });
            if (i < response.partitions.size()) {
                reject_this_request = true;
            }
        }

        // handle the stored replicas & the gc replicas
        if (!reject_this_request && request.__isset.stored_replicas) {
            if (ns != nullptr)
                ns->set_replicas_collect_flag(true);
            const std::vector<replica_info> &replicas = request.stored_replicas;
            meta_function_level::type level = _meta_svc->get_function_level();
            // if the node serve the replica on the meta server, then we ignore it
            // if the dropped servers on the meta servers are enough, we need to gc it
            // there are not enough dropped servers, we need to add it to dropped
            // the app is deleted but not expired, we need to ignore it
            // if the app is deleted and expired, we need to gc it
            for (const replica_info &rep : replicas) {
                LOG_DEBUG("receive stored replica from {}, pid({})", node, rep.pid);
                std::shared_ptr<app_state> app = get_app(rep.pid.get_app_id());
                if (app == nullptr || rep.pid.get_partition_index() >= app->partition_count) {
                    // This app has garbage partition after cancel split, the canceled child
                    // partition should be gc
                    if (app != nullptr &&
                        rep.pid.get_partition_index() < app->partition_count * 2 &&
                        rep.status == partition_status::PS_ERROR) {
                        response.gc_replicas.push_back(rep);
                        LOG_WARNING(
                            "notify node({}) to gc replica({}) because it is useless partition "
                            "which is caused by cancel split",
                            node,
                            rep.pid);
                    } else {
                        // app is not recognized or partition is not recognized
                        CHECK(false,
                              "gpid({}) on node({}) is not exist on meta server, administrator "
                              "should check consistency of meta data",
                              rep.pid,
                              node);
                    }
                } else if (app->status == app_status::AS_DROPPED) {
                    if (app->expire_second == 0) {
                        LOG_INFO("gpid({}) on node({}) is of dropped table, but expire second is "
                                 "not specified, do not delete it for safety reason",
                                 rep.pid,
                                 node);
                    } else if (has_seconds_expired(app->expire_second)) {
                        // can delete replica only when expire second is explicitely specified and
                        // expired.
                        if (level <= meta_function_level::fl_steady) {
                            LOG_INFO("gpid({}) on node({}) is of dropped and expired table, but "
                                     "current function level is {}, do not delete it for safety "
                                     "reason",
                                     rep.pid,
                                     node,
                                     _meta_function_level_VALUES_TO_NAMES.find(level)->second);
                        } else {
                            response.gc_replicas.push_back(rep);
                            LOG_WARNING("notify node({}) to gc replica({}) coz the app is "
                                        "dropped and expired",
                                        node,
                                        rep.pid);
                        }
                    }
                } else if (app->status == app_status::AS_AVAILABLE) {
                    bool is_useful_replica = collect_replica({&_all_apps, &_nodes}, node, rep);
                    if (!is_useful_replica) {
                        if (level <= meta_function_level::fl_steady) {
                            LOG_INFO("gpid({}) on node({}) is useless, but current function "
                                     "level is {}, do not delete it for safety reason",
                                     rep.pid,
                                     node,
                                     _meta_function_level_VALUES_TO_NAMES.find(level)->second);
                        } else {
                            response.gc_replicas.push_back(rep);
                            LOG_WARNING("notify node({}) to gc replica({}) coz it is useless",
                                        node,
                                        rep.pid);
                        }
                    }
                }
            }

            if (!response.gc_replicas.empty()) {
                response.__isset.gc_replicas = true;
            }
        }
    }

    if (reject_this_request) {
        response.err = ERR_BUSY;
        response.partitions.clear();
    }
    LOG_INFO("send config sync response to {}, err({}), partitions_count({}), "
             "gc_replicas_count({})",
             node,
             response.err,
             response.partitions.size(),
             response.gc_replicas.size());
}

bool server_state::query_configuration_by_gpid(dsn::gpid id,
                                               /*out*/ partition_configuration &pc)
{
    zauto_read_lock l(_lock);
    const auto *ppc = get_config(_all_apps, id);
    if (ppc != nullptr) {
        pc = *ppc;
        return true;
    }
    return false;
}

void server_state::query_configuration_by_index(const query_cfg_request &request,
                                                /*out*/ query_cfg_response &response)
{
    zauto_read_lock l(_lock);
    auto iter = _exist_apps.find(request.app_name);
    if (iter == _exist_apps.end()) {
        response.err = ERR_OBJECT_NOT_FOUND;
        return;
    }

    std::shared_ptr<app_state> &app = iter->second;
    if (app->status != app_status::AS_AVAILABLE) {
        LOG_ERROR("invalid status({}) in exist app({}), app_id({})",
                  enum_to_string(app->status),
                  app->app_name,
                  app->app_id);

        switch (app->status) {
        case app_status::AS_CREATING:
        case app_status::AS_RECALLING:
            response.err = ERR_BUSY_CREATING;
            break;
        case app_status::AS_DROPPING:
            response.err = ERR_BUSY_DROPPING;
            break;
        default:
            response.err = ERR_UNKNOWN;
        }
        return;
    }

    response.err = ERR_OK;
    response.app_id = app->app_id;
    response.partition_count = app->partition_count;
    response.is_stateful = app->is_stateful;

    for (const int32_t &index : request.partition_indices) {
        if (index >= 0 && index < app->pcs.size()) {
            response.partitions.push_back(app->pcs[index]);
        }
    }
    if (response.partitions.empty()) {
        response.partitions = app->pcs;
    }
}

void server_state::init_app_partition_node(std::shared_ptr<app_state> &app,
                                           int pidx,
                                           task_ptr callback)
{
    auto on_create_app_partition = [this, pidx, app, callback](error_code ec) mutable {
        LOG_DEBUG("create partition node: gpid({}.{}), result: {}", app->app_id, pidx, ec);
        if (ERR_OK == ec || ERR_NODE_ALREADY_EXIST == ec) {
            {
                zauto_write_lock l(_lock);
                process_one_partition(app);
            }
            if (callback) {
                callback->enqueue();
            }
        } else if (ERR_TIMEOUT == ec) {
            LOG_WARNING(
                "create partition node failed, gpid({}.{}), retry later", app->app_id, pidx);
            // TODO: add parameter of the retry time interval in config file
            tasking::enqueue(
                LPC_META_STATE_HIGH,
                tracker(),
                std::bind(&server_state::init_app_partition_node, this, app, pidx, callback),
                0,
                std::chrono::milliseconds(1000));
        } else {
            CHECK(false,
                  "we can't handle this error in init app partition nodes err({}), gpid({}.{})",
                  ec,
                  app->app_id,
                  pidx);
        }
    };

    std::string app_partition_path = get_partition_path(*app, pidx);
    dsn::blob value = dsn::json::json_forwarder<partition_configuration>::encode(app->pcs[pidx]);
    _meta_svc->get_remote_storage()->create_node(
        app_partition_path, LPC_META_STATE_HIGH, on_create_app_partition, value);
}

void server_state::do_app_create(std::shared_ptr<app_state> &app)
{
    auto on_create_app_root = [this, app](error_code ec) mutable {
        if (ERR_OK == ec || ERR_NODE_ALREADY_EXIST == ec) {
            LOG_DEBUG("create app({}) on storage service ok", app->get_logname());
            for (unsigned int i = 0; i != app->partition_count; ++i) {
                init_app_partition_node(app, i, nullptr);
            }
        } else if (ERR_TIMEOUT == ec) {
            LOG_WARNING("the storage service is not available currently, continue to create later");
            tasking::enqueue(LPC_META_STATE_HIGH,
                             tracker(),
                             std::bind(&server_state::do_app_create, this, app),
                             0,
                             std::chrono::seconds(1));
        } else {
            CHECK(false, "we can't handle this right now, err({})", ec);
        }
    };

    std::string app_dir = get_app_path(*app);
    blob value = app->to_json(app_status::AS_AVAILABLE);
    _meta_svc->get_remote_storage()->create_node(
        app_dir, LPC_META_STATE_HIGH, on_create_app_root, value);
}

void server_state::create_app(dsn::message_ex *msg)
{
    configuration_create_app_request request;
    dsn::unmarshall(msg, request);

    const auto &master_cluster =
        request.options.envs.find(duplication_constants::kEnvMasterClusterKey);
    bool duplicating = master_cluster != request.options.envs.end();
    LOG_INFO("create app request, name({}), type({}), partition_count({}), replica_count({}), "
             "duplication({})",
             request.app_name,
             request.options.app_type,
             request.options.partition_count,
             request.options.replica_count,
             duplicating
                 ? fmt::format("master_cluster_name={}, master_app_name={}",
                               master_cluster->second,
                               gutil::FindWithDefault(request.options.envs,
                                                      duplication_constants::kEnvMasterAppNameKey))
                 : "false");

    auto option_match_check = [](const create_app_options &opt, const app_state &exist_app) {
        return opt.partition_count == exist_app.partition_count &&
               opt.app_type == exist_app.app_type && opt.envs == exist_app.envs &&
               opt.is_stateful == exist_app.is_stateful &&
               opt.replica_count == exist_app.max_replica_count;
    };

    auto level = _meta_svc->get_function_level();
    if (level <= meta_function_level::fl_freezed) {
        LOG_ERROR("current meta function level is freezed, since there are too few alive nodes");
        FAIL_CREATE_APP_RESPONSE(msg, response, ERR_STATE_FREEZED);
    }

    if (request.options.partition_count <= 0) {
        LOG_ERROR("partition_count({}) is invalid", request.options.partition_count);
        FAIL_CREATE_APP_RESPONSE(msg, response, ERR_INVALID_PARAMETERS);
    }

    if (!validate_target_max_replica_count(request.options.replica_count)) {
        FAIL_CREATE_APP_RESPONSE(msg, response, ERR_INVALID_PARAMETERS);
    }

    if (!_app_env_validator.validate_app_envs(request.options.envs)) {
        FAIL_CREATE_APP_RESPONSE(msg, response, ERR_INVALID_PARAMETERS);
    }

    zauto_write_lock l(_lock);

    auto app = get_app(request.app_name);
    if (app) {
        configuration_create_app_response response;

        switch (app->status) {
        case app_status::AS_AVAILABLE:
            if (!request.options.success_if_exist) {
                if (duplicating) {
                    process_create_follower_app_status(msg, request, master_cluster->second, app);
                    return;
                }

                response.err = ERR_APP_EXIST;
            } else if (!option_match_check(request.options, *app)) {
                response.err = ERR_INVALID_PARAMETERS;
            } else {
                response.err = ERR_OK;
                response.appid = app->app_id;
            }
            break;
        case app_status::AS_CREATING:
        case app_status::AS_RECALLING:
            response.err = ERR_BUSY_CREATING;
            break;
        case app_status::AS_DROPPING:
            response.err = ERR_BUSY_DROPPING;
            break;
        default:
            break;
        }

        REPLY_TO_CLIENT_AND_RETURN(msg, response);
    }

    app_info info;
    info.app_id = next_app_id();
    info.app_name = request.app_name;
    info.app_type = request.options.app_type;
    info.envs = std::move(request.options.envs);
    info.is_stateful = request.options.is_stateful;
    info.max_replica_count = request.options.replica_count;
    info.partition_count = request.options.partition_count;
    info.status = app_status::AS_CREATING;
    info.create_second = static_cast<int64_t>(dsn_now_s());
    info.init_partition_count = request.options.partition_count;

    // No need to check `request.options.__isset.atomic_idempotent`, since by default
    // it is true (because `request.options.atomic_idempotent` has default value false).
    info.__set_atomic_idempotent(request.options.atomic_idempotent);

    app = app_state::create(info);
    app->helpers->pending_response = msg;
    app->helpers->partitions_in_progress.store(info.partition_count);

    _all_apps.emplace(app->app_id, app);
    _exist_apps.emplace(request.app_name, app);
    _table_metric_entities.create_entity(app->app_id, app->partition_count);

    do_app_create(app);
}

// It is idempotent for the repeated requests.
#define SUCC_IDEMPOTENT_CREATE_FOLLOWER_APP_STATUS()                                               \
    LOG_INFO("repeated request that updates env {} of the follower app from {} to {}, "            \
             "just ignore: app_name={}, app_id={}",                                                \
             duplication_constants::kEnvFollowerAppStatusKey,                                      \
             my_status->second,                                                                    \
             req_status->second,                                                                   \
             app->app_name,                                                                        \
             app->app_id);                                                                         \
    SUCC_CREATE_APP_RESPONSE(msg, response, app->app_id)

// Failed due to invalid creating status.
#define FAIL_UNDEFINED_CREATE_FOLLOWER_APP_STATUS(val, desc)                                       \
    LOG_ERROR("undefined value({}) of env {} in the {}: app_name={}, app_id={}",                   \
              val,                                                                                 \
              duplication_constants::kEnvFollowerAppStatusKey,                                     \
              desc,                                                                                \
              app->app_name,                                                                       \
              app->app_id);                                                                        \
    FAIL_CREATE_APP_RESPONSE(msg, response, ERR_INVALID_PARAMETERS)

void server_state::process_create_follower_app_status(
    message_ex *msg,
    const configuration_create_app_request &request,
    const std::string &req_master_cluster,
    std::shared_ptr<app_state> &app)
{
    const auto &my_master_cluster = app->envs.find(duplication_constants::kEnvMasterClusterKey);
    if (my_master_cluster == app->envs.end() || my_master_cluster->second != req_master_cluster) {
        // The source cluster is not matched.
        LOG_ERROR("env {} are not matched between the request({}) and the follower "
                  "app({}): app_name={}, app_id={}",
                  duplication_constants::kEnvMasterClusterKey,
                  req_master_cluster,
                  my_master_cluster == app->envs.end() ? "<nil>" : my_master_cluster->second,
                  app->app_name,
                  app->app_id);
        FAIL_CREATE_APP_RESPONSE(msg, response, ERR_APP_EXIST);
    }

    const auto &req_status =
        request.options.envs.find(duplication_constants::kEnvFollowerAppStatusKey);
    if (req_status == request.options.envs.end()) {
        // Still reply with ERR_APP_EXIST to the master cluster of old versions.
        LOG_ERROR("no env {} in the request: app_name={}, app_id={}",
                  duplication_constants::kEnvFollowerAppStatusKey,
                  app->app_name,
                  app->app_id);
        FAIL_CREATE_APP_RESPONSE(msg, response, ERR_APP_EXIST);
    }

    const auto &my_status = app->envs.find(duplication_constants::kEnvFollowerAppStatusKey);
    if (my_status == app->envs.end()) {
        // Since currently this table have been AS_AVAILABLE, it should have the env of
        // creating status.
        LOG_ERROR("no env {} in the follower app: app_name={}, app_id={}",
                  duplication_constants::kEnvFollowerAppStatusKey,
                  app->app_name,
                  app->app_id);
        FAIL_CREATE_APP_RESPONSE(msg, response, ERR_INVALID_STATE);
        return;
    }

    if (my_status->second == duplication_constants::kEnvFollowerAppStatusCreating) {
        if (req_status->second == duplication_constants::kEnvFollowerAppStatusCreating) {
            SUCC_IDEMPOTENT_CREATE_FOLLOWER_APP_STATUS();
        }

        if (req_status->second == duplication_constants::kEnvFollowerAppStatusCreated) {
            // Mark the status as created both on the remote storage and local memory.
            update_create_follower_app_status(msg,
                                              duplication_constants::kEnvFollowerAppStatusCreating,
                                              duplication_constants::kEnvFollowerAppStatusCreated,
                                              app);
            return;
        }

        FAIL_UNDEFINED_CREATE_FOLLOWER_APP_STATUS(req_status->second, "request");
    }

    if (my_status->second == duplication_constants::kEnvFollowerAppStatusCreated) {
        if (req_status->second == duplication_constants::kEnvFollowerAppStatusCreating) {
            // The status of the duplication should have been DS_APP since the follower app
            // has been marked as created. Thus, the master cluster should never send the
            // request with creating status again.
            LOG_ERROR("the master cluster should never send the request with env {} valued {} "
                      "again since it has been {} in the follower app: app_name={}, app_id={}",
                      duplication_constants::kEnvFollowerAppStatusKey,
                      req_status->second,
                      my_status->second,
                      app->app_name,
                      app->app_id);
            FAIL_CREATE_APP_RESPONSE(msg, response, ERR_APP_EXIST);
        }

        if (req_status->second == duplication_constants::kEnvFollowerAppStatusCreated) {
            SUCC_IDEMPOTENT_CREATE_FOLLOWER_APP_STATUS();
        }

        FAIL_UNDEFINED_CREATE_FOLLOWER_APP_STATUS(req_status->second, "request");
    }

    // Some undefined creating status from the target table.
    FAIL_UNDEFINED_CREATE_FOLLOWER_APP_STATUS(my_status->second, "follower app");
}

#undef FAIL_UNDEFINED_CREATE_FOLLOWER_APP_STATUS
#undef SUCC_IDEMPOTENT_CREATE_FOLLOWER_APP_STATUS

void server_state::update_create_follower_app_status(message_ex *msg,
                                                     const std::string &old_status,
                                                     const std::string &new_status,
                                                     std::shared_ptr<app_state> &app)
{
    app_info ainfo = *app;
    ainfo.envs[duplication_constants::kEnvFollowerAppStatusKey] = new_status;
    auto app_path = get_app_path(*app);

    LOG_INFO("ready to update env {} of follower app from {} to {}, app_name={}, app_id={}, ",
             duplication_constants::kEnvFollowerAppStatusKey,
             old_status,
             new_status,
             app->app_name,
             app->app_id);

    do_update_app_info(
        app_path, ainfo, [this, msg, old_status, new_status, app](error_code ec) mutable {
            {
                zauto_write_lock l(_lock);

                if (ec != ERR_OK) {
                    LOG_ERROR("failed to update remote env of creating follower app status: "
                              "error_code={}, app_name={}, app_id={}, {}={} => {}",
                              ec,
                              app->app_name,
                              app->app_id,
                              duplication_constants::kEnvFollowerAppStatusKey,
                              old_status,
                              new_status);
                    FAIL_CREATE_APP_RESPONSE(msg, response, ec);
                }

                app->envs[duplication_constants::kEnvFollowerAppStatusKey] = new_status;
                LOG_INFO("both remote and local env of creating follower app status have been "
                         "updated successfully: app_name={}, app_id={}, {}={} => {}",
                         app->app_name,
                         app->app_id,
                         duplication_constants::kEnvFollowerAppStatusKey,
                         old_status,
                         new_status);
                SUCC_CREATE_APP_RESPONSE(msg, response, app->app_id);
            }
        });
}

void server_state::do_app_drop(std::shared_ptr<app_state> &app)
{
    auto after_mark_app_dropped = [this, app](error_code ec) mutable {
        if (ERR_OK == ec) {
            zauto_write_lock l(_lock);
            _exist_apps.erase(app->app_name);
            _table_metric_entities.remove_entity(app->app_id);
            for (int i = 0; i < app->partition_count; ++i) {
                drop_partition(app, i);
            }
        } else if (ERR_TIMEOUT == ec) {
            LOG_DEBUG("drop app({}) prepare timeout, continue to drop later", app->get_logname());
            tasking::enqueue(LPC_META_STATE_HIGH,
                             tracker(),
                             std::bind(&server_state::do_app_drop, this, app),
                             0,
                             std::chrono::seconds(1));
        } else {
            CHECK(false, "we can't handle this, error({})", ec);
        }
    };

    blob json_app = app->to_json(app_status::AS_DROPPED);
    std::string app_path = get_app_path(*app);
    _meta_svc->get_remote_storage()->set_data(
        app_path, json_app, LPC_META_STATE_HIGH, after_mark_app_dropped);
}

void server_state::drop_app(dsn::message_ex *msg)
{
    configuration_drop_app_request request;
    configuration_drop_app_response response;

    bool do_dropping = false;
    std::shared_ptr<app_state> app;
    dsn::unmarshall(msg, request);
    LOG_INFO("drop app request, name({})", request.app_name);
    {
        zauto_write_lock l(_lock);
        app = get_app(request.app_name);
        if (nullptr == app) {
            response.err = request.options.success_if_not_exist ? ERR_OK : ERR_APP_NOT_EXIST;
        } else {
            switch (app->status) {
            case app_status::AS_AVAILABLE:
                if (app->splitting()) {
                    // not drop splitting app
                    response.err = ERR_SPLITTING;
                    break;
                }
                do_dropping = true;
                app->status = app_status::AS_DROPPING;
                app->drop_second = dsn_now_ms() / 1000;
                if (request.options.__isset.reserve_seconds &&
                    request.options.reserve_seconds > 0) {
                    app->expire_second = app->drop_second + request.options.reserve_seconds;
                } else {
                    app->expire_second = app->drop_second + FLAGS_hold_seconds_for_dropped_app;
                }
                app->helpers->pending_response = msg;
                CHECK_EQ(app->helpers->partitions_in_progress.load(), 0);
                app->helpers->partitions_in_progress.store(app->partition_count);

                break;
            case app_status::AS_CREATING:
            case app_status::AS_RECALLING:
                response.err = ERR_BUSY_CREATING;
                break;
            case app_status::AS_DROPPING:
                response.err = ERR_BUSY_DROPPING;
                break;
            default:
                CHECK(false, "invalid app status, status = {}", ::dsn::enum_to_string(app->status));
                break;
            }
        }
    }

    if (!do_dropping) {
        REPLY_TO_CLIENT_AND_RETURN(msg, response);
    }

    do_app_drop(app);
}

void server_state::rename_app(configuration_rename_app_rpc rpc)
{
    auto &response = rpc.response();
    bool do_rename = false;

    const auto &old_app_name = rpc.request().old_app_name;
    const auto &new_app_name = rpc.request().new_app_name;
    LOG_INFO("rename app request, old_app_name({}), new_app_name({})", old_app_name, new_app_name);

    zauto_write_lock l(_lock);
    auto target_app = get_app(old_app_name);

    if (target_app == nullptr) {
        response.err = ERR_APP_NOT_EXIST;
        response.hint_message = fmt::format("ERROR: app({}) not exist!", old_app_name);
        return;
    }

    switch (target_app->status) {
    case app_status::AS_AVAILABLE: {
        if (_exist_apps.find(new_app_name) != _exist_apps.end()) {
            response.err = ERR_INVALID_PARAMETERS;
            response.hint_message = fmt::format("ERROR: app({}) already exist!", new_app_name);
            return;
        }
        do_rename = true;
    } break;
    case app_status::AS_CREATING:
    case app_status::AS_RECALLING: {
        response.err = ERR_BUSY_CREATING;
    } break;
    case app_status::AS_DROPPING: {
        response.err = ERR_BUSY_DROPPING;
    } break;
    case app_status::AS_DROPPED: {
        response.err = ERR_APP_DROPPED;
    } break;
    default: {
        response.err = ERR_INVALID_STATE;
    } break;
    }

    if (!do_rename) {
        response.hint_message =
            fmt::format("ERROR: app({}) status can't execute rename.", old_app_name);
        return;
    }

    auto app_id = target_app->app_id;

    auto ainfo = *(reinterpret_cast<app_info *>(target_app.get()));
    ainfo.app_name = new_app_name;
    auto app_path = get_app_path(*target_app);

    target_app->app_name = new_app_name;
    _exist_apps.emplace(new_app_name, target_app);

    do_update_app_info(
        app_path, ainfo, [this, app_id, new_app_name, old_app_name](error_code ec) mutable {
            CHECK_EQ_MSG(
                ec,
                ERR_OK,
                "update remote app info failed: app_id={}, old_app_name={}, new_app_name={}",
                app_id,
                old_app_name,
                new_app_name);

            zauto_write_lock l(_lock);
            _exist_apps.erase(old_app_name);

            LOG_INFO("both remote and local app info of app_name have been updated "
                     "successfully: app_id={}, old_app_name={}, new_app_name={}",
                     app_id,
                     old_app_name,
                     new_app_name);
        });
}

void server_state::do_app_recall(std::shared_ptr<app_state> &app)
{
    auto after_recall_app = [this, app](dsn::error_code ec) mutable {
        zauto_write_lock l(_lock);
        for (int i = 0; i < app->partition_count; ++i) {
            recall_partition(app, i);
        }
    };

    std::string app_path = get_app_path(*app);
    blob value = app->to_json(app_status::AS_AVAILABLE);
    _meta_svc->get_remote_storage()->set_data(
        app_path, value, LPC_META_STATE_HIGH, after_recall_app);
}

void server_state::recall_app(dsn::message_ex *msg)
{
    configuration_recall_app_request request;
    configuration_recall_app_response response;
    std::shared_ptr<app_state> target_app;

    dsn::unmarshall(msg, request);
    LOG_INFO("recall app request, app_id({})", request.app_id);

    bool do_recalling = false;
    {
        zauto_write_lock l(_lock);
        target_app = get_app(request.app_id);
        if (target_app == nullptr) {
            response.err = ERR_APP_NOT_EXIST;
        } else if (target_app->status != app_status::AS_DROPPED) {
            if (target_app->status == app_status::AS_CREATING ||
                target_app->status == app_status::AS_RECALLING)
                response.err = ERR_BUSY_CREATING;
            else if (target_app->status == app_status::AS_DROPPING)
                response.err = ERR_BUSY_DROPPING;
            else
                response.err = ERR_APP_EXIST;
        } else {
            if (has_seconds_expired(target_app->expire_second)) {
                response.err = ERR_APP_NOT_EXIST;
            } else {
                std::string &new_app_name = (request.new_app_name == "") ? target_app->app_name
                                                                         : request.new_app_name;
                if (_exist_apps.find(new_app_name) != _exist_apps.end()) {
                    response.err = ERR_INVALID_PARAMETERS;
                } else {
                    do_recalling = true;
                    target_app->app_name = new_app_name;
                    target_app->status = app_status::AS_RECALLING;
                    CHECK_EQ(target_app->helpers->partitions_in_progress.load(), 0);
                    target_app->helpers->partitions_in_progress.store(target_app->partition_count);
                    target_app->helpers->pending_response = msg;

                    _exist_apps.emplace(target_app->app_name, target_app);
                    _table_metric_entities.create_entity(target_app->app_id,
                                                         target_app->partition_count);
                }
            }
        }
    }

    if (!do_recalling) {
        REPLY_TO_CLIENT_AND_RETURN(msg, response);
    }

    do_app_recall(target_app);
}

void server_state::list_apps(const configuration_list_apps_request &request,
                             configuration_list_apps_response &response,
                             dsn::message_ex *msg) const
{
    LOG_DEBUG("list app request: {}{}status={}",
              request.__isset.app_name_pattern
                  ? fmt::format("app_name_pattern={}, ", request.app_name_pattern)
                  : "",
              request.__isset.match_type
                  ? fmt::format("match_type={}, ", enum_to_string(request.match_type))
                  : "",
              request.status);

    zauto_read_lock l(_lock);

    for (const auto &[_, app] : _all_apps) {
        // If the pattern is provided in the request, any table chosen to be listed must match it.
        if (request.__isset.app_name_pattern && request.__isset.match_type) {
            const auto &result =
                utils::pattern_match(app->app_name, request.app_name_pattern, request.match_type);
            if (result.code() == ERR_NOT_MATCHED) {
                continue;
            }

            if (result.code() != ERR_OK) {
                response.err = result.code();
                response.__set_hint_message(result.message());
                LOG_ERROR("{}, app_name_pattern={}", result, request.app_name_pattern);

                return;
            }
        }

        // Only in the following two cases would a table be chosen to be listed, according to
        // the requested status:
        // - `app_status::AS_INVALID` means no filter, in other words, any table with any status
        // could be chosen;
        // - or, current status of a table is the same as the requested status.
        if (request.status != app_status::AS_INVALID && request.status != app->status) {
            continue;
        }

        if (msg == nullptr || _meta_svc->get_access_controller()->allowed(msg, app->app_name)) {
            response.infos.push_back(*app);
        }
    }

    response.err = dsn::ERR_OK;
}

void server_state::send_proposal(const host_port &target,
                                 const configuration_update_request &proposal)
{
    LOG_INFO("send proposal {} for gpid({}), ballot = {}, target = {}, node = {}",
             ::dsn::enum_to_string(proposal.type),
             proposal.config.pid,
             proposal.config.ballot,
             target,
             FMT_HOST_PORT_AND_IP(proposal, node));
    dsn::message_ex *msg =
        dsn::message_ex::create_request(RPC_CONFIG_PROPOSAL, 0, proposal.config.pid.thread_hash());
    dsn::marshall(msg, proposal);
    _meta_svc->send_message(target, msg);
}

void server_state::send_proposal(const configuration_proposal_action &action,
                                 const partition_configuration &pc,
                                 const app_state &app)
{
    configuration_update_request request;
    request.info = app;
    request.type = action.type;
    SET_OBJ_IP_AND_HOST_PORT(request, node, action, node);
    request.config = pc;
    host_port target;
    GET_HOST_PORT(action, target, target);
    send_proposal(target, request);
}

void server_state::request_check(const partition_configuration &old_pc,
                                 const configuration_update_request &request)
{
    const auto &new_pc = request.config;
    switch (request.type) {
    case config_type::CT_ASSIGN_PRIMARY:
        if (request.__isset.hp_node) {
            CHECK_NE(old_pc.hp_primary, request.hp_node);
            CHECK(!utils::contains(old_pc.hp_secondaries, request.hp_node), "");
        } else {
            CHECK_NE(old_pc.primary, request.node);
            CHECK(!utils::contains(old_pc.secondaries, request.node), "");
        }
        break;
    case config_type::CT_UPGRADE_TO_PRIMARY:
        if (request.__isset.hp_node) {
            CHECK_NE(old_pc.hp_primary, request.hp_node);
            CHECK(utils::contains(old_pc.hp_secondaries, request.hp_node), "");
        } else {
            CHECK_NE(old_pc.primary, request.node);
            CHECK(utils::contains(old_pc.secondaries, request.node), "");
        }
        break;
    case config_type::CT_DOWNGRADE_TO_SECONDARY:
        if (request.__isset.hp_node) {
            CHECK_EQ(old_pc.hp_primary, request.hp_node);
            CHECK(!utils::contains(old_pc.hp_secondaries, request.hp_node), "");
        } else {
            CHECK_EQ(old_pc.primary, request.node);
            CHECK(!utils::contains(old_pc.secondaries, request.node), "");
        }
        break;
    case config_type::CT_DOWNGRADE_TO_INACTIVE:
    case config_type::CT_REMOVE:
        if (request.__isset.hp_node) {
            CHECK(old_pc.hp_primary == request.hp_node ||
                      utils::contains(old_pc.hp_secondaries, request.hp_node),
                  "");
        } else {
            CHECK(old_pc.primary == request.node ||
                      utils::contains(old_pc.secondaries, request.node),
                  "");
        }
        break;
    case config_type::CT_UPGRADE_TO_SECONDARY:
        if (request.__isset.hp_node) {
            CHECK_NE(old_pc.hp_primary, request.hp_node);
            CHECK(!utils::contains(old_pc.hp_secondaries, request.hp_node), "");
        } else {
            CHECK_NE(old_pc.primary, request.node);
            CHECK(!utils::contains(old_pc.secondaries, request.node), "");
        }
        break;
    case config_type::CT_PRIMARY_FORCE_UPDATE_BALLOT: {
        if (request.__isset.hp_node) {
            CHECK_EQ(old_pc.hp_primary, new_pc.hp_primary);
            CHECK(old_pc.hp_secondaries == new_pc.hp_secondaries, "");
        } else {
            CHECK_EQ(old_pc.primary, new_pc.primary);
            CHECK(old_pc.secondaries == new_pc.secondaries, "");
        }
        break;
    }
    default:
        break;
    }
}

void server_state::update_configuration_locally(
    app_state &app, std::shared_ptr<configuration_update_request> &config_request)
{
    dsn::gpid &gpid = config_request->config.pid;
    partition_configuration &old_pc = app.pcs[gpid.get_partition_index()];
    partition_configuration &new_pc = config_request->config;

    int min_2pc_count =
        _meta_svc->get_options().app_mutation_2pc_min_replica_count(app.max_replica_count);
    health_status old_health_status = partition_health_status(old_pc, min_2pc_count);
    health_status new_health_status = partition_health_status(new_pc, min_2pc_count);

    host_port node;
    GET_HOST_PORT(*config_request, node, node);

    if (app.is_stateful) {
        CHECK(old_pc.ballot == invalid_ballot || old_pc.ballot + 1 == new_pc.ballot,
              "invalid configuration update request, old ballot {}, new ballot {}",
              old_pc.ballot,
              new_pc.ballot);

        node_state *ns = nullptr;
        if (config_request->type != config_type::CT_DROP_PARTITION) {
            ns = get_node_state(_nodes, node, false);
            CHECK_NOTNULL(ns, "invalid node: {}", node);
        }
#ifndef NDEBUG
        request_check(old_pc, *config_request);
#endif
        switch (config_request->type) {
        case config_type::CT_ASSIGN_PRIMARY:
        case config_type::CT_UPGRADE_TO_PRIMARY:
            ns->put_partition(gpid, true);
            break;

        case config_type::CT_UPGRADE_TO_SECONDARY:
            ns->put_partition(gpid, false);
            break;

        case config_type::CT_DOWNGRADE_TO_SECONDARY:
            ns->remove_partition(gpid, true);
            break;

        case config_type::CT_DOWNGRADE_TO_INACTIVE:
        case config_type::CT_REMOVE:
            ns->remove_partition(gpid, false);
            break;
        // nothing to handle, the ballot will updated in below
        case config_type::CT_PRIMARY_FORCE_UPDATE_BALLOT:
            break;

        case config_type::CT_DROP_PARTITION: {
            for (const auto &last_drop : new_pc.hp_last_drops) {
                ns = get_node_state(_nodes, last_drop, false);
                if (ns != nullptr) {
                    ns->remove_partition(gpid, false);
                }
            }
            break;
        }
        case config_type::CT_ADD_SECONDARY:
        case config_type::CT_ADD_SECONDARY_FOR_LB:
            CHECK(false, "invalid execution work flow");
            break;
        case config_type::CT_REGISTER_CHILD: {
            ns->put_partition(gpid, true);
            // TODO(yingchun): optimize the duplicate loops.
            if (config_request->config.__isset.hp_secondaries) {
                for (const auto &secondary : config_request->config.hp_secondaries) {
                    auto *secondary_node = get_node_state(_nodes, secondary, false);
                    secondary_node->put_partition(gpid, false);
                }
            } else {
                for (const auto &secondary : config_request->config.secondaries) {
                    const auto hp = host_port::from_address(secondary);
                    if (!hp) {
                        LOG_ERROR("The registering secondary {} for pid {} can no be reverse "
                                  "resolved, skip registering it, please check the network "
                                  "configuration",
                                  secondary,
                                  config_request->config.pid);
                        continue;
                    }
                    auto secondary_node = get_node_state(_nodes, hp, false);
                    secondary_node->put_partition(gpid, false);
                }
            }
            break;
        }
        default:
            CHECK(false, "");
            break;
        }
    } else {
        CHECK_EQ(old_pc.ballot, new_pc.ballot);
        const auto host_node = host_port::from_address(config_request->host_node);
        // The non-stateful app is just for testing, so just check the host_node is resolvable.
        CHECK(host_node, "'{}' can not be reverse resolved", config_request->host_node);
        new_pc = old_pc;
        partition_configuration_stateless pcs(new_pc);
        if (config_request->type == config_type::type::CT_ADD_SECONDARY) {
            pcs.hosts().emplace_back(host_node);
            pcs.workers().emplace_back(node);
        } else {
            auto it = std::remove(pcs.hosts().begin(), pcs.hosts().end(), host_node);
            pcs.hosts().erase(it);

            it = std::remove(pcs.workers().begin(), pcs.workers().end(), node);
            pcs.workers().erase(it);
        }

        auto it = _nodes.find(host_node);
        CHECK(it != _nodes.end(), "invalid node: {}", host_node);
        if (config_type::CT_REMOVE == config_request->type) {
            it->second.remove_partition(gpid, false);
        } else {
            it->second.put_partition(gpid, false);
        }
    }

    // we assume config in config_request stores the proper new config
    // as we sync to remote storage according to it
    std::string old_config_str = boost::lexical_cast<std::string>(old_pc);
    old_pc = config_request->config;
    auto find_name = _config_type_VALUES_TO_NAMES.find(config_request->type);
    if (find_name != _config_type_VALUES_TO_NAMES.end()) {
        LOG_INFO("meta update config ok: type({}), old_config={}, {}",
                 find_name->second,
                 old_config_str,
                 boost::lexical_cast<std::string>(*config_request));
    } else {
        LOG_INFO("meta update config ok: type({}), old_config={}, {}",
                 config_request->type,
                 old_config_str,
                 boost::lexical_cast<std::string>(*config_request));
    }

#ifndef NDEBUG
    check_consistency(gpid);
#endif
    if (_config_change_subscriber) {
        _config_change_subscriber(_all_apps);
    }

    METRIC_INCREMENT(_table_metric_entities, partition_configuration_changes, gpid);
    if (old_health_status >= HS_WRITABLE_ILL && new_health_status < HS_WRITABLE_ILL) {
        METRIC_INCREMENT(_table_metric_entities, unwritable_partition_changes, gpid);
    }
    if (old_health_status < HS_WRITABLE_ILL && new_health_status >= HS_WRITABLE_ILL) {
        METRIC_INCREMENT(_table_metric_entities, writable_partition_changes, gpid);
    }
}

task_ptr server_state::update_configuration_on_remote(
    std::shared_ptr<configuration_update_request> &config_request)
{
    meta_function_level::type l = _meta_svc->get_function_level();
    if (l <= meta_function_level::fl_blind) {
        LOG_INFO("ignore update configuration on remote due to level is {}",
                 _meta_function_level_VALUES_TO_NAMES.find(l)->second);
        // NOTICE: pending_sync_task need to be reassigned
        return tasking::enqueue(
            LPC_META_STATE_HIGH,
            tracker(),
            [this, config_request]() mutable {
                std::shared_ptr<app_state> app = get_app(config_request->config.pid.get_app_id());
                config_context &cc =
                    app->helpers->contexts[config_request->config.pid.get_partition_index()];
                cc.pending_sync_task = update_configuration_on_remote(config_request);
            },
            0,
            std::chrono::seconds(1));
    }

    partition_configuration &pc = config_request->config;
    std::string storage_path = get_partition_path(pc.pid);

    blob json_config = dsn::json::json_forwarder<partition_configuration>::encode(pc);
    return _meta_svc->get_remote_storage()->set_data(
        storage_path,
        json_config,
        LPC_META_STATE_HIGH,
        std::bind(&server_state::on_update_configuration_on_remote_reply,
                  this,
                  std::placeholders::_1,
                  config_request),
        tracker());
}

void server_state::on_update_configuration_on_remote_reply(
    error_code ec, std::shared_ptr<configuration_update_request> &config_request)
{
    zauto_write_lock l(_lock);
    dsn::gpid &gpid = config_request->config.pid;
    std::shared_ptr<app_state> app = get_app(gpid.get_app_id());
    config_context &cc = app->helpers->contexts[gpid.get_partition_index()];

    // if multiple threads exist in the thread pool, the check may be failed
    CHECK(app->status == app_status::AS_AVAILABLE || app->status == app_status::AS_DROPPING,
          "if app removed, this task should be cancelled");
    if (ec == ERR_TIMEOUT) {
        cc.pending_sync_task = tasking::enqueue(
            LPC_META_STATE_HIGH,
            tracker(),
            [this, config_request, &cc]() mutable {
                cc.pending_sync_task = update_configuration_on_remote(config_request);
            },
            0,
            std::chrono::seconds(1));
    } else if (ec == ERR_OK) {
        update_configuration_locally(*app, config_request);
        cc.pending_sync_task = nullptr;
        cc.pending_sync_request.reset();
        cc.stage = config_status::not_pending;
        if (cc.msg) {
            configuration_update_response resp;
            resp.err = ERR_OK;
            resp.config = config_request->config;
            REPLY_TO_CLIENT(cc.msg, resp);
            cc.msg = nullptr;
        }

        _meta_svc->get_partition_guardian()->reconfig({&_all_apps, &_nodes}, *config_request);
        if (config_request->type == config_type::CT_DROP_PARTITION) {
            process_one_partition(app);
        } else {
            configuration_proposal_action action;
            _meta_svc->get_partition_guardian()->cure({&_all_apps, &_nodes}, gpid, action);
            if (action.type != config_type::CT_INVALID) {
                if (_add_secondary_enable_flow_control &&
                    (action.type == config_type::CT_ADD_SECONDARY ||
                     action.type == config_type::CT_ADD_SECONDARY_FOR_LB)) {
                    // ignore adding secondary if add_secondary_enable_flow_control = true
                } else {
                    config_request->type = action.type;
                    SET_OBJ_IP_AND_HOST_PORT(*config_request, node, action, node);
                    config_request->info = *app;
                    host_port target;
                    GET_HOST_PORT(action, target, target);
                    send_proposal(target, *config_request);
                }
            }
        }
    } else {
        CHECK(false, "we can't handle this right now, err = {}", ec);
    }
}

void server_state::recall_partition(std::shared_ptr<app_state> &app, int pidx)
{
    auto on_recall_partition = [this, app, pidx](dsn::error_code error) mutable {
        if (error == dsn::ERR_OK) {
            zauto_write_lock l(_lock);
            app->pcs[pidx].partition_flags &= (~pc_flags::dropped);
            process_one_partition(app);
        } else if (error == dsn::ERR_TIMEOUT) {
            tasking::enqueue(LPC_META_STATE_HIGH,
                             tracker(),
                             std::bind(&server_state::recall_partition, this, app, pidx),
                             server_state::sStateHash,
                             std::chrono::seconds(1));
        } else {
            CHECK(false, "unable to handle this({}) right now", error);
        }
    };

    partition_configuration &pc = app->pcs[pidx];
    CHECK((pc.partition_flags & pc_flags::dropped), "");

    pc.partition_flags = 0;
    blob json_partition = dsn::json::json_forwarder<partition_configuration>::encode(pc);
    std::string partition_path = get_partition_path(pc.pid);
    _meta_svc->get_remote_storage()->set_data(
        partition_path, json_partition, LPC_META_STATE_HIGH, on_recall_partition);
}

void server_state::drop_partition(std::shared_ptr<app_state> &app, int pidx)
{
    partition_configuration &pc = app->pcs[pidx];
    config_context &cc = app->helpers->contexts[pidx];

    std::shared_ptr<configuration_update_request> req =
        std::make_shared<configuration_update_request>();
    configuration_update_request &request = *req;

    request.info = *app;
    request.type = config_type::CT_DROP_PARTITION;
    SET_OBJ_IP_AND_HOST_PORT(request, node, pc, primary);

    request.config = pc;
    for (const auto &secondary : pc.hp_secondaries) {
        maintain_drops(request.config.hp_last_drops, secondary, request.type);
    }
    for (const auto &secondary : pc.secondaries) {
        maintain_drops(request.config.last_drops, secondary, request.type);
    }
    if (pc.hp_primary) {
        maintain_drops(request.config.hp_last_drops, pc.hp_primary, request.type);
    }
    if (pc.primary) {
        maintain_drops(request.config.last_drops, pc.primary, request.type);
    }
    RESET_IP_AND_HOST_PORT(request.config, primary);
    CLEAR_IP_AND_HOST_PORT(request.config, secondaries);

    CHECK_EQ((pc.partition_flags & pc_flags::dropped), 0);
    request.config.partition_flags |= pc_flags::dropped;

    // NOTICE this mis-understanding: if a old state is DDD, we may not need to udpate the ballot.
    // Actually it is necessary. Coz we may send a proposal due to the old DDD state
    // and laterly a update_config may arrive.
    // An updated ballot annouces a previous state is INVALID and all actions taken
    // due to the old one should be staled
    request.config.ballot++;

    if (config_status::pending_remote_sync == cc.stage) {
        LOG_WARNING("gpid({}.{}) is syncing another request with remote, cancel it due to "
                    "partition is dropped",
                    app->app_id,
                    pidx);
        cc.cancel_sync();
    }
    cc.stage = config_status::pending_remote_sync;
    cc.pending_sync_request = req;
    cc.msg = nullptr;

    cc.pending_sync_task = update_configuration_on_remote(req);
}

void server_state::downgrade_primary_to_inactive(std::shared_ptr<app_state> &app, int pidx)
{
    partition_configuration &pc = app->pcs[pidx];
    config_context &cc = app->helpers->contexts[pidx];

    if (config_status::pending_remote_sync == cc.stage) {
        if (cc.pending_sync_request->type == config_type::CT_DROP_PARTITION) {
            CHECK_EQ_MSG(app->status,
                         app_status::AS_DROPPING,
                         "app({}) not in dropping state ({})",
                         app->get_logname(),
                         enum_to_string(app->status));
            LOG_WARNING(
                "stop downgrade primary as the partitions({}.{}) is dropping", app->app_id, pidx);
            return;
        } else {
            LOG_WARNING("gpid({}) is syncing another request with remote, cancel it due to the "
                        "primary({}) is down",
                        pc.pid,
                        FMT_HOST_PORT_AND_IP(pc, primary));
            cc.cancel_sync();
        }
    }

    std::shared_ptr<configuration_update_request> req =
        std::make_shared<configuration_update_request>();
    configuration_update_request &request = *req;
    request.info = *app;
    request.config = pc;
    request.type = config_type::CT_DOWNGRADE_TO_INACTIVE;
    SET_OBJ_IP_AND_HOST_PORT(request, node, pc, primary);
    request.config.ballot++;
    RESET_IP_AND_HOST_PORT(request.config, primary);
    maintain_drops(request.config.hp_last_drops, pc.hp_primary, request.type);
    maintain_drops(request.config.last_drops, pc.primary, request.type);

    cc.stage = config_status::pending_remote_sync;
    cc.pending_sync_request = req;
    cc.msg = nullptr;

    cc.pending_sync_task = update_configuration_on_remote(req);
}

void server_state::downgrade_secondary_to_inactive(std::shared_ptr<app_state> &app,
                                                   int pidx,
                                                   const host_port &node)
{
    partition_configuration &pc = app->pcs[pidx];
    config_context &cc = app->helpers->contexts[pidx];

    CHECK(pc.hp_primary, "this shouldn't be called if the primary is invalid");
    if (config_status::pending_remote_sync != cc.stage) {
        configuration_update_request request;
        request.info = *app;
        request.config = pc;
        request.type = config_type::CT_DOWNGRADE_TO_INACTIVE;
        SET_IP_AND_HOST_PORT_BY_DNS(request, node, node);
        host_port primary;
        GET_HOST_PORT(pc, primary, primary);
        send_proposal(primary, request);
    } else {
        LOG_INFO("gpid({}.{}) is syncing with remote storage, ignore the remove seconary({})",
                 app->app_id,
                 pidx,
                 node);
    }
}

void server_state::downgrade_stateless_nodes(std::shared_ptr<app_state> &app,
                                             int pidx,
                                             const host_port &node)
{
    auto req = std::make_shared<configuration_update_request>();
    req->info = *app;
    req->type = config_type::CT_REMOVE;
    req->host_node = dsn::dns_resolver::instance().resolve_address(node);
    RESET_IP_AND_HOST_PORT(*req, node);
    req->config = app->pcs[pidx];

    config_context &cc = app->helpers->contexts[pidx];
    partition_configuration &pc = req->config;

    unsigned i = 0;
    for (; i < pc.hp_secondaries.size(); ++i) {
        if (pc.hp_secondaries[i] == node) {
            SET_OBJ_IP_AND_HOST_PORT(*req, node, pc, last_drops[i]);
            break;
        }
    }
    host_port req_node;
    GET_HOST_PORT(*req, node, req_node);
    CHECK(req_node, "invalid node: {}", req_node);
    // remove host_node & node from secondaries/last_drops, as it will be sync to remote
    // storage
    CHECK(pc.__isset.hp_secondaries, "hp_secondaries not set");
    for (++i; i < pc.hp_secondaries.size(); ++i) {
        pc.secondaries[i - 1] = pc.secondaries[i];
        pc.last_drops[i - 1] = pc.last_drops[i];
        pc.hp_secondaries[i - 1] = pc.hp_secondaries[i];
        pc.hp_last_drops[i - 1] = pc.hp_last_drops[i];
    }
    pc.secondaries.pop_back();
    pc.last_drops.pop_back();
    pc.hp_secondaries.pop_back();
    pc.hp_last_drops.pop_back();

    if (config_status::pending_remote_sync == cc.stage) {
        LOG_WARNING("gpid({}) is syncing another request with remote, cancel it due to meta is "
                    "removing host({}) worker({})",
                    pc.pid,
                    req->host_node,
                    req_node);
        cc.cancel_sync();
    }
    cc.stage = config_status::pending_remote_sync;
    cc.pending_sync_request = req;
    cc.msg = nullptr;

    cc.pending_sync_task = update_configuration_on_remote(req);
}

void server_state::on_update_configuration(
    std::shared_ptr<configuration_update_request> &cfg_request, dsn::message_ex *msg)
{
    zauto_write_lock l(_lock);
    dsn::gpid &gpid = cfg_request->config.pid;
    std::shared_ptr<app_state> app = get_app(gpid.get_app_id());
    partition_configuration &pc = app->pcs[gpid.get_partition_index()];
    config_context &cc = app->helpers->contexts[gpid.get_partition_index()];
    configuration_update_response response;
    response.err = ERR_IO_PENDING;

    CHECK(app, "get get app for app id({})", gpid.get_app_id());
    CHECK(app->is_stateful, "don't support stateless apps currently, id({})", gpid.get_app_id());
    auto find_name = _config_type_VALUES_TO_NAMES.find(cfg_request->type);
    if (find_name != _config_type_VALUES_TO_NAMES.end()) {
        LOG_INFO("recv update config request: type({}), {}",
                 find_name->second,
                 boost::lexical_cast<std::string>(*cfg_request));
    } else {
        LOG_INFO("recv update config request: type({}), {}",
                 cfg_request->type,
                 boost::lexical_cast<std::string>(*cfg_request));
    }

    if (is_partition_config_equal(pc, cfg_request->config)) {
        LOG_INFO("duplicated update request for gpid({}), ballot: {}", gpid, pc.ballot);
        response.err = ERR_OK;
        //
        // NOTICE:
        //    if a replica server resend a update-request,
        //    the meta has update the last_drops, and we should reply with new last_drops
        //
        response.config = pc;
    } else if (pc.ballot + 1 != cfg_request->config.ballot) {
        LOG_INFO("update configuration for gpid({}) reject coz ballot not match, request ballot: "
                 "{}, meta ballot: {}",
                 gpid,
                 cfg_request->config.ballot,
                 pc.ballot);
        response.err = ERR_INVALID_VERSION;
        response.config = pc;
    } else if (config_status::pending_remote_sync == cc.stage) {
        LOG_INFO("another request is syncing with remote storage, ignore current request, "
                 "gpid({}), request ballot({})",
                 gpid,
                 cfg_request->config.ballot);
        // we don't reply the replica server, expect it to retry
        msg->release_ref();
        return;
    } else {
        maintain_drops(cfg_request->config.hp_last_drops, cfg_request->hp_node, cfg_request->type);
        maintain_drops(cfg_request->config.last_drops, cfg_request->node, cfg_request->type);
    }

    if (response.err != ERR_IO_PENDING) {
        REPLY_TO_CLIENT_AND_RETURN(msg, response);
    }

    CHECK(config_status::not_pending == cc.stage,
          "invalid config status, cc.stage = {}",
          enum_to_string(cc.stage));
    cc.stage = config_status::pending_remote_sync;
    cc.pending_sync_request = cfg_request;
    cc.msg = msg;
    cc.pending_sync_task = update_configuration_on_remote(cfg_request);
}

void server_state::on_partition_node_dead(std::shared_ptr<app_state> &app,
                                          int pidx,
                                          const dsn::host_port &node)
{
    const auto &pc = app->pcs[pidx];
    if (!app->is_stateful) {
        downgrade_stateless_nodes(app, pidx, node);
        return;
    }

    if (is_primary(pc, node)) {
        downgrade_primary_to_inactive(app, pidx);
        return;
    }

    if (!is_secondary(pc, node)) {
        return;
    }

    if (pc.hp_primary) {
        downgrade_secondary_to_inactive(app, pidx, node);
        return;
    }

    CHECK(is_secondary(pc, node), "");
    LOG_INFO("gpid({}): secondary({}) is down, ignored it due to no primary for this partition "
             "available",
             pc.pid,
             node);
}

void server_state::on_change_node_state(const host_port &node, bool is_alive)
{
    LOG_DEBUG("change node({}) state to {}", node, is_alive ? "alive" : "dead");
    zauto_write_lock l(_lock);
    if (is_alive) {
        get_node_state(_nodes, node, true)->set_alive(true);
        return;
    }

    auto iter = _nodes.find(node);
    if (iter == _nodes.end()) {
        LOG_INFO("node({}) doesn't exist in the node state, just ignore", node);
        return;
    }

    node_state &ns = iter->second;
    ns.set_alive(false);
    ns.set_replicas_collect_flag(false);
    ns.for_each_partition([&, this](const dsn::gpid &pid) {
        std::shared_ptr<app_state> app = get_app(pid.get_app_id());
        CHECK(app != nullptr && app->status != app_status::AS_DROPPED,
              "invalid app, app_id = {}",
              pid.get_app_id());
        on_partition_node_dead(app, pid.get_partition_index(), node);
        return true;
    });
}

void server_state::on_propose_balancer(const configuration_balancer_request &request,
                                       configuration_balancer_response &response)
{
    zauto_write_lock l(_lock);
    std::shared_ptr<app_state> app = get_app(request.gpid.get_app_id());
    if (app == nullptr || app->status != app_status::AS_AVAILABLE ||
        request.gpid.get_partition_index() < 0 ||
        request.gpid.get_partition_index() >= app->partition_count) {
        response.err = ERR_INVALID_PARAMETERS;
        return;
    }

    if (request.force) {
        const auto &pc = *get_config(_all_apps, request.gpid);
        for (const auto &act : request.action_list) {
            send_proposal(act, pc, *app);
        }
        response.err = ERR_OK;
        return;
    }

    _meta_svc->get_balancer()->register_proposals({&_all_apps, &_nodes}, request, response);
}

namespace {

bool app_info_compatible_equal(const app_info &l, const app_info &r)
{
    // Some fields like `app_type`, `app_id` and `create_second` are initialized and
    // persisted into .app-info file when the replica is created, and will NEVER be
    // changed during their lifetime even if the table is dropped or recalled. Their
    // consistency must be checked.
    //
    // Some fields may be updated during their lifetime, but will NEVER be persisted
    // into .app-info, such as most environments in `envs`. Their consistency do not
    // need to be checked.
    //
    // Some fields may be updated during their lifetime, and will also be persited into
    // .app-info file:
    // - For the fields such as `app_name`, `max_replica_count` and `atomic_idempotent`
    // without compatibility problems, their consistency should be checked.
    // - For the fields such as `duplicating` whose compatibility varies between primary
    // and secondaries in 2.1.x, 2.2.x and 2.3.x release, their consistency are not
    // checked.
    return l.status == r.status && l.app_type == r.app_type && l.app_name == r.app_name &&
           l.app_id == r.app_id && l.partition_count == r.partition_count &&
           l.is_stateful == r.is_stateful && l.max_replica_count == r.max_replica_count &&
           l.expire_second == r.expire_second && l.create_second == r.create_second &&
           l.drop_second == r.drop_second && l.atomic_idempotent == r.atomic_idempotent;
}

} // anonymous namespace

error_code
server_state::construct_apps(const std::vector<query_app_info_response> &query_app_responses,
                             const std::vector<dsn::host_port> &replica_nodes,
                             std::string &hint_message)
{
    int max_app_id = 0;
    for (size_t i = 0; i < query_app_responses.size(); ++i) {
        const auto &query_resp = query_app_responses[i];
        if (query_resp.err != dsn::ERR_OK) {
            continue;
        }

        for (const app_info &info : query_resp.apps) {
            CHECK_GT_MSG(info.app_id, 0, "invalid app id");
            const auto iter = std::as_const(_all_apps).find(info.app_id);
            if (iter == _all_apps.end()) {
                std::shared_ptr<app_state> app = app_state::create(info);
                LOG_INFO("create app info from ({}) for id({}): {}",
                         replica_nodes[i],
                         info.app_id,
                         boost::lexical_cast<std::string>(info));
                _all_apps.emplace(app->app_id, app);
                max_app_id = std::max(app->app_id, max_app_id);
                continue;
            }

            app_info *old_info = iter->second.get();
            if (info == *old_info) {
                continue;
            }

            CHECK(app_info_compatible_equal(info, *old_info),
                  "conflict app info from ({}) for id({}): new_info({}), old_info({})",
                  replica_nodes[i],
                  info.app_id,
                  boost::lexical_cast<std::string>(info),
                  boost::lexical_cast<std::string>(*old_info));
        }
    }

    // create placeholder for dropped table
    for (int app_id = 1; app_id <= max_app_id; ++app_id) {
        auto iter = _all_apps.find(app_id);
        if (iter == _all_apps.end()) {
            app_info dropped_holder;
            dropped_holder.app_id = app_id;
            dropped_holder.app_name = "__drop_holder__" + boost::lexical_cast<std::string>(app_id);
            dropped_holder.app_type = "pegasus";
            dropped_holder.is_stateful = true;
            dropped_holder.max_replica_count = 3;
            // in remote-storage-interaction module,
            // we assume there is at least one partition
            dropped_holder.partition_count = 1;
            dropped_holder.status = app_status::AS_DROPPING;
            dropped_holder.expire_second = dsn_now_ms() / 1000;

            _all_apps.emplace(app_id, app_state::create(dropped_holder));
        } else {
            app_info *app_info = iter->second.get();
            app_info->status = (app_status::AS_AVAILABLE == app_info->status)
                                   ? app_status::AS_CREATING
                                   : app_status::AS_DROPPING;
        }
    }

    // check conflict table name
    std::map<std::string, int32_t> checked_names;
    for (int app_id = max_app_id; app_id >= 1; --app_id) {
        CHECK(_all_apps.find(app_id) != _all_apps.end(), "invalid app_id, app_id = {}", app_id);
        std::shared_ptr<app_state> &app = _all_apps[app_id];
        std::string old_name = app->app_name;
        while (checked_names.find(app->app_name) != checked_names.end()) {
            app->app_name = app->app_name + "__" + boost::lexical_cast<std::string>(app_id);
        }
        if (app->app_name != old_name) {
            LOG_WARNING("app({})'s old name({}) is conflict with others, rename it to ({})",
                        app_id,
                        old_name,
                        app->app_name);
            std::ostringstream oss;
            oss << "WARNING: app(" << app_id << ")'s old name(" << old_name
                << ") is conflict with others, rename it to (" << app->app_name << ")" << std::endl;
            hint_message += oss.str();
        }
        checked_names.emplace(app->app_name, app_id);
    }

    LOG_INFO("construct apps done, max_app_id = {}", max_app_id);

    return dsn::ERR_OK;
}

error_code server_state::construct_partitions(
    const std::vector<query_replica_info_response> &query_replica_responses,
    const std::vector<dsn::host_port> &replica_nodes,
    bool skip_lost_partitions,
    std::string &hint_message)
{
    for (unsigned int i = 0; i < query_replica_responses.size(); ++i) {
        query_replica_info_response query_resp = query_replica_responses[i];
        if (query_resp.err != dsn::ERR_OK)
            continue;

        for (replica_info &r : query_resp.replicas) {
            CHECK(_all_apps.find(r.pid.get_app_id()) != _all_apps.end(), "");
            bool is_accepted = collect_replica({&_all_apps, &_nodes}, replica_nodes[i], r);
            if (is_accepted) {
                LOG_INFO("accept replica({}) from node({})",
                         boost::lexical_cast<std::string>(r),
                         replica_nodes[i]);
            } else {
                LOG_INFO("ignore replica({}) from node({})",
                         boost::lexical_cast<std::string>(r),
                         replica_nodes[i]);
            }
        }
    }

    int succeed_count = 0;
    int failed_count = 0;
    for (auto &app_kv : _all_apps) {
        std::shared_ptr<app_state> &app = app_kv.second;
        CHECK(app->status == app_status::AS_CREATING || app->status == app_status::AS_DROPPING,
              "invalid app status, status = {}",
              enum_to_string(app->status));
        if (app->status == app_status::AS_DROPPING) {
            LOG_INFO("ignore constructing partitions for dropping app({})", app->app_id);
        } else {
            for (const auto &pc : app->pcs) {
                bool is_succeed =
                    construct_replica({&_all_apps, &_nodes}, pc.pid, app->max_replica_count);
                if (is_succeed) {
                    LOG_INFO("construct partition({}.{}) succeed: {}",
                             app->app_id,
                             pc.pid.get_partition_index(),
                             boost::lexical_cast<std::string>(pc));
                    if (pc.hp_last_drops.size() + 1 < pc.max_replica_count) {
                        hint_message += fmt::format("WARNING: partition({}.{}) only collects {}/{} "
                                                    "of replicas, may lost data",
                                                    app->app_id,
                                                    pc.pid.get_partition_index(),
                                                    pc.hp_last_drops.size() + 1,
                                                    pc.max_replica_count);
                    }
                    succeed_count++;
                } else {
                    LOG_WARNING("construct partition({}.{}) failed",
                                app->app_id,
                                pc.pid.get_partition_index());
                    std::ostringstream oss;
                    if (skip_lost_partitions) {
                        oss << "WARNING: partition(" << app->app_id << "."
                            << pc.pid.get_partition_index()
                            << ") has no replica collected, force "
                               "recover the lost partition to empty"
                            << std::endl;
                    } else {
                        oss << "ERROR: partition(" << app->app_id << "."
                            << pc.pid.get_partition_index()
                            << ") has no replica collected, you can force recover it by set "
                               "skip_lost_partitions option"
                            << std::endl;
                    }
                    hint_message += oss.str();
                    failed_count++;
                }
            }
        }
    }

    LOG_INFO("construct partition done, succeed_count = {}, failed_count = {}, "
             "skip_lost_partitions = {}",
             succeed_count,
             failed_count,
             skip_lost_partitions ? "true" : "false");

    if (failed_count > 0 && !skip_lost_partitions) {
        return dsn::ERR_TRY_AGAIN;
    } else {
        return dsn::ERR_OK;
    }
}

dsn::error_code
server_state::sync_apps_from_replica_nodes(const std::vector<dsn::host_port> &replica_nodes,
                                           bool skip_bad_nodes,
                                           bool skip_lost_partitions,
                                           std::string &hint_message)
{
    int n_replicas = replica_nodes.size();
    std::vector<query_app_info_response> query_app_responses(n_replicas);
    std::vector<query_replica_info_response> query_replica_responses(n_replicas);
    std::vector<dsn::error_code> query_app_errors(n_replicas);
    std::vector<dsn::error_code> query_replica_errors(n_replicas);

    dsn::task_tracker tracker;
    for (int i = 0; i < n_replicas; ++i) {
        LOG_INFO("send query app and replica request to node({})", replica_nodes[i]);

        auto app_query_req = std::make_unique<query_app_info_request>();
        SET_IP_AND_HOST_PORT(
            *app_query_req, meta_server, dsn_primary_address(), dsn_primary_host_port());
        query_app_info_rpc app_rpc(std::move(app_query_req), RPC_QUERY_APP_INFO);
        const auto addr = dsn::dns_resolver::instance().resolve_address(replica_nodes[i]);
        app_rpc.call(addr,
                     &tracker,
                     [app_rpc, i, &replica_nodes, &query_app_errors, &query_app_responses](
                         error_code err) mutable {
                         auto resp = app_rpc.response();
                         LOG_INFO(
                             "received query app response from node({}), err({}), apps_count({})",
                             replica_nodes[i],
                             err,
                             resp.apps.size());
                         query_app_errors[i] = err;
                         if (err == dsn::ERR_OK) {
                             query_app_responses[i] = std::move(resp);
                         }
                     });

        auto replica_query_req = std::make_unique<query_replica_info_request>();
        SET_IP_AND_HOST_PORT_BY_DNS(*replica_query_req, node1, replica_nodes[i]);
        query_replica_info_rpc replica_rpc(std::move(replica_query_req), RPC_QUERY_REPLICA_INFO);
        replica_rpc.call(
            addr,
            &tracker,
            [replica_rpc, i, &replica_nodes, &query_replica_errors, &query_replica_responses](
                error_code err) mutable {
                auto resp = replica_rpc.response();
                LOG_INFO(
                    "received query replica response from node({}), err({}), replicas_count({})",
                    replica_nodes[i],
                    err,
                    resp.replicas.size());
                query_replica_errors[i] = err;
                if (err == dsn::ERR_OK) {
                    query_replica_responses[i] = std::move(resp);
                }
            });
    }

    tracker.wait_outstanding_tasks();
    int failed_count = 0;
    int succeed_count = 0;
    for (int i = 0; i < n_replicas; ++i) {
        error_code err = dsn::ERR_OK;
        if (query_app_errors[i] != dsn::ERR_OK) {
            LOG_WARNING("query app info from node({}) failed, reason: {}",
                        replica_nodes[i],
                        query_app_errors[i]);
            err = query_app_errors[i];
        }
        if (query_replica_errors[i] != dsn::ERR_OK) {
            LOG_WARNING("query replica info from node({}) failed, reason: {}",
                        replica_nodes[i],
                        query_replica_errors[i]);
            err = query_replica_errors[i];
        }
        if (err != dsn::ERR_OK) {
            failed_count++;
            query_app_errors[i] = err;
            query_replica_errors[i] = err;
            if (skip_bad_nodes) {
                hint_message += fmt::format("WARNING: collect app and replica info from node({}) "
                                            "failed with err({}), skip the bad node",
                                            replica_nodes[i],
                                            err);
            } else {
                hint_message +=
                    fmt::format("ERROR: collect app and replica info from node({}) failed with "
                                "err({}), you can skip it by set skip_bad_nodes option",
                                replica_nodes[i],
                                err);
            }
        } else {
            succeed_count++;
        }
    }

    LOG_INFO("sync apps and replicas from replica nodes done, succeed_count = {}, failed_count = "
             "{}, skip_bad_nodes = {}",
             succeed_count,
             failed_count,
             skip_bad_nodes ? "true" : "false");

    if (failed_count > 0 && !skip_bad_nodes) {
        return dsn::ERR_TRY_AGAIN;
    }

    zauto_write_lock l(_lock);

    dsn::error_code err = construct_apps(query_app_responses, replica_nodes, hint_message);
    if (err != dsn::ERR_OK) {
        LOG_ERROR("construct apps failed, err = {}", err);
        return err;
    }

    err = construct_partitions(
        query_replica_responses, replica_nodes, skip_lost_partitions, hint_message);
    if (err != dsn::ERR_OK) {
        LOG_ERROR("construct partitions failed, err = {}", err);
        return err;
    }

    return dsn::ERR_OK;
}

void server_state::on_start_recovery(const configuration_recovery_request &req,
                                     configuration_recovery_response &resp)
{
    LOG_INFO("start recovery, node_count = {}, skip_bad_nodes = {}, skip_lost_partitions = {}",
             req.recovery_nodes.size(),
             req.skip_bad_nodes ? "true" : "false",
             req.skip_lost_partitions ? "true" : "false");

    std::vector<host_port> recovery_nodes;
    GET_HOST_PORTS(req, recovery_nodes, recovery_nodes);
    resp.err = sync_apps_from_replica_nodes(
        recovery_nodes, req.skip_bad_nodes, req.skip_lost_partitions, resp.hint_message);

    if (resp.err != dsn::ERR_OK) {
        LOG_ERROR("sync apps from replica nodes failed when do recovery, err = {}", resp.err);
        _all_apps.clear();
        return;
    }

    resp.err = sync_apps_to_remote_storage();
    CHECK_EQ_MSG(resp.err,
                 dsn::ERR_OK,
                 "sync apps to remote storage failed when do recovery, need "
                 "to manually clear contents from remote storage and "
                 "restart the service");

    initialize_node_state();
}

void server_state::clear_proposals()
{
    LOG_INFO("clear all exist proposals");
    zauto_write_lock l(_lock);
    for (auto &kv : _exist_apps) {
        std::shared_ptr<app_state> &app = kv.second;
        app->helpers->clear_proposals();
    }
}

bool server_state::can_run_balancer()
{
    // dead nodes check
    for (auto iter = _nodes.begin(); iter != _nodes.end();) {
        if (!iter->second.alive()) {
            if (iter->second.partition_count() != 0) {
                LOG_INFO(
                    "don't do replica migration coz dead node({}) has {} partitions not removed",
                    iter->second.host_port(),
                    iter->second.partition_count());
                return false;
            }
            _nodes.erase(iter++);
        } else
            ++iter;
    }

    // table stability check
    int c = count_staging_app();
    if (c != 0) {
        LOG_INFO("don't do replica migration coz {} table(s) is(are) in staging state", c);
        return false;
    }
    return true;
}

void server_state::update_partition_metrics()
{
    auto func = [&](const std::shared_ptr<app_state> &app) {
        int counters[HS_MAX_VALUE] = {0};

        int min_2pc_count =
            _meta_svc->get_options().app_mutation_2pc_min_replica_count(app->max_replica_count);
        CHECK_EQ(app->partition_count, app->pcs.size());
        for (const auto &pc : app->pcs) {
            health_status st = partition_health_status(pc, min_2pc_count);
            counters[st]++;
        }

        METRIC_SET_TABLE_HEALTH_STATS(_table_metric_entities,
                                      app->app_id,
                                      counters[HS_DEAD],
                                      counters[HS_UNREADABLE],
                                      counters[HS_UNWRITABLE],
                                      counters[HS_WRITABLE_ILL],
                                      counters[HS_HEALTHY]);

        return true;
    };

    for_each_available_app(_all_apps, func);
}

bool server_state::check_all_partitions()
{
    int healthy_partitions = 0;
    int total_partitions = 0;
    meta_function_level::type level = _meta_svc->get_function_level();

    zauto_write_lock l(_lock);

    update_partition_metrics();

    // first the cure stage
    if (level <= meta_function_level::fl_freezed) {
        LOG_INFO("service is in level({}), don't do any cure or balancer actions",
                 _meta_function_level_VALUES_TO_NAMES.find(level)->second);
        return false;
    }
    LOG_INFO("start to check all partitions, add_secondary_enable_flow_control = {}, "
             "add_secondary_max_count_for_one_node = {}",
             _add_secondary_enable_flow_control ? "true" : "false",
             _add_secondary_max_count_for_one_node);
    _meta_svc->get_partition_guardian()->clear_ddd_partitions();
    int send_proposal_count = 0;
    std::vector<configuration_proposal_action> add_secondary_actions;
    std::vector<gpid> add_secondary_gpids;
    std::vector<bool> add_secondary_proposed;
    std::map<host_port, int> add_secondary_running_nodes; // node --> running_count
    for (auto &app_pair : _exist_apps) {
        std::shared_ptr<app_state> &app = app_pair.second;
        if (app->status == app_status::AS_CREATING || app->status == app_status::AS_DROPPING) {
            LOG_INFO("ignore app({})({}) because it's status is {}",
                     app->app_name,
                     app->app_id,
                     ::dsn::enum_to_string(app->status));
            continue;
        }
        for (unsigned int i = 0; i != app->partition_count; ++i) {
            const auto &pc = app->pcs[i];
            const auto &cc = app->helpers->contexts[i];
            // partition is under re-configuration or is child partition
            if (cc.stage != config_status::pending_remote_sync && pc.ballot != invalid_ballot) {
                configuration_proposal_action action;
                pc_status s = _meta_svc->get_partition_guardian()->cure(
                    {&_all_apps, &_nodes}, pc.pid, action);
                LOG_DEBUG("gpid({}) is in status({})", pc.pid, enum_to_string(s));
                if (pc_status::healthy != s) {
                    if (action.type != config_type::CT_INVALID) {
                        if (action.type == config_type::CT_ADD_SECONDARY ||
                            action.type == config_type::CT_ADD_SECONDARY_FOR_LB) {
                            add_secondary_actions.push_back(std::move(action));
                            add_secondary_gpids.push_back(pc.pid);
                            add_secondary_proposed.push_back(false);
                        } else {
                            send_proposal(action, pc, *app);
                            send_proposal_count++;
                        }
                    }
                } else {
                    healthy_partitions++;
                }
            } else {
                LOG_INFO("ignore gpid({}) as it's stage is pending_remote_sync", pc.pid);
            }
        }
        total_partitions += app->partition_count;
    }

    // assign secondary for urgent
    for (int i = 0; i < add_secondary_actions.size(); ++i) {
        gpid &pid = add_secondary_gpids[i];
        const auto *pc = get_config(_all_apps, pid);
        if (!add_secondary_proposed[i] && pc->hp_secondaries.empty()) {
            const auto &action = add_secondary_actions[i];
            CHECK(action.hp_node, "");
            if (_add_secondary_enable_flow_control && add_secondary_running_nodes[action.hp_node] >=
                                                          _add_secondary_max_count_for_one_node) {
                // ignore
                continue;
            }
            std::shared_ptr<app_state> app = get_app(pid.get_app_id());
            send_proposal(action, *pc, *app);
            send_proposal_count++;
            add_secondary_proposed[i] = true;
            add_secondary_running_nodes[action.hp_node]++;
        }
    }

    // assign secondary for all
    for (int i = 0; i < add_secondary_actions.size(); ++i) {
        if (!add_secondary_proposed[i]) {
            const auto &action = add_secondary_actions[i];
            CHECK(action.hp_node, "");
            gpid pid = add_secondary_gpids[i];
            const auto *pc = get_config(_all_apps, pid);
            if (_add_secondary_enable_flow_control && add_secondary_running_nodes[action.hp_node] >=
                                                          _add_secondary_max_count_for_one_node) {
                LOG_INFO("do not send {} proposal for gpid({}) for flow control reason, target = "
                         "{}, node = {}",
                         ::dsn::enum_to_string(action.type),
                         pc->pid,
                         FMT_HOST_PORT_AND_IP(action, target),
                         FMT_HOST_PORT_AND_IP(action, node));
                continue;
            }
            std::shared_ptr<app_state> app = get_app(pid.get_app_id());
            send_proposal(action, *pc, *app);
            send_proposal_count++;
            add_secondary_proposed[i] = true;
            add_secondary_running_nodes[action.hp_node]++;
        }
    }

    int ignored_add_secondary_count = 0;
    int add_secondary_count = 0;
    for (int i = 0; i < add_secondary_actions.size(); ++i) {
        if (!add_secondary_proposed[i]) {
            ignored_add_secondary_count++;
        } else {
            add_secondary_count++;
        }
    }

    LOG_INFO("check all partitions done, send_proposal_count = {}, add_secondary_count = {}, "
             "ignored_add_secondary_count = {}",
             send_proposal_count,
             add_secondary_count,
             ignored_add_secondary_count);

    // then the balancer stage
    if (level < meta_function_level::fl_steady) {
        LOG_INFO("don't do replica migration coz meta server is in level({})",
                 _meta_function_level_VALUES_TO_NAMES.find(level)->second);
        return false;
    }

    if (healthy_partitions != total_partitions) {
        LOG_INFO("don't do replica migration coz {}/{} partitions aren't healthy",
                 total_partitions - healthy_partitions,
                 total_partitions);
        return false;
    }

    if (!can_run_balancer()) {
        LOG_INFO("don't do replica migration coz can_run_balancer() returns false");
        return false;
    }

    if (level == meta_function_level::fl_steady) {
        LOG_INFO("check if any replica migration can be done when meta server is in level({})",
                 _meta_function_level_VALUES_TO_NAMES.find(level)->second);
        _meta_svc->get_balancer()->check({&_all_apps, &_nodes}, _temporary_list);
        LOG_INFO("balance checker operation count = {}", _temporary_list.size());
        // update balance checker operation count
        _meta_svc->get_balancer()->report(_temporary_list, true);
        return false;
    }

    if (_meta_svc->get_balancer()->balance({&_all_apps, &_nodes}, _temporary_list)) {
        LOG_INFO("try to do replica migration");
        _meta_svc->get_balancer()->apply_balancer({&_all_apps, &_nodes}, _temporary_list);
        // update balancer action details
        _meta_svc->get_balancer()->report(_temporary_list, false);
        if (_replica_migration_subscriber)
            _replica_migration_subscriber(_temporary_list);
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         _meta_svc->tracker(),
                         std::bind(&meta_service::balancer_run, _meta_svc));
        return false;
    }

    LOG_INFO("check if any replica migration left");
    _meta_svc->get_balancer()->check({&_all_apps, &_nodes}, _temporary_list);
    LOG_INFO("balance checker operation count = {}", _temporary_list.size());
    // update balance checker operation count
    _meta_svc->get_balancer()->report(_temporary_list, true);

    return true;
}

void server_state::get_cluster_balance_score(double &primary_stddev, double &total_stddev)
{
    zauto_read_lock l(_lock);
    _meta_svc->get_balancer()->score({&_all_apps, &_nodes}, primary_stddev, total_stddev);
}

void server_state::check_consistency(const dsn::gpid &gpid)
{
    auto iter = _all_apps.find(gpid.get_app_id());
    CHECK(iter != _all_apps.end(), "invalid gpid({})", gpid);

    auto &app = *(iter->second);
    auto &pc = app.pcs[gpid.get_partition_index()];

    if (app.is_stateful) {
        if (pc.hp_primary) {
            const auto it = _nodes.find(pc.hp_primary);
            CHECK(it != _nodes.end(), "invalid primary: {}", pc.hp_primary);
            CHECK_EQ(it->second.served_as(gpid), partition_status::PS_PRIMARY);
            CHECK(!utils::contains(pc.hp_last_drops, pc.hp_primary),
                  "primary({}) shouldn't appear in last_drops",
                  pc.hp_primary);
        }

        for (const auto &secondary : pc.hp_secondaries) {
            const auto it = _nodes.find(secondary);
            CHECK(it != _nodes.end(), "invalid secondary: {}", secondary);
            CHECK_EQ(it->second.served_as(gpid), partition_status::PS_SECONDARY);
            CHECK(!utils::contains(pc.hp_last_drops, secondary),
                  "secondary({}) shouldn't appear in last_drops",
                  secondary);
        }
    } else {
        partition_configuration_stateless pcs(pc);
        CHECK_EQ(pcs.hosts().size(), pcs.workers().size());
        for (const auto &secondary : pcs.hosts()) {
            auto it = _nodes.find(secondary);
            CHECK(it != _nodes.end(), "invalid secondary: {}", secondary);
            CHECK_EQ(it->second.served_as(gpid), partition_status::PS_SECONDARY);
        }
    }
}

void server_state::lock_read(zauto_read_lock &other)
{
    zauto_read_lock l(_lock);
    l.swap(other);
}

void server_state::lock_write(zauto_write_lock &other)
{
    zauto_write_lock l(_lock);
    l.swap(other);
}

void server_state::do_update_app_info(const std::string &app_path,
                                      const app_info &info,
                                      const std::function<void(error_code ec)> &cb)
{
    // persistent envs to zookeeper
    blob value = dsn::json::json_forwarder<app_info>::encode(info);
    auto new_cb = [this, app_path, info, user_cb = std::move(cb)](error_code ec) {
        if (ec == ERR_OK) {
            user_cb(ec);
        } else if (ec == ERR_TIMEOUT) {
            LOG_WARNING(
                "update app_info(app = {}) to remote storage timeout, continue to update later",
                info.app_name);
            tasking::enqueue(
                LPC_META_STATE_NORMAL,
                tracker(),
                std::bind(
                    &server_state::do_update_app_info, this, app_path, info, std::move(user_cb)),
                0,
                std::chrono::seconds(1));
        } else {
            CHECK(false, "we can't handle this, error({})", ec);
        }
    };
    // TODO(cailiuyang): callback scheduling order may be undefined if multiple requests are
    // sending to the remote storage concurrently.
    _meta_svc->get_remote_storage()->set_data(
        app_path, value, LPC_META_STATE_NORMAL, std::move(new_cb), tracker());
}

void server_state::set_app_envs(const app_env_rpc &env_rpc)
{
    const auto &request = env_rpc.request();
    if (!request.__isset.keys || !request.__isset.values ||
        request.keys.size() != request.values.size() || request.keys.empty()) {
        env_rpc.response().err = ERR_INVALID_PARAMETERS;
        LOG_WARNING("set app envs failed with invalid request");
        return;
    }

    const auto &keys = request.keys;
    const auto &values = request.values;
    const auto &app_name = request.app_name;

    std::ostringstream os;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (i != 0) {
            os << ", ";
        }

        if (!_app_env_validator.validate_app_env(
                keys[i], values[i], env_rpc.response().hint_message)) {
            LOG_WARNING("app env '{}={}' is invalid, hint_message: {}",
                        keys[i],
                        values[i],
                        env_rpc.response().hint_message);
            env_rpc.response().err = ERR_INVALID_PARAMETERS;
            return;
        }

        os << keys[i] << "=" << values[i];
    }

    LOG_INFO("set app envs for app({}) from remote({}): kvs = {}",
             app_name,
             env_rpc.remote_address(),
             os.str());

    app_info ainfo;
    std::string app_path;
    {
        FAIL_POINT_INJECT_NOT_RETURN_F("set_app_envs_failed", [app_name, this](std::string_view s) {
            zauto_write_lock l(_lock);

            if (s == "not_found") {
                CHECK_EQ(_exist_apps.erase(app_name), 1);
                return;
            }

            if (s == "dropping") {
                gutil::FindOrDie(_exist_apps, app_name)->status = app_status::AS_DROPPING;
                return;
            }
        });

        zauto_read_lock l(_lock);

        const auto &app = get_app(app_name);
        if (!app) {
            LOG_WARNING("set app envs failed since app_name({}) cannot be found", app_name);
            env_rpc.response().err = ERR_APP_NOT_EXIST;
            env_rpc.response().hint_message = "app cannot be found";
            return;
        }

        if (app->status == app_status::AS_DROPPING) {
            LOG_WARNING("set app envs failed since app(name={}, id={}) is being dropped",
                        app_name,
                        app->app_id);
            env_rpc.response().err = ERR_BUSY_DROPPING;
            env_rpc.response().hint_message = "app is being dropped";
            return;
        }

        ainfo = *app;
        app_path = get_app_path(*app);
    }

    for (size_t idx = 0; idx < keys.size(); ++idx) {
        ainfo.envs[keys[idx]] = values[idx];
    }

    do_update_app_info(app_path, ainfo, [this, app_name, keys, values, env_rpc](error_code ec) {
        CHECK_EQ_MSG(ec, ERR_OK, "update app({}) info to remote storage failed", app_name);

        zauto_write_lock l(_lock);

        FAIL_POINT_INJECT_NOT_RETURN_F("set_app_envs_failed", [app_name, this](std::string_view s) {
            if (s == "dropped_after") {
                CHECK_EQ(_exist_apps.erase(app_name), 1);
                return;
            }
        });

        auto app = get_app(app_name);

        // The table might be removed just before the callback function is invoked, thus we must
        // check if this table still exists.
        //
        // TODO(wangdan): should make updates to remote storage sequential by supporting atomic
        // set, otherwise update might be missing. For example, an update is setting the envs
        // while another is dropping a table. The update setting the envs does not contain the
        // dropped state. Once it is applied by remote storage after another update dropping
        // the table, the state of the table would always be non-dropped on remote storage.
        if (!app) {
            LOG_ERROR("set app envs failed since app({}) has just been dropped", app_name);
            env_rpc.response().err = ERR_APP_DROPPED;
            env_rpc.response().hint_message = "app has just been dropped";
            return;
        }

        env_rpc.response().err = ERR_OK;

        const auto &old_envs = dsn::utils::kv_map_to_string(app->envs, ',', '=');

        // Update envs of local memory.
        for (size_t idx = 0; idx < keys.size(); ++idx) {
            app->envs[keys[idx]] = values[idx];
        }

        const auto &new_envs = dsn::utils::kv_map_to_string(app->envs, ',', '=');
        LOG_INFO("app envs changed: old_envs = {}, new_envs = {}", old_envs, new_envs);
    });
}

void server_state::del_app_envs(const app_env_rpc &env_rpc)
{
    const auto &request = env_rpc.request();
    if (!request.__isset.keys || request.keys.empty()) {
        env_rpc.response().err = ERR_INVALID_PARAMETERS;
        LOG_WARNING("del app envs failed with invalid request");
        return;
    }

    const auto &keys = request.keys;
    const auto &app_name = request.app_name;

    LOG_INFO("del app envs for app({}) from remote({}): keys = {}",
             app_name,
             env_rpc.remote_address(),
             boost::join(keys, ","));

    app_info ainfo;
    std::string app_path;
    {
        FAIL_POINT_INJECT_NOT_RETURN_F("del_app_envs_failed", [app_name, this](std::string_view s) {
            zauto_write_lock l(_lock);

            if (s == "not_found") {
                CHECK_EQ(_exist_apps.erase(app_name), 1);
                return;
            }

            if (s == "dropping") {
                gutil::FindOrDie(_exist_apps, app_name)->status = app_status::AS_DROPPING;
                return;
            }
        });

        zauto_read_lock l(_lock);

        const auto &app = get_app(app_name);
        if (!app) {
            LOG_WARNING("del app envs failed since app_name({}) cannot be found", app_name);
            env_rpc.response().err = ERR_APP_NOT_EXIST;
            env_rpc.response().hint_message = "app cannot be found";
            return;
        }

        if (app->status == app_status::AS_DROPPING) {
            LOG_WARNING("del app envs failed since app(name={}, id={}) is being dropped",
                        app_name,
                        app->app_id);
            env_rpc.response().err = ERR_BUSY_DROPPING;
            env_rpc.response().hint_message = "app is being dropped";
            return;
        }

        ainfo = *app;
        app_path = get_app_path(*app);
    }

    std::string deleted_keys_info("deleted keys:");
    size_t deleted_count = 0;
    for (const auto &key : keys) {
        if (ainfo.envs.erase(key) == 0) {
            continue;
        }

        fmt::format_to(std::back_inserter(deleted_keys_info), "\n    {}", key);
        ++deleted_count;
    }

    if (deleted_count == 0) {
        LOG_INFO("no key needs to be deleted for app({})", app_name);
        env_rpc.response().err = ERR_OK;
        env_rpc.response().hint_message = "no key needs to be deleted";
        return;
    }

    env_rpc.response().hint_message = std::move(deleted_keys_info);

    do_update_app_info(app_path, ainfo, [this, app_name, keys, env_rpc](error_code ec) {
        CHECK_EQ_MSG(ec, ERR_OK, "update app({}) info to remote storage failed", app_name);

        zauto_write_lock l(_lock);

        FAIL_POINT_INJECT_NOT_RETURN_F("del_app_envs_failed", [app_name, this](std::string_view s) {
            if (s == "dropped_after") {
                CHECK_EQ(_exist_apps.erase(app_name), 1);
                return;
            }
        });

        auto app = get_app(app_name);

        // The table might be removed just before the callback function is invoked, thus we must
        // check if this table still exists.
        //
        // TODO(wangdan): should make updates to remote storage sequential by supporting atomic
        // set, otherwise update might be missing. For example, an update is setting the envs
        // while another is dropping a table. The update setting the envs does not contain the
        // dropped state. Once it is applied by remote storage after another update dropping
        // the table, the state of the table would always be non-dropped on remote storage.
        if (!app) {
            LOG_ERROR("del app envs failed since app({}) has just been dropped", app_name);
            env_rpc.response().err = ERR_APP_DROPPED;
            env_rpc.response().hint_message = "app has just been dropped";
            return;
        }

        env_rpc.response().err = ERR_OK;

        const auto &old_envs = dsn::utils::kv_map_to_string(app->envs, ',', '=');

        for (const auto &key : keys) {
            app->envs.erase(key);
        }

        const auto &new_envs = dsn::utils::kv_map_to_string(app->envs, ',', '=');
        LOG_INFO("app envs changed: old_envs = {}, new_envs = {}", old_envs, new_envs);
    });
}

void server_state::clear_app_envs(const app_env_rpc &env_rpc)
{
    const auto &request = env_rpc.request();
    if (!request.__isset.clear_prefix) {
        env_rpc.response().err = ERR_INVALID_PARAMETERS;
        LOG_WARNING("clear app envs failed with invalid request");
        return;
    }

    const auto &prefix = request.clear_prefix;
    const auto &app_name = request.app_name;
    LOG_INFO("clear app envs for app({}) from remote({}): prefix = {}",
             app_name,
             env_rpc.remote_address(),
             prefix);

    app_info ainfo;
    std::string app_path;
    {
        FAIL_POINT_INJECT_NOT_RETURN_F(
            "clear_app_envs_failed", [app_name, this](std::string_view s) {
                zauto_write_lock l(_lock);

                if (s == "not_found") {
                    CHECK_EQ(_exist_apps.erase(app_name), 1);
                    return;
                }

                if (s == "dropping") {
                    gutil::FindOrDie(_exist_apps, app_name)->status = app_status::AS_DROPPING;
                    return;
                }
            });

        zauto_read_lock l(_lock);

        const auto &app = get_app(app_name);
        if (!app) {
            LOG_WARNING("clear app envs failed since app_name({}) cannot be found", app_name);
            env_rpc.response().err = ERR_APP_NOT_EXIST;
            env_rpc.response().hint_message = "app cannot be found";
            return;
        }

        if (app->status == app_status::AS_DROPPING) {
            LOG_WARNING("clear app envs failed since app(name={}, id={}) is being dropped",
                        app_name,
                        app->app_id);
            env_rpc.response().err = ERR_BUSY_DROPPING;
            env_rpc.response().hint_message = "app is being dropped";
            return;
        }

        ainfo = *app;
        app_path = get_app_path(*app);
    }

    if (ainfo.envs.empty()) {
        LOG_INFO("no key needs to be deleted for app({})", app_name);
        env_rpc.response().err = ERR_OK;
        env_rpc.response().hint_message = "no key needs to be deleted";
        return;
    }

    std::set<std::string> deleted_keys;
    std::string deleted_keys_info("deleted keys:");

    if (prefix.empty()) {
        // Empty prefix means deleting all environments.
        for (const auto &[key, _] : ainfo.envs) {
            fmt::format_to(std::back_inserter(deleted_keys_info), "\n    {}", key);
        }
        ainfo.envs.clear();
    } else {
        // The full prefix is the prefix plus the separator dot(.).
        const size_t full_prefix_len = prefix.size() + sizeof('.');
        for (const auto &[key, _] : ainfo.envs) {
            // The key is not the target if it is shorter than, or just has the same length
            // as the full prefix.
            if (key.size() <= full_prefix_len) {
                continue;
            }

            // The key is not the target if the prefix is not matched.
            if (!boost::algorithm::starts_with(key, prefix)) {
                continue;
            }

            // The key is not the target if the separator is not dot(.).
            if (key[prefix.size()] != '.') {
                continue;
            }

            deleted_keys.emplace(key);
        }

        for (const auto &key : deleted_keys) {
            fmt::format_to(std::back_inserter(deleted_keys_info), "\n    {}", key);
            ainfo.envs.erase(key);
        }

        if (deleted_keys.empty()) {
            LOG_INFO("no key needs to be deleted for app({})", app_name);
            env_rpc.response().err = ERR_OK;
            env_rpc.response().hint_message = "no key needs to be deleted";
            return;
        }
    }

    env_rpc.response().hint_message = std::move(deleted_keys_info);

    do_update_app_info(app_path, ainfo, [this, app_name, deleted_keys, env_rpc](error_code ec) {
        CHECK_EQ_MSG(ec, ERR_OK, "update app({}) info to remote storage failed", app_name);

        zauto_write_lock l(_lock);

        FAIL_POINT_INJECT_NOT_RETURN_F("clear_app_envs_failed",
                                       [app_name, this](std::string_view s) {
                                           if (s == "dropped_after") {
                                               CHECK_EQ(_exist_apps.erase(app_name), 1);
                                               return;
                                           }
                                       });

        auto app = get_app(app_name);

        // The table might be removed just before the callback function is invoked, thus we must
        // check if this table still exists.
        //
        // TODO(wangdan): should make updates to remote storage sequential by supporting atomic
        // set, otherwise update might be missing. For example, an update is setting the envs
        // while another is dropping a table. The update setting the envs does not contain the
        // dropped state. Once it is applied by remote storage after another update dropping
        // the table, the state of the table would always be non-dropped on remote storage.
        if (!app) {
            LOG_ERROR("clear app envs failed since app({}) has just been dropped", app_name);
            env_rpc.response().err = ERR_APP_DROPPED;
            env_rpc.response().hint_message = "app has just been dropped";
            return;
        }

        env_rpc.response().err = ERR_OK;

        const auto &old_envs = dsn::utils::kv_map_to_string(app->envs, ',', '=');

        if (deleted_keys.empty()) {
            // `deleted_keys` would be empty only when `prefix` is empty. Therefore, empty
            // `deleted_keys` means deleting all environments.
            app->envs.clear();
        } else {
            for (const auto &key : deleted_keys) {
                app->envs.erase(key);
            }
        }

        const auto &new_envs = dsn::utils::kv_map_to_string(app->envs, ',', '=');
        LOG_INFO("app envs changed: old_envs = {}, new_envs = {}", old_envs, new_envs);
    });
}

namespace {

bool validate_target_max_replica_count_internal(int32_t max_replica_count,
                                                int32_t alive_node_count,
                                                std::string &hint_message)
{
    if (max_replica_count > FLAGS_max_allowed_replica_count ||
        max_replica_count < FLAGS_min_allowed_replica_count) {
        hint_message = fmt::format("requested replica count({}) must be "
                                   "within the range of [min={}, max={}]",
                                   max_replica_count,
                                   FLAGS_min_allowed_replica_count,
                                   FLAGS_max_allowed_replica_count);
        return false;
    }

    if (max_replica_count > alive_node_count) {
        hint_message = fmt::format("there are not enough alive replica servers({}) "
                                   "for the requested replica count({})",
                                   alive_node_count,
                                   max_replica_count);
        return false;
    }

    return true;
}

} // anonymous namespace

bool server_state::validate_target_max_replica_count(int32_t max_replica_count,
                                                     std::string &hint_message) const
{
    const auto alive_node_count = static_cast<int32_t>(_meta_svc->get_alive_node_count());

    return validate_target_max_replica_count_internal(
        max_replica_count, alive_node_count, hint_message);
}

bool server_state::validate_target_max_replica_count(int32_t max_replica_count) const
{
    std::string hint_message;
    const auto valid = validate_target_max_replica_count(max_replica_count, hint_message);
    if (!valid) {
        LOG_ERROR("target max replica count is invalid: message={}", hint_message);
    }

    return valid;
}

void server_state::on_start_manual_compact(start_manual_compact_rpc rpc)
{
    const std::string &app_name = rpc.request().app_name;
    auto &response = rpc.response();

    std::map<std::string, std::string> envs;
    {
        zauto_read_lock l(_lock);
        auto app = get_app(app_name);
        if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
            response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
            response.hint_msg =
                fmt::format("app {} is {}",
                            app_name,
                            response.err == ERR_APP_NOT_EXIST ? "not existed" : "not available");
            LOG_ERROR("{}", response.hint_msg);
            return;
        }
        envs = app->envs;
    }

    auto iter = envs.find(replica_envs::MANUAL_COMPACT_DISABLED);
    if (iter != envs.end() && iter->second == "true") {
        response.err = ERR_OPERATION_DISABLED;
        response.hint_msg = fmt::format("app {} disable manual compaction", app_name);
        LOG_ERROR("{}", response.hint_msg);
        return;
    }

    std::vector<std::string> keys;
    std::vector<std::string> values;
    if (!parse_compaction_envs(rpc, keys, values)) {
        return;
    }

    update_compaction_envs_on_remote_storage(rpc, keys, values);

    // update local manual compaction status
    {
        zauto_write_lock l(_lock);
        auto app = get_app(app_name);
        app->helpers->reset_manual_compact_status();
    }
}

bool server_state::parse_compaction_envs(start_manual_compact_rpc rpc,
                                         std::vector<std::string> &keys,
                                         std::vector<std::string> &values)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();

    int32_t target_level = -1;
    if (request.__isset.target_level) {
        target_level = request.target_level;
        if (target_level < -1) {
            response.err = ERR_INVALID_PARAMETERS;
            response.hint_msg = fmt::format(
                "invalid target_level({}), should in range of [-1, num_levels]", target_level);
            LOG_ERROR("{}", response.hint_msg);
            return false;
        }
    }
    keys.emplace_back(replica_envs::MANUAL_COMPACT_ONCE_TARGET_LEVEL);
    values.emplace_back(std::to_string(target_level));

    if (request.__isset.max_running_count) {
        if (request.max_running_count < 0) {
            response.err = ERR_INVALID_PARAMETERS;
            response.hint_msg =
                fmt::format("invalid max_running_count({}), should be greater than 0",
                            request.max_running_count);
            LOG_ERROR("{}", response.hint_msg);
            return false;
        }
        if (request.max_running_count > 0) {
            keys.emplace_back(replica_envs::MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT);
            values.emplace_back(std::to_string(request.max_running_count));
        }
    }

    std::string bottommost = replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP;
    if (request.__isset.bottommost && request.bottommost) {
        bottommost = replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_FORCE;
    }
    keys.emplace_back(replica_envs::MANUAL_COMPACT_ONCE_BOTTOMMOST_LEVEL_COMPACTION);
    values.emplace_back(bottommost);

    int64_t trigger_time = dsn_now_s();
    if (request.__isset.trigger_time) {
        trigger_time = request.trigger_time;
    }
    keys.emplace_back(replica_envs::MANUAL_COMPACT_ONCE_TRIGGER_TIME);
    values.emplace_back(std::to_string(trigger_time));

    return true;
}

void server_state::update_compaction_envs_on_remote_storage(start_manual_compact_rpc rpc,
                                                            const std::vector<std::string> &keys,
                                                            const std::vector<std::string> &values)
{
    const std::string &app_name = rpc.request().app_name;
    std::string app_path = "";
    app_info ainfo;
    {
        zauto_read_lock l(_lock);
        auto app = get_app(app_name);
        ainfo = *(reinterpret_cast<app_info *>(app.get()));
        app_path = get_app_path(*app);
    }
    for (auto idx = 0; idx < keys.size(); idx++) {
        ainfo.envs[keys[idx]] = values[idx];
    }
    do_update_app_info(app_path, ainfo, [this, app_name, keys, values, rpc](error_code ec) {
        CHECK_EQ_MSG(ec, ERR_OK, "update app_info to remote storage failed");

        zauto_write_lock l(_lock);
        auto app = get_app(app_name);
        std::string old_envs = dsn::utils::kv_map_to_string(app->envs, ',', '=');
        for (int idx = 0; idx < keys.size(); idx++) {
            app->envs[keys[idx]] = values[idx];
        }
        std::string new_envs = dsn::utils::kv_map_to_string(app->envs, ',', '=');
        LOG_INFO("update manual compaction envs succeed: old_envs = {}, new_envs = {}",
                 old_envs,
                 new_envs);

        rpc.response().err = ERR_OK;
        rpc.response().hint_msg = "succeed";
    });
}

void server_state::on_query_manual_compact_status(query_manual_compact_rpc rpc)
{
    const std::string &app_name = rpc.request().app_name;
    auto &response = rpc.response();

    std::shared_ptr<app_state> app;
    {
        zauto_read_lock l(_lock);
        app = get_app(app_name);
    }

    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        response.err = app == nullptr ? ERR_APP_NOT_EXIST : ERR_APP_DROPPED;
        response.hint_msg =
            fmt::format("app {} is {}",
                        app_name,
                        response.err == ERR_APP_NOT_EXIST ? "not existed" : "not available");
        LOG_ERROR("{}", response.hint_msg);
        return;
    }

    int32_t total_progress = 0;
    if (!app->helpers->get_manual_compact_progress(total_progress)) {
        response.err = ERR_INVALID_STATE;
        response.hint_msg = fmt::format("app {} is not manual compaction", app_name);
        LOG_WARNING("{}", response.hint_msg);
        return;
    }

    LOG_INFO("query app {} manual compact succeed, total_progress = {}", app_name, total_progress);
    response.err = ERR_OK;
    response.hint_msg = "succeed";
    response.__set_progress(total_progress);
}

template <typename Response>
std::shared_ptr<app_state> server_state::get_app_and_check_exist(const std::string &app_name,
                                                                 Response &response) const
{
    auto app = get_app(app_name);
    if (!app) {
        response.err = ERR_APP_NOT_EXIST;
        response.hint_message = fmt::format("app({}) does not exist", app_name);
    }

    return app;
}

template <typename Response>
bool server_state::check_max_replica_count_consistent(const std::shared_ptr<app_state> &app,
                                                      Response &response) const
{
    for (const auto &pc : app->pcs) {
        if (pc.max_replica_count == app->max_replica_count) {
            continue;
        }

        response.err = ERR_INCONSISTENT_STATE;
        response.hint_message = fmt::format("partition_max_replica_count({}) != "
                                            "app_max_replica_count({}) for partition {}",
                                            pc.max_replica_count,
                                            app->max_replica_count,
                                            pc.pid);
        return false;
    }

    return true;
}

// ThreadPool: THREAD_POOL_META_STATE
void server_state::get_max_replica_count(configuration_get_max_replica_count_rpc rpc) const
{
    const auto &app_name = rpc.request().app_name;
    auto &response = rpc.response();

    zauto_read_lock l(_lock);

    auto app = get_app_and_check_exist(app_name, response);
    if (app == nullptr) {
        response.max_replica_count = 0;
        LOG_WARNING("failed to get max_replica_count: app_name={}, error_code={}, hint_message={}",
                    app_name,
                    response.err,
                    response.hint_message);
        return;
    }

    if (!check_max_replica_count_consistent(app, response)) {
        response.max_replica_count = 0;
        LOG_ERROR("failed to get max_replica_count: app_name={}, app_id={}, error_code={}, "
                  "hint_message={}",
                  app_name,
                  app->app_id,
                  response.err,
                  response.hint_message);
        return;
    }

    response.err = ERR_OK;
    response.max_replica_count = app->max_replica_count;

    LOG_INFO("get max_replica_count successfully: app_name={}, app_id={}, "
             "max_replica_count={}",
             app_name,
             app->app_id,
             response.max_replica_count);
}

// ThreadPool: THREAD_POOL_META_STATE
void server_state::set_max_replica_count(configuration_set_max_replica_count_rpc rpc)
{
    const auto &app_name = rpc.request().app_name;
    const auto new_max_replica_count = rpc.request().max_replica_count;
    auto &response = rpc.response();

    int32_t app_id = 0;
    std::shared_ptr<app_state> app;

    {
        zauto_read_lock l(_lock);

        app = get_app_and_check_exist(app_name, response);
        if (app == nullptr) {
            response.old_max_replica_count = 0;
            LOG_WARNING(
                "failed to set max_replica_count: app_name={}, error_code={}, hint_message={}",
                app_name,
                response.err,
                response.hint_message);
            return;
        }

        app_id = app->app_id;

        if (!check_max_replica_count_consistent(app, response)) {
            response.old_max_replica_count = 0;
            LOG_ERROR("failed to set max_replica_count: app_name={}, app_id={}, error_code={}, "
                      "hint_message={}",
                      app_name,
                      app_id,
                      response.err,
                      response.hint_message);
            return;
        }

        response.old_max_replica_count = app->max_replica_count;

        if (app->status != app_status::AS_AVAILABLE) {
            response.err = ERR_INVALID_STATE;
            response.hint_message = fmt::format("app({}) is not in available status", app_name);
            LOG_ERROR("failed to set max_replica_count: app_name={}, app_id={}, error_code={}, "
                      "hint_message={}",
                      app_name,
                      app_id,
                      response.err,
                      response.hint_message);
            return;
        }
    }

    auto level = _meta_svc->get_function_level();
    if (level <= meta_function_level::fl_freezed) {
        response.err = ERR_STATE_FREEZED;
        response.hint_message =
            "current meta function level is freezed, since there are too few alive nodes";
        LOG_ERROR(
            "failed to set max_replica_count: app_name={}, app_id={}, error_code={}, message={}",
            app_name,
            app_id,
            response.err,
            response.hint_message);
        return;
    }

    if (!validate_target_max_replica_count(new_max_replica_count, response.hint_message)) {
        response.err = ERR_INVALID_PARAMETERS;
        LOG_WARNING(
            "failed to set max_replica_count: app_name={}, app_id={}, error_code={}, message={}",
            app_name,
            app_id,
            response.err,
            response.hint_message);
        return;
    }

    if (new_max_replica_count == response.old_max_replica_count) {
        response.err = ERR_OK;
        response.hint_message = "no need to update max_replica_count since it's not changed";
        LOG_WARNING("{}: app_name={}, app_id={}", response.hint_message, app_name, app_id);
        return;
    }

    LOG_INFO("request for {} max_replica_count: app_name={}, app_id={}, "
             "old_max_replica_count={}, new_max_replica_count={}",
             new_max_replica_count > response.old_max_replica_count ? "increasing" : "decreasing",
             app_name,
             app_id,
             response.old_max_replica_count,
             new_max_replica_count);

    set_max_replica_count_env_updating(app, rpc);
}

// ThreadPool: THREAD_POOL_META_STATE
void server_state::set_max_replica_count_env_updating(std::shared_ptr<app_state> &app,
                                                      configuration_set_max_replica_count_rpc rpc)
{
    zauto_write_lock l(_lock);

    auto iter = app->envs.find(replica_envs::UPDATE_MAX_REPLICA_COUNT);
    if (iter != app->envs.end()) {
        std::vector<std::string> args;
        utils::split_args(iter->second.c_str(), args, ';');
        if (args[0] == "updating") {
            auto &response = rpc.response();
            response.err = ERR_OPERATION_DISABLED;
            response.hint_message = fmt::format("max_replica_count of app({}) is being updated, "
                                                "thus this request would be rejected",
                                                app->app_name);
            LOG_ERROR("failed to set max_replica_count: app_name={}, app_id={}, error_code={}, "
                      "hint_message={}",
                      app->app_name,
                      app->app_id,
                      response.err,
                      response.hint_message);
            return;
        }
    }

    const auto new_max_replica_count = rpc.request().max_replica_count;
    const auto old_max_replica_count = rpc.response().old_max_replica_count;

    LOG_INFO("ready to update remote env of max_replica_count: app_name={}, app_id={}, "
             "old_max_replica_count={}, new_max_replica_count={}, {}={}",
             app->app_name,
             app->app_id,
             old_max_replica_count,
             new_max_replica_count,
             replica_envs::UPDATE_MAX_REPLICA_COUNT,
             app->envs[replica_envs::UPDATE_MAX_REPLICA_COUNT]);

    auto ainfo = *(reinterpret_cast<app_info *>(app.get()));
    ainfo.envs[replica_envs::UPDATE_MAX_REPLICA_COUNT] =
        fmt::format("updating;{}", new_max_replica_count);
    auto app_path = get_app_path(*app);
    do_update_app_info(app_path, ainfo, [this, app, rpc](error_code ec) mutable {
        {
            const auto new_max_replica_count = rpc.request().max_replica_count;

            zauto_write_lock l(_lock);

            CHECK_EQ_MSG(ec,
                         ERR_OK,
                         "An error that can't be handled occurs while updating remote env of "
                         "max_replica_count: error_code={}, app_name={}, app_id={}, "
                         "new_max_replica_count={}, {}={}",
                         ec,
                         app->app_name,
                         app->app_id,
                         new_max_replica_count,
                         replica_envs::UPDATE_MAX_REPLICA_COUNT,
                         app->envs[replica_envs::UPDATE_MAX_REPLICA_COUNT]);

            app->envs[replica_envs::UPDATE_MAX_REPLICA_COUNT] =
                fmt::format("updating;{}", new_max_replica_count);
            LOG_INFO("both remote and local env of max_replica_count have been updated "
                     "successfully: app_name={}, app_id={}, new_max_replica_count={}, {}={}",
                     app->app_name,
                     app->app_id,
                     new_max_replica_count,
                     replica_envs::UPDATE_MAX_REPLICA_COUNT,
                     app->envs[replica_envs::UPDATE_MAX_REPLICA_COUNT]);
        }

        do_update_max_replica_count(app, rpc);
    });
}

// ThreadPool: THREAD_POOL_META_STATE
void server_state::do_update_max_replica_count(std::shared_ptr<app_state> &app,
                                               configuration_set_max_replica_count_rpc rpc)
{
    std::shared_ptr<std::vector<error_code>> results;
    {
        zauto_write_lock l(_lock);

        results.reset(new std::vector<error_code>(app->partition_count));
        app->helpers->partitions_in_progress.store(app->partition_count);
    }

    auto on_partition_updated = [this, app, rpc, results](error_code ec,
                                                          int32_t partition_index) mutable {
        const auto &app_name = rpc.request().app_name;
        const auto new_max_replica_count = rpc.request().max_replica_count;

        results->at(partition_index) = ec;

        auto uncompleted = --app->helpers->partitions_in_progress;
        CHECK_GE_MSG(uncompleted,
                     0,
                     "the uncompleted number should be >= 0 while updating partition-level"
                     "max_replica_count: uncompleted={}, app_name={}, app_id={}, "
                     "partition_index={}, partition_count={}, new_max_replica_count={}",
                     uncompleted,
                     app_name,
                     app->app_id,
                     partition_index,
                     app->partition_count,
                     new_max_replica_count);

        if (uncompleted > 0) {
            return;
        }

        for (int32_t i = 0; i < app->partition_count; ++i) {
            if (results->at(i) == ERR_OK) {
                continue;
            }

            CHECK(false,
                  "An error that can't be handled occurs while updating partition-level"
                  "max_replica_count: error_code={}, app_name={}, app_id={}, "
                  "partition_index={}, partition_count={}, new_max_replica_count={}",
                  ec,
                  app_name,
                  app->app_id,
                  i,
                  app->partition_count,
                  new_max_replica_count);
        }

        LOG_INFO("all partitions have been changed to the new max_replica_count, ready to update "
                 "the app-level max_replica_count: app_name={}, app_id={}, partition_count={}, "
                 "new_max_replica_count={}",
                 app_name,
                 app->app_id,
                 app->partition_count,
                 new_max_replica_count);

        update_app_max_replica_count(app, rpc);
    };

    {
        const auto new_max_replica_count = rpc.request().max_replica_count;

        zauto_write_lock l(_lock);
        for (int32_t i = 0; i < app->partition_count; ++i) {
            update_partition_max_replica_count(app, i, new_max_replica_count, on_partition_updated);
        }
    }
}

// ThreadPool: THREAD_POOL_META_STATE
void server_state::update_app_max_replica_count(std::shared_ptr<app_state> &app,
                                                configuration_set_max_replica_count_rpc rpc)
{
    const auto new_max_replica_count = rpc.request().max_replica_count;
    const auto old_max_replica_count = rpc.response().old_max_replica_count;

    LOG_INFO("ready to update remote app-level max_replica_count: app_name={}, app_id={}, "
             "old_max_replica_count={}, new_max_replica_count={}, {}={}",
             app->app_name,
             app->app_id,
             old_max_replica_count,
             new_max_replica_count,
             replica_envs::UPDATE_MAX_REPLICA_COUNT,
             app->envs[replica_envs::UPDATE_MAX_REPLICA_COUNT]);

    auto ainfo = *(reinterpret_cast<app_info *>(app.get()));
    ainfo.max_replica_count = new_max_replica_count;
    ainfo.envs.erase(replica_envs::UPDATE_MAX_REPLICA_COUNT);
    auto app_path = get_app_path(*app);
    do_update_app_info(app_path, ainfo, [this, app, rpc](error_code ec) mutable {
        const auto new_max_replica_count = rpc.request().max_replica_count;
        const auto old_max_replica_count = rpc.response().old_max_replica_count;

        zauto_write_lock l(_lock);

        CHECK_EQ_MSG(ec,
                     ERR_OK,
                     "An error that can't be handled occurs while updating remote app-level "
                     "max_replica_count: error_code={}, app_name={}, app_id={}, "
                     "old_max_replica_count={}, new_max_replica_count={}, {}={}",
                     ec,
                     app->app_name,
                     app->app_id,
                     old_max_replica_count,
                     new_max_replica_count,
                     replica_envs::UPDATE_MAX_REPLICA_COUNT,
                     app->envs[replica_envs::UPDATE_MAX_REPLICA_COUNT]);

        CHECK_EQ_MSG(old_max_replica_count,
                     app->max_replica_count,
                     "app-level max_replica_count has been updated to remote storage, however "
                     "old_max_replica_count from response is not consistent with current local "
                     "max_replica_count: app_name={}, app_id={}, old_max_replica_count={}, "
                     "local_max_replica_count={}, new_max_replica_count={}",
                     app->app_name,
                     app->app_id,
                     old_max_replica_count,
                     app->max_replica_count,
                     new_max_replica_count);

        app->max_replica_count = new_max_replica_count;
        app->envs.erase(replica_envs::UPDATE_MAX_REPLICA_COUNT);
        LOG_INFO("both remote and local app-level max_replica_count have been updated "
                 "successfully: app_name={}, app_id={}, old_max_replica_count={}, "
                 "new_max_replica_count={}",
                 app->app_name,
                 app->app_id,
                 old_max_replica_count,
                 new_max_replica_count);

        auto &response = rpc.response();
        response.err = ERR_OK;
    });
}

// ThreadPool: THREAD_POOL_META_STATE
void server_state::update_partition_max_replica_count(std::shared_ptr<app_state> &app,
                                                      int32_t partition_index,
                                                      int32_t new_max_replica_count,
                                                      partition_callback on_partition_updated)
{
    CHECK_LT_MSG(partition_index,
                 app->partition_count,
                 "partition_index should be < partition_count: app_name={}, app_id={}, "
                 "partition_index={}, partition_count={}, new_max_replica_count={}",
                 app->app_name,
                 app->app_id,
                 partition_index,
                 app->partition_count,
                 new_max_replica_count);

    const auto &old_pc = app->pcs[partition_index];
    const auto old_max_replica_count = old_pc.max_replica_count;

    if (new_max_replica_count == old_max_replica_count) {
        LOG_WARNING("partition-level max_replica_count has been updated: app_name={}, "
                    "app_id={}, partition_index={}, new_max_replica_count={}",
                    app->app_name,
                    app->app_id,
                    partition_index,
                    new_max_replica_count);
        return;
    }

    auto &context = app->helpers->contexts[partition_index];
    if (context.stage == config_status::pending_remote_sync) {
        LOG_INFO("have to wait until another request which is syncing with remote storage "
                 "is finished, then process the current request of updating max_replica_count: "
                 "app_name={}, app_id={}, partition_index={}, new_max_replica_count={}",
                 app->app_name,
                 app->app_id,
                 partition_index,
                 new_max_replica_count);

        tasking::enqueue(
            LPC_META_STATE_HIGH,
            tracker(),
            [this, app, partition_index, new_max_replica_count, on_partition_updated]() mutable {
                update_partition_max_replica_count(
                    app, partition_index, new_max_replica_count, on_partition_updated);
            },
            server_state::sStateHash,
            std::chrono::milliseconds(100));
        return;
    }

    CHECK(context.stage == config_status::not_pending,
          "invalid config status while updating max_replica_count: context.stage={}, "
          "app_name={}, app_id={}, partition_index={}, new_max_replica_count={}",
          enum_to_string(context.stage),
          app->app_name,
          app->app_id,
          partition_index,
          new_max_replica_count);

    context.stage = config_status::pending_remote_sync;
    context.pending_sync_request.reset();
    context.msg = nullptr;

    auto new_pc = old_pc;
    new_pc.max_replica_count = new_max_replica_count;
    ++(new_pc.ballot);
    context.pending_sync_task =
        update_partition_max_replica_count_on_remote(app, new_pc, on_partition_updated);
}

// ThreadPool: THREAD_POOL_META_STATE
task_ptr
server_state::update_partition_max_replica_count_on_remote(std::shared_ptr<app_state> &app,
                                                           const partition_configuration &new_pc,
                                                           partition_callback on_partition_updated)
{
    const auto &gpid = new_pc.pid;
    const auto partition_index = gpid.get_partition_index();
    const auto new_max_replica_count = new_pc.max_replica_count;
    const auto new_ballot = new_pc.ballot;

    const auto level = _meta_svc->get_function_level();
    if (level <= meta_function_level::fl_blind) {
        LOG_WARNING("have to wait until meta level becomes more than fl_blind, then process the "
                    "current request of updating max_replica_count: current_meta_level={}, "
                    "app_name={}, app_id={}, partition_index={}, new_max_replica_count={}, "
                    "new_ballot={}",
                    _meta_function_level_VALUES_TO_NAMES.find(level)->second,
                    app->app_name,
                    app->app_id,
                    partition_index,
                    new_max_replica_count,
                    new_ballot);

        // NOTICE: pending_sync_task should be reassigned
        return tasking::enqueue(
            LPC_META_STATE_HIGH,
            tracker(),
            [this, app, new_pc, on_partition_updated]() mutable {
                const auto &gpid = new_pc.pid;
                const auto partition_index = gpid.get_partition_index();

                zauto_write_lock l(_lock);

                auto &context = app->helpers->contexts[partition_index];
                context.pending_sync_task =
                    update_partition_max_replica_count_on_remote(app, new_pc, on_partition_updated);
            },
            server_state::sStateHash,
            std::chrono::seconds(1));
    }

    LOG_INFO("request for updating partition-level max_replica_count on remote storage: "
             "app_name={}, app_id={}, partition_id={}, new_max_replica_count={}, new_ballot={}",
             app->app_name,
             app->app_id,
             partition_index,
             new_max_replica_count,
             new_ballot);

    auto partition_path = get_partition_path(gpid);
    auto json_config = dsn::json::json_forwarder<partition_configuration>::encode(new_pc);
    return _meta_svc->get_remote_storage()->set_data(
        partition_path,
        json_config,
        LPC_META_STATE_HIGH,
        std::bind(&server_state::on_update_partition_max_replica_count_on_remote_reply,
                  this,
                  std::placeholders::_1,
                  app,
                  new_pc,
                  on_partition_updated),
        tracker());
}

// ThreadPool: THREAD_POOL_META_STATE
void server_state::on_update_partition_max_replica_count_on_remote_reply(
    error_code ec,
    std::shared_ptr<app_state> &app,
    const partition_configuration &new_pc,
    partition_callback on_partition_updated)
{
    const auto &gpid = new_pc.pid;
    const auto partition_index = gpid.get_partition_index();
    const auto new_max_replica_count = new_pc.max_replica_count;
    const auto new_ballot = new_pc.ballot;

    zauto_write_lock l(_lock);

    LOG_INFO("reply for updating partition-level max_replica_count on remote storage: "
             "error_code={}, app_name={}, app_id={}, partition_id={}, new_max_replica_count={}, "
             "new_ballot={}",
             ec,
             app->app_name,
             app->app_id,
             partition_index,
             new_max_replica_count,
             new_ballot);

    auto &context = app->helpers->contexts[partition_index];
    if (ec == ERR_TIMEOUT) {
        // NOTICE: pending_sync_task need to be reassigned
        context.pending_sync_task = tasking::enqueue(
            LPC_META_STATE_HIGH,
            tracker(),
            [this, app, new_pc, on_partition_updated]() mutable {
                const auto &gpid = new_pc.pid;
                const auto partition_index = gpid.get_partition_index();

                zauto_write_lock l(_lock);

                auto &context = app->helpers->contexts[partition_index];
                context.pending_sync_task =
                    update_partition_max_replica_count_on_remote(app, new_pc, on_partition_updated);
            },
            server_state::sStateHash,
            std::chrono::seconds(1));
        return;
    }

    if (ec != ERR_OK) {
        on_partition_updated(ec, partition_index);
        return;
    }

    update_partition_max_replica_count_locally(app, new_pc);

    context.pending_sync_task = nullptr;
    context.pending_sync_request.reset();
    context.stage = config_status::not_pending;
    context.msg = nullptr;

    on_partition_updated(ec, partition_index);
}

// ThreadPool: THREAD_POOL_META_STATE
void server_state::update_partition_max_replica_count_locally(std::shared_ptr<app_state> &app,
                                                              const partition_configuration &new_pc)
{
    const auto &gpid = new_pc.pid;
    const auto partition_index = gpid.get_partition_index();
    const auto new_max_replica_count = new_pc.max_replica_count;
    const auto new_ballot = new_pc.ballot;

    auto &old_pc = app->pcs[gpid.get_partition_index()];
    const auto old_max_replica_count = old_pc.max_replica_count;
    const auto old_ballot = old_pc.ballot;

    CHECK_EQ_MSG(old_ballot + 1,
                 new_ballot,
                 "invalid ballot while updating local max_replica_count: app_name={}, app_id={}, "
                 "partition_id={}, old_max_replica_count={}, new_max_replica_count={}, "
                 "old_ballot={}, new_ballot={}",
                 app->app_name,
                 app->app_id,
                 partition_index,
                 old_max_replica_count,
                 new_max_replica_count,
                 old_ballot,
                 new_ballot);

    std::string old_config_str(boost::lexical_cast<std::string>(old_pc));
    std::string new_config_str(boost::lexical_cast<std::string>(new_pc));

    old_pc = new_pc;

    LOG_INFO("local partition-level max_replica_count has been changed successfully: ",
             "app_name={}, app_id={}, partition_id={}, old_pc={}, "
             "new_pc={}",
             app->app_name,
             app->app_id,
             partition_index,
             old_config_str,
             new_config_str);
}

// ThreadPool: THREAD_POOL_META_SERVER
void server_state::recover_from_max_replica_count_env()
{
    std::vector<std::pair<std::shared_ptr<app_state>, int32_t>> tasks;
    {
        zauto_read_lock l(_lock);
        for (auto &e : _exist_apps) {
            auto &app = e.second;
            if (app->status != app_status::AS_AVAILABLE) {
                continue;
            }

            auto iter = app->envs.find(replica_envs::UPDATE_MAX_REPLICA_COUNT);
            if (iter == app->envs.end()) {
                continue;
            }

            std::vector<std::string> args;
            utils::split_args(iter->second.c_str(), args, ';');
            if (args.empty() || args[0] != "updating") {
                continue;
            }

            int32_t max_replica_count = 0;
            if (args.size() < 2 || !dsn::buf2int32(args[1], max_replica_count) ||
                max_replica_count <= 0) {
                CHECK(false,
                      "invalid max_replica_count_env: app_name={}, app_id={}, "
                      "max_replica_count={}, {}={}",
                      app->app_name,
                      app->app_id,
                      app->max_replica_count,
                      replica_envs::UPDATE_MAX_REPLICA_COUNT,
                      iter->second);
            }

            tasks.emplace_back(app, max_replica_count);
        }
    }

    dsn::task_tracker tracker;

    for (auto &task : tasks) {
        recover_all_partitions_max_replica_count(task.first, task.second, tracker);
    }
    tracker.wait_outstanding_tasks();

    for (auto &task : tasks) {
        recover_app_max_replica_count(task.first, task.second, tracker);
    }
    tracker.wait_outstanding_tasks();
}

// ThreadPool: THREAD_POOL_META_SERVER
void server_state::recover_all_partitions_max_replica_count(std::shared_ptr<app_state> &app,
                                                            int32_t new_max_replica_count,
                                                            dsn::task_tracker &tracker)
{
    for (int i = 0; i < app->partition_count; ++i) {
        zauto_read_lock l(_lock);

        auto new_pc = app->pcs[i];
        if (new_pc.max_replica_count == new_max_replica_count) {
            LOG_WARNING("no need to recover partition-level max_replica_count since it has been "
                        "updated before: app_name={}, app_id={}, partition_index={}, "
                        "partition_count={}, new_max_replica_count={}",
                        app->app_name,
                        app->app_id,
                        i,
                        app->partition_count,
                        new_max_replica_count);
            continue;
        }

        LOG_INFO("ready to recover partition-level max_replica_count: app_name={}, app_id={}, "
                 "partition_index={}, partition_count={}, old_max_replica_count={}, "
                 "new_max_replica_count={}",
                 app->app_name,
                 app->app_id,
                 i,
                 app->partition_count,
                 app->max_replica_count,
                 new_max_replica_count);

        new_pc.max_replica_count = new_max_replica_count;
        ++(new_pc.ballot);
        auto partition_path = get_partition_path(new_pc.pid);
        auto value = dsn::json::json_forwarder<partition_configuration>::encode(new_pc);
        _meta_svc->get_remote_storage()->set_data(
            partition_path,
            value,
            LPC_META_CALLBACK,
            [this, app, i, new_pc](error_code ec) mutable {
                zauto_write_lock l(_lock);

                auto &old_pc = app->pcs[i];
                std::string old_pc_str(boost::lexical_cast<std::string>(old_pc));
                std::string new_pc_str(boost::lexical_cast<std::string>(new_pc));

                CHECK_EQ_MSG(ec,
                             ERR_OK,
                             "An error that can't be handled occurs while recovering remote "
                             "partition-level max_replica_count: error_code={}, app_name={}, "
                             "app_id={}, partition_index={}, partition_count={}, "
                             "old_partition_config={}, new_partition_config={}",
                             ec,
                             app->app_name,
                             app->app_id,
                             i,
                             app->partition_count,
                             old_pc_str,
                             new_pc_str);

                CHECK_EQ_MSG(old_pc.ballot + 1,
                             new_pc.ballot,
                             "invalid ballot while recovering max_replica_count: app_name={}, "
                             "app_id={}, partition_index={}, partition_count={}, "
                             "old_partition_config={}, new_partition_config={}",
                             app->app_name,
                             app->app_id,
                             i,
                             app->partition_count,
                             old_pc_str,
                             new_pc_str);

                old_pc = new_pc;

                LOG_INFO("partition-level max_replica_count has been recovered successfully: "
                         "app_name={}, app_id={}, partition_index={}, partition_count={}, "
                         "old_partition_config={}, new_partition_config={}",
                         app->app_name,
                         app->app_id,
                         i,
                         app->partition_count,
                         old_pc_str,
                         new_pc_str);
            },
            &tracker);
    }
}

// ThreadPool: THREAD_POOL_META_SERVER
void server_state::recover_app_max_replica_count(std::shared_ptr<app_state> &app,
                                                 int32_t new_max_replica_count,
                                                 dsn::task_tracker &tracker)
{
    zauto_read_lock l(_lock);

    LOG_INFO("ready to recover app-level max_replica_count: app_name={}, app_id={}, "
             "old_max_replica_count={}, new_max_replica_count={}, {}={}",
             app->app_name,
             app->app_id,
             app->max_replica_count,
             new_max_replica_count,
             replica_envs::UPDATE_MAX_REPLICA_COUNT,
             app->envs[replica_envs::UPDATE_MAX_REPLICA_COUNT]);

    auto ainfo = *(reinterpret_cast<app_info *>(app.get()));
    ainfo.max_replica_count = new_max_replica_count;
    ainfo.envs.erase(replica_envs::UPDATE_MAX_REPLICA_COUNT);
    auto app_path = get_app_path(*app);
    auto value = dsn::json::json_forwarder<app_info>::encode(ainfo);
    _meta_svc->get_remote_storage()->set_data(
        app_path,
        value,
        LPC_META_CALLBACK,
        [this, app, new_max_replica_count](error_code ec) mutable {
            zauto_write_lock l(_lock);

            auto old_max_replica_count = app->max_replica_count;
            CHECK_EQ_MSG(ec,
                         ERR_OK,
                         "An error that can't be handled occurs while recovering remote "
                         "app-level max_replica_count: error_code={}, app_name={}, app_id={}, "
                         "old_max_replica_count={}, new_max_replica_count={}",
                         ec,
                         app->app_name,
                         app->app_id,
                         old_max_replica_count,
                         new_max_replica_count);

            app->max_replica_count = new_max_replica_count;
            app->envs.erase(replica_envs::UPDATE_MAX_REPLICA_COUNT);

            LOG_INFO("app-level max_replica_count has been recovered successfully: "
                     "app_name={}, app_id={}, old_max_replica_count={}, "
                     "new_max_replica_count={}",
                     app->app_name,
                     app->app_id,
                     old_max_replica_count,
                     app->max_replica_count);
        },
        &tracker);
}

// ThreadPool: THREAD_POOL_META_STATE
void server_state::get_atomic_idempotent(configuration_get_atomic_idempotent_rpc rpc) const
{
    const auto &app_name = rpc.request().app_name;
    auto &response = rpc.response();

    zauto_read_lock l(_lock);

    auto app = get_app_and_check_exist(app_name, response);
    if (!app) {
        response.atomic_idempotent = false;
        LOG_WARNING("failed to get atomic_idempotent: app_name={}, "
                    "error_code={}, hint_message={}",
                    app_name,
                    response.err,
                    response.hint_message);
        return;
    }

    response.err = ERR_OK;

    // No need to check `app->__isset.atomic_idempotent`, since by default it is true
    // (because `app->atomic_idempotent` has default value false).
    response.atomic_idempotent = app->atomic_idempotent;

    LOG_INFO("get atomic_idempotent successfully: app_name={}, app_id={}, "
             "atomic_idempotent={}",
             app_name,
             app->app_id,
             response.atomic_idempotent);
}

// ThreadPool: THREAD_POOL_META_STATE
void server_state::set_atomic_idempotent(configuration_set_atomic_idempotent_rpc rpc)
{
    const auto &app_name = rpc.request().app_name;
    const auto new_atomic_idempotent = rpc.request().atomic_idempotent;
    auto &response = rpc.response();

    int32_t app_id = 0;
    std::shared_ptr<app_state> app;

    {
        zauto_read_lock l(_lock);

        app = get_app_and_check_exist(app_name, response);
        if (!app) {
            response.old_atomic_idempotent = false;
            LOG_WARNING("failed to set atomic_idempotent: app_name={}, "
                        "error_code={}, hint_message={}",
                        app_name,
                        response.err,
                        response.hint_message);
            return;
        }

        app_id = app->app_id;

        // No need to check `app->__isset.atomic_idempotent`, since by default it is true
        // (because `app->atomic_idempotent` has default value false).
        response.old_atomic_idempotent = app->atomic_idempotent;

        if (app->status != app_status::AS_AVAILABLE) {
            response.err = ERR_INVALID_STATE;
            response.hint_message = fmt::format("app({}) is not in available status", app_name);
            LOG_ERROR("failed to set atomic_idempotent: app_name={}, app_id={}, "
                      "error_code={}, hint_message={}",
                      app_name,
                      app_id,
                      response.err,
                      response.hint_message);
            return;
        }
    }

    const auto level = _meta_svc->get_function_level();
    if (level <= meta_function_level::fl_freezed) {
        response.err = ERR_STATE_FREEZED;
        response.hint_message =
            "current meta function level is freezed, since there are too few alive nodes";
        LOG_ERROR("failed to set atomic_idempotent: app_name={}, app_id={}, "
                  "error_code={}, message={}",
                  app_name,
                  app_id,
                  response.err,
                  response.hint_message);
        return;
    }

    if (new_atomic_idempotent == response.old_atomic_idempotent) {
        response.err = ERR_OK;
        response.hint_message = "no need to update atomic_idempotent since it's unchanged";
        LOG_WARNING("{}: app_name={}, app_id={}", response.hint_message, app_name, app_id);
        return;
    }

    LOG_INFO("request for updating atomic_idempotent: app_name={}, app_id={}, "
             "old_atomic_idempotent={}, new_atomic_idempotent={}",
             app_name,
             app_id,
             response.old_atomic_idempotent,
             new_atomic_idempotent);

    update_app_atomic_idempotent_on_remote(app, rpc);
}

// ThreadPool: THREAD_POOL_META_STATE
void server_state::update_app_atomic_idempotent_on_remote(
    std::shared_ptr<app_state> &app, configuration_set_atomic_idempotent_rpc rpc)
{
    app_info ainfo = *app;
    ainfo.atomic_idempotent = rpc.request().atomic_idempotent;
    do_update_app_info(get_app_path(*app), ainfo, [this, app, rpc](error_code ec) mutable {
        const auto new_atomic_idempotent = rpc.request().atomic_idempotent;
        const auto old_atomic_idempotent = rpc.response().old_atomic_idempotent;

        zauto_write_lock l(_lock);

        CHECK_EQ_MSG(ec,
                     ERR_OK,
                     "An error that cannot be handled occurred while updating atomic_idempotent "
                     "on remote: error_code={}, app_name={}, app_id={}, "
                     "old_atomic_idempotent={}, new_atomic_idempotent={}",
                     ec,
                     app->app_name,
                     app->app_id,
                     old_atomic_idempotent,
                     new_atomic_idempotent);

        CHECK_EQ_MSG(rpc.request().app_name,
                     app->app_name,
                     "atomic_idempotent was updated to remote storage, however app_name "
                     "has been changed since then: old_app_name={}, new_app_name={}, "
                     "app_id={}, old_atomic_idempotent={}, new_atomic_idempotent={}",
                     rpc.request().app_name,
                     app->app_name,
                     app->app_id,
                     old_atomic_idempotent,
                     new_atomic_idempotent);

        // No need to check `app->__isset.atomic_idempotent`, since by default it is true
        // (because `app->atomic_idempotent` has default value false).
        CHECK_EQ_MSG(old_atomic_idempotent,
                     app->atomic_idempotent,
                     "atomic_idempotent has been updated to remote storage, however "
                     "old_atomic_idempotent from response is not consistent with current local "
                     "atomic_idempotent: app_name={}, app_id={}, old_atomic_idempotent={}, "
                     "local_atomic_idempotent={}, new_atomic_idempotent={}",
                     app->app_name,
                     app->app_id,
                     old_atomic_idempotent,
                     app->atomic_idempotent,
                     new_atomic_idempotent);

        app->__set_atomic_idempotent(new_atomic_idempotent);
        LOG_INFO("both remote and local app-level atomic_idempotent have been updated "
                 "successfully: app_name={}, app_id={}, old_atomic_idempotent={}, "
                 "new_atomic_idempotent={}",
                 app->app_name,
                 app->app_id,
                 old_atomic_idempotent,
                 new_atomic_idempotent);

        auto &response = rpc.response();
        response.err = ERR_OK;
    });
}

#undef SUCC_CREATE_APP_RESPONSE
#undef FAIL_CREATE_APP_RESPONSE
#undef INIT_CREATE_APP_RESPONSE_WITH_OK
#undef INIT_CREATE_APP_RESPONSE_WITH_ERR

#undef REPLY_TO_CLIENT_AND_RETURN
#undef REPLY_TO_CLIENT

} // namespace dsn::replication
