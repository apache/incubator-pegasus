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

/*
 * Description:
 *     the meta server's server_state, impl file
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     2016-04-25, Weijie Sun(sunweijie at xiaomi.com), refactor
 */

#include <dsn/utility/factory_store.h>
#include <dsn/utility/string_conv.h>
#include <dsn/tool-api/task.h>
#include <dsn/tool-api/command_manager.h>
#include <dsn/tool-api/async_calls.h>
#include <sstream>
#include <cinttypes>
#include <string>
#include <boost/lexical_cast.hpp>

#include "server_state.h"
#include "server_load_balancer.h"

#include "dump_file.h"

using namespace dsn;

namespace dsn {
namespace replication {

static const char *lock_state = "lock";
static const char *unlock_state = "unlock";

server_state::server_state()
    : _meta_svc(nullptr),
      _add_secondary_enable_flow_control(false),
      _add_secondary_max_count_for_one_node(0),
      _cli_dump_handle(nullptr),
      _ctrl_add_secondary_enable_flow_control(nullptr),
      _ctrl_add_secondary_max_count_for_one_node(nullptr)
{
}

server_state::~server_state()
{
    _tracker.cancel_outstanding_tasks();
    if (_cli_dump_handle != nullptr) {
        dsn::command_manager::instance().deregister_command(_cli_dump_handle);
        _cli_dump_handle = nullptr;
    }
    if (_ctrl_add_secondary_enable_flow_control != nullptr) {
        dsn::command_manager::instance().deregister_command(
            _ctrl_add_secondary_enable_flow_control);
        _ctrl_add_secondary_enable_flow_control = nullptr;
    }
    if (_ctrl_add_secondary_max_count_for_one_node != nullptr) {
        dsn::command_manager::instance().deregister_command(
            _ctrl_add_secondary_max_count_for_one_node);
        _ctrl_add_secondary_max_count_for_one_node = nullptr;
    }
}

void server_state::register_cli_commands()
{
    _cli_dump_handle = dsn::command_manager::instance().register_app_command(
        {"dump"},
        "dump: dump app_states of meta server to local file",
        "dump -t|--target target_file",
        [this](const std::vector<std::string> &args) {
            dsn::error_code err;
            if (args.size() != 2) {
                err = ERR_INVALID_PARAMETERS;
            } else {
                const char *target_file = nullptr;
                for (int i = 0; i < args.size(); i += 2) {
                    if (strcmp(args[i].c_str(), "-t") == 0 ||
                        strcmp(args[i].c_str(), "--target") == 0)
                        target_file = args[i + 1].c_str();
                }
                if (target_file == nullptr) {
                    err = ERR_INVALID_PARAMETERS;
                } else {
                    err = this->dump_from_remote_storage(target_file, false);
                }
            }
            return std::string(err.to_string());
        });
    dassert(_cli_dump_handle != nullptr, "register cli handler failed");

    _ctrl_add_secondary_enable_flow_control = dsn::command_manager::instance().register_app_command(
        {"lb.add_secondary_enable_flow_control"},
        "lb.add_secondary_enable_flow_control <true|false>",
        "control whether enable add secondary flow control",
        [this](const std::vector<std::string> &args) {
            HANDLE_CLI_FLAGS(_add_secondary_enable_flow_control, args);
        });
    dassert(_ctrl_add_secondary_enable_flow_control, "register cli handler failed");

    _ctrl_add_secondary_max_count_for_one_node =
        dsn::command_manager::instance().register_app_command(
            {"lb.add_secondary_max_count_for_one_node"},
            "lb.add_secondary_max_count_for_one_node [num | DEFAULT]",
            "control the max count to add secondary for one node",
            [this](const std::vector<std::string> &args) {
                std::string result("OK");
                if (args.empty()) {
                    result = std::to_string(_add_secondary_max_count_for_one_node);
                } else {
                    if (args[0] == "DEFAULT") {
                        _add_secondary_max_count_for_one_node =
                            _meta_svc->get_meta_options().add_secondary_max_count_for_one_node;
                    } else {
                        int32_t v = 0;
                        if (!dsn::buf2int32(args[0], v) || v < 0) {
                            result = std::string("ERR: invalid arguments");
                        } else {
                            _add_secondary_max_count_for_one_node = v;
                        }
                    }
                }
                return result;
            });
    dassert(_ctrl_add_secondary_max_count_for_one_node, "register cli handler failed");
}

void server_state::initialize(meta_service *meta_svc, const std::string &apps_root)
{
    _meta_svc = meta_svc;
    _apps_root = apps_root;
    _add_secondary_enable_flow_control =
        _meta_svc->get_meta_options().add_secondary_enable_flow_control;
    _add_secondary_max_count_for_one_node =
        _meta_svc->get_meta_options().add_secondary_max_count_for_one_node;

    _dead_partition_count.init_app_counter("eon.server_state",
                                           "dead_partition_count",
                                           COUNTER_TYPE_NUMBER,
                                           "current dead partition count");
    _unreadable_partition_count.init_app_counter("eon.server_state",
                                                 "unreadable_partition_count",
                                                 COUNTER_TYPE_NUMBER,
                                                 "current unreadable partition count");
    _unwritable_partition_count.init_app_counter("eon.server_state",
                                                 "unwritable_partition_count",
                                                 COUNTER_TYPE_NUMBER,
                                                 "current unwritable partition count");
    _writable_ill_partition_count.init_app_counter("eon.server_state",
                                                   "writable_ill_partition_count",
                                                   COUNTER_TYPE_NUMBER,
                                                   "current writable ill partition count");
    _healthy_partition_count.init_app_counter("eon.server_state",
                                              "healthy_partition_count",
                                              COUNTER_TYPE_NUMBER,
                                              "current healthy partition count");
    _recent_update_config_count.init_app_counter("eon.server_state",
                                                 "recent_update_config_count",
                                                 COUNTER_TYPE_VOLATILE_NUMBER,
                                                 "update configuration count in the recent period");
    _recent_partition_change_unwritable_count.init_app_counter(
        "eon.server_state",
        "recent_partition_change_unwritable_count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "partition change to unwritable count in the recent period");
    _recent_partition_change_writable_count.init_app_counter(
        "eon.server_state",
        "recent_partition_change_writable_count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "partition change to writable count in the recent period");
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
        ddebug("there are (%d) apps still in staging, just wait...", c);
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
        configuration_create_app_response resp;
        resp.err = dsn::ERR_OK;
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
        dassert(false,
                "app(%s) not in staging state(%s)",
                app->get_logname(),
                enum_to_string(app->status));
    }

    ddebug("app(%s) transfer from %s to %s",
           app->get_logname(),
           enum_to_string(old_status),
           enum_to_string(app->status));
#undef send_response
}

void server_state::process_one_partition(std::shared_ptr<app_state> &app)
{
    int ans = --app->helpers->partitions_in_progress;
    if (ans > 0) {
        dinfo("app(%s) in status %s, can't transfer to stable state as some partition is in "
              "progressing",
              app->get_logname(),
              enum_to_string(app->status));
        return;
    } else if (ans == 0) {
        transition_staging_state(app);
    } else {
        dassert(false, "partitions in progress(%d) shouldn't be negetive", ans);
    }
}

error_code server_state::dump_app_states(const char *local_path,
                                         const std::function<app_state *()> &iterator)
{
    std::shared_ptr<dump_file> file = dump_file::open_file(local_path, true);
    if (file == nullptr) {
        derror("open file failed, file(%s)", local_path);
        return ERR_FILE_OPERATION_FAILED;
    }

    file->append_buffer("binary", 6);
    app_state *app;
    while ((app = iterator()) != nullptr) {
        dassert(app->status == app_status::AS_AVAILABLE || app->status == app_status::AS_DROPPED,
                "invalid app status");
        binary_writer writer;
        dsn::marshall(writer, *app, DSF_THRIFT_BINARY);
        file->append_buffer(writer.get_buffer());
        for (const partition_configuration &pc : app->partitions) {
            binary_writer writer;
            dsn::marshall(writer, pc, DSF_THRIFT_BINARY);
            file->append_buffer(writer.get_buffer());
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
            ddebug("remote storage is empty, just stop the dump");
            return ERR_OK;
        } else if (ec != ERR_OK) {
            derror("sync from remote storage failed, err(%s)", ec.to_string());
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
                ddebug("there are apps in staging, skip this dump");
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
        derror("open file failed, file(%s)", local_path);
        return ERR_FILE_OPERATION_FAILED;
    }

    blob data;
    dassert(file->read_next_buffer(data) == 1, "read format header fail");
    _all_apps.clear();

    dassert(memcmp(data.data(), "binary", 6) == 0, "");
    while (true) {
        int ans = file->read_next_buffer(data);
        dassert(ans != -1, "read file failed");
        if (ans == 0) // file end
            break;

        app_info info;
        binary_reader reader(data);
        unmarshall(reader, info, DSF_THRIFT_BINARY);
        std::shared_ptr<app_state> app = app_state::create(info);
        _all_apps.emplace(app->app_id, app);

        for (unsigned int i = 0; i != app->partition_count; ++i) {
            ans = file->read_next_buffer(data);
            binary_reader reader(data);
            dassert(ans == 1, "unexpect read buffer, ret(%d)", ans);
            unmarshall(reader, app->partitions[i], DSF_THRIFT_BINARY);
            dassert(app->partitions[i].pid.get_partition_index() == i,
                    "uncorrect partition data, gpid(%d.%d), appname(%s)",
                    app->app_id,
                    i,
                    app->app_name.c_str());
        }
    }

    for (auto &iter : _all_apps) {
        if (iter.second->status == app_status::AS_AVAILABLE)
            iter.second->status = app_status::AS_CREATING;
        else {
            dassert(iter.second->status == app_status::AS_DROPPED,
                    "invalid app_status, status = %s",
                    enum_to_string(iter.second->status));
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
    std::vector<const char *> sections;
    dsn_config_get_all_sections(sections);
    ddebug("start to do initialize");

    app_info default_app;
    for (int i = 0; i < sections.size(); i++) {
        if (strstr(sections[i], "meta_server.apps") == sections[i] ||
            strcmp(sections[i], "replication.app") == 0) {
            const char *s = sections[i];

            default_app.status = app_status::AS_CREATING;
            default_app.app_id = _all_apps.size() + 1;

            default_app.app_name = dsn_config_get_value_string(s, "app_name", "", "app name");
            if (default_app.app_name.length() == 0) {
                dwarn("'[%s] app_name' not specified, ignore this section", s);
                continue;
            }

            default_app.app_type = dsn_config_get_value_string(s, "app_type", "", "app type name");
            default_app.partition_count = (int)dsn_config_get_value_uint64(
                s, "partition_count", 1, "how many partitions the app should have");
            default_app.is_stateful =
                dsn_config_get_value_bool(s, "stateful", true, "whether this is a stateful app");
            default_app.max_replica_count = (int)dsn_config_get_value_uint64(
                s, "max_replica_count", 3, "max replica count in app");
            default_app.create_second = dsn_now_ms() / 1000;
            std::string envs_str = dsn_config_get_value_string(s, "envs", "", "app envs");
            bool parse = dsn::utils::parse_kv_map(envs_str.c_str(), default_app.envs, ',', '=');

            dassert(default_app.app_type.length() > 0, "'[%s] app_type' not specified", s);
            dassert(default_app.partition_count > 0, "'[%s] partition_count' should > 0", s);
            dassert(parse, "'[%s] envs' is invalid, envs = %s", s, envs_str.c_str());

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
    for (auto &kv_pair : _all_apps) {
        if (kv_pair.second->status == app_status::AS_CREATING) {
            dassert(_exist_apps.find(kv_pair.second->app_name) == _exist_apps.end(),
                    "invalid app name, name = %s",
                    kv_pair.second->app_name.c_str());
            _exist_apps.emplace(kv_pair.second->app_name, kv_pair.second);
        }
    }

    // create cluster_root/apps node
    std::string &apps_path = _apps_root;
    error_code err;
    dist::meta_state_service *storage = _meta_svc->get_remote_storage();

    auto t = storage->create_node(apps_path,
                                  LPC_META_CALLBACK,
                                  [&err](error_code ec) { err = ec; },
                                  blob(lock_state, 0, strlen(lock_state)));
    t->wait();

    if (err != ERR_NODE_ALREADY_EXIST && err != ERR_OK) {
        derror("create root node /apps in meta store failed, err = %s", err.to_string());
        return err;
    } else {
        ddebug("set %s to lock state in remote storage", _apps_root.c_str());
    }

    err = ERR_OK;
    dsn::task_tracker tracker;
    for (auto &kv : _all_apps) {
        std::shared_ptr<app_state> &app = kv.second;
        std::string path = get_app_path(*app);

        dassert(app->status == app_status::AS_CREATING || app->status == app_status::AS_DROPPING,
                "invalid app status");
        blob value = app->to_json(app_status::AS_CREATING == app->status ? app_status::AS_AVAILABLE
                                                                         : app_status::AS_DROPPED);
        storage->create_node(path,
                             LPC_META_CALLBACK,
                             [&err, path](error_code ec) {
                                 if (ec != ERR_OK && ec != ERR_NODE_ALREADY_EXIST) {
                                     dwarn("create app node failed, path(%s) reason(%s)",
                                           path.c_str(),
                                           ec.to_string());
                                     err = ec;
                                 } else {
                                     ddebug("create app node %s ok", path.c_str());
                                 }
                             },
                             value,
                             &tracker);
    }
    tracker.wait_outstanding_tasks();

    if (err != ERR_OK) {
        _exist_apps.clear();
        return err;
    }
    for (auto &kv : _all_apps) {
        std::shared_ptr<app_state> &app = kv.second;
        for (unsigned int i = 0; i != app->partition_count; ++i) {
            task_ptr init_callback =
                tasking::create_task(LPC_META_STATE_HIGH, &tracker, [] {}, sStateHash);
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
        ddebug("set %s to unlock state in remote storage", _apps_root.c_str());
        return err;
    } else {
        derror("set %s to unlock state in remote storage failed, reason(%s)",
               _apps_root.c_str(),
               err.to_string());
        return err;
    }
}

dsn::error_code server_state::sync_apps_from_remote_storage()
{
    dsn::error_code err;
    dsn::task_tracker tracker;

    dist::meta_state_service *storage = _meta_svc->get_remote_storage();
    auto sync_partition = [this, storage, &err, &tracker](
        std::shared_ptr<app_state> &app, int partition_id, const std::string &partition_path) {
        storage->get_data(
            partition_path,
            LPC_META_CALLBACK,
            [this, app, partition_id, partition_path, &err](error_code ec,
                                                            const blob &value) mutable {
                if (ec == ERR_OK) {
                    partition_configuration pc;
                    dsn::json::json_forwarder<partition_configuration>::decode(value, pc);

                    dassert(pc.pid.get_app_id() == app->app_id &&
                                pc.pid.get_partition_index() == partition_id,
                            "invalid partition config");
                    {
                        zauto_write_lock l(_lock);
                        app->partitions[partition_id] = pc;
                        for (const dsn::rpc_address &addr : pc.last_drops) {
                            app->helpers->contexts[partition_id].record_drop_history(addr);
                        }

                        if (app->status == app_status::AS_CREATING &&
                            (pc.partition_flags & pc_flags::dropped) != 0) {
                            recall_partition(app, partition_id);
                        } else if (app->status == app_status::AS_DROPPING &&
                                   (pc.partition_flags & pc_flags::dropped) == 0) {
                            drop_partition(app, partition_id);
                        } else
                            process_one_partition(app);
                    }
                } else if (ec == ERR_OBJECT_NOT_FOUND) {
                    dwarn("partition node %s not exist on remote storage, may half create before",
                          partition_path.c_str());
                    init_app_partition_node(app, partition_id, nullptr);
                } else {
                    derror("get partition node failed, reason(%s)", ec.to_string());
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
                    dassert(dsn::json::json_forwarder<app_info>::decode(value, info),
                            "invalid json data");
                    std::shared_ptr<app_state> app = app_state::create(info);
                    {
                        zauto_write_lock l(_lock);
                        _all_apps.emplace(app->app_id, app);
                        if (app->status == app_status::AS_AVAILABLE) {
                            app->status = app_status::AS_CREATING;
                            _exist_apps.emplace(app->app_name, app);
                        } else if (app->status == app_status::AS_DROPPED) {
                            app->status = app_status::AS_DROPPING;
                        } else {
                            dassert(false,
                                    "invalid status(%s) for app(%s) in remote storage",
                                    enum_to_string(app->status),
                                    app->get_logname());
                        }
                    }

                    for (int i = 0; i < app->partition_count; i++) {
                        std::string partition_path =
                            app_path + "/" + boost::lexical_cast<std::string>(i);
                        sync_partition(app, i, partition_path);
                    }
                } else {
                    derror("get app info from meta state service failed, path = %s, err = %s",
                           app_path.c_str(),
                           ec.to_string());
                    err = ec;
                }
            },
            &tracker);
    };

    _all_apps.clear();
    _exist_apps.clear();

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
    dassert(ERR_OK == err, "can't handle this error (%s)", err.to_string());
    dassert(transaction_state == std::string(unlock_state) || transaction_state.empty(),
            "invalid transaction state(%s)",
            transaction_state.c_str());

    storage->get_children(
        _apps_root,
        LPC_META_CALLBACK,
        [&](error_code ec, const std::vector<std::string> &apps) {
            if (ec == ERR_OK) {
                for (const auto &appid_str : apps) {
                    sync_app(_apps_root + "/" + appid_str);
                }
            } else {
                derror("get app list from meta state service failed, path = %s, err = %s",
                       _apps_root.c_str(),
                       ec.to_string());
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
        for (partition_configuration &pc : app.partitions) {
            if (!pc.primary.is_invalid()) {
                node_state *ns = get_node_state(_nodes, pc.primary, true);
                ns->put_partition(pc.pid, true);
            }
            for (auto &ep : pc.secondaries) {
                dassert(!ep.is_invalid(), "invalid secondary address, addr = %s", ep.to_string());
                node_state *ns = get_node_state(_nodes, ep, true);
                ns->put_partition(pc.pid, false);
            }
        }
    }
    for (auto &node : _nodes) {
        node.second.set_alive(true);
    }
    for (auto &app_pair : _all_apps) {
        app_state &app = *(app_pair.second);
        for (const partition_configuration &pc : app.partitions) {
            check_consistency(pc.pid);
        }
    }
}

error_code server_state::initialize_data_structure()
{
    error_code err = sync_apps_from_remote_storage();
    if (err == ERR_OBJECT_NOT_FOUND) {
        if (_meta_svc->get_meta_options().recover_from_replica_server) {
            return ERR_OBJECT_NOT_FOUND;
        } else {
            ddebug("can't find apps from remote storage, start to initialize default apps");
            err = initialize_default_apps();
        }
    } else if (err == ERR_OK) {
        if (_meta_svc->get_meta_options().recover_from_replica_server) {
            dassert(false,
                    "find apps from remote storage, but "
                    "[meta_server].recover_from_replica_server = true");
        } else {
            ddebug("sync apps from remote storage ok, get %d apps, init the node state accordingly",
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

// client => meta server
void server_state::query_configuration_by_node(
    const configuration_query_by_node_request &request,
    /*out*/ configuration_query_by_node_response &response)
{
    zauto_read_lock l(_lock);
    node_state *ns = get_node_state(_nodes, request.node, false);
    if (ns == nullptr) {
        response.err = ERR_OBJECT_NOT_FOUND;
    } else {
        response.err = ERR_OK;
        response.partitions.resize(ns->partition_count());
        int i = 0;
        ns->for_each_partition([&, this](const gpid &pid) {
            std::shared_ptr<app_state> app = get_app(pid.get_app_id());
            dassert(app != nullptr, "invalid app_id, app_id = %d", pid.get_app_id());
            response.partitions[i].info = *app;
            response.partitions[i].host_node = request.node;
            response.partitions[i].config = app->partitions[pid.get_partition_index()];
            ++i;
            return true;
        });
    }
}

// partition server => meta server
// this is done in meta_state_thread_pool
void server_state::on_config_sync(dsn::message_ex *msg)
{
    configuration_query_by_node_request request;
    configuration_query_by_node_response response;

    dsn::unmarshall(msg, request);

    bool reject_this_request = false;
    response.__isset.gc_replicas = false;
    ddebug("got config sync request from %s, stored_replicas_count(%d)",
           request.node.to_string(),
           (int)request.stored_replicas.size());

    {
        zauto_read_lock l(_lock);

        // sync the partitions to the replica server
        node_state *ns = get_node_state(_nodes, request.node, false);
        if (ns == nullptr) {
            ddebug("node(%s) not found in meta server", request.node.to_string());
            response.err = ERR_OBJECT_NOT_FOUND;
        } else {
            response.err = ERR_OK;
            unsigned int i = 0;
            response.partitions.resize(ns->partition_count());
            ns->for_each_partition([&, this](const gpid &pid) {
                std::shared_ptr<app_state> app = get_app(pid.get_app_id());
                dassert(app != nullptr, "invalid app_id, app_id = %d", pid.get_app_id());
                config_context &cc = app->helpers->contexts[pid.get_partition_index()];

                // config sync need the newest data to keep the perfect FD,
                // so if the syncing config is related to the node, we may need to reject this
                // request
                if (cc.stage == config_status::pending_remote_sync) {
                    configuration_update_request *req = cc.pending_sync_request.get();
                    if (req->node == request.node)
                        return false;
                }

                response.partitions[i].info = *app;
                response.partitions[i].config = app->partitions[pid.get_partition_index()];
                response.partitions[i].host_node = request.node;
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
            std::vector<replica_info> &replicas = request.stored_replicas;
            meta_function_level::type level = _meta_svc->get_function_level();
            // if the node serve the replica on the meta server, then we ignore it
            // if the dropped servers on the meta servers are enough, we need to gc it
            // there are not enough dropped servers, we need to add it to dropped
            // the app is deleted but not expired, we need to ignore it
            // if the app is deleted and expired, we need to gc it
            for (replica_info &rep : replicas) {
                dinfo("receive stored replica from %s, pid(%d.%d)",
                      request.node.to_string(),
                      rep.pid.get_app_id(),
                      rep.pid.get_partition_index());
                std::shared_ptr<app_state> app = get_app(rep.pid.get_app_id());
                if (app == nullptr || rep.pid.get_partition_index() >= app->partition_count) {
                    // app is not recognized or partition is not recognized
                    dassert(false,
                            "gpid(%d.%d) on node(%s) is not exist on meta server, administrator "
                            "should check consistency of meta data",
                            rep.pid.get_app_id(),
                            rep.pid.get_partition_index(),
                            request.node.to_string());
                } else if (app->status == app_status::AS_DROPPED) {
                    if (app->expire_second == 0) {
                        ddebug("gpid(%d.%d) on node(%s) is of dropped table, but expire second is "
                               "not specified, do not delete it for safety reason",
                               rep.pid.get_app_id(),
                               rep.pid.get_partition_index(),
                               request.node.to_string());
                    } else if (has_seconds_expired(app->expire_second)) {
                        // can delete replica only when expire second is explicitely specified and
                        // expired.
                        if (level <= meta_function_level::fl_steady) {
                            ddebug("gpid(%d.%d) on node(%s) is of dropped and expired table, but "
                                   "current function level is %s, do not delete it for safety "
                                   "reason",
                                   rep.pid.get_app_id(),
                                   rep.pid.get_partition_index(),
                                   request.node.to_string(),
                                   _meta_function_level_VALUES_TO_NAMES.find(level)->second);
                        } else {
                            response.gc_replicas.push_back(rep);
                            dwarn("notify node(%s) to gc replica(%d.%d) coz the app is dropped and "
                                  "expired",
                                  request.node.to_string(),
                                  rep.pid.get_app_id(),
                                  rep.pid.get_partition_index());
                        }
                    }
                } else if (app->status == app_status::AS_AVAILABLE) {
                    bool is_useful_replica = _meta_svc->get_balancer()->collect_replica(
                        {&_all_apps, &_nodes}, request.node, rep);
                    if (!is_useful_replica) {
                        if (level <= meta_function_level::fl_steady) {
                            ddebug("gpid(%d.%d) on node(%s) is useless, but current function level "
                                   "is %s, do not delete it for safety reason",
                                   rep.pid.get_app_id(),
                                   rep.pid.get_partition_index(),
                                   request.node.to_string(),
                                   _meta_function_level_VALUES_TO_NAMES.find(level)->second);
                        } else {
                            response.gc_replicas.push_back(rep);
                            dwarn("notify node(%s) to gc replica(%d.%d) coz it is useless",
                                  request.node.to_string(),
                                  rep.pid.get_app_id(),
                                  rep.pid.get_partition_index());
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
    ddebug("send config sync response to %s, err(%s), partitions_count(%d), gc_replicas_count(%d)",
           request.node.to_string(),
           response.err.to_string(),
           (int)response.partitions.size(),
           (int)response.gc_replicas.size());
    _meta_svc->reply_data(msg, response);
    msg->release_ref();
}

bool server_state::query_configuration_by_gpid(dsn::gpid id,
                                               /*out*/ partition_configuration &config)
{
    zauto_read_lock l(_lock);
    const partition_configuration *pc = get_config(_all_apps, id);
    if (pc != nullptr) {
        config = *pc;
        return true;
    }
    return false;
}

void server_state::query_configuration_by_index(
    const configuration_query_by_index_request &request,
    /*out*/ configuration_query_by_index_response &response)
{
    zauto_read_lock l(_lock);
    auto iter = _exist_apps.find(request.app_name.c_str());
    if (iter == _exist_apps.end()) {
        response.err = ERR_OBJECT_NOT_FOUND;
        return;
    }

    std::shared_ptr<app_state> &app = iter->second;
    if (app->status != app_status::AS_AVAILABLE) {
        dassert(app->status == app_status::AS_CREATING || app->status == app_status::AS_DROPPING,
                "invalid status in exist app");
        response.err =
            (app->status == app_status::AS_CREATING ? ERR_BUSY_CREATING : ERR_BUSY_DROPPING);
        return;
    }

    response.err = ERR_OK;
    response.app_id = app->app_id;
    response.partition_count = app->partition_count;
    response.is_stateful = app->is_stateful;

    for (const int32_t &index : request.partition_indices) {
        if (index >= 0 && index < app->partitions.size())
            response.partitions.push_back(app->partitions[index]);
    }
    if (response.partitions.empty())
        response.partitions = app->partitions;
}

void server_state::init_app_partition_node(std::shared_ptr<app_state> &app,
                                           int pidx,
                                           task_ptr callback)
{
    auto on_create_app_partition = [this, pidx, app, callback](error_code ec) mutable {
        dinfo("create partition node: gpid(%d.%d), result: %s", app->app_id, pidx, ec.to_string());
        if (ERR_OK == ec || ERR_NODE_ALREADY_EXIST == ec) {
            {
                zauto_write_lock l(_lock);
                process_one_partition(app);
            }
            if (callback) {
                callback->enqueue();
            }
        } else if (ERR_TIMEOUT == ec) {
            dwarn("create partition node failed, gpid(%d.%d), retry later", app->app_id, pidx);
            // TODO: add parameter of the retry time interval in config file
            tasking::enqueue(
                LPC_META_STATE_HIGH,
                nullptr,
                std::bind(&server_state::init_app_partition_node, this, app, pidx, callback),
                0,
                std::chrono::milliseconds(1000));
        } else {
            dassert(false,
                    "we can't handle this error in init app partition nodes err(%s), gpid(%d.%d)",
                    ec.to_string(),
                    app->app_id,
                    pidx);
        }
    };

    std::string app_partition_path = get_partition_path(*app, pidx);
    dsn::blob value =
        dsn::json::json_forwarder<partition_configuration>::encode(app->partitions[pidx]);
    _meta_svc->get_remote_storage()->create_node(
        app_partition_path, LPC_META_STATE_HIGH, on_create_app_partition, value);
}

void server_state::do_app_create(std::shared_ptr<app_state> &app)
{
    auto on_create_app_root = [this, app](error_code ec) mutable {
        configuration_create_app_response resp;
        if (ERR_OK == ec || ERR_NODE_ALREADY_EXIST == ec) {
            dinfo("create app(%s) on storage service ok", app->get_logname());
            for (unsigned int i = 0; i != app->partition_count; ++i) {
                init_app_partition_node(app, i, nullptr);
            }
        } else if (ERR_TIMEOUT == ec) {
            dwarn("the storage service is not available currently, continue to create later");
            tasking::enqueue(LPC_META_STATE_HIGH,
                             nullptr,
                             std::bind(&server_state::do_app_create, this, app),
                             0,
                             std::chrono::seconds(1));
        } else {
            dassert(false, "we can't handle this right now, err(%s)", ec.to_string());
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
    configuration_create_app_response response;
    std::shared_ptr<app_state> app;
    bool will_create_app = false;
    dsn::unmarshall(msg, request);

    ddebug("create app request, name(%s), type(%s), partition_count(%d), replica_count(%d)",
           request.app_name.c_str(),
           request.options.app_type.c_str(),
           request.options.partition_count,
           request.options.replica_count);

    auto option_match_check = [](const create_app_options &opt, const app_state &exist_app) {
        return opt.partition_count == exist_app.partition_count &&
               opt.app_type == exist_app.app_type && opt.envs == exist_app.envs &&
               opt.is_stateful == exist_app.is_stateful &&
               opt.replica_count == exist_app.max_replica_count;
    };

    if (request.options.partition_count <= 0 || request.options.replica_count <= 0) {
        response.err = ERR_INVALID_PARAMETERS;
        will_create_app = false;
    } else {
        zauto_write_lock l(_lock);
        app = get_app(request.app_name);
        if (nullptr != app) {
            switch (app->status) {
            case app_status::AS_AVAILABLE:
                if (!request.options.success_if_exist || !option_match_check(request.options, *app))
                    response.err = ERR_INVALID_PARAMETERS;
                else {
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
        } else {
            will_create_app = true;

            app_info info;
            info.app_id = next_app_id();
            info.app_name = request.app_name;
            info.app_type = request.options.app_type;
            info.envs = std::move(request.options.envs);
            info.is_stateful = request.options.is_stateful;
            info.max_replica_count = request.options.replica_count;
            info.partition_count = request.options.partition_count;
            info.status = app_status::AS_CREATING;
            info.create_second = dsn_now_ms() / 1000;

            app = app_state::create(info);
            app->helpers->pending_response = msg;
            app->helpers->partitions_in_progress.store(info.partition_count);

            _all_apps.emplace(app->app_id, app);
            _exist_apps.emplace(request.app_name, app);
        }
    }

    if (will_create_app) {
        do_app_create(app);
    } else {
        _meta_svc->reply_data(msg, response);
        msg->release_ref();
    }
}

void server_state::do_app_drop(std::shared_ptr<app_state> &app)
{
    auto after_mark_app_dropped = [this, app](error_code ec) mutable {
        if (ERR_OK == ec) {
            zauto_write_lock l(_lock);
            _exist_apps.erase(app->app_name);
            for (int i = 0; i < app->partition_count; ++i) {
                drop_partition(app, i);
            }
        } else if (ERR_TIMEOUT == ec) {
            dinfo("drop app(%s) prepare timeout, continue to drop later", app->get_logname());
            tasking::enqueue(LPC_META_STATE_HIGH,
                             nullptr,
                             std::bind(&server_state::do_app_drop, this, app),
                             0,
                             std::chrono::seconds(1));
        } else {
            dassert(false, "we can't handle this, error(%s)", ec.to_string());
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
    ddebug("drop app request, name(%s)", request.app_name.c_str());
    {
        zauto_write_lock l(_lock);
        app = get_app(request.app_name);
        if (nullptr == app) {
            response.err = request.options.success_if_not_exist ? ERR_OK : ERR_APP_NOT_EXIST;
        } else {
            switch (app->status) {
            case app_status::AS_AVAILABLE:
                do_dropping = true;
                app->status = app_status::AS_DROPPING;
                app->drop_second = dsn_now_ms() / 1000;
                if (request.options.__isset.reserve_seconds &&
                    request.options.reserve_seconds > 0) {
                    app->expire_second = app->drop_second + request.options.reserve_seconds;
                } else {
                    app->expire_second = app->drop_second +
                                         _meta_svc->get_meta_options().hold_seconds_for_dropped_app;
                }
                app->helpers->pending_response = msg;
                dassert(app->helpers->partitions_in_progress.load() == 0,
                        "partition_in_progress_cnt = %d",
                        app->helpers->partitions_in_progress.load());
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
                dassert(
                    false, "invalid app status, status = %s", ::dsn::enum_to_string(app->status));
                break;
            }
        }
    }
    if (do_dropping) {
        do_app_drop(app);
    } else {
        _meta_svc->reply_data(msg, response);
        msg->release_ref();
    }
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
    ddebug("recall app request, app_id(%d)", request.app_id);

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
                std::string &new_app_name =
                    (request.new_app_name == "") ? target_app->app_name : request.new_app_name;
                if (_exist_apps.find(new_app_name) != _exist_apps.end()) {
                    response.err = ERR_INVALID_PARAMETERS;
                } else {
                    do_recalling = true;
                    target_app->app_name = new_app_name;
                    target_app->status = app_status::AS_RECALLING;
                    dassert(target_app->helpers->partitions_in_progress.load() == 0,
                            "partition_in_progress_cnt = %d",
                            target_app->helpers->partitions_in_progress.load());
                    target_app->helpers->partitions_in_progress.store(target_app->partition_count);
                    target_app->helpers->pending_response = msg;

                    _exist_apps.emplace(target_app->app_name, target_app);
                }
            }
        }
    }

    if (!do_recalling) {
        _meta_svc->reply_data(msg, response);
        msg->release_ref();
        return;
    }
    do_app_recall(target_app);
}

void server_state::list_apps(const configuration_list_apps_request &request,
                             configuration_list_apps_response &response)
{
    dinfo("list app request, status(%d)", request.status);
    zauto_read_lock l(_lock);
    for (auto &kv : _all_apps) {
        app_state &app = *(kv.second);
        if (request.status == app_status::AS_INVALID || request.status == app.status) {
            response.infos.push_back(app);
        }
    }
    response.err = dsn::ERR_OK;
}

void server_state::send_proposal(rpc_address target, const configuration_update_request &proposal)
{
    ddebug("send proposal %s for gpid(%d.%d), ballot = %" PRId64 ", target = %s, node = %s",
           ::dsn::enum_to_string(proposal.type),
           proposal.config.pid.get_app_id(),
           proposal.config.pid.get_partition_index(),
           proposal.config.ballot,
           target.to_string(),
           proposal.node.to_string());
    dsn::message_ex *msg =
        dsn::message_ex::create_request(RPC_CONFIG_PROPOSAL, 0, proposal.config.pid.thread_hash());
    ::marshall(msg, proposal);
    _meta_svc->send_message(target, msg);
}

void server_state::send_proposal(const configuration_proposal_action &action,
                                 const partition_configuration &pc,
                                 const app_state &app)
{
    configuration_update_request request;
    request.info = app;
    request.type = action.type;
    request.node = action.node;
    request.config = pc;
    send_proposal(action.target, request);
}

void server_state::request_check(const partition_configuration &old,
                                 const configuration_update_request &request)
{
    const partition_configuration &new_config = request.config;

    switch (request.type) {
    case config_type::CT_ASSIGN_PRIMARY:
        dassert(old.primary != request.node,
                "%s VS %s",
                old.primary.to_string(),
                request.node.to_string());
        dassert(std::find(old.secondaries.begin(), old.secondaries.end(), request.node) ==
                    old.secondaries.end(),
                "");
        break;
    case config_type::CT_UPGRADE_TO_PRIMARY:
        dassert(old.primary != request.node,
                "%s VS %s",
                old.primary.to_string(),
                request.node.to_string());
        dassert(std::find(old.secondaries.begin(), old.secondaries.end(), request.node) !=
                    old.secondaries.end(),
                "");
        break;
    case config_type::CT_DOWNGRADE_TO_SECONDARY:
        dassert(old.primary == request.node,
                "%s VS %s",
                old.primary.to_string(),
                request.node.to_string());
        dassert(std::find(old.secondaries.begin(), old.secondaries.end(), request.node) ==
                    old.secondaries.end(),
                "");
        break;
    case config_type::CT_DOWNGRADE_TO_INACTIVE:
    case config_type::CT_REMOVE:
        dassert(old.primary == request.node ||
                    std::find(old.secondaries.begin(), old.secondaries.end(), request.node) !=
                        old.secondaries.end(),
                "");
        break;
    case config_type::CT_UPGRADE_TO_SECONDARY:
        dassert(old.primary != request.node,
                " %s VS %s",
                old.primary.to_string(),
                request.node.to_string());
        dassert(std::find(old.secondaries.begin(), old.secondaries.end(), request.node) ==
                    old.secondaries.end(),
                "");
        break;
    case config_type::CT_PRIMARY_FORCE_UPDATE_BALLOT:
        dassert(old.primary == new_config.primary,
                "%s VS %s",
                old.primary.to_string(),
                new_config.primary.to_string());
        dassert(old.secondaries == new_config.secondaries, "");
        break;
    default:
        break;
    }
}

void server_state::update_configuration_locally(
    app_state &app, std::shared_ptr<configuration_update_request> &config_request)
{
    dsn::gpid &gpid = config_request->config.pid;
    partition_configuration &old_cfg = app.partitions[gpid.get_partition_index()];
    partition_configuration &new_cfg = config_request->config;

    int min_2pc_count = _meta_svc->get_options().mutation_2pc_min_replica_count;
    health_status old_health_status = partition_health_status(old_cfg, min_2pc_count);
    health_status new_health_status = partition_health_status(new_cfg, min_2pc_count);

    if (app.is_stateful) {
        dassert(old_cfg.ballot + 1 == new_cfg.ballot,
                "invalid configuration update request, old ballot %" PRId64 ", new ballot %" PRId64
                "",
                old_cfg.ballot,
                new_cfg.ballot);

        node_state *ns = nullptr;
        if (config_request->type != config_type::CT_DROP_PARTITION) {
            ns = get_node_state(_nodes, config_request->node, false);
            dassert(ns != nullptr,
                    "invalid node address, address = %s",
                    config_request->node.to_string());
        }
#ifndef NDEBUG
        request_check(old_cfg, *config_request);
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

        case config_type::CT_DROP_PARTITION:
            for (const rpc_address &node : new_cfg.last_drops) {
                ns = get_node_state(_nodes, node, false);
                if (ns != nullptr)
                    ns->remove_partition(gpid, false);
            }
            break;

        case config_type::CT_ADD_SECONDARY:
        case config_type::CT_ADD_SECONDARY_FOR_LB:
            dassert(false, "invalid execution work flow");
            break;
        default:
            dassert(false, "");
            break;
        }
    } else {
        dassert(old_cfg.ballot == new_cfg.ballot,
                "invalid ballot, %" PRId64 " VS %" PRId64 "",
                old_cfg.ballot,
                new_cfg.ballot);

        new_cfg = old_cfg;
        partition_configuration_stateless pcs(new_cfg);
        if (config_request->type == config_type::type::CT_ADD_SECONDARY) {
            pcs.hosts().emplace_back(config_request->host_node);
            pcs.workers().emplace_back(config_request->node);
        } else {
            auto it =
                std::remove(pcs.hosts().begin(), pcs.hosts().end(), config_request->host_node);
            pcs.hosts().erase(it);

            it = std::remove(pcs.workers().begin(), pcs.workers().end(), config_request->node);
            pcs.workers().erase(it);
        }

        auto it = _nodes.find(config_request->host_node);
        dassert(it != _nodes.end(),
                "invalid node address, address = %s",
                config_request->host_node.to_string());
        if (config_type::CT_REMOVE == config_request->type) {
            it->second.remove_partition(gpid, false);
        } else {
            it->second.put_partition(gpid, false);
        }
    }

    // we assume config in config_request stores the proper new config
    // as we sync to remote storage according to it
    std::string old_config_str = boost::lexical_cast<std::string>(old_cfg);
    old_cfg = config_request->config;
    auto find_name = _config_type_VALUES_TO_NAMES.find(config_request->type);
    if (find_name != _config_type_VALUES_TO_NAMES.end()) {
        ddebug("meta update config ok: type(%s), old_config=%s, %s",
               find_name->second,
               old_config_str.c_str(),
               boost::lexical_cast<std::string>(*config_request).c_str());
    } else {
        ddebug("meta update config ok: type(%d), old_config=%s, %s",
               config_request->type,
               old_config_str.c_str(),
               boost::lexical_cast<std::string>(*config_request).c_str());
    }

#ifndef NDEBUG
    check_consistency(gpid);
#endif
    if (_config_change_subscriber) {
        _config_change_subscriber(_all_apps);
    }

    _recent_update_config_count->increment();
    if (old_health_status >= HS_WRITABLE_ILL && new_health_status < HS_WRITABLE_ILL) {
        _recent_partition_change_unwritable_count->increment();
    }
    if (old_health_status < HS_WRITABLE_ILL && new_health_status >= HS_WRITABLE_ILL) {
        _recent_partition_change_writable_count->increment();
    }
}

task_ptr server_state::update_configuration_on_remote(
    std::shared_ptr<configuration_update_request> &config_request)
{
    meta_function_level::type l = _meta_svc->get_function_level();
    if (l <= meta_function_level::fl_blind) {
        ddebug("ignore update configuration on remote due to level is %s",
               _meta_function_level_VALUES_TO_NAMES.find(l)->second);
        // NOTICE: pending_sync_task need to be reassigned
        return tasking::enqueue(
            LPC_META_STATE_HIGH,
            nullptr,
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
                  config_request));
}

void server_state::on_update_configuration_on_remote_reply(
    error_code ec, std::shared_ptr<configuration_update_request> &config_request)
{
    zauto_write_lock l(_lock);
    dsn::gpid &gpid = config_request->config.pid;
    std::shared_ptr<app_state> app = get_app(gpid.get_app_id());
    config_context &cc = app->helpers->contexts[gpid.get_partition_index()];

    // if multiple threads exist in the thread pool, the check may be failed
    dassert(app->status == app_status::AS_AVAILABLE || app->status == app_status::AS_DROPPING,
            "if app removed, this task should be cancelled");
    if (ec == ERR_TIMEOUT) {
        cc.pending_sync_task =
            tasking::enqueue(LPC_META_STATE_HIGH,
                             nullptr,
                             [this, config_request, &cc]() mutable {
                                 cc.pending_sync_task =
                                     update_configuration_on_remote(config_request);
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
            _meta_svc->reply_data(cc.msg, resp);
            cc.msg->release_ref();
            cc.msg = nullptr;
        }

        _meta_svc->get_balancer()->reconfig({&_all_apps, &_nodes}, *config_request);
        if (config_request->type == config_type::CT_DROP_PARTITION) {
            process_one_partition(app);
        } else {
            configuration_proposal_action action;
            _meta_svc->get_balancer()->cure({&_all_apps, &_nodes}, gpid, action);
            if (action.type != config_type::CT_INVALID) {
                if (_add_secondary_enable_flow_control &&
                    (action.type == config_type::CT_ADD_SECONDARY ||
                     action.type == config_type::CT_ADD_SECONDARY_FOR_LB)) {
                    // ignore adding secondary if add_secondary_enable_flow_control = true
                } else {
                    config_request->type = action.type;
                    config_request->node = action.node;
                    config_request->info = *app;
                    send_proposal(action.target, *config_request);
                }
            }
        }
    } else {
        dassert(false, "we can't handle this right now, err = %s", ec.to_string());
    }
}

void server_state::recall_partition(std::shared_ptr<app_state> &app, int pidx)
{
    auto on_recall_partition = [this, app, pidx](dsn::error_code error) mutable {
        if (error == dsn::ERR_OK) {
            zauto_write_lock l(_lock);
            app->partitions[pidx].partition_flags &= (~pc_flags::dropped);
            process_one_partition(app);
        } else if (error == dsn::ERR_TIMEOUT) {
            tasking::enqueue(LPC_META_STATE_HIGH,
                             nullptr,
                             std::bind(&server_state::recall_partition, this, app, pidx),
                             server_state::sStateHash,
                             std::chrono::seconds(1));
        } else {
            dassert(false, "unable to handle this(%s) right now", error.to_string());
        }
    };

    partition_configuration &pc = app->partitions[pidx];
    dassert((pc.partition_flags & pc_flags::dropped), "");

    pc.partition_flags = 0;
    blob json_partition = dsn::json::json_forwarder<partition_configuration>::encode(pc);
    std::string partition_path = get_partition_path(pc.pid);
    _meta_svc->get_remote_storage()->set_data(
        partition_path, json_partition, LPC_META_STATE_HIGH, on_recall_partition);
}

void server_state::drop_partition(std::shared_ptr<app_state> &app, int pidx)
{
    partition_configuration &pc = app->partitions[pidx];
    config_context &cc = app->helpers->contexts[pidx];

    std::shared_ptr<configuration_update_request> req =
        std::make_shared<configuration_update_request>();
    configuration_update_request &request = *req;

    request.info = *app;
    request.type = config_type::CT_DROP_PARTITION;
    request.node = pc.primary;

    request.config = pc;
    for (auto &node : pc.secondaries) {
        maintain_drops(request.config.last_drops, node, request.type);
    }
    if (!pc.primary.is_invalid()) {
        maintain_drops(request.config.last_drops, pc.primary, request.type);
    }
    request.config.primary.set_invalid();
    request.config.secondaries.clear();

    dassert((pc.partition_flags & pc_flags::dropped) == 0, "");
    request.config.partition_flags |= pc_flags::dropped;

    // NOTICE this mis-understanding: if a old state is DDD, we may not need to udpate the ballot.
    // Actually it is necessary. Coz we may send a proposal due to the old DDD state
    // and laterly a update_config may arrive.
    // An updated ballot annouces a previous state is INVALID and all actions taken
    // due to the old one should be staled
    request.config.ballot++;

    if (config_status::pending_remote_sync == cc.stage) {
        dwarn("gpid(%d.%d) is syncing another request with remote, cancel it due to partition is "
              "dropped",
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
    partition_configuration &pc = app->partitions[pidx];
    config_context &cc = app->helpers->contexts[pidx];

    if (config_status::pending_remote_sync == cc.stage) {
        if (cc.pending_sync_request->type == config_type::CT_DROP_PARTITION) {
            dassert(app->status == app_status::AS_DROPPING,
                    "app(%s) not in dropping state (%s)",
                    app->get_logname(),
                    enum_to_string(app->status));
            dwarn("stop downgrade primary as the partitions(%d.%d) is dropping", app->app_id, pidx);
            return;
        } else {
            dwarn("gpid(%d.%d) is syncing another request with remote, cancel it due to the "
                  "primary(%s) is down",
                  pc.pid.get_app_id(),
                  pc.pid.get_partition_index(),
                  pc.primary.to_string());
            cc.cancel_sync();
        }
    }

    std::shared_ptr<configuration_update_request> req =
        std::make_shared<configuration_update_request>();
    configuration_update_request &request = *req;
    request.info = *app;
    request.config = pc;
    request.type = config_type::CT_DOWNGRADE_TO_INACTIVE;
    request.node = pc.primary;
    request.config.ballot++;
    request.config.primary.set_invalid();
    maintain_drops(request.config.last_drops, pc.primary, request.type);

    cc.stage = config_status::pending_remote_sync;
    cc.pending_sync_request = req;
    cc.msg = nullptr;

    cc.pending_sync_task = update_configuration_on_remote(req);
}

void server_state::downgrade_secondary_to_inactive(std::shared_ptr<app_state> &app,
                                                   int pidx,
                                                   const rpc_address &node)
{
    partition_configuration &pc = app->partitions[pidx];
    config_context &cc = app->helpers->contexts[pidx];

    dassert(!pc.primary.is_invalid(), "this shouldn't be called if the primary is invalid");
    if (config_status::pending_remote_sync != cc.stage) {
        configuration_update_request request;
        request.info = *app;
        request.config = pc;
        request.type = config_type::CT_DOWNGRADE_TO_INACTIVE;
        request.node = node;
        send_proposal(pc.primary, request);
    } else {
        ddebug("gpid(%d.%d) is syncing with remote storage, ignore the remove seconary(%s)",
               app->app_id,
               pidx,
               node.to_string());
    }
}

void server_state::downgrade_stateless_nodes(std::shared_ptr<app_state> &app,
                                             int pidx,
                                             const rpc_address &address)
{
    std::shared_ptr<configuration_update_request> req =
        std::make_shared<configuration_update_request>();
    req->info = *app;
    req->type = config_type::CT_REMOVE;
    req->host_node = address;
    req->node.set_invalid();
    req->config = app->partitions[pidx];

    config_context &cc = app->helpers->contexts[pidx];
    partition_configuration &pc = req->config;

    unsigned i = 0;
    for (; i < pc.secondaries.size(); ++i) {
        if (pc.secondaries[i] == address) {
            req->node = pc.last_drops[i];
            break;
        }
    }
    dassert(!req->node.is_invalid(), "invalid node address, address = %s", req->node.to_string());
    // remove host_node & node from secondaries/last_drops, as it will be sync to remote storage
    for (++i; i < pc.secondaries.size(); ++i) {
        pc.secondaries[i - 1] = pc.secondaries[i];
        pc.last_drops[i - 1] = pc.last_drops[i];
    }
    pc.secondaries.pop_back();
    pc.last_drops.pop_back();

    if (config_status::pending_remote_sync == cc.stage) {
        dwarn("gpid(%d.%d) is syncing another request with remote, cancel it due to meta is "
              "removing host(%s) worker(%s)",
              pc.pid.get_app_id(),
              pc.pid.get_partition_index(),
              req->host_node.to_string(),
              req->node.to_string());
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
    partition_configuration &pc = app->partitions[gpid.get_partition_index()];
    config_context &cc = app->helpers->contexts[gpid.get_partition_index()];
    configuration_update_response response;
    response.err = ERR_IO_PENDING;

    dassert(app != nullptr, "get get app for app id(%d)", gpid.get_app_id());
    dassert(app->is_stateful, "don't support stateless apps currently, id(%d)", gpid.get_app_id());
    auto find_name = _config_type_VALUES_TO_NAMES.find(cfg_request->type);
    if (find_name != _config_type_VALUES_TO_NAMES.end()) {
        ddebug("recv update config request: type(%s), %s",
               find_name->second,
               boost::lexical_cast<std::string>(*cfg_request).c_str());
    } else {
        ddebug("recv update config request: type(%d), %s",
               cfg_request->type,
               boost::lexical_cast<std::string>(*cfg_request).c_str());
    }

    if (is_partition_config_equal(pc, cfg_request->config)) {
        ddebug("duplicated update request for gpid(%d.%d), ballot: %" PRId64 "",
               gpid.get_app_id(),
               gpid.get_partition_index(),
               pc.ballot);
        response.err = ERR_OK;
        //
        // NOTICE:
        //    if a replica server resend a update-request,
        //    the meta has update the last_drops, and we should reply with new last_drops
        //
        response.config = pc;
    } else if (pc.ballot + 1 != cfg_request->config.ballot) {
        ddebug("update configuration for gpid(%d.%d) reject coz ballot not match, request ballot: "
               "%" PRId64 ", meta ballot: %" PRId64 "",
               gpid.get_app_id(),
               gpid.get_partition_index(),
               cfg_request->config.ballot,
               pc.ballot);
        response.err = ERR_INVALID_VERSION;
        response.config = pc;
    } else if (config_status::pending_remote_sync == cc.stage) {
        ddebug("another request is syncing with remote storage, ignore current request, "
               "gpid(%d.%d), request ballot(%" PRId64 ")",
               gpid.get_app_id(),
               gpid.get_partition_index(),
               cfg_request->config.ballot);
        // we don't reply the replica server, expect it to retry
        msg->release_ref();
        return;
    } else {
        maintain_drops(cfg_request->config.last_drops, cfg_request->node, cfg_request->type);
    }

    if (response.err != ERR_IO_PENDING) {
        _meta_svc->reply_data(msg, response);
        msg->release_ref();
    } else {
        dassert(config_status::not_pending == cc.stage ||
                    config_status::pending_proposal == cc.stage,
                "invalid config status, cc.stage = %s",
                enum_to_string(cc.stage));
        cc.stage = config_status::pending_remote_sync;
        cc.pending_sync_request = cfg_request;
        cc.msg = msg;
        cc.pending_sync_task = update_configuration_on_remote(cfg_request);
    }
}

void server_state::on_partition_node_dead(std::shared_ptr<app_state> &app,
                                          int pidx,
                                          const dsn::rpc_address &address)
{
    partition_configuration &pc = app->partitions[pidx];
    if (app->is_stateful) {
        if (is_primary(pc, address))
            downgrade_primary_to_inactive(app, pidx);
        else if (is_secondary(pc, address)) {
            if (!pc.primary.is_invalid())
                downgrade_secondary_to_inactive(app, pidx, address);
            else if (is_secondary(pc, address)) {
                ddebug("gpid(%d.%d): secondary(%s) is down, ignored it due to no primary for this "
                       "partition available",
                       pc.pid.get_app_id(),
                       pc.pid.get_partition_index(),
                       address.to_string());
            } else {
                dassert(false,
                        "no primary/secondary on this node, node address = %s",
                        address.to_string());
            }
        }
    } else {
        downgrade_stateless_nodes(app, pidx, address);
    }
}

void server_state::on_change_node_state(rpc_address node, bool is_alive)
{
    dinfo("change node(%s) state to %s", node.to_string(), is_alive ? "alive" : "dead");
    zauto_write_lock l(_lock);
    if (!is_alive) {
        auto iter = _nodes.find(node);
        if (iter == _nodes.end()) {
            ddebug("node(%s) doesn't exist in the node state, just ignore", node.to_string());
        } else {
            node_state &ns = iter->second;
            ns.set_alive(false);
            ns.set_replicas_collect_flag(false);
            ns.for_each_partition([&, this](const dsn::gpid &pid) {
                std::shared_ptr<app_state> app = get_app(pid.get_app_id());
                dassert(app != nullptr && app->status != app_status::AS_DROPPED,
                        "invalid app, app_id = %d",
                        pid.get_app_id());
                on_partition_node_dead(app, pid.get_partition_index(), node);
                return true;
            });
        }
    } else {
        get_node_state(_nodes, node, true)->set_alive(true);
    }
}

void server_state::on_propose_balancer(const configuration_balancer_request &request,
                                       configuration_balancer_response &response)
{
    zauto_write_lock l(_lock);
    std::shared_ptr<app_state> app = get_app(request.gpid.get_app_id());
    if (app == nullptr || app->status != app_status::AS_AVAILABLE ||
        request.gpid.get_partition_index() < 0 ||
        request.gpid.get_partition_index() >= app->partition_count)
        response.err = ERR_INVALID_PARAMETERS;
    else {
        if (request.force) {
            partition_configuration &pc = *get_config(_all_apps, request.gpid);
            for (const configuration_proposal_action &act : request.action_list) {
                send_proposal(act, pc, *app);
            }
            response.err = ERR_OK;
        } else {
            _meta_svc->get_balancer()->register_proposals({&_all_apps, &_nodes}, request, response);
        }
    }
}

error_code
server_state::construct_apps(const std::vector<query_app_info_response> &query_app_responses,
                             const std::vector<dsn::rpc_address> &replica_nodes,
                             std::string &hint_message)
{
    int max_app_id = 0;
    for (unsigned int i = 0; i < query_app_responses.size(); ++i) {
        query_app_info_response query_resp = query_app_responses[i];
        if (query_resp.err != dsn::ERR_OK)
            continue;

        for (const app_info &info : query_resp.apps) {
            dassert(info.app_id >= 1, "invalid app_id, app_id = %d", info.app_id);
            auto iter = _all_apps.find(info.app_id);
            if (iter == _all_apps.end()) {
                std::shared_ptr<app_state> app = app_state::create(info);
                ddebug("create app info from (%s) for id(%d): %s",
                       replica_nodes[i].to_string(),
                       info.app_id,
                       boost::lexical_cast<std::string>(info).c_str());
                _all_apps.emplace(app->app_id, app);
                max_app_id = std::max(app->app_id, max_app_id);
            } else {
                app_info *old_info = iter->second.get();
                // all info in all replica servers should be the same
                // coz the app info is only initialized when the replica is
                // created, and it will NEVER change even if the app is dropped/recalled...
                if (info != *old_info) // app_info::operator !=
                {
                    dassert(false,
                            "conflict app info from (%s) for id(%d): new_info(%s), old_info(%s)",
                            replica_nodes[i].to_string(),
                            info.app_id,
                            boost::lexical_cast<std::string>(info).c_str(),
                            boost::lexical_cast<std::string>(*old_info).c_str());
                }
            }
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
        dassert(_all_apps.find(app_id) != _all_apps.end(), "invalid app_id, app_id = %d", app_id);
        std::shared_ptr<app_state> &app = _all_apps[app_id];
        std::string old_name = app->app_name;
        while (checked_names.find(app->app_name) != checked_names.end()) {
            app->app_name = app->app_name + "__" + boost::lexical_cast<std::string>(app_id);
        }
        if (app->app_name != old_name) {
            dwarn("app(%d)'s old name(%s) is conflict with others, rename it to (%s)",
                  app_id,
                  old_name.c_str(),
                  app->app_name.c_str());
            std::ostringstream oss;
            oss << "WARNING: app(" << app_id << ")'s old name(" << old_name
                << ") is conflict with others, rename it to (" << app->app_name << ")" << std::endl;
            hint_message += oss.str();
        }
        checked_names.emplace(app->app_name, app_id);
    }

    ddebug("construct apps done, max_app_id = %d", max_app_id);

    return dsn::ERR_OK;
}

error_code server_state::construct_partitions(
    const std::vector<query_replica_info_response> &query_replica_responses,
    const std::vector<dsn::rpc_address> &replica_nodes,
    bool skip_lost_partitions,
    std::string &hint_message)
{
    for (unsigned int i = 0; i < query_replica_responses.size(); ++i) {
        query_replica_info_response query_resp = query_replica_responses[i];
        if (query_resp.err != dsn::ERR_OK)
            continue;

        for (replica_info &r : query_resp.replicas) {
            dassert(_all_apps.find(r.pid.get_app_id()) != _all_apps.end(), "");
            bool is_accepted = _meta_svc->get_balancer()->collect_replica(
                {&_all_apps, &_nodes}, replica_nodes[i], r);
            if (is_accepted) {
                ddebug("accept replica(%s) from node(%s)",
                       boost::lexical_cast<std::string>(r).c_str(),
                       replica_nodes[i].to_string());
            } else {
                ddebug("ignore replica(%s) from node(%s)",
                       boost::lexical_cast<std::string>(r).c_str(),
                       replica_nodes[i].to_string());
            }
        }
    }

    int succeed_count = 0;
    int failed_count = 0;
    for (auto &app_kv : _all_apps) {
        std::shared_ptr<app_state> &app = app_kv.second;
        dassert(app->status == app_status::AS_CREATING || app->status == app_status::AS_DROPPING,
                "invalid app status, status = %s",
                enum_to_string(app->status));
        if (app->status == app_status::AS_DROPPING) {
            ddebug("ignore constructing partitions for dropping app(%d)", app->app_id);
        } else {
            for (partition_configuration &pc : app->partitions) {
                bool is_succeed = _meta_svc->get_balancer()->construct_replica(
                    {&_all_apps, &_nodes}, pc.pid, app->max_replica_count);
                if (is_succeed) {
                    ddebug("construct partition(%d.%d) succeed: %s",
                           app->app_id,
                           pc.pid.get_partition_index(),
                           boost::lexical_cast<std::string>(pc).c_str());
                    if (pc.last_drops.size() + 1 < pc.max_replica_count) {
                        std::ostringstream oss;
                        oss << "WARNING: partition(" << app->app_id << "."
                            << pc.pid.get_partition_index() << ") only collects "
                            << (pc.last_drops.size() + 1) << "/" << pc.max_replica_count
                            << " of replicas, may lost data" << std::endl;
                        hint_message += oss.str();
                    }
                    succeed_count++;
                } else {
                    dwarn("construct partition(%d.%d) failed",
                          app->app_id,
                          pc.pid.get_partition_index());
                    std::ostringstream oss;
                    if (skip_lost_partitions) {
                        oss << "WARNING: partition(" << app->app_id << "."
                            << pc.pid.get_partition_index() << ") has no replica collected, force "
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

    ddebug("construct partition done, succeed_count = %d, failed_count = %d, skip_lost_partitions "
           "= %s",
           succeed_count,
           failed_count,
           (skip_lost_partitions ? "true" : "false"));

    if (failed_count > 0 && !skip_lost_partitions) {
        return dsn::ERR_TRY_AGAIN;
    } else {
        return dsn::ERR_OK;
    }
}

dsn::error_code
server_state::sync_apps_from_replica_nodes(const std::vector<dsn::rpc_address> &replica_nodes,
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
        ddebug("send query app and replica request to node(%s)", replica_nodes[i].to_string());

        query_app_info_request app_query;
        app_query.meta_server = dsn_primary_address();

        rpc::call(replica_nodes[i],
                  RPC_QUERY_APP_INFO,
                  app_query,
                  &tracker,
                  [i, &replica_nodes, &query_app_errors, &query_app_responses](
                      dsn::error_code err, query_app_info_response &&resp) mutable {
                      ddebug("received query app response from node(%s), err(%s), apps_count(%d)",
                             replica_nodes[i].to_string(),
                             err.to_string(),
                             (int)resp.apps.size());
                      query_app_errors[i] = err;
                      if (err == dsn::ERR_OK) {
                          query_app_responses[i] = std::move(resp);
                      }
                  });

        query_replica_info_request replica_query;
        replica_query.node = replica_nodes[i];
        rpc::call(
            replica_nodes[i],
            RPC_QUERY_REPLICA_INFO,
            replica_query,
            &tracker,
            [i, &replica_nodes, &query_replica_errors, &query_replica_responses](
                dsn::error_code err, query_replica_info_response &&resp) mutable {
                ddebug("received query replica response from node(%s), err(%s), replicas_count(%d)",
                       replica_nodes[i].to_string(),
                       err.to_string(),
                       (int)resp.replicas.size());
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
            dwarn("query app info from node(%s) failed, reason: %s",
                  replica_nodes[i].to_string(),
                  query_app_errors[i].to_string());
            err = query_app_errors[i];
        }
        if (query_replica_errors[i] != dsn::ERR_OK) {
            dwarn("query replica info from node(%s) failed, reason: %s",
                  replica_nodes[i].to_string(),
                  query_replica_errors[i].to_string());
            err = query_replica_errors[i];
        }
        if (err != dsn::ERR_OK) {
            failed_count++;
            query_app_errors[i] = err;
            query_replica_errors[i] = err;
            std::ostringstream oss;
            if (skip_bad_nodes) {
                oss << "WARNING: collect app and replica info from node("
                    << replica_nodes[i].to_string() << ") failed with err(" << err.to_string()
                    << "), skip the bad node" << std::endl;
            } else {
                oss << "ERROR: collect app and replica info from node("
                    << replica_nodes[i].to_string() << ") failed with err(" << err.to_string()
                    << "), you can skip it by set skip_bad_nodes option" << std::endl;
            }
            hint_message += oss.str();
        } else {
            succeed_count++;
        }
    }

    ddebug("sync apps and replicas from replica nodes done, succeed_count = %d, failed_count = %d, "
           "skip_bad_nodes = %s",
           succeed_count,
           failed_count,
           (skip_bad_nodes ? "true" : "false"));

    if (failed_count > 0 && !skip_bad_nodes) {
        return dsn::ERR_TRY_AGAIN;
    }

    zauto_write_lock l(_lock);

    dsn::error_code err = construct_apps(query_app_responses, replica_nodes, hint_message);
    if (err != dsn::ERR_OK) {
        derror("construct apps failed, err = %s", err.to_string());
        return err;
    }

    err = construct_partitions(
        query_replica_responses, replica_nodes, skip_lost_partitions, hint_message);
    if (err != dsn::ERR_OK) {
        derror("construct partitions failed, err = %s", err.to_string());
        return err;
    }

    return dsn::ERR_OK;
}

void server_state::on_start_recovery(const configuration_recovery_request &req,
                                     configuration_recovery_response &resp)
{
    ddebug("start recovery, node_count = %d, skip_bad_nodes = %s, skip_lost_partitions = %s",
           (int)req.recovery_set.size(),
           req.skip_bad_nodes ? "true" : "false",
           req.skip_lost_partitions ? "true" : "false");

    resp.err = sync_apps_from_replica_nodes(
        req.recovery_set, req.skip_bad_nodes, req.skip_lost_partitions, resp.hint_message);
    if (resp.err != dsn::ERR_OK) {
        derror("sync apps from replica nodes failed when do recovery, err = %s",
               resp.err.to_string());
        _all_apps.clear();
        return;
    }

    resp.err = sync_apps_to_remote_storage();
    if (resp.err != dsn::ERR_OK) {
        dassert(false,
                "sync apps to remote storage failed when do recovery, err = %s, "
                "need to manually clear things from remote storage and restart the service",
                resp.err.to_string());
    }

    initialize_node_state();
}

void server_state::clear_proposals()
{
    ddebug("clear all exist proposals");
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
                ddebug("don't do replica migration coz dead node(%s) has %d partitions not removed",
                       iter->second.addr().to_string(),
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
        ddebug("don't do replica migration coz %d table(s) is(are) in staging state", c);
        return false;
    }
    return true;
}

void server_state::update_partition_perf_counter()
{
    int counters[HS_MAX_VALUE];
    ::memset(counters, 0, sizeof(counters));
    int min_2pc_count = _meta_svc->get_options().mutation_2pc_min_replica_count;
    auto func = [&](const std::shared_ptr<app_state> &app) {
        for (unsigned int i = 0; i != app->partition_count; ++i) {
            health_status st = partition_health_status(app->partitions[i], min_2pc_count);
            counters[st]++;
        }
        return true;
    };
    for_each_available_app(_all_apps, func);
    _dead_partition_count->set(counters[HS_DEAD]);
    _unreadable_partition_count->set(counters[HS_UNREADABLE]);
    _unwritable_partition_count->set(counters[HS_UNWRITABLE]);
    _writable_ill_partition_count->set(counters[HS_WRITABLE_ILL]);
    _healthy_partition_count->set(counters[HS_HEALTHY]);
}

bool server_state::check_all_partitions()
{
    int healthy_partitions = 0;
    int total_partitions = 0;
    meta_function_level::type level = _meta_svc->get_function_level();

    zauto_write_lock l(_lock);

    update_partition_perf_counter();

    // first the cure stage
    if (level <= meta_function_level::fl_freezed) {
        ddebug("service is in level(%s), don't do any cure or balancer actions",
               _meta_function_level_VALUES_TO_NAMES.find(level)->second);
        return false;
    }
    ddebug("start to check all partitions, add_secondary_enable_flow_control = %s, "
           "add_secondary_max_count_for_one_node = %d",
           _add_secondary_enable_flow_control ? "true" : "false",
           _add_secondary_max_count_for_one_node);
    _meta_svc->get_balancer()->clear_ddd_partitions();
    int send_proposal_count = 0;
    std::vector<configuration_proposal_action> add_secondary_actions;
    std::vector<gpid> add_secondary_gpids;
    std::vector<bool> add_secondary_proposed;
    std::map<rpc_address, int> add_secondary_running_nodes; // node --> running_count
    for (auto &app_pair : _exist_apps) {
        std::shared_ptr<app_state> &app = app_pair.second;
        if (app->status == app_status::AS_CREATING || app->status == app_status::AS_DROPPING) {
            ddebug("ignore app(%s)(%d) because it's status is %s",
                   app->app_name.c_str(),
                   app->app_id,
                   ::dsn::enum_to_string(app->status));
            continue;
        }
        for (unsigned int i = 0; i != app->partition_count; ++i) {
            partition_configuration &pc = app->partitions[i];
            config_context &cc = app->helpers->contexts[i];

            if (cc.stage != config_status::pending_remote_sync) {
                configuration_proposal_action action;
                pc_status s =
                    _meta_svc->get_balancer()->cure({&_all_apps, &_nodes}, pc.pid, action);
                dinfo("gpid(%d.%d) is in status(%s)",
                      pc.pid.get_app_id(),
                      pc.pid.get_partition_index(),
                      enum_to_string(s));
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
                ddebug("ignore gpid(%d.%d) as it's stage is pending_remote_sync",
                       pc.pid.get_app_id(),
                       pc.pid.get_partition_index());
            }
        }
        total_partitions += app->partition_count;
    }

    // assign secondary for urgent
    for (int i = 0; i < add_secondary_actions.size(); ++i) {
        gpid &pid = add_secondary_gpids[i];
        partition_configuration &pc = *get_config(_all_apps, pid);
        if (!add_secondary_proposed[i] && pc.secondaries.empty()) {
            configuration_proposal_action &action = add_secondary_actions[i];
            if (_add_secondary_enable_flow_control &&
                add_secondary_running_nodes[action.node] >= _add_secondary_max_count_for_one_node) {
                // ignore
                continue;
            }
            std::shared_ptr<app_state> app = get_app(pid.get_app_id());
            send_proposal(action, pc, *app);
            send_proposal_count++;
            add_secondary_proposed[i] = true;
            add_secondary_running_nodes[action.node]++;
        }
    }

    // assign secondary for all
    for (int i = 0; i < add_secondary_actions.size(); ++i) {
        if (!add_secondary_proposed[i]) {
            configuration_proposal_action &action = add_secondary_actions[i];
            gpid pid = add_secondary_gpids[i];
            partition_configuration &pc = *get_config(_all_apps, pid);
            if (_add_secondary_enable_flow_control &&
                add_secondary_running_nodes[action.node] >= _add_secondary_max_count_for_one_node) {
                ddebug("do not send %s proposal for gpid(%d.%d) for flow control reason, target = "
                       "%s, node = %s",
                       ::dsn::enum_to_string(action.type),
                       pc.pid.get_app_id(),
                       pc.pid.get_partition_index(),
                       action.target.to_string(),
                       action.node.to_string());
                continue;
            }
            std::shared_ptr<app_state> app = get_app(pid.get_app_id());
            send_proposal(action, pc, *app);
            send_proposal_count++;
            add_secondary_proposed[i] = true;
            add_secondary_running_nodes[action.node]++;
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

    ddebug("check all partitions done, send_proposal_count = %d, add_secondary_count = %d, "
           "ignored_add_secondary_count = %d",
           send_proposal_count,
           add_secondary_count,
           ignored_add_secondary_count);

    // then the balancer stage
    if (level < meta_function_level::fl_steady) {
        ddebug("don't do replica migration coz meta server is in level(%s)",
               _meta_function_level_VALUES_TO_NAMES.find(level)->second);
        return false;
    }

    if (healthy_partitions != total_partitions) {
        ddebug("don't do replica migration coz %d of %d partitions aren't healthy",
               total_partitions - healthy_partitions,
               total_partitions);
        return false;
    }

    if (!can_run_balancer()) {
        ddebug("don't do replica migration coz can_run_balancer() returns false");
        return false;
    }

    if (level == meta_function_level::fl_steady) {
        ddebug("check if any replica migration can be done when meta server is in level(%s)",
               _meta_function_level_VALUES_TO_NAMES.find(level)->second);
        _meta_svc->get_balancer()->check({&_all_apps, &_nodes}, _temporary_list);
        ddebug("balance checker operation count = %d", _temporary_list.size());
        // update balance checker operation count
        _meta_svc->get_balancer()->report(_temporary_list, true);
        return false;
    }

    if (_meta_svc->get_balancer()->balance({&_all_apps, &_nodes}, _temporary_list)) {
        ddebug("try to do replica migration");
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

    ddebug("check if any replica migration left");
    _meta_svc->get_balancer()->check({&_all_apps, &_nodes}, _temporary_list);
    ddebug("balance checker operation count = %d", _temporary_list.size());
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
    dassert(iter != _all_apps.end(),
            "invalid gpid(%d.%d)",
            gpid.get_app_id(),
            gpid.get_partition_index());

    app_state &app = *(iter->second);
    partition_configuration &config = app.partitions[gpid.get_partition_index()];

    if (app.is_stateful) {
        if (config.primary.is_invalid() == false) {
            auto it = _nodes.find(config.primary);
            dassert(it != _nodes.end(),
                    "invalid primary address, address = %s",
                    config.primary.to_string());
            dassert(it->second.served_as(gpid) == partition_status::PS_PRIMARY,
                    "node should serve as PS_PRIMARY, but status = %s",
                    dsn::enum_to_string(it->second.served_as(gpid)));

            auto it2 =
                std::find(config.last_drops.begin(), config.last_drops.end(), config.primary);
            dassert(it2 == config.last_drops.end(),
                    "primary shouldn't appear in last_drops, address = %s",
                    config.primary.to_string());
        }

        for (auto &ep : config.secondaries) {
            auto it = _nodes.find(ep);
            dassert(it != _nodes.end(), "invalid secondary address, address = %s", ep.to_string());
            dassert(it->second.served_as(gpid) == partition_status::PS_SECONDARY,
                    "node should serve as PS_SECONDARY, but status = %s",
                    dsn::enum_to_string(it->second.served_as(gpid)));

            auto it2 = std::find(config.last_drops.begin(), config.last_drops.end(), ep);
            dassert(it2 == config.last_drops.end(),
                    "secondary shouldn't appear in last_drops, address = %s",
                    ep.to_string());
        }
    } else {
        partition_configuration_stateless pcs(config);
        dassert(pcs.hosts().size() == pcs.workers().size(),
                "%d VS %d",
                pcs.hosts().size(),
                pcs.workers().size());
        for (auto &ep : pcs.hosts()) {
            auto it = _nodes.find(ep);
            dassert(it != _nodes.end(), "invalid host, address = %s", ep.to_string());
            dassert(it->second.served_as(gpid) == partition_status::PS_SECONDARY,
                    "node should serve as PS_SECONDARY, but status = %s",
                    dsn::enum_to_string(it->second.served_as(gpid)));
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
    auto new_cb = [ this, app_path, info, user_cb = std::move(cb) ](error_code ec)
    {
        if (ec == ERR_OK) {
            user_cb(ec);
        } else if (ec == ERR_TIMEOUT) {
            dwarn("update app_info(app = %s) to remote storage timeout, continue to update later",
                  info.app_name.c_str());
            tasking::enqueue(
                LPC_META_STATE_NORMAL,
                tracker(),
                std::bind(
                    &server_state::do_update_app_info, this, app_path, info, std::move(user_cb)),
                0,
                std::chrono::seconds(1));
        } else {
            dassert(false, "we can't handle this, error(%s)", ec.to_string());
        }
    };
    // TODO(cailiuyang): callback scheduling order may be undefined if multiple requests are
    // sending to the remote storage concurrently.
    _meta_svc->get_remote_storage()->set_data(
        app_path, value, LPC_META_STATE_NORMAL, std::move(new_cb), tracker());
}

void server_state::set_app_envs(const app_env_rpc &env_rpc)
{
    const configuration_update_app_env_request &request = env_rpc.request();
    if (!request.__isset.keys || !request.__isset.values ||
        request.keys.size() != request.values.size() || request.keys.size() <= 0) {
        env_rpc.response().err = ERR_INVALID_PARAMETERS;
        dwarn("set app envs failed with invalid request");
        return;
    }
    const std::vector<std::string> &keys = request.keys;
    const std::vector<std::string> &values = request.values;
    const std::string &app_name = request.app_name;

    std::ostringstream os;
    for (int i = 0; i < keys.size(); i++) {
        if (i != 0)
            os << ", ";
        os << keys[i] << "=" << values[i];
    }
    ddebug("set app envs for app(%s) from remote(%s): kvs = {%s}",
           app_name.c_str(),
           env_rpc.remote_address().to_string(),
           os.str().c_str());

    app_info ainfo;
    std::string app_path;
    {
        zauto_read_lock l(_lock);
        std::shared_ptr<app_state> app = get_app(app_name);
        if (app == nullptr) {
            dwarn("set app envs failed with invalid app_name(%s)", app_name.c_str());
            env_rpc.response().err = ERR_INVALID_PARAMETERS;
            env_rpc.response().hint_message = "invalid app name";
            return;
        } else {
            ainfo = *(reinterpret_cast<app_info *>(app.get()));
            app_path = get_app_path(*app);
        }
    }
    for (int idx = 0; idx < keys.size(); idx++) {
        ainfo.envs[keys[idx]] = values[idx];
    }
    do_update_app_info(app_path, ainfo, [this, app_name, keys, values, env_rpc](error_code ec) {
        dassert(
            ec == ERR_OK, "update app_info to remote storage failed with err = %s", ec.to_string());

        zauto_write_lock l(_lock);
        std::shared_ptr<app_state> app = get_app(app_name);
        std::string old_envs = dsn::utils::kv_map_to_string(app->envs, ',', '=');
        for (int idx = 0; idx < keys.size(); idx++) {
            app->envs[keys[idx]] = values[idx];
        }
        std::string new_envs = dsn::utils::kv_map_to_string(app->envs, ',', '=');
        ddebug("app envs changed: old_envs = {%s}, new_envs = {%s}",
               old_envs.c_str(),
               new_envs.c_str());
    });
}

void server_state::del_app_envs(const app_env_rpc &env_rpc)
{
    const configuration_update_app_env_request &request = env_rpc.request();
    if (!request.__isset.keys || request.keys.size() <= 0) {
        env_rpc.response().err = ERR_INVALID_PARAMETERS;
        dwarn("del app envs failed with invalid request");
        return;
    }
    const std::vector<std::string> &keys = request.keys;
    const std::string &app_name = request.app_name;

    std::ostringstream os;
    for (int i = 0; i < keys.size(); i++) {
        if (i != 0)
            os << ",";
        os << keys[i];
    }
    ddebug("del app envs for app(%s) from remote(%s): keys = {%s}",
           app_name.c_str(),
           env_rpc.remote_address().to_string(),
           os.str().c_str());

    app_info ainfo;
    std::string app_path;
    {
        zauto_read_lock l(_lock);
        std::shared_ptr<app_state> app = get_app(app_name);
        if (app == nullptr) {
            dwarn("del app envs failed with invalid app_name(%s)", app_name.c_str());
            env_rpc.response().err = ERR_INVALID_PARAMETERS;
            env_rpc.response().hint_message = "invalid app name";
            return;
        } else {
            ainfo = *(reinterpret_cast<app_info *>(app.get()));
            app_path = get_app_path(*app);
        }
    }

    std::ostringstream oss;
    oss << "deleted keys:";
    int deleted = 0;
    for (const auto &key : keys) {
        if (ainfo.envs.erase(key) > 0) {
            oss << std::endl << "    " << key;
            deleted++;
        }
    }

    if (deleted == 0) {
        ddebug("no key need to delete");
        env_rpc.response().hint_message = "no key need to delete";
        return;
    } else {
        env_rpc.response().hint_message = oss.str();
    }

    do_update_app_info(app_path, ainfo, [this, app_name, keys, env_rpc](error_code ec) {
        dassert(
            ec == ERR_OK, "update app_info to remote storage failed with err = %s", ec.to_string());

        zauto_write_lock l(_lock);
        std::shared_ptr<app_state> app = get_app(app_name);
        std::string old_envs = dsn::utils::kv_map_to_string(app->envs, ',', '=');
        for (const auto &key : keys) {
            app->envs.erase(key);
        }
        std::string new_envs = dsn::utils::kv_map_to_string(app->envs, ',', '=');
        ddebug("app envs changed: old_envs = {%s}, new_envs = {%s}",
               old_envs.c_str(),
               new_envs.c_str());
    });
}

void server_state::clear_app_envs(const app_env_rpc &env_rpc)
{
    const configuration_update_app_env_request &request = env_rpc.request();
    if (!request.__isset.clear_prefix) {
        env_rpc.response().err = ERR_INVALID_PARAMETERS;
        dwarn("clear app envs failed with invalid request");
        return;
    }

    const std::string &prefix = request.clear_prefix;
    const std::string &app_name = request.app_name;
    ddebug("clear app envs for app(%s) from remote(%s): prefix = {%s}",
           app_name.c_str(),
           env_rpc.remote_address().to_string(),
           prefix.c_str());

    app_info ainfo;
    std::string app_path;
    {
        zauto_read_lock l(_lock);
        std::shared_ptr<app_state> app = get_app(app_name);
        if (app == nullptr) {
            dwarn("clear app envs failed with invalid app_name(%s)", app_name.c_str());
            env_rpc.response().err = ERR_INVALID_PARAMETERS;
            env_rpc.response().hint_message = "invalid app name";
            return;
        } else {
            ainfo = *(reinterpret_cast<app_info *>(app.get()));
            app_path = get_app_path(*app);
        }
    }

    if (ainfo.envs.empty()) {
        ddebug("no key need to delete");
        env_rpc.response().hint_message = "no key need to delete";
        return;
    }

    std::set<std::string> erase_keys;
    std::ostringstream oss;
    oss << "deleted keys:";

    if (prefix.empty()) {
        // ignore prefix
        for (auto &kv : ainfo.envs) {
            oss << std::endl << "    " << kv.first;
        }
        ainfo.envs.clear();
    } else {
        // acquire key
        for (const auto &pair : ainfo.envs) {
            const std::string &key = pair.first;
            // normal : key = prefix.xxx
            if (key.size() > prefix.size() + 1) {
                if (key.substr(0, prefix.size()) == prefix && key.at(prefix.size()) == '.') {
                    erase_keys.emplace(key);
                }
            }
        }
        // erase
        for (const auto &key : erase_keys) {
            oss << std::endl << "    " << key;
            ainfo.envs.erase(key);
        }
    }

    if (!prefix.empty() && erase_keys.empty()) {
        // no need update app_info
        ddebug("no key need to delete");
        env_rpc.response().hint_message = "no key need to delete";
        return;
    } else {
        env_rpc.response().hint_message = oss.str();
    }

    do_update_app_info(
        app_path, ainfo, [this, app_name, prefix, erase_keys, env_rpc](error_code ec) {
            dassert(ec == ERR_OK,
                    "update app_info to remote storage failed with err = %s",
                    ec.to_string());

            zauto_write_lock l(_lock);
            std::shared_ptr<app_state> app = get_app(app_name);
            std::string old_envs = dsn::utils::kv_map_to_string(app->envs, ',', '=');
            if (prefix.empty()) {
                app->envs.clear();
            } else {
                for (const auto &key : erase_keys) {
                    app->envs.erase(key);
                }
            }
            std::string new_envs = dsn::utils::kv_map_to_string(app->envs, ',', '=');
            ddebug("app envs changed: old_envs = {%s}, new_envs = {%s}",
                   old_envs.c_str(),
                   new_envs.c_str());
        });
}
}
}
