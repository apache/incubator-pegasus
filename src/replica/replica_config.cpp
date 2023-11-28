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
#include <boost/lexical_cast.hpp>
#include <fmt/format.h>
#include <stdint.h>
#include <algorithm>
#include <chrono>
#include <ios>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bulk_load/replica_bulk_loader.h"
#include "common/gpid.h"
#include "common/replica_envs.h"
#include "common/replication.codes.h"
#include "common/replication_common.h"
#include "common/replication_enums.h"
#include "common/replication_other_types.h"
#include "consensus_types.h"
#include "dsn.layer2_types.h"
#include "failure_detector/failure_detector_multimaster.h"
#include "meta_admin_types.h"
#include "metadata_types.h"
#include "mutation.h"
#include "replica.h"
#include "replica/prepare_list.h"
#include "replica/replica_context.h"
#include "replica/replication_app_base.h"
#include "replica_stub.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/rpc/serialization.h"
#include "runtime/security/access_controller.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task.h"
#include "split/replica_split_manager.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"
#include "utils/fmt_logging.h"
#include "utils/string_conv.h"
#include "absl/strings/string_view.h"
#include "utils/strings.h"
#include "utils/thread_access_checker.h"

namespace dsn {
namespace replication {

// The configuration management part of replica.

bool get_bool_envs(const std::map<std::string, std::string> &envs,
                   const std::string &name,
                   bool &value)
{
    auto iter = envs.find(name);
    if (iter != envs.end()) {
        if (!buf2bool(iter->second, value)) {
            return false;
        }
    }
    return true;
}

void replica::on_config_proposal(configuration_update_request &proposal)
{
    _checker.only_one_thread_access();

    LOG_INFO_PREFIX(
        "process config proposal {} for {}", enum_to_string(proposal.type), proposal.node);

    if (proposal.config.ballot < get_ballot()) {
        LOG_WARNING_PREFIX(
            "on_config_proposal out-dated, {} vs {}", proposal.config.ballot, get_ballot());
        return;
    }

    if (_primary_states.reconfiguration_task != nullptr) {
        LOG_DEBUG_PREFIX("reconfiguration on the way, skip the incoming proposal");
        return;
    }

    if (proposal.config.ballot > get_ballot()) {
        if (!update_configuration(proposal.config)) {
            // is closing or update failed
            return;
        }
    }

    _app_info.__set_duplicating(proposal.info.duplicating);
    switch (proposal.type) {
    case config_type::CT_ASSIGN_PRIMARY:
    case config_type::CT_UPGRADE_TO_PRIMARY:
        assign_primary(proposal);
        break;
    case config_type::CT_ADD_SECONDARY:
    case config_type::CT_ADD_SECONDARY_FOR_LB:
        add_potential_secondary(proposal);
        break;
    case config_type::CT_DOWNGRADE_TO_SECONDARY:
        downgrade_to_secondary_on_primary(proposal);
        break;
    case config_type::CT_DOWNGRADE_TO_INACTIVE:
        downgrade_to_inactive_on_primary(proposal);
        break;
    case config_type::CT_REMOVE:
        remove(proposal);
        break;
    default:
        CHECK(false, "invalid config_type, type = {}", enum_to_string(proposal.type));
    }
}

void replica::assign_primary(configuration_update_request &proposal)
{
    CHECK_EQ(proposal.node, _stub->_primary_address);

    if (status() == partition_status::PS_PRIMARY) {
        LOG_WARNING_PREFIX("invalid assgin primary proposal as the node is in {}",
                           enum_to_string(status()));
        return;
    }

    if (proposal.type == config_type::CT_UPGRADE_TO_PRIMARY &&
        (status() != partition_status::PS_SECONDARY || _secondary_states.checkpoint_is_running) &&
        status() != partition_status::PS_PARTITION_SPLIT) {
        LOG_WARNING_PREFIX(
            "invalid upgrade to primary proposal as the node is in {} or during checkpointing",
            enum_to_string(status()));

        // TODO: tell meta server so new primary is built more quickly
        return;
    }

    proposal.config.primary = _stub->_primary_address;
    replica_helper::remove_node(_stub->_primary_address, proposal.config.secondaries);

    update_configuration_on_meta_server(proposal.type, proposal.node, proposal.config);
}

// run on primary to send ADD_LEARNER request to candidate replica server
void replica::add_potential_secondary(configuration_update_request &proposal)
{
    if (status() != partition_status::PS_PRIMARY) {
        LOG_WARNING_PREFIX("ignore add secondary proposal for invalid state, state = {}",
                           enum_to_string(status()));
        return;
    }

    CHECK_EQ(proposal.config.ballot, get_ballot());
    CHECK_EQ(proposal.config.pid, _primary_states.membership.pid);
    CHECK_EQ(proposal.config.primary, _primary_states.membership.primary);
    CHECK(proposal.config.secondaries == _primary_states.membership.secondaries, "");
    CHECK(!_primary_states.check_exist(proposal.node, partition_status::PS_PRIMARY),
          "node = {}",
          proposal.node);
    CHECK(!_primary_states.check_exist(proposal.node, partition_status::PS_SECONDARY),
          "node = {}",
          proposal.node);

    int potential_secondaries_count =
        _primary_states.membership.secondaries.size() + _primary_states.learners.size();
    if (potential_secondaries_count >= _primary_states.membership.max_replica_count - 1) {
        if (proposal.type == config_type::CT_ADD_SECONDARY) {
            if (_primary_states.learners.find(proposal.node) == _primary_states.learners.end()) {
                LOG_INFO_PREFIX(
                    "already have enough secondaries or potential secondaries, ignore new "
                    "potential secondary proposal");
                return;
            }
        } else if (proposal.type == config_type::CT_ADD_SECONDARY_FOR_LB) {
            if (potential_secondaries_count >= _primary_states.membership.max_replica_count) {
                LOG_INFO_PREFIX("only allow one extra (potential) secondary, ingnore new potential "
                                "secondary proposal");
                return;
            } else {
                LOG_INFO_PREFIX("add a new secondary({}) for future load balancer", proposal.node);
            }
        } else {
            CHECK(false, "invalid config_type, type = {}", enum_to_string(proposal.type));
        }
    }

    remote_learner_state state;
    state.prepare_start_decree = invalid_decree;
    state.timeout_task = nullptr; // TODO: add timer for learner task

    auto it = _primary_states.learners.find(proposal.node);
    if (it != _primary_states.learners.end()) {
        state.signature = it->second.signature;
    } else {
        state.signature = ++_primary_states.next_learning_version;
        _primary_states.learners[proposal.node] = state;
        _primary_states.statuses[proposal.node] = partition_status::PS_POTENTIAL_SECONDARY;
    }

    group_check_request request;
    request.app = _app_info;
    request.node = proposal.node;
    _primary_states.get_replica_config(
        partition_status::PS_POTENTIAL_SECONDARY, request.config, state.signature);
    request.last_committed_decree = last_committed_decree();

    LOG_INFO_PREFIX("call one way {} to start learning with signature [{:#018x}]",
                    proposal.node,
                    state.signature);

    rpc::call_one_way_typed(
        proposal.node, RPC_LEARN_ADD_LEARNER, request, get_gpid().thread_hash());
}

void replica::upgrade_to_secondary_on_primary(::dsn::rpc_address node)
{
    LOG_INFO_PREFIX("upgrade potential secondary {} to secondary", node);

    partition_configuration newConfig = _primary_states.membership;

    // add secondary
    newConfig.secondaries.push_back(node);

    update_configuration_on_meta_server(config_type::CT_UPGRADE_TO_SECONDARY, node, newConfig);
}

void replica::downgrade_to_secondary_on_primary(configuration_update_request &proposal)
{
    if (proposal.config.ballot != get_ballot() || status() != partition_status::PS_PRIMARY)
        return;

    CHECK_EQ(proposal.config.pid, _primary_states.membership.pid);
    CHECK_EQ(proposal.config.primary, _primary_states.membership.primary);
    CHECK(proposal.config.secondaries == _primary_states.membership.secondaries, "");
    CHECK_EQ(proposal.node, proposal.config.primary);

    proposal.config.primary.set_invalid();
    proposal.config.secondaries.push_back(proposal.node);

    update_configuration_on_meta_server(
        config_type::CT_DOWNGRADE_TO_SECONDARY, proposal.node, proposal.config);
}

void replica::downgrade_to_inactive_on_primary(configuration_update_request &proposal)
{
    if (proposal.config.ballot != get_ballot() || status() != partition_status::PS_PRIMARY)
        return;

    CHECK_EQ(proposal.config.pid, _primary_states.membership.pid);
    CHECK_EQ(proposal.config.primary, _primary_states.membership.primary);
    CHECK(proposal.config.secondaries == _primary_states.membership.secondaries, "");

    if (proposal.node == proposal.config.primary) {
        proposal.config.primary.set_invalid();
    } else {
        CHECK(replica_helper::remove_node(proposal.node, proposal.config.secondaries),
              "remove node failed, node = {}",
              proposal.node);
    }

    update_configuration_on_meta_server(
        config_type::CT_DOWNGRADE_TO_INACTIVE, proposal.node, proposal.config);
}

void replica::remove(configuration_update_request &proposal)
{
    if (proposal.config.ballot != get_ballot() || status() != partition_status::PS_PRIMARY)
        return;

    CHECK_EQ(proposal.config.pid, _primary_states.membership.pid);
    CHECK_EQ(proposal.config.primary, _primary_states.membership.primary);
    CHECK(proposal.config.secondaries == _primary_states.membership.secondaries, "");

    auto st = _primary_states.get_node_status(proposal.node);

    switch (st) {
    case partition_status::PS_PRIMARY:
        CHECK_EQ(proposal.config.primary, proposal.node);
        proposal.config.primary.set_invalid();
        break;
    case partition_status::PS_SECONDARY: {
        CHECK(replica_helper::remove_node(proposal.node, proposal.config.secondaries),
              "remove_node failed, node = {}",
              proposal.node);
    } break;
    case partition_status::PS_POTENTIAL_SECONDARY:
        break;
    default:
        break;
    }

    update_configuration_on_meta_server(config_type::CT_REMOVE, proposal.node, proposal.config);
}

// from primary
void replica::on_remove(const replica_configuration &request)
{
    if (request.ballot < get_ballot())
        return;

    //
    // - meta-server requires primary r1 to remove this secondary r2
    // - primary update config from {3,r1,[r2,r3]} to {4,r1,[r3]}
    // - primary send one way RPC_REMOVE_REPLICA to r2, but this message is delay by network
    // - meta-server requires primary r1 to add new secondary on r2 again (though this case would
    // not occur generally)
    // - primary send RPC_LEARN_ADD_LEARNER to r2 with config of {4,r1,[r3]}, then r2 start to learn
    // - when r2 is on learning, the remove request is arrived, with the same ballot
    // - here we ignore the lately arrived remove request, which is proper
    //
    if (request.ballot == get_ballot() && partition_status::PS_POTENTIAL_SECONDARY == status()) {
        LOG_WARNING("this implies that a config proposal request (e.g. add secondary) "
                    "with the same ballot arrived before this remove request, "
                    "current status is {}",
                    enum_to_string(status()));
        return;
    }

    CHECK_EQ(request.status, partition_status::PS_INACTIVE);
    update_local_configuration(request);
}

void replica::update_configuration_on_meta_server(config_type::type type,
                                                  ::dsn::rpc_address node,
                                                  partition_configuration &newConfig)
{
    // type should never be `CT_REGISTER_CHILD`
    // if this happens, it means serious mistake happened during partition split
    // assert here to stop split and avoid splitting wrong
    CHECK_NE_PREFIX(type, config_type::CT_REGISTER_CHILD);

    newConfig.last_committed_decree = last_committed_decree();

    if (type == config_type::CT_PRIMARY_FORCE_UPDATE_BALLOT) {
        CHECK(status() == partition_status::PS_INACTIVE && _inactive_is_transient &&
                  _is_initializing,
              "");
        CHECK_EQ(newConfig.primary, node);
    } else if (type != config_type::CT_ASSIGN_PRIMARY &&
               type != config_type::CT_UPGRADE_TO_PRIMARY) {
        CHECK_EQ(status(), partition_status::PS_PRIMARY);
        CHECK_EQ(newConfig.ballot, _primary_states.membership.ballot);
    }

    // disable 2pc during reconfiguration
    // it is possible to do this only for config_type::CT_DOWNGRADE_TO_SECONDARY,
    // but we choose to disable 2pc during all reconfiguration types
    // for simplicity at the cost of certain write throughput
    update_local_configuration_with_no_ballot_change(partition_status::PS_INACTIVE);
    set_inactive_state_transient(true);

    dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_UPDATE_PARTITION_CONFIGURATION);

    std::shared_ptr<configuration_update_request> request(new configuration_update_request);
    request->info = _app_info;
    request->config = newConfig;
    request->config.ballot++;
    request->type = type;
    request->node = node;

    ::dsn::marshall(msg, *request);

    if (nullptr != _primary_states.reconfiguration_task) {
        _primary_states.reconfiguration_task->cancel(true);
    }

    LOG_INFO_PREFIX(
        "send update configuration request to meta server, ballot = {}, type = {}, node = {}",
        request->config.ballot,
        enum_to_string(request->type),
        request->node);

    rpc_address target(_stub->_failure_detector->get_servers());
    _primary_states.reconfiguration_task =
        rpc::call(target,
                  msg,
                  &_tracker,
                  [=](error_code err, dsn::message_ex *reqmsg, dsn::message_ex *response) {
                      on_update_configuration_on_meta_server_reply(err, reqmsg, response, request);
                  },
                  get_gpid().thread_hash());
}

void replica::on_update_configuration_on_meta_server_reply(
    error_code err,
    dsn::message_ex *request,
    dsn::message_ex *response,
    std::shared_ptr<configuration_update_request> req)
{
    _checker.only_one_thread_access();

    if (partition_status::PS_INACTIVE != status() || _stub->is_connected() == false) {
        _primary_states.reconfiguration_task = nullptr;
        return;
    }

    configuration_update_response resp;
    if (err == ERR_OK) {
        ::dsn::unmarshall(response, resp);
        err = resp.err;
    }

    if (err != ERR_OK) {
        LOG_INFO_PREFIX(
            "update configuration reply with err {}, request ballot {}", err, req->config.ballot);

        if (err != ERR_INVALID_VERSION) {
            // when the rpc call timeout, we would delay to do the recall
            request->add_ref(); // will be released after recall
            _primary_states.reconfiguration_task = tasking::enqueue(
                LPC_DELAY_UPDATE_CONFIG,
                &_tracker,
                [ this, request, req2 = std::move(req) ]() {
                    rpc_address target(_stub->_failure_detector->get_servers());
                    rpc_response_task_ptr t = rpc::create_rpc_response_task(
                        request,
                        &_tracker,
                        [this, req2](
                            error_code err, dsn::message_ex *request, dsn::message_ex *response) {
                            on_update_configuration_on_meta_server_reply(
                                err, request, response, std::move(req2));
                        },
                        get_gpid().thread_hash());
                    _primary_states.reconfiguration_task = t;
                    dsn_rpc_call(target, t.get());
                    request->release_ref();
                },
                get_gpid().thread_hash(),
                std::chrono::seconds(1));
            return;
        }
    }

    LOG_INFO_PREFIX(
        "update configuration {}, reply with err {}, ballot {}, local ballot {}, local status {}",
        enum_to_string(req->type),
        resp.err,
        resp.config.ballot,
        get_ballot(),
        enum_to_string(status()));

    if (resp.config.ballot < get_ballot()) {
        _primary_states.reconfiguration_task = nullptr;
        return;
    }

    // post-update work items?
    if (resp.err == ERR_OK) {
        CHECK_EQ(req->config.pid, resp.config.pid);
        CHECK_EQ(req->config.primary, resp.config.primary);
        CHECK(req->config.secondaries == resp.config.secondaries, "");

        switch (req->type) {
        case config_type::CT_UPGRADE_TO_PRIMARY:
            _primary_states.last_prepare_decree_on_new_primary = _prepare_list->max_decree();
            break;
        case config_type::CT_ASSIGN_PRIMARY:
            _primary_states.last_prepare_decree_on_new_primary = 0;
            break;
        case config_type::CT_DOWNGRADE_TO_SECONDARY:
        case config_type::CT_DOWNGRADE_TO_INACTIVE:
        case config_type::CT_UPGRADE_TO_SECONDARY:
            break;
        case config_type::CT_REMOVE:
            if (req->node != _stub->_primary_address) {
                replica_configuration rconfig;
                replica_helper::get_replica_config(resp.config, req->node, rconfig);
                rpc::call_one_way_typed(
                    req->node, RPC_REMOVE_REPLICA, rconfig, get_gpid().thread_hash());
            }
            break;
        case config_type::CT_PRIMARY_FORCE_UPDATE_BALLOT:
            CHECK(_is_initializing, "");
            _is_initializing = false;
            break;
        default:
            CHECK(false, "invalid config_type, type = {}", enum_to_string(req->type));
        }
    }

    update_configuration(resp.config);
    _primary_states.reconfiguration_task = nullptr;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::update_app_envs(const std::map<std::string, std::string> &envs)
{
    if (_app) {
        update_app_envs_internal(envs);
        _app->update_app_envs(envs);
    }
}

void replica::update_app_envs_internal(const std::map<std::string, std::string> &envs)
{
    update_bool_envs(envs, replica_envs::SPLIT_VALIDATE_PARTITION_HASH, _validate_partition_hash);

    update_throttle_envs(envs);

    update_ac_allowed_users(envs);

    update_ac_ranger_policies(envs);

    update_allow_ingest_behind(envs);

    update_deny_client(envs);
}

void replica::update_bool_envs(const std::map<std::string, std::string> &envs,
                               const std::string &name,
                               bool &value)
{
    bool new_value = false;
    if (!get_bool_envs(envs, name, new_value)) {
        LOG_WARNING_PREFIX("invalid value of env {}", name);
        return;
    }
    if (new_value != value) {
        LOG_INFO_PREFIX("switch env[{}] from {} to {}", name, value, new_value);
        value = new_value;
    }
}

void replica::update_ac_allowed_users(const std::map<std::string, std::string> &envs)
{
    std::string allowed_users;
    auto iter = envs.find(replica_envs::REPLICA_ACCESS_CONTROLLER_ALLOWED_USERS);
    if (iter != envs.end()) {
        allowed_users = iter->second;
    }

    _access_controller->update_allowed_users(allowed_users);
}

void replica::update_ac_ranger_policies(const std::map<std::string, std::string> &envs)
{
    auto iter = envs.find(replica_envs::REPLICA_ACCESS_CONTROLLER_RANGER_POLICIES);
    if (iter != envs.end()) {
        _access_controller->update_ranger_policies(iter->second);
    }
}

void replica::update_allow_ingest_behind(const std::map<std::string, std::string> &envs)
{
    bool new_value = false;
    if (!get_bool_envs(envs, replica_envs::ROCKSDB_ALLOW_INGEST_BEHIND, new_value)) {
        return;
    }
    if (new_value != _allow_ingest_behind) {
        auto info = _app_info;
        info.envs = envs;
        if (store_app_info(info) != ERR_OK) {
            return;
        }
        LOG_INFO_PREFIX("switch env[{}] from {} to {}",
                        replica_envs::ROCKSDB_ALLOW_INGEST_BEHIND,
                        _allow_ingest_behind,
                        new_value);
        _allow_ingest_behind = new_value;
    }
}

void replica::update_deny_client(const std::map<std::string, std::string> &envs)
{
    auto env_iter = envs.find(replica_envs::DENY_CLIENT_REQUEST);
    if (env_iter == envs.end()) {
        _deny_client.reset();
        return;
    }

    std::vector<std::string> sub_sargs;
    utils::split_args(env_iter->second.c_str(), sub_sargs, '*', true);
    CHECK_EQ_PREFIX(sub_sargs.size(), 2);

    _deny_client.reconfig = (sub_sargs[0] == "reconfig");
    _deny_client.read = (sub_sargs[1] == "read" || sub_sargs[1] == "all");
    _deny_client.write = (sub_sargs[1] == "write" || sub_sargs[1] == "all");
}

void replica::query_app_envs(/*out*/ std::map<std::string, std::string> &envs)
{
    if (_app) {
        _app->query_app_envs(envs);
    }
}

bool replica::update_configuration(const partition_configuration &config)
{
    CHECK_GE(config.ballot, get_ballot());

    replica_configuration rconfig;
    replica_helper::get_replica_config(config, _stub->_primary_address, rconfig);

    if (rconfig.status == partition_status::PS_PRIMARY &&
        (rconfig.ballot > get_ballot() || status() != partition_status::PS_PRIMARY)) {
        _primary_states.reset_membership(config, config.primary != _stub->_primary_address);
    }

    if (config.ballot > get_ballot() ||
        is_same_ballot_status_change_allowed(status(), rconfig.status)) {
        return update_local_configuration(rconfig, true);
    } else
        return false;
}

bool replica::is_same_ballot_status_change_allowed(partition_status::type olds,
                                                   partition_status::type news)
{
    return
        // add learner
        (olds == partition_status::PS_INACTIVE && news == partition_status::PS_POTENTIAL_SECONDARY)

        // learner ready for secondary
        ||
        (olds == partition_status::PS_POTENTIAL_SECONDARY && news == partition_status::PS_SECONDARY)

        // meta server come back
        || (olds == partition_status::PS_INACTIVE && news == partition_status::PS_SECONDARY &&
            _inactive_is_transient)

        // meta server come back
        || (olds == partition_status::PS_INACTIVE && news == partition_status::PS_PRIMARY &&
            _inactive_is_transient)

        // no change
        || (olds == news);
}

bool replica::update_local_configuration(const replica_configuration &config,
                                         bool same_ballot /* = false*/)
{
    FAIL_POINT_INJECT_F("replica_update_local_configuration", [=](absl::string_view) -> bool {
        auto old_status = status();
        _config = config;
        LOG_INFO_PREFIX(
            "update status from {} to {}", enum_to_string(old_status), enum_to_string(status()));
        return true;
    });

    CHECK(config.ballot > get_ballot() || (same_ballot && config.ballot == get_ballot()),
          "invalid ballot, {} VS {}",
          config.ballot,
          get_ballot());
    CHECK_EQ(config.pid, get_gpid());

    partition_status::type old_status = status();
    ballot old_ballot = get_ballot();

    // skip unnecessary configuration change
    if (old_status == config.status && old_ballot == config.ballot) {
        return true;
    }

    // skip invalid change
    // but do not disable transitions to partition_status::PS_ERROR as errors
    // must be handled immediately
    switch (old_status) {
    case partition_status::PS_ERROR: {
        LOG_WARNING_PREFIX("status change from {} @ {} to {} @ {} is not allowed",
                           enum_to_string(old_status),
                           old_ballot,
                           enum_to_string(config.status),
                           config.ballot);
        return false;
    } break;
    case partition_status::PS_INACTIVE:
        if ((config.status == partition_status::PS_PRIMARY ||
             config.status == partition_status::PS_SECONDARY) &&
            !_inactive_is_transient) {
            LOG_WARNING_PREFIX("status change from {} @ {} to {} @ {} is not allowed when "
                               "inactive state is not transient",
                               enum_to_string(old_status),
                               old_ballot,
                               enum_to_string(config.status),
                               config.ballot);
            return false;
        }
        break;
    case partition_status::PS_POTENTIAL_SECONDARY:
        if (config.status == partition_status::PS_INACTIVE) {
            if (!_potential_secondary_states.cleanup(false)) {
                LOG_WARNING_PREFIX("status change from {} @ {} to {} @ {} is not allowed coz "
                                   "learning remote state is still running",
                                   enum_to_string(old_status),
                                   old_ballot,
                                   enum_to_string(config.status),
                                   config.ballot);
                return false;
            }
        }
        break;
    case partition_status::PS_SECONDARY:
        if (config.status != partition_status::PS_SECONDARY &&
            config.status != partition_status::PS_ERROR) {
            if (!_secondary_states.cleanup(false)) {
                // TODO(sunweijie): totally remove this
                dsn::task *native_handle;
                if (_secondary_states.checkpoint_task)
                    native_handle = _secondary_states.checkpoint_task.get();
                else if (_secondary_states.checkpoint_completed_task)
                    native_handle = _secondary_states.checkpoint_completed_task.get();
                else if (_secondary_states.catchup_with_private_log_task)
                    native_handle = _secondary_states.catchup_with_private_log_task.get();
                else
                    native_handle = nullptr;

                LOG_WARNING_PREFIX("status change from {} @ {} to {} @ {} is not allowed coz "
                                   "checkpointing {} is still running",
                                   enum_to_string(old_status),
                                   old_ballot,
                                   enum_to_string(config.status),
                                   config.ballot,
                                   fmt::ptr(native_handle));
                return false;
            }
        }
        break;
    case partition_status::PS_PARTITION_SPLIT:
        if (config.status == partition_status::PS_INACTIVE) {
            LOG_WARNING_PREFIX("status change from {} @ {} to {} @ {} is not allowed",
                               enum_to_string(old_status),
                               old_ballot,
                               enum_to_string(config.status),
                               config.ballot);
            return false;
        }
        break;
    default:
        break;
    }

    uint64_t old_ts = _last_config_change_time_ms;
    _config = config;
    // we should durable the new ballot to prevent the inconsistent state
    if (_config.ballot > old_ballot) {
        dsn::error_code result = _app->update_init_info_ballot_and_decree(this);
        if (result == dsn::ERR_OK) {
            LOG_INFO_PREFIX(
                "update ballot to init file from {} to {} OK", old_ballot, _config.ballot);
        } else {
            LOG_WARNING_PREFIX(
                "update ballot to init file from {} to {} {}", old_ballot, _config.ballot, result);
        }
        _split_mgr->parent_cleanup_split_context();
    }
    _last_config_change_time_ms = dsn_now_ms();
    CHECK_GE(max_prepared_decree(), last_committed_decree());

    _bulk_loader->clear_bulk_load_states_if_needed(old_status, config.status);

    // Notice: there has five ways that primary can change its partition_status
    //   1, primary change partition config, such as add/remove secondary
    //   2, downgrage to secondary because of load balance
    //   3, disnconnected with meta-server
    //   4, connectied with meta-server
    //   5, crash
    // here, we just need to care about case １, 2, 3 and 4, ignore case 5
    // the way that partition status change is:
    //   case 1: primary -> ps_inactive & _inactive_is_transient = true -> primary
    //   case 2: primary -> ps_inavtive & _inactive_is_transient = true -> secondary
    //   case 3: primary -> ps_inactive & _inactive_is_transient = ture
    //   case 4: ps_inactive & _inactive_is_transient = true -> primary or secondary
    // the way we process whether primary stop uploading backup checkpoint is that case-1 continue
    // uploading, others just stop uploading
    switch (old_status) {
    case partition_status::PS_PRIMARY:
        cleanup_preparing_mutations(false);
        switch (config.status) {
        case partition_status::PS_PRIMARY:
            replay_prepare_list();
            break;
        case partition_status::PS_INACTIVE:
            _primary_states.cleanup(old_ballot != config.ballot);
            // here we use wheather ballot changes and wheather disconnecting with meta to
            // distinguish different case above mentioned
            if (old_ballot == config.ballot && _stub->is_connected()) {
                // case 1 and case 2, just continue uploading
                //(when case2, we stop uploading when it change to secondary)
            } else {
                set_backup_context_cancel();
                clear_cold_backup_state();
            }
            break;
        case partition_status::PS_SECONDARY:
        case partition_status::PS_ERROR:
            _primary_states.cleanup(true);
            // only load balance will occur primary -> secondary
            // and we just stop upload and release the cold_backup_state, and let new primary to
            // upload
            set_backup_context_cancel();
            clear_cold_backup_state();
            break;
        case partition_status::PS_POTENTIAL_SECONDARY:
            CHECK(false, "invalid execution path");
            break;
        default:
            CHECK(false, "invalid execution path");
        }
        break;
    case partition_status::PS_SECONDARY:
        cleanup_preparing_mutations(false);
        if (config.status != partition_status::PS_SECONDARY) {
            // if primary change the ballot, secondary will update ballot from A to
            // A+1, we don't need clear cold backup context when this case
            //
            // if secondary upgrade to primary, we must cancel & clear cold_backup_state, because
            // new-primary must check whether backup is already completed by previous-primary

            set_backup_context_cancel();
            clear_cold_backup_state();
        }
        switch (config.status) {
        case partition_status::PS_PRIMARY:
            init_group_check();
            replay_prepare_list();
            break;
        case partition_status::PS_SECONDARY:
            break;
        case partition_status::PS_POTENTIAL_SECONDARY:
            // prevent further 2pc
            // wait next group check or explicit learn for real learning
            _potential_secondary_states.learning_status = learner_status::LearningWithoutPrepare;
            break;
        case partition_status::PS_INACTIVE:
            break;
        case partition_status::PS_ERROR:
            // _secondary_states.cleanup(true); => do it in close as it may block
            break;
        default:
            CHECK(false, "invalid execution path");
        }
        break;
    case partition_status::PS_POTENTIAL_SECONDARY:
        switch (config.status) {
        case partition_status::PS_PRIMARY:
            CHECK(false, "invalid execution path");
            break;
        case partition_status::PS_SECONDARY:
            _prepare_list->truncate(_app->last_committed_decree());

            // using force cleanup now as all tasks must be done already
            CHECK_PREFIX_MSG(_potential_secondary_states.cleanup(true),
                             "potential secondary context cleanup failed");

            check_state_completeness();
            break;
        case partition_status::PS_POTENTIAL_SECONDARY:
            break;
        case partition_status::PS_INACTIVE:
            break;
        case partition_status::PS_ERROR:
            _prepare_list->reset(_app->last_committed_decree());
            _potential_secondary_states.cleanup(false);
            // => do this in close as it may block
            // CHECK_PREFIX_MSG(_potential_secondary_states.cleanup(true),
            //                  "potential secondary context cleanup failed");
            break;
        default:
            CHECK(false, "invalid execution path");
        }
        break;
    case partition_status::PS_PARTITION_SPLIT:
        switch (config.status) {
        case partition_status::PS_PRIMARY:
            _split_states.cleanup(true);
            init_group_check();
            replay_prepare_list();
            break;
        case partition_status::PS_SECONDARY:
            _split_states.cleanup(true);
            break;
        case partition_status::PS_POTENTIAL_SECONDARY:
            CHECK(false, "invalid execution path");
            break;
        case partition_status::PS_INACTIVE:
            break;
        case partition_status::PS_ERROR:
            _split_states.cleanup(false);
            break;
        default:
            CHECK(false, "invalid execution path");
        }
        break;
    case partition_status::PS_INACTIVE:
        if (config.status != partition_status::PS_PRIMARY || !_inactive_is_transient) {
            // except for case 1, we need stop uploading backup checkpoint
            set_backup_context_cancel();
            clear_cold_backup_state();
        }
        switch (config.status) {
        case partition_status::PS_PRIMARY:
            CHECK(_inactive_is_transient, "must be in transient state for being primary next");
            _inactive_is_transient = false;
            init_group_check();
            replay_prepare_list();
            break;
        case partition_status::PS_SECONDARY:
            CHECK(_inactive_is_transient, "must be in transient state for being secondary next");
            _inactive_is_transient = false;
            break;
        case partition_status::PS_POTENTIAL_SECONDARY:
            _inactive_is_transient = false;
            break;
        case partition_status::PS_INACTIVE:
            break;
        case partition_status::PS_ERROR:
            // => do this in close as it may block
            // if (_inactive_is_transient)
            // {
            //    _secondary_states.cleanup(true);
            // }

            if (_inactive_is_transient) {
                _primary_states.cleanup(true);
                _secondary_states.cleanup(false);
            }
            _inactive_is_transient = false;
            break;
        default:
            CHECK(false, "invalid execution path");
        }
        break;
    case partition_status::PS_ERROR:
        switch (config.status) {
        case partition_status::PS_PRIMARY:
            CHECK(false, "invalid execution path");
            break;
        case partition_status::PS_SECONDARY:
            CHECK(false, "invalid execution path");
            break;
        case partition_status::PS_POTENTIAL_SECONDARY:
            CHECK(false, "invalid execution path");
            break;
        case partition_status::PS_INACTIVE:
            CHECK(false, "invalid execution path");
            break;
        case partition_status::PS_ERROR:
            break;
        default:
            CHECK(false, "invalid execution path");
        }
        break;
    default:
        CHECK(false, "invalid execution path");
    }

    LOG_INFO_PREFIX(
        "status change {} @ {} => {} @ {}, pre({}, {}), app({}, {}), duration = {} ms, {}",
        enum_to_string(old_status),
        old_ballot,
        enum_to_string(status()),
        get_ballot(),
        _prepare_list->max_decree(),
        _prepare_list->last_committed_decree(),
        _app->last_committed_decree(),
        _app->last_durable_decree(),
        _last_config_change_time_ms - old_ts,
        boost::lexical_cast<std::string>(_config));

    if (status() != old_status) {
        bool is_closing =
            (status() == partition_status::PS_ERROR ||
             (status() == partition_status::PS_INACTIVE && get_ballot() > old_ballot));
        _stub->notify_replica_state_update(config, is_closing);

        if (is_closing) {
            LOG_INFO_PREFIX("being close ...");
            _stub->begin_close_replica(this);
            return false;
        }
    } else {
        _stub->notify_replica_state_update(config, false);
    }

    // start pending mutations if necessary
    if (status() == partition_status::PS_PRIMARY) {
        mutation_ptr next = _primary_states.write_queue.check_possible_work(
            static_cast<int>(_prepare_list->max_decree() - last_committed_decree()));
        if (next) {
            init_prepare(next, false);
        }

        if (_primary_states.membership.secondaries.size() + 1 <
            _options->app_mutation_2pc_min_replica_count(_app_info.max_replica_count)) {
            std::vector<mutation_ptr> queued;
            _primary_states.write_queue.clear(queued);
            for (auto &m : queued) {
                for (auto &r : m->client_requests) {
                    response_client_write(r, ERR_NOT_ENOUGH_MEMBER);
                }
            }
        }
    }

    return true;
}

bool replica::update_local_configuration_with_no_ballot_change(partition_status::type s)
{
    if (status() == s) {
        return false;
    }

    auto config = _config;
    config.status = s;
    return update_local_configuration(config, true);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::on_config_sync(const app_info &info,
                             const partition_configuration &config,
                             split_status::type meta_split_status)
{
    LOG_DEBUG_PREFIX("configuration sync");
    // no outdated update
    if (config.ballot < get_ballot())
        return;

    update_app_max_replica_count(info.max_replica_count);
    update_app_name(info.app_name);
    update_app_envs(info.envs);
    _is_duplication_master = info.duplicating;

    if (status() == partition_status::PS_PRIMARY) {
        if (nullptr != _primary_states.reconfiguration_task) {
            // already under reconfiguration, skip configuration sync
        } else if (info.partition_count != _app_info.partition_count) {
            _split_mgr->trigger_primary_parent_split(info.partition_count, meta_split_status);
        }
    } else {
        if (_is_initializing) {
            // in initializing, when replica still primary, need to inc ballot
            if (config.primary == _stub->_primary_address &&
                status() == partition_status::PS_INACTIVE && _inactive_is_transient) {
                update_configuration_on_meta_server(config_type::CT_PRIMARY_FORCE_UPDATE_BALLOT,
                                                    config.primary,
                                                    const_cast<partition_configuration &>(config));
                return;
            }
            _is_initializing = false;
        }

        update_configuration(config);

        if (status() == partition_status::PS_INACTIVE && !_inactive_is_transient) {
            if (config.primary == _stub->_primary_address // dead primary
                ||
                config.primary.is_invalid() // primary is dead (otherwise let primary remove this)
                ) {
                LOG_INFO_PREFIX("downgrade myself as inactive is not transient, remote_config({})",
                                boost::lexical_cast<std::string>(config));
                _stub->remove_replica_on_meta_server(_app_info, config);
            } else {
                LOG_INFO_PREFIX("state is non-transient inactive, waiting primary to remove me");
            }
        }
    }
}

void replica::update_app_name(const std::string &app_name)
{
    if (app_name == _app_info.app_name) {
        return;
    }

    auto old_app_name = _app_info.app_name;
    _app_info.app_name = app_name;

    CHECK_EQ_PREFIX_MSG(store_app_info(_app_info),
                        ERR_OK,
                        "store_app_info for app_name failed: "
                        "app_name={}, app_id={}, old_app_name={}",
                        _app_info.app_name,
                        _app_info.app_id,
                        old_app_name);
}

void replica::update_app_max_replica_count(int32_t max_replica_count)
{
    if (max_replica_count == _app_info.max_replica_count) {
        return;
    }

    auto old_max_replica_count = _app_info.max_replica_count;
    _app_info.max_replica_count = max_replica_count;

    CHECK_EQ_PREFIX_MSG(store_app_info(_app_info),
                        ERR_OK,
                        "store_app_info for max_replica_count failed: app_name={}, "
                        "app_id={}, old_max_replica_count={}, new_max_replica_count={}",
                        _app_info.app_name,
                        _app_info.app_id,
                        old_max_replica_count,
                        _app_info.max_replica_count);
}

void replica::replay_prepare_list()
{
    decree start = last_committed_decree() + 1;
    decree end = _prepare_list->max_decree();

    LOG_INFO_PREFIX("replay prepare list from {} to {}, ballot = {}", start, end, get_ballot());

    for (decree decree = start; decree <= end; decree++) {
        mutation_ptr old = _prepare_list->get_mutation_by_decree(decree);
        mutation_ptr mu = new_mutation(decree);

        if (old != nullptr) {
            LOG_DEBUG_PREFIX(
                "copy mutation from mutation_tid={} to mutation_tid={}", old->tid(), mu->tid());
            mu->copy_from(old);
        } else {
            mu->add_client_request(RPC_REPLICATION_WRITE_EMPTY, nullptr);

            LOG_INFO_PREFIX("emit empty mutation {} with mutation_tid={} when replay prepare list",
                            mu->name(),
                            mu->tid());
        }

        init_prepare(mu, true);
    }
}

error_code replica::update_init_info_ballot_and_decree()
{
    return _app->update_init_info_ballot_and_decree(this);
}

} // namespace replication
} // namespace dsn
