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

#include <climits>

#include <gtest/gtest.h>
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "runtime/rpc/rpc_address.h"
#include "common/replication_other_types.h"
#include "common/replication.codes.h"

#include "meta/meta_data.h"
#include "meta/meta_service.h"
#include "meta/server_state.h"
#include "meta/meta_server_failure_detector.h"
#include "meta/greedy_load_balancer.h"
#include "meta/test/misc/misc.h"
#include "meta_test_base.h"
#include "meta_service_test_app.h"
#include "dummy_balancer.h"

namespace dsn {
namespace replication {

typedef std::shared_ptr<configuration_update_request> cur_ptr;

// apply request in request.type to request.config
static void apply_update_request(/*in-out*/ configuration_update_request &update_req)
{
    dsn::partition_configuration &pc = update_req.config;
    pc.ballot++;

    switch (update_req.type) {
    case config_type::CT_ASSIGN_PRIMARY:
    case config_type::CT_UPGRADE_TO_PRIMARY:
        pc.primary = update_req.node;
        replica_helper::remove_node(update_req.node, pc.secondaries);
        break;

    case config_type::CT_ADD_SECONDARY:
    case config_type::CT_ADD_SECONDARY_FOR_LB:
        pc.secondaries.push_back(update_req.node);
        update_req.type = config_type::CT_UPGRADE_TO_SECONDARY;
        break;

    case config_type::CT_REMOVE:
    case config_type::CT_DOWNGRADE_TO_INACTIVE:
        if (update_req.node == pc.primary)
            pc.primary.set_invalid();
        else
            replica_helper::remove_node(update_req.node, pc.secondaries);
        break;

    case config_type::CT_DOWNGRADE_TO_SECONDARY:
        pc.secondaries.push_back(pc.primary);
        pc.primary.set_invalid();
        break;
    default:
        break;
    }
}

static auto default_filter = [](const dsn::rpc_address &target, dsn::message_ex *request) {
    dsn::message_ex *recv_request = create_corresponding_receive(request);
    cur_ptr update_req = std::make_shared<configuration_update_request>();
    ::dsn::unmarshall(recv_request, *update_req);
    destroy_message(recv_request);
    apply_update_request(*update_req);
    return update_req;
};

class meta_partition_guardian_test : public meta_test_base
{
public:
    void cure_test();
    void cure();
    void from_proposal_test();

    void call_update_configuration(
        meta_service *svc, std::shared_ptr<dsn::replication::configuration_update_request> &request)
    {
        dsn::message_ex *fake_request =
            dsn::message_ex::create_request(RPC_CM_UPDATE_PARTITION_CONFIGURATION);
        ::dsn::marshall(fake_request, *request);
        fake_request->add_ref();

        dsn::tasking::enqueue(
            LPC_META_STATE_HIGH,
            nullptr,
            std::bind(
                &server_state::on_update_configuration, svc->_state.get(), request, fake_request),
            server_state::sStateHash);
    }
};

class message_filter : public dsn::replication::meta_service
{
public:
    typedef std::function<cur_ptr(const dsn::rpc_address &target, dsn::message_ex *request)> filter;
    message_filter(meta_partition_guardian_test *app) : meta_service(), _app(app) {}
    void set_filter(const filter &f) { _filter = f; }
    virtual void reply_message(dsn::message_ex *request, dsn::message_ex *response) override
    {
        destroy_message(response);
    }

    virtual void send_message(const dsn::rpc_address &target, dsn::message_ex *request) override
    {
        // we expect this is a configuration_update_request proposal
        cur_ptr update_request = _filter(target, request);
        destroy_message(request);

        if (update_request != nullptr) {
            _app->call_update_configuration(this, update_request);
        }
    }

private:
    meta_partition_guardian_test *_app;
    filter _filter;
};

void meta_partition_guardian_test::cure_test()
{
    dsn::error_code ec;
    dsn::task_ptr t;
    std::shared_ptr<message_filter> svc(new message_filter(this));
    svc->_failure_detector.reset(new dsn::replication::meta_server_failure_detector(svc.get()));
    bool proposal_sent;
    dsn::rpc_address last_addr;

    ec = svc->remote_storage_initialize();
    ASSERT_EQ(ec, dsn::ERR_OK);
    svc->_partition_guardian.reset(new partition_guardian(svc.get()));
    svc->_balancer.reset(new dummy_balancer(svc.get()));

    server_state *state = svc->_state.get();
    state->initialize(svc.get(), meta_options::concat_path_unix_style(svc->_cluster_root, "apps"));
    dsn::app_info info;
    info.is_stateful = true;
    info.status = dsn::app_status::AS_CREATING;
    info.app_id = 1;
    info.app_name = "simple_kv.instance0";
    info.app_type = "simple_kv";
    info.max_replica_count = 3;
    info.partition_count = 1;
    std::shared_ptr<app_state> app = app_state::create(info);
    state->_all_apps.emplace(1, app);
    state->sync_apps_to_remote_storage();
    ASSERT_TRUE(state->spin_wait_staging(20));
    svc->_started = true;

    std::vector<dsn::rpc_address> nodes;
    generate_node_list(nodes, 4, 4);

    dsn::partition_configuration &pc = app->partitions[0];
    config_context &cc = *get_config_context(state->_all_apps, dsn::gpid(1, 0));

#define PROPOSAL_FLAG_CHECK                                                                        \
    ASSERT_TRUE(proposal_sent);                                                                    \
    proposal_sent = false

#define CONDITION_CHECK(cond) ASSERT_TRUE(spin_wait_condition(cond, 20))

    std::cerr << "Case: upgrade secondary to primary, and message lost" << std::endl;
    // initialize
    state->_nodes.clear();
    pc.primary.set_invalid();
    pc.secondaries = {nodes[0], nodes[1]};
    pc.ballot = 1;
    state->initialize_node_state();
    svc->set_node_state(nodes, true);
    proposal_sent = false;

    // check partitions, then ignore the proposal
    svc->set_filter([&](const dsn::rpc_address &target, dsn::message_ex *req) -> cur_ptr {
        dsn::message_ex *recv_request = create_corresponding_receive(req);
        cur_ptr update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);
        destroy_message(recv_request);

        EXPECT_EQ(update_req->type, config_type::CT_UPGRADE_TO_PRIMARY);
        EXPECT_TRUE(is_secondary(pc, update_req->node));
        EXPECT_EQ(target, update_req->node);

        last_addr = update_req->node;
        proposal_sent = true;
        return nullptr;
    });

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    PROPOSAL_FLAG_CHECK;

    // check partitions again
    svc->set_filter([&](const dsn::rpc_address &target, dsn::message_ex *req) -> cur_ptr {
        dsn::message_ex *recv_request = create_corresponding_receive(req);
        cur_ptr update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);
        destroy_message(recv_request);

        EXPECT_EQ(config_type::CT_UPGRADE_TO_PRIMARY, update_req->type);
        EXPECT_EQ(update_req->node, last_addr);
        EXPECT_EQ(target, update_req->node);

        proposal_sent = true;
        apply_update_request(*update_req);

        svc->set_filter(default_filter);
        return update_req;
    });

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    PROPOSAL_FLAG_CHECK;
    CONDITION_CHECK([&] { return pc.primary == last_addr; });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::cerr << "Case: upgrade secondary to primary, and the candidate died" << std::endl;
    // initialize
    state->_nodes.clear();
    pc.primary.set_invalid();
    pc.secondaries = {nodes[0], nodes[1]};
    pc.ballot = 1;
    state->initialize_node_state();
    svc->set_node_state(nodes, true);
    proposal_sent = false;

    // check partitions, then inject a event that node[0] is dead
    svc->set_filter([&](const dsn::rpc_address &target, dsn::message_ex *req) -> cur_ptr {
        dsn::message_ex *recv_request = create_corresponding_receive(req);
        cur_ptr update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);
        destroy_message(recv_request);

        EXPECT_EQ(update_req->type, config_type::CT_UPGRADE_TO_PRIMARY);
        EXPECT_TRUE(is_secondary(pc, update_req->node));
        EXPECT_EQ(target, update_req->node);

        proposal_sent = true;
        last_addr = update_req->node;
        svc->set_node_state({target}, false);
        return nullptr;
    });

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    PROPOSAL_FLAG_CHECK;

    // check partitions again
    svc->set_filter([&](const dsn::rpc_address &target, dsn::message_ex *req) -> cur_ptr {
        dsn::message_ex *recv_request = create_corresponding_receive(req);
        cur_ptr update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);
        destroy_message(recv_request);

        EXPECT_EQ(update_req->type, config_type::CT_UPGRADE_TO_PRIMARY);
        EXPECT_TRUE(is_secondary(pc, update_req->node));
        EXPECT_EQ(target, update_req->node);
        EXPECT_NE(target, last_addr);

        proposal_sent = true;
        apply_update_request(*update_req);
        svc->set_filter(default_filter);
        return update_req;
    });

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    PROPOSAL_FLAG_CHECK;
    CONDITION_CHECK([&] { return !pc.primary.is_invalid() && pc.primary != last_addr; });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::cerr << "Case: add secondary, and the message lost" << std::endl;
    // initialize
    state->_nodes.clear();
    pc.primary = nodes[0];
    pc.secondaries = {nodes[1]};
    pc.ballot = 1;
    state->initialize_node_state();
    svc->set_node_state(nodes, true);
    proposal_sent = false;

    // check partitions, then ignore the proposal
    svc->set_filter([&](const dsn::rpc_address &target, dsn::message_ex *req) -> cur_ptr {
        dsn::message_ex *recv_request = create_corresponding_receive(req);
        cur_ptr update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);
        destroy_message(recv_request);

        EXPECT_EQ(update_req->type, config_type::CT_ADD_SECONDARY);
        EXPECT_FALSE(is_secondary(pc, update_req->node));
        EXPECT_EQ(target, nodes[0]);

        last_addr = update_req->node;
        proposal_sent = true;
        return nullptr;
    });

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    PROPOSAL_FLAG_CHECK;

    // check partitions again
    svc->set_filter([&](const dsn::rpc_address &target, dsn::message_ex *req) -> cur_ptr {
        dsn::message_ex *recv_request = create_corresponding_receive(req);
        cur_ptr update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);
        destroy_message(recv_request);

        EXPECT_EQ(update_req->type, config_type::CT_ADD_SECONDARY);
        EXPECT_EQ(update_req->node, last_addr);
        EXPECT_EQ(target, nodes[0]);

        proposal_sent = true;
        apply_update_request(*update_req);
        svc->set_filter(default_filter);
        return update_req;
    });

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    PROPOSAL_FLAG_CHECK;
    CONDITION_CHECK([&] { return pc.secondaries.size() == 2 && is_secondary(pc, last_addr); });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::cerr << "Case: add secondary, but the primary is removing another" << std::endl;
    // initialize
    state->_nodes.clear();
    pc.primary = nodes[0];
    pc.secondaries = {nodes[1]};
    pc.ballot = 1;
    state->initialize_node_state();
    svc->set_node_state(nodes, true);
    proposal_sent = false;

    // check partitions, then inject another update_request
    svc->set_filter([&](const dsn::rpc_address &target, dsn::message_ex *req) -> cur_ptr {
        dsn::message_ex *recv_request = create_corresponding_receive(req);
        cur_ptr update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);
        destroy_message(recv_request);

        EXPECT_EQ(update_req->type, config_type::CT_ADD_SECONDARY);
        EXPECT_FALSE(is_secondary(pc, update_req->node));
        EXPECT_EQ(target, nodes[0]);

        update_req->config.ballot++;
        update_req->type = config_type::CT_DOWNGRADE_TO_INACTIVE;
        update_req->node = update_req->config.secondaries[0];
        update_req->config.secondaries.clear();

        proposal_sent = true;

        svc->set_filter(default_filter);
        return update_req;
    });

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    PROPOSAL_FLAG_CHECK;
    CONDITION_CHECK([&] { return pc.secondaries.size() == 2; });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::cerr << "Case: add secondary, and the added secondary is dead" << std::endl;
    // initialize
    state->_nodes.clear();
    pc.primary = nodes[0];
    pc.secondaries = {nodes[1]};
    pc.ballot = 1;
    state->initialize_node_state();
    svc->set_node_state(nodes, true);
    proposal_sent = false;

    // check partitions, then inject the nodes[2] dead
    svc->set_filter([&](const dsn::rpc_address &target, dsn::message_ex *req) -> cur_ptr {
        dsn::message_ex *recv_request = create_corresponding_receive(req);
        cur_ptr update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);
        destroy_message(recv_request);

        EXPECT_EQ(update_req->type, config_type::CT_ADD_SECONDARY);
        EXPECT_FALSE(is_secondary(pc, update_req->node));
        EXPECT_EQ(target, nodes[0]);

        last_addr = update_req->node;
        svc->set_node_state({update_req->node}, false);
        proposal_sent = true;
        return nullptr;
    });

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    PROPOSAL_FLAG_CHECK;

    // check partitions again
    svc->set_filter([&](const dsn::rpc_address &target, dsn::message_ex *req) -> cur_ptr {
        dsn::message_ex *recv_request = create_corresponding_receive(req);
        cur_ptr update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);
        destroy_message(recv_request);

        EXPECT_EQ(update_req->type, config_type::CT_ADD_SECONDARY);
        EXPECT_NE(update_req->node, last_addr);
        EXPECT_FALSE(is_secondary(pc, update_req->node));
        EXPECT_EQ(target, nodes[0]);

        proposal_sent = true;
        last_addr = update_req->node;
        apply_update_request(*update_req);
        svc->set_filter(default_filter);
        return update_req;
    });

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    PROPOSAL_FLAG_CHECK;
    CONDITION_CHECK([&] { return pc.secondaries.size() == 2 && is_secondary(pc, last_addr); });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::cerr << "Case: add secondary, and the primary is dead" << std::endl;
    // initialize
    state->_nodes.clear();
    pc.primary = nodes[0];
    pc.secondaries = {nodes[1]};
    pc.ballot = 1;
    state->initialize_node_state();
    svc->set_node_state(nodes, true);
    proposal_sent = false;

    // check partitions, then ignore the proposal
    svc->set_filter([&](const dsn::rpc_address &target, dsn::message_ex *req) -> cur_ptr {
        dsn::message_ex *recv_request = create_corresponding_receive(req);
        cur_ptr update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);
        destroy_message(recv_request);

        EXPECT_EQ(update_req->type, config_type::CT_ADD_SECONDARY);
        EXPECT_FALSE(is_secondary(pc, update_req->node));
        EXPECT_EQ(target, pc.primary);

        proposal_sent = true;
        svc->set_node_state({pc.primary}, false);
        svc->set_filter(default_filter);
        return nullptr;
    });

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    PROPOSAL_FLAG_CHECK;
    CONDITION_CHECK([&] { return pc.primary == nodes[1]; });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    state->_nodes.clear();
    pc.primary.set_invalid();
    pc.secondaries.clear();
    pc.last_drops = {nodes[0], nodes[1], nodes[2]};
    pc.ballot = 4;
    state->initialize_node_state();
    svc->set_node_state(nodes, true);

    svc->set_filter([&](const dsn::rpc_address &target, dsn::message_ex *req) -> cur_ptr {
        dsn::message_ex *recv_request = create_corresponding_receive(req);
        cur_ptr update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);
        destroy_message(recv_request);

        EXPECT_EQ(update_req->type, config_type::CT_ASSIGN_PRIMARY);
        EXPECT_EQ(update_req->node, nodes[2]);
        EXPECT_EQ(target, nodes[2]);

        proposal_sent = true;
        svc->set_filter(default_filter);
        apply_update_request(*update_req);
        return update_req;
    });

    std::cerr << "Case: recover from DDD state, nodes[1] isn't alive" << std::endl;
    svc->set_node_state({nodes[1]}, false);
    cc.dropped = {
        dropped_replica{nodes[0], dropped_replica::INVALID_TIMESTAMP, 1, 1, 1},
        dropped_replica{nodes[1], dropped_replica::INVALID_TIMESTAMP, 1, 1, 1},
        dropped_replica{nodes[2], dropped_replica::INVALID_TIMESTAMP, 1, 1, 1},
    };
    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    ASSERT_FALSE(proposal_sent);
    CONDITION_CHECK([&] { return pc.primary.is_invalid(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::cerr << "Case: recover from DDD state, nodes[2] is not in dropped" << std::endl;
    svc->set_node_state({nodes[1]}, true);
    cc.dropped = {dropped_replica{nodes[0], dropped_replica::INVALID_TIMESTAMP, 1, 1, 1},
                  dropped_replica{nodes[1], dropped_replica::INVALID_TIMESTAMP, 1, 1, 1}};

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    ASSERT_FALSE(proposal_sent);
    CONDITION_CHECK([&] { return pc.primary.is_invalid(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::cerr << "Case: recover from DDD state, haven't collect nodes[2]'s info from replica, and "
                 "nodes[2]'s info haven't updated"
              << std::endl;
    cc.dropped = {dropped_replica{nodes[0], dropped_replica::INVALID_TIMESTAMP, 1, 1, 1},
                  dropped_replica{nodes[1], dropped_replica::INVALID_TIMESTAMP, 1, 1, 1},
                  dropped_replica{nodes[2], 500, -1, -1, -1}};

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    ASSERT_FALSE(proposal_sent);
    CONDITION_CHECK([&] { return pc.primary.is_invalid(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::cerr << "Case: recover from DDD state, haven't collect nodes[2]'s info from replica, and "
                 "nodes[2]'s info have updated"
              << std::endl;
    svc->set_filter([&](const dsn::rpc_address &target, dsn::message_ex *req) -> cur_ptr {
        dsn::message_ex *recv_request = create_corresponding_receive(req);
        cur_ptr update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);
        destroy_message(recv_request);

        EXPECT_EQ(update_req->type, config_type::CT_ASSIGN_PRIMARY);
        EXPECT_EQ(update_req->node, nodes[1]);
        EXPECT_EQ(target, nodes[1]);

        proposal_sent = true;
        svc->set_filter(default_filter);
        apply_update_request(*update_req);
        return update_req;
    });

    cc.dropped = {dropped_replica{nodes[0], dropped_replica::INVALID_TIMESTAMP, 1, 1, 1},
                  dropped_replica{nodes[1], dropped_replica::INVALID_TIMESTAMP, 1, 1, 1},
                  dropped_replica{nodes[2], 500, -1, -1, -1}};
    pc.last_committed_decree = 0;
    get_node_state(state->_nodes, nodes[2], false)->set_replicas_collect_flag(true);
    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);

    t->wait();
    PROPOSAL_FLAG_CHECK;
    CONDITION_CHECK([&] { return pc.primary == nodes[1]; });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::cerr << "Case: recover from DDD, haven't collect nodes[1/2]'s info from replica, and "
                 "nodes[1/2]'s info both have updated"
              << std::endl;
    cc.dropped = {dropped_replica{nodes[0], dropped_replica::INVALID_TIMESTAMP, 1, 1, 1},
                  dropped_replica{nodes[1], 500, -1, -1, -1},
                  dropped_replica{nodes[2], 500, -1, -1, -1}};
    get_node_state(state->_nodes, nodes[1], false)->set_replicas_collect_flag(true);
    get_node_state(state->_nodes, nodes[2], false)->set_replicas_collect_flag(true);

    pc.primary.set_invalid();
    pc.secondaries.clear();
    pc.last_drops = {nodes[0], nodes[1], nodes[2]};

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    ASSERT_FALSE(proposal_sent);
    CONDITION_CHECK([&] { return pc.primary.is_invalid(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::cerr << "Case: recover from DDD state, larger ballot not match with larger decree"
              << std::endl;
    cc.dropped = {
        dropped_replica{nodes[0], dropped_replica::INVALID_TIMESTAMP, 1, 1, 1},
        dropped_replica{nodes[1], dropped_replica::INVALID_TIMESTAMP, 1, 0, 1},
        dropped_replica{nodes[2], dropped_replica::INVALID_TIMESTAMP, 0, 1, 1},
    };

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    ASSERT_FALSE(proposal_sent);
    CONDITION_CHECK([&] { return pc.primary.is_invalid(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::cerr << "Case: recover from DDD state, committed decree less than meta's" << std::endl;
    cc.dropped = {
        dropped_replica{nodes[0], dropped_replica::INVALID_TIMESTAMP, 1, 1, 1},
        dropped_replica{nodes[1], dropped_replica::INVALID_TIMESTAMP, 1, 10, 15},
        dropped_replica{nodes[2], dropped_replica::INVALID_TIMESTAMP, 1, 15, 15},
    };
    pc.last_committed_decree = 30;
    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    ASSERT_FALSE(proposal_sent);
    CONDITION_CHECK([&] { return pc.primary.is_invalid(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::cerr << "Case: recover from DDD state, select primary from config_context::dropped"
              << std::endl;
    cc.dropped = {
        dropped_replica{nodes[0], 12344, -1, -1, -1},
        dropped_replica{nodes[2], dropped_replica::INVALID_TIMESTAMP, 4, 2, 4},
        dropped_replica{nodes[1], dropped_replica::INVALID_TIMESTAMP, 4, 3, 4},
    };
    pc.last_committed_decree = 2;
    svc->set_filter([&](const dsn::rpc_address &target, dsn::message_ex *req) -> cur_ptr {
        dsn::message_ex *recv_request = create_corresponding_receive(req);
        cur_ptr update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);
        destroy_message(recv_request);

        EXPECT_EQ(update_req->type, config_type::CT_ASSIGN_PRIMARY);
        EXPECT_EQ(update_req->node, nodes[1]);
        EXPECT_EQ(target, nodes[1]);

        proposal_sent = true;
        svc->set_filter(default_filter);
        apply_update_request(*update_req);
        return update_req;
    });

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    PROPOSAL_FLAG_CHECK;
    CONDITION_CHECK([&] { return pc.primary == nodes[1]; });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::cerr << "Case: recover from DDD state, only one primary" << std::endl;
    svc->set_filter([&](const dsn::rpc_address &target, dsn::message_ex *req) -> cur_ptr {
        dsn::message_ex *recv_request = create_corresponding_receive(req);
        cur_ptr update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);
        destroy_message(recv_request);

        EXPECT_EQ(update_req->type, config_type::CT_ASSIGN_PRIMARY);
        EXPECT_EQ(update_req->node, nodes[0]);
        EXPECT_EQ(target, nodes[0]);

        proposal_sent = true;
        svc->set_filter(default_filter);
        apply_update_request(*update_req);
        return update_req;
    });

    pc.primary.set_invalid();
    pc.secondaries.clear();
    pc.last_drops = {nodes[0]};
    state->_nodes.clear();
    pc.ballot = 1;
    state->initialize_node_state();
    svc->set_node_state({nodes[0], nodes[1], nodes[2]}, true);

    t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                              nullptr,
                              std::bind(&server_state::check_all_partitions, state),
                              server_state::sStateHash);
    t->wait();
    PROPOSAL_FLAG_CHECK;
    CONDITION_CHECK([&] { return pc.primary == nodes[0]; });
}

static void check_nodes_loads(node_mapper &nodes)
{
    unsigned int min_primaries = UINT_MAX, min_partitions = UINT_MAX;
    unsigned int max_primaries = 0, max_partitions = 0;
    for (auto &pairs : nodes) {
        const node_state &ns = pairs.second;
        min_primaries = std::min(min_primaries, ns.primary_count());
        min_partitions = std::min(min_partitions, ns.partition_count());
        max_primaries = std::max(max_primaries, ns.primary_count());
        max_partitions = std::max(max_partitions, ns.partition_count());
    }

    ASSERT_TRUE(max_primaries - min_primaries <= 1);
    ASSERT_TRUE(max_partitions - min_partitions <= 1);
}

void meta_partition_guardian_test::cure()
{
    std::vector<dsn::rpc_address> node_list;
    generate_node_list(node_list, 20, 100);

    app_mapper app;
    node_mapper nodes;
    meta_service svc;
    partition_guardian guardian(&svc);

    dsn::app_info info;
    info.app_id = 1;
    info.is_stateful = true;
    info.status = dsn::app_status::AS_AVAILABLE;
    info.app_name = "test";
    info.app_type = "test";
    info.max_replica_count = 3;
    info.partition_count = 1024;
    std::shared_ptr<app_state> the_app = app_state::create(info);

    app.emplace(the_app->app_id, the_app);
    for (const auto &address : node_list) {
        get_node_state(nodes, address, true)->set_alive(true);
    }

    bool all_partitions_healthy = false;
    while (!all_partitions_healthy) {
        configuration_proposal_action action;
        pc_status status;
        all_partitions_healthy = true;

        for (int i = 0; i != the_app->partition_count; ++i) {
            dsn::gpid &pid = the_app->partitions[i].pid;
            status = guardian.cure({&app, &nodes}, pid, action);
            if (status != pc_status::healthy) {
                all_partitions_healthy = false;
                proposal_action_check_and_apply(action, pid, app, nodes, nullptr);

                configuration_update_request fake_request;
                fake_request.info = *the_app;
                fake_request.config = the_app->partitions[i];
                fake_request.type = action.type;
                fake_request.node = action.node;
                fake_request.host_node = action.node;

                guardian.reconfig({&app, &nodes}, fake_request);
                check_nodes_loads(nodes);
            }
        }
    }
}

void meta_partition_guardian_test::from_proposal_test()
{
    std::vector<dsn::rpc_address> node_list;
    generate_node_list(node_list, 3, 3);

    app_mapper app;
    node_mapper nodes;
    meta_service svc;

    partition_guardian guardian(&svc);

    dsn::app_info info;
    info.app_id = 1;
    info.is_stateful = true;
    info.status = dsn::app_status::AS_AVAILABLE;
    info.app_name = "test";
    info.app_type = "test";
    info.max_replica_count = 3;
    info.partition_count = 1;
    std::shared_ptr<app_state> the_app = app_state::create(info);

    app.emplace(the_app->app_id, the_app);
    for (const dsn::rpc_address &addr : node_list) {
        get_node_state(nodes, addr, true)->set_alive(true);
    }

    meta_view mv{&app, &nodes};
    dsn::gpid p(1, 0);
    configuration_proposal_action cpa;
    configuration_proposal_action cpa2;

    dsn::partition_configuration &pc = *get_config(app, p);
    config_context &cc = *get_config_context(app, p);

    std::cerr << "Case 1: test no proposals in config_context" << std::endl;
    ASSERT_FALSE(guardian.from_proposals(mv, p, cpa));
    ASSERT_EQ(config_type::CT_INVALID, cpa.type);

    std::cerr << "Case 2: test invalid proposal: invalid target" << std::endl;
    cpa2 =
        new_proposal_action(dsn::rpc_address(), node_list[0], config_type::CT_UPGRADE_TO_PRIMARY);
    cc.lb_actions.assign_balancer_proposals({cpa2});
    ASSERT_FALSE(guardian.from_proposals(mv, p, cpa));
    ASSERT_EQ(config_type::CT_INVALID, cpa.type);

    std::cerr << "Case 3: test invalid proposal: invalid node" << std::endl;
    cpa2 =
        new_proposal_action(node_list[0], dsn::rpc_address(), config_type::CT_UPGRADE_TO_PRIMARY);
    cc.lb_actions.assign_balancer_proposals({cpa2});
    ASSERT_FALSE(guardian.from_proposals(mv, p, cpa));
    ASSERT_EQ(config_type::CT_INVALID, cpa.type);

    std::cerr << "Case 4: test invalid proposal: dead target" << std::endl;
    cpa2 = new_proposal_action(node_list[0], node_list[0], config_type::CT_UPGRADE_TO_PRIMARY);
    cc.lb_actions.assign_balancer_proposals({cpa2});
    get_node_state(nodes, node_list[0], false)->set_alive(false);
    ASSERT_FALSE(guardian.from_proposals(mv, p, cpa));
    ASSERT_EQ(config_type::CT_INVALID, cpa.type);
    get_node_state(nodes, node_list[0], false)->set_alive(true);

    std::cerr << "Case 5: test invalid proposal: dead node" << std::endl;
    cpa2 = new_proposal_action(node_list[0], node_list[1], config_type::CT_ADD_SECONDARY);
    cc.lb_actions.assign_balancer_proposals({cpa2});
    get_node_state(nodes, node_list[1], false)->set_alive(false);
    ASSERT_FALSE(guardian.from_proposals(mv, p, cpa));
    ASSERT_EQ(config_type::CT_INVALID, cpa.type);
    get_node_state(nodes, node_list[1], false)->set_alive(true);

    std::cerr << "Case 6: test invalid proposal: already have priamry but assign" << std::endl;
    cpa2 = new_proposal_action(node_list[0], node_list[0], config_type::CT_ASSIGN_PRIMARY);
    cc.lb_actions.assign_balancer_proposals({cpa2});
    pc.primary = node_list[1];
    ASSERT_FALSE(guardian.from_proposals(mv, p, cpa));
    ASSERT_EQ(config_type::CT_INVALID, cpa.type);

    std::cerr << "Case 7: test invalid proposal: upgrade non-secondary" << std::endl;
    cpa2 = new_proposal_action(node_list[0], node_list[0], config_type::CT_UPGRADE_TO_PRIMARY);
    cc.lb_actions.assign_balancer_proposals({cpa2});
    pc.primary.set_invalid();
    ASSERT_FALSE(guardian.from_proposals(mv, p, cpa));
    ASSERT_EQ(config_type::CT_INVALID, cpa.type);

    std::cerr << "Case 8: test invalid proposal: add exist secondary" << std::endl;
    cpa2 = new_proposal_action(node_list[0], node_list[1], config_type::CT_ADD_SECONDARY);
    cc.lb_actions.assign_balancer_proposals({cpa2});
    pc.primary = node_list[0];
    pc.secondaries = {node_list[1]};
    ASSERT_FALSE(guardian.from_proposals(mv, p, cpa));
    ASSERT_EQ(config_type::CT_INVALID, cpa.type);

    std::cerr << "Case 9: test invalid proposal: downgrade non member" << std::endl;
    cpa2 = new_proposal_action(node_list[0], node_list[1], config_type::CT_REMOVE);
    cc.lb_actions.assign_balancer_proposals({cpa2});
    pc.primary = node_list[0];
    pc.secondaries.clear();
    ASSERT_FALSE(guardian.from_proposals(mv, p, cpa));
    ASSERT_EQ(config_type::CT_INVALID, cpa.type);

    std::cerr << "Case 10: test abnormal learning detect" << std::endl;
    cpa2 = new_proposal_action(node_list[0], node_list[1], config_type::CT_ADD_SECONDARY);
    pc.primary = node_list[0];
    pc.secondaries.clear();
    cc.lb_actions.assign_balancer_proposals({cpa2});

    replica_info i;
    i.pid = p;
    i.status = partition_status::PS_POTENTIAL_SECONDARY;
    i.ballot = 10;
    i.last_durable_decree = 10;
    i.last_committed_decree = 10;
    i.last_prepared_decree = 10;

    collect_replica(mv, node_list[1], i);
    ASSERT_TRUE(guardian.from_proposals(mv, p, cpa));
    ASSERT_EQ(config_type::CT_ADD_SECONDARY, cpa.type);

    i.status = partition_status::PS_ERROR;
    collect_replica(mv, node_list[1], i);
    ASSERT_FALSE(guardian.from_proposals(mv, p, cpa));
    ASSERT_EQ(config_type::CT_INVALID, cpa.type);
}
} // namespace replication
} // namespace dsn
