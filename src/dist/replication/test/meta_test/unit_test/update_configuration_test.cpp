#include <gtest/gtest.h>

#include <dsn/service_api_c.h>
#include <dsn/service_api_cpp.h>
#include <dsn/tool-api/zlocks.h>

#include "dist/replication/meta_server/meta_service.h"
#include "dist/replication/meta_server/server_state.h"
#include "dist/replication/meta_server/greedy_load_balancer.h"
#include "dist/replication/meta_server/meta_server_failure_detector.h"
#include "dist/replication/test/meta_test/misc/misc.h"

#include "meta_service_test_app.h"

using namespace dsn::replication;

class fake_sender_meta_service : public dsn::replication::meta_service
{
private:
    meta_service_test_app *_app;

public:
    fake_sender_meta_service(meta_service_test_app *app) : meta_service(), _app(app) {}

    virtual void reply_message(dsn::message_ex *request, dsn::message_ex *response) override {}
    virtual void send_message(const dsn::rpc_address &target, dsn::message_ex *request) override
    {
        // we expect this is a configuration_update_request proposal
        dsn::message_ex *recv_request = create_corresponding_receive(request);

        std::shared_ptr<configuration_update_request> update_req =
            std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);

        destroy_message(request);
        destroy_message(recv_request);

        dsn::partition_configuration &pc = update_req->config;
        pc.ballot++;

        switch (update_req->type) {
        case config_type::CT_ASSIGN_PRIMARY:
        case config_type::CT_UPGRADE_TO_PRIMARY:
            pc.primary = update_req->node;
            replica_helper::remove_node(update_req->node, pc.secondaries);
            break;

        case config_type::CT_ADD_SECONDARY:
        case config_type::CT_ADD_SECONDARY_FOR_LB:
            pc.secondaries.push_back(update_req->node);
            update_req->type = config_type::CT_UPGRADE_TO_SECONDARY;
            break;

        case config_type::CT_REMOVE:
        case config_type::CT_DOWNGRADE_TO_INACTIVE:
            if (update_req->node == pc.primary)
                pc.primary.set_invalid();
            else
                replica_helper::remove_node(update_req->node, pc.secondaries);
            break;

        case config_type::CT_DOWNGRADE_TO_SECONDARY:
            pc.secondaries.push_back(pc.primary);
            pc.primary.set_invalid();
            break;
        default:
            break;
        }

        _app->call_update_configuration(this, update_req);
    }
};

class null_meta_service : public dsn::replication::meta_service
{
public:
    void send_message(const dsn::rpc_address &target, dsn::message_ex *request)
    {
        ddebug("send request to %s", target.to_string());
        request->add_ref();
        request->release_ref();
    }
};

class dummy_balancer : public dsn::replication::server_load_balancer
{
public:
    dummy_balancer(meta_service *s) : server_load_balancer(s) {}
    virtual pc_status
    cure(meta_view view, const dsn::gpid &gpid, configuration_proposal_action &action)
    {
        action.type = config_type::CT_INVALID;
        const dsn::partition_configuration &pc = *get_config(*view.apps, gpid);
        if (!pc.primary.is_invalid() && pc.secondaries.size() == 2)
            return pc_status::healthy;
        return pc_status::ill;
    }
    virtual void reconfig(meta_view view, const configuration_update_request &request) {}
    virtual bool balance(meta_view view, migration_list &list) { return false; }
    virtual bool check(meta_view view, migration_list &list) { return false; }
    virtual void report(const migration_list &list, bool balance_checker) {}
    virtual std::string get_balance_operation_count(const std::vector<std::string> &args)
    {
        return std::string("unknown");
    }
    virtual void score(meta_view view, double &primary_stddev, double &total_stddev) {}
    virtual bool
    collect_replica(meta_view view, const dsn::rpc_address &node, const replica_info &info)
    {
        return false;
    }
    virtual bool construct_replica(meta_view view, const dsn::gpid &pid, int max_replica_count)
    {
        return false;
    }
};

void meta_service_test_app::call_update_configuration(
    meta_service *svc, std::shared_ptr<dsn::replication::configuration_update_request> &request)
{
    dsn::message_ex *fake_request =
        dsn::message_ex::create_request(RPC_CM_UPDATE_PARTITION_CONFIGURATION);
    ::dsn::marshall(fake_request, *request);
    fake_request->add_ref();

    dsn::tasking::enqueue(
        LPC_META_STATE_HIGH,
        nullptr,
        std::bind(&server_state::on_update_configuration, svc->_state.get(), request, fake_request),
        server_state::sStateHash);
}

void meta_service_test_app::call_config_sync(
    meta_service *svc, std::shared_ptr<configuration_query_by_node_request> &request)
{
    dsn::message_ex *fake_request = dsn::message_ex::create_request(RPC_CM_CONFIG_SYNC);
    ::dsn::marshall(fake_request, *request);

    dsn::message_ex *recvd_request = create_corresponding_receive(fake_request);
    recvd_request->add_ref();

    destroy_message(fake_request);

    dsn::tasking::enqueue(
        LPC_META_STATE_HIGH,
        nullptr,
        std::bind(&server_state::on_config_sync, svc->_state.get(), recvd_request),
        server_state::sStateHash);
}

bool meta_service_test_app::wait_state(server_state *ss, const state_validator &validator, int time)
{
    for (int i = 0; i != time;) {
        dsn::task_ptr t = dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                                                nullptr,
                                                std::bind(&server_state::check_all_partitions, ss),
                                                server_state::sStateHash,
                                                std::chrono::seconds(1));
        t->wait();

        {
            dsn::zauto_read_lock l(ss->_lock);
            if (validator(ss->_all_apps))
                return true;
        }
        if (time != -1)
            ++i;
    }
    return false;
}

void meta_service_test_app::update_configuration_test()
{
    dsn::error_code ec;
    std::shared_ptr<fake_sender_meta_service> svc(new fake_sender_meta_service(this));
    svc->_failure_detector.reset(new dsn::replication::meta_server_failure_detector(svc.get()));
    ec = svc->remote_storage_initialize();
    ASSERT_EQ(ec, dsn::ERR_OK);
    svc->_balancer.reset(new simple_load_balancer(svc.get()));

    server_state *ss = svc->_state.get();
    ss->initialize(svc.get(), meta_options::concat_path_unix_style(svc->_cluster_root, "apps"));
    dsn::app_info info;
    info.is_stateful = true;
    info.status = dsn::app_status::AS_CREATING;
    info.app_id = 1;
    info.app_name = "simple_kv.instance0";
    info.app_type = "simple_kv";
    info.max_replica_count = 3;
    info.partition_count = 2;
    std::shared_ptr<app_state> app = app_state::create(info);

    ss->_all_apps.emplace(1, app);

    std::vector<dsn::rpc_address> nodes;
    generate_node_list(nodes, 4, 4);

    dsn::partition_configuration &pc0 = app->partitions[0];
    pc0.primary = nodes[0];
    pc0.secondaries.push_back(nodes[1]);
    pc0.secondaries.push_back(nodes[2]);
    pc0.ballot = 3;

    dsn::partition_configuration &pc1 = app->partitions[1];
    pc1.primary = nodes[1];
    pc1.secondaries.push_back(nodes[0]);
    pc1.secondaries.push_back(nodes[2]);
    pc1.ballot = 3;

    ss->sync_apps_to_remote_storage();
    ASSERT_TRUE(ss->spin_wait_staging(30));
    ss->initialize_node_state();
    svc->set_node_state({nodes[0], nodes[1], nodes[2]}, true);
    svc->_started = true;

    // test remove primary
    state_validator validator1 = [pc0](const app_mapper &apps) {
        const dsn::partition_configuration *pc = get_config(apps, pc0.pid);
        return pc->ballot == pc0.ballot + 2 && pc->secondaries.size() == 1 &&
               std::find(pc0.secondaries.begin(), pc0.secondaries.end(), pc->primary) !=
                   pc0.secondaries.end();
    };

    // test kickoff secondary
    dsn::rpc_address addr = nodes[0];
    state_validator validator2 = [pc1, addr](const app_mapper &apps) {
        const dsn::partition_configuration *pc = get_config(apps, pc1.pid);
        return pc->ballot == pc1.ballot + 1 && pc->secondaries.size() == 1 &&
               pc->secondaries.front() != addr;
    };

    svc->set_node_state({nodes[0]}, false);
    ASSERT_TRUE(wait_state(ss, validator1, 30));
    ASSERT_TRUE(wait_state(ss, validator2, 30));

    // test add secondary
    svc->set_node_state({nodes[3]}, true);
    state_validator validator3 = [pc0](const app_mapper &apps) {
        const dsn::partition_configuration *pc = get_config(apps, pc0.pid);
        return pc->ballot == pc0.ballot + 1 && pc->secondaries.size() == 2;
    };
    // the default delay for add node is 5 miniutes
    ASSERT_FALSE(wait_state(ss, validator3, 10));
    svc->_meta_opts._lb_opts.replica_assign_delay_ms_for_dropouts = 0;
    svc->_balancer.reset(new simple_load_balancer(svc.get()));
    ASSERT_TRUE(wait_state(ss, validator3, 10));
}

void meta_service_test_app::adjust_dropped_size()
{
    dsn::error_code ec;
    std::shared_ptr<null_meta_service> svc(new null_meta_service());
    svc->_failure_detector.reset(new dsn::replication::meta_server_failure_detector(svc.get()));
    ec = svc->remote_storage_initialize();
    ASSERT_EQ(ec, dsn::ERR_OK);
    svc->_balancer.reset(new simple_load_balancer(svc.get()));

    server_state *ss = svc->_state.get();
    ss->initialize(svc.get(), meta_options::concat_path_unix_style(svc->_cluster_root, "apps"));
    dsn::app_info info;
    info.is_stateful = true;
    info.status = dsn::app_status::AS_CREATING;
    info.app_id = 1;
    info.app_name = "simple_kv.instance0";
    info.app_type = "simple_kv";
    info.max_replica_count = 3;
    info.partition_count = 1;
    std::shared_ptr<app_state> app = app_state::create(info);

    ss->_all_apps.emplace(1, app);

    std::vector<dsn::rpc_address> nodes;
    generate_node_list(nodes, 10, 10);

    // first, the replica is healthy, and there are 2 dropped
    dsn::partition_configuration &pc = app->partitions[0];
    pc.primary = nodes[0];
    pc.secondaries = {nodes[1], nodes[2]};
    pc.ballot = 10;

    config_context &cc = *get_config_context(ss->_all_apps, pc.pid);
    cc.dropped = {
        dropped_replica{nodes[3], dropped_replica::INVALID_TIMESTAMP, 7, 11, 14},
        dropped_replica{nodes[4], 20, invalid_ballot, invalid_decree, invalid_decree},
    };

    ss->sync_apps_to_remote_storage();
    generate_node_mapper(ss->_nodes, ss->_all_apps, nodes);

    // then we receive a request for upgrade a node to secondary
    std::shared_ptr<configuration_update_request> req =
        std::make_shared<configuration_update_request>();
    req->config = pc;
    req->config.ballot++;
    req->config.secondaries.push_back(nodes[5]);
    req->info = info;
    req->node = nodes[5];
    req->type = config_type::CT_UPGRADE_TO_SECONDARY;
    call_update_configuration(svc.get(), req);

    spin_wait_condition([&pc]() { return pc.ballot == 11; }, 10);

    // then receive a config_sync request fro nodes[4], which has less data than node[3]
    std::shared_ptr<configuration_query_by_node_request> req2 =
        std::make_shared<configuration_query_by_node_request>();
    req2->__set_node(nodes[4]);

    replica_info rep_info;
    rep_info.pid = pc.pid;
    rep_info.ballot = 6;
    rep_info.status = partition_status::PS_ERROR;
    rep_info.last_committed_decree = 9;
    rep_info.last_prepared_decree = 10;
    rep_info.last_durable_decree = 5;
    rep_info.app_type = "pegasus";

    req2->__set_stored_replicas({rep_info});
    call_config_sync(svc.get(), req2);

    auto status_check = [&cc, &nodes, &rep_info] {
        if (cc.dropped.size() != 1)
            return false;
        dropped_replica &d = cc.dropped[0];
        if (d.time != dropped_replica::INVALID_TIMESTAMP)
            return false;
        if (d.node != nodes[4])
            return false;
        if (d.last_committed_decree != rep_info.last_committed_decree)
            return false;
        return true;
    };

    spin_wait_condition(status_check, 10);
}

static void clone_app_mapper(app_mapper &output, const app_mapper &input)
{
    output.clear();
    for (auto &iter : input) {
        const std::shared_ptr<app_state> &old_app = iter.second;
        dsn::app_info info = *old_app;
        std::shared_ptr<app_state> new_app = app_state::create(info);
        for (unsigned int i = 0; i != old_app->partition_count; ++i)
            new_app->partitions[i] = old_app->partitions[i];
        output.emplace(new_app->app_id, new_app);
    }
}

void meta_service_test_app::apply_balancer_test()
{
    dsn::error_code ec;
    std::shared_ptr<fake_sender_meta_service> meta_svc(new fake_sender_meta_service(this));
    ec = meta_svc->remote_storage_initialize();
    ASSERT_EQ(dsn::ERR_OK, ec);

    meta_svc->_failure_detector.reset(
        new dsn::replication::meta_server_failure_detector(meta_svc.get()));
    meta_svc->_balancer.reset(new greedy_load_balancer(meta_svc.get()));

    // initialize data structure
    std::vector<dsn::rpc_address> node_list;
    generate_node_list(node_list, 5, 10);

    server_state *ss = meta_svc->_state.get();
    generate_apps(ss->_all_apps, node_list, 5, 5, std::pair<uint32_t, uint32_t>(2, 5), false);

    app_mapper backed_app;
    node_mapper backed_nodes;

    clone_app_mapper(backed_app, ss->_all_apps);
    generate_node_mapper(backed_nodes, backed_app, node_list);

    // before initialize, we need to mark apps to AS_CREATING:
    for (auto &kv : ss->_all_apps) {
        kv.second->status = dsn::app_status::AS_CREATING;
    }
    ss->initialize(meta_svc.get(), "/meta_test/apps");
    ASSERT_EQ(dsn::ERR_OK, meta_svc->_state->sync_apps_to_remote_storage());
    ASSERT_TRUE(ss->spin_wait_staging(30));
    ss->initialize_node_state();

    meta_svc->_started = true;
    meta_svc->set_node_state(node_list, true);

    app_mapper_compare(backed_app, ss->_all_apps);
    // run balancer
    bool result;

    auto migration_actions = [&backed_app, &backed_nodes](const migration_list &list) {
        migration_list result;
        for (auto &iter : list) {
            std::shared_ptr<configuration_balancer_request> req =
                std::make_shared<configuration_balancer_request>(*(iter.second));
            result.emplace(iter.first, req);
        }
        migration_check_and_apply(backed_app, backed_nodes, result, nullptr);
    };

    ss->set_replica_migration_subscriber_for_test(migration_actions);
    while (true) {
        dsn::task_ptr tsk =
            dsn::tasking::enqueue(LPC_META_STATE_NORMAL,
                                  nullptr,
                                  [&result, ss]() { result = ss->check_all_partitions(); },
                                  server_state::sStateHash);
        tsk->wait();
        if (result)
            break;
        else
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    app_mapper_compare(backed_app, ss->_all_apps);
}

void meta_service_test_app::cannot_run_balancer_test()
{
    std::shared_ptr<null_meta_service> svc(new null_meta_service());
    svc->_meta_opts.min_live_node_count_for_unfreeze = 0;
    svc->_meta_opts.node_live_percentage_threshold_for_update = 0;

    svc->_state->initialize(svc.get(), "/");
    svc->_failure_detector.reset(new meta_server_failure_detector(svc.get()));
    svc->_balancer.reset(new dummy_balancer(svc.get()));

    std::vector<dsn::rpc_address> nodes;
    generate_node_list(nodes, 10, 10);

    dsn::app_info info;
    info.app_id = 1;
    info.app_name = "test";
    info.app_type = "pegasus";
    info.expire_second = 0;
    info.is_stateful = true;
    info.max_replica_count = 3;
    info.partition_count = 1;
    info.status = dsn::app_status::AS_AVAILABLE;

    std::shared_ptr<app_state> the_app = app_state::create(info);
    svc->_state->_all_apps.emplace(info.app_id, the_app);
    svc->_state->_exist_apps.emplace(info.app_name, the_app);

    dsn::partition_configuration &pc = the_app->partitions[0];
    pc.primary = nodes[0];
    pc.secondaries = {nodes[1], nodes[2]};

#define REGENERATE_NODE_MAPPER                                                                     \
    svc->_state->_nodes.clear();                                                                   \
    generate_node_mapper(svc->_state->_nodes, svc->_state->_all_apps, nodes)

    REGENERATE_NODE_MAPPER;
    // stage are freezed
    svc->_function_level.store(meta_function_level::fl_freezed);
    ASSERT_FALSE(svc->_state->check_all_partitions());

    // stage are steady
    svc->_function_level.store(meta_function_level::fl_steady);
    ASSERT_FALSE(svc->_state->check_all_partitions());

    // all the partitions are not healthy
    svc->_function_level.store(meta_function_level::fl_lively);
    pc.primary.set_invalid();
    REGENERATE_NODE_MAPPER;

    ASSERT_FALSE(svc->_state->check_all_partitions());

    // some dropped node still exists in nodes
    pc.primary = nodes[0];
    REGENERATE_NODE_MAPPER;
    get_node_state(svc->_state->_nodes, pc.primary, true)->set_alive(false);
    ASSERT_FALSE(svc->_state->check_all_partitions());

    // some apps are staging
    REGENERATE_NODE_MAPPER;
    the_app->status = dsn::app_status::AS_DROPPING;
    ASSERT_FALSE(svc->_state->check_all_partitions());

    // call function can run balancer
    the_app->status = dsn::app_status::AS_AVAILABLE;
    ASSERT_TRUE(svc->_state->can_run_balancer());
}
