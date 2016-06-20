#include <gtest/gtest.h>

#include <dsn/service_api_c.h>
#include <dsn/service_api_cpp.h>

#include "meta_service.h"
#include "server_state.h"
#include "greedy_load_balancer.h"
#include "meta_service_test_app.h"
#include "../misc/misc.h"

using namespace dsn::replication;

class fake_sender_meta_service: public dsn::replication::meta_service {
private:
    meta_service_test_app* _app;
public:
    fake_sender_meta_service(meta_service_test_app* app): meta_service(), _app(app)
    {
    }

    virtual void reply_message(dsn_message_t request, dsn_message_t response) override {}
    virtual void send_message(const dsn::rpc_address& target, dsn_message_t request) override
    {
        //we expect this is a configuration_update_request proposal
        dsn_message_t recv_request = create_corresponding_receive(request);

        std::shared_ptr<configuration_update_request> update_req = std::make_shared<configuration_update_request>();
        ::dsn::unmarshall(recv_request, *update_req);

        dsn_msg_add_ref(request);
        dsn_msg_release_ref(request);

        dsn_msg_add_ref(recv_request);
        dsn_msg_release_ref(recv_request);

        dsn::partition_configuration& pc = update_req->config;
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

void meta_service_test_app::call_update_configuration(meta_service *svc,
    std::shared_ptr<dsn::replication::configuration_update_request> &request)
{
    dsn_message_t fake_request = dsn_msg_create_request(RPC_CM_UPDATE_PARTITION_CONFIGURATION);
    ::dsn::marshall(fake_request, *request);
    dsn_msg_add_ref(fake_request);

    dsn::tasking::enqueue(
        LPC_META_STATE_HIGH,
        nullptr,
        std::bind(&server_state::on_update_configuration, svc->_state.get(), request, fake_request),
        server_state::s_state_write_hash,
        std::chrono::milliseconds( random32(1, 1000) )
    );
}

bool meta_service_test_app::wait_state(
    server_state *ss,
    const state_validator& validator,
    int time)
{
    for (int i=0; i!=time; )
    {
        dsn::task_ptr t = dsn::tasking::enqueue(
            LPC_META_STATE_NORMAL,
            nullptr,
            std::bind(&server_state::check_all_partitions, ss),
            server_state::s_state_write_hash,
            std::chrono::seconds(1)
        );
        t->wait();

        {
            zauto_read_lock l(ss->_lock);
            if (validator(ss->_all_apps))
                return true;
        }
        if (time!=-1)
            ++i;
    }
    return false;
}

void meta_service_test_app::update_configuration_test()
{
    dsn::error_code ec;
    std::shared_ptr<fake_sender_meta_service> svc(new fake_sender_meta_service(this));
    ec = svc->remote_storage_initialize();
    ASSERT_EQ(ec, dsn::ERR_OK);
    svc->_balancer.reset(new simple_load_balancer(svc.get()));

    server_state* ss = svc->_state.get();
    ss->initialize(svc.get(), meta_options::concat_path_unix_style(svc->_cluster_root, "apps"));
    std::shared_ptr<app_state> app = app_state::create("simple_kv.instance0", "simple_kv", 1);
    app->init_partitions(2, 3);
    ss->_all_apps.emplace(1, app);

    std::vector<dsn::rpc_address> nodes;
    generate_node_list(nodes, 4, 4);

    dsn::partition_configuration& pc0 = app->partitions[0];
    pc0.primary = nodes[0];
    pc0.secondaries.push_back(nodes[1]);
    pc0.secondaries.push_back(nodes[2]);
    pc0.ballot = 3;

    dsn::partition_configuration& pc1 = app->partitions[1];
    pc1.primary = nodes[1];
    pc1.secondaries.push_back(nodes[0]);
    pc1.secondaries.push_back(nodes[2]);
    pc1.ballot = 3;

    ss->sync_apps_to_remote_storage();
    ASSERT_TRUE(ss->spin_wait_creating(30));
    ss->initialize_node_state();
    svc->set_node_state({nodes[0], nodes[1], nodes[2]}, true);
    svc->_started = true;

    //test remove primary
    state_validator validator1 = [pc0](const app_mapper& apps)
    {
        const dsn::partition_configuration* pc = get_config(apps, pc0.pid);
        return pc->ballot==pc0.ballot+2 && pc->secondaries.size()==1 &&
            std::find(pc0.secondaries.begin(), pc0.secondaries.end(), pc->primary)!=pc0.secondaries.end();
    };

    //test kickoff secondary
    dsn::rpc_address addr = nodes[0];
    state_validator validator2 = [pc1, addr](const app_mapper& apps) {
        const dsn::partition_configuration* pc = get_config(apps, pc1.pid);
        return pc->ballot==pc1.ballot+1 && pc->secondaries.size()==1 &&
            pc->secondaries.front()!=addr;
    };

    svc->set_node_state({nodes[0]}, false);
    ASSERT_TRUE(wait_state(ss, validator1, 30));
    ASSERT_TRUE(wait_state(ss, validator2, 30));

    //test add secondary
    svc->set_node_state({nodes[3]}, true);
    state_validator validator3 = [pc0](const app_mapper& apps) {
        const dsn::partition_configuration* pc = get_config(apps, pc0.pid);
        return pc->ballot==pc0.ballot+1 && pc->secondaries.size()==2;
    };
    //the default delay for add node is 5 miniutes
    ASSERT_FALSE(wait_state(ss, validator3, 10));
    svc->_meta_opts.replica_assign_delay_ms_for_dropouts = 0;
    svc->_balancer.reset(new simple_load_balancer(svc.get()));
    ASSERT_TRUE(wait_state(ss, validator3, 10));
}

static void generate_apps(app_mapper& mapper, const std::vector<dsn::rpc_address>& node_list)
{
    mapper.clear();
    for (int i=1; i<=5; ++i)
    {
        std::shared_ptr<app_state> app = app_state::create("test_app" + boost::lexical_cast<std::string>(i), "simple_kv", i);
        generate_app(app, node_list, random32(2, 5));
        mapper.emplace(app->app_id, app);
    }
}

static void clone_app_mapper(app_mapper& output, const app_mapper& input)
{
    output.clear();
    for (auto& iter: input)
    {
        const std::shared_ptr<app_state>& old_app = iter.second;
        std::shared_ptr<app_state> new_app = app_state::create(old_app->app_name, old_app->app_type, old_app->app_id);
        new_app->init_partitions(old_app->partition_count, old_app->partitions[0].max_replica_count);
        for (unsigned int i=0; i!=old_app->partition_count; ++i)
            new_app->partitions[i] = old_app->partitions[i];
        new_app->status = dsn::app_status::AS_AVAILABLE;
        output.emplace(new_app->app_id, new_app);
    }
}

void meta_service_test_app::apply_balancer_test()
{
    dsn::error_code ec;
    fake_sender_meta_service* meta_svc = new fake_sender_meta_service(this);
    ec = meta_svc->remote_storage_initialize();    
    ASSERT_EQ(dsn::ERR_OK, ec);

    meta_svc->_balancer.reset(new greedy_load_balancer(meta_svc));

    //initialize data structure
    std::vector<dsn::rpc_address> node_list;
    generate_node_list(node_list, 5, 10);

    server_state* ss = meta_svc->_state.get();
    generate_apps(ss->_all_apps, node_list);

    app_mapper backed_app;
    node_mapper backed_nodes;
    clone_app_mapper(backed_app, ss->_all_apps);
    generate_node_mapper(backed_nodes, backed_app, node_list);

    ss->initialize(meta_svc, "/meta_test/apps");
    ASSERT_EQ(dsn::ERR_OK, meta_svc->_state->sync_apps_to_remote_storage());
    ASSERT_TRUE(ss->spin_wait_creating(30));
    ss->initialize_node_state();

    meta_svc->_started = true;
    meta_svc->set_node_state(node_list, true);

    app_mapper_compare(backed_app, ss->_all_apps);
    //run balancer
    bool result;

    auto migration_actions = [&backed_app, &backed_nodes](const migration_list& list)
    {
        migration_list result;
        for (auto& iter: list)
        {
            std::shared_ptr<configuration_balancer_request> req = std::make_shared<configuration_balancer_request>(*iter);
            result.emplace_back(req);
        }
        migration_check_and_apply(backed_app, backed_nodes, result);
    };

    ss->set_replica_migration_subscriber_for_test(migration_actions);
    while (true)
    {
        dsn::task_ptr tsk = dsn::tasking::enqueue(
            LPC_META_STATE_NORMAL,
            nullptr,
            [&result, ss]() { result = ss->check_all_partitions(); },
            server_state::s_state_write_hash
            );
        tsk->wait();
        if (result)
            break;
        else
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    app_mapper_compare(backed_app, ss->_all_apps);
}
