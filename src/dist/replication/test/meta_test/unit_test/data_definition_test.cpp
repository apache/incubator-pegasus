#include <gtest/gtest.h>

#include <dsn/service_api_c.h>
#include <dsn/service_api_cpp.h>

#include <atomic>

#include "dist/replication/meta_server/meta_service.h"
#include "dist/replication/meta_server/server_state.h"
#include "dist/replication/meta_server/meta_server_failure_detector.h"
#include "dist/replication/meta_server/server_load_balancer.h"
#include "dist/replication/test/meta_test/misc/misc.h"

#include "meta_service_test_app.h"

using namespace dsn;
using namespace dsn::replication;

dsn::message_ex *create_corresponding_receive(dsn::message_ex *request_msg)
{
    return request_msg->copy(true, true);
}

#define fake_create_app(state, request_data)                                                       \
    fake_rpc_call(                                                                                 \
        RPC_CM_CREATE_APP, LPC_META_STATE_NORMAL, state, &server_state::create_app, request_data)

#define fake_drop_app(state, request_data)                                                         \
    fake_rpc_call(                                                                                 \
        RPC_CM_DROP_APP, LPC_META_STATE_NORMAL, state, &server_state::drop_app, request_data)

#define fake_recall_app(state, request_data)                                                       \
    fake_rpc_call(                                                                                 \
        RPC_CM_RECALL_APP, LPC_META_STATE_NORMAL, state, &server_state::recall_app, request_data)

inline void test_logger(const char *str)
{
    fprintf(stderr, "%s", str);
    fprintf(stderr, "\n");
}

//
// NOTICE:
// this test is run by fault-injector enabled. And in fault injector, the remote-storage visit task
// is delayed. So we expect all successive rpc-requests are handled before a remote-storage visit.
//
#define clear_test_state()                                                                         \
    nodes->clear();                                                                                \
    for (partition_configuration & pc : default_app->partitions) {                                 \
        pc.primary.set_invalid();                                                                  \
        pc.secondaries.clear();                                                                    \
        pc.last_drops.clear();                                                                     \
    }

void meta_service_test_app::data_definition_op_test()
{
    std::shared_ptr<fake_receiver_meta_service> svc =
        std::make_shared<fake_receiver_meta_service>();
    dsn::error_code ec = svc->remote_storage_initialize();
    ASSERT_EQ(ec, dsn::ERR_OK);

    svc->_balancer.reset(new simple_load_balancer(svc.get()));
    svc->_failure_detector.reset(new meta_server_failure_detector(svc.get()));
    server_state *state = svc->_state.get();

    state->initialize(svc.get(), svc->_cluster_root + "/apps");
    ec = state->initialize_data_structure();
    ASSERT_EQ(ec, dsn::ERR_OK);

    svc->_started = true;

    app_mapper *apps = state->get_meta_view().apps;

    std::vector<rpc_address> server_nodes;
    generate_node_list(server_nodes, 5, 10);

    std::shared_ptr<reply_context> result, result2, result3;
    configuration_create_app_request create_request;
    configuration_create_app_response create_response;
    int last_app_id;

    const std::string test_app2_name = "test_app2";
    const std::string test_app2_recalled_name = "test_app2_recalled";

    // normal create app
    create_request.app_name = test_app2_name;
    create_request.options.app_type = "simple_kv";
    create_request.options.partition_count = 0;
    create_request.options.replica_count = 0;
    create_request.options.success_if_exist = false;
    create_request.options.is_stateful = true;

    {
        test_logger("create app with invalid partition_count");
        result = fake_create_app(state, create_request);
        fake_wait_rpc(result, create_response);
        ASSERT_EQ(dsn::ERR_INVALID_PARAMETERS, create_response.err);
    }

    {
        test_logger("create app with invalid replica count");
        create_request.options.partition_count = 10;
        result = fake_create_app(state, create_request);
        fake_wait_rpc(result, create_response);
        ASSERT_EQ(dsn::ERR_INVALID_PARAMETERS, create_response.err);
    }

    {
        test_logger("normal create app test");
        create_request.options.partition_count = 8;
        create_request.options.replica_count = 3;

        result = fake_create_app(state, create_request);
        fake_wait_rpc(result, create_response);
        ASSERT_EQ(create_response.err, dsn::ERR_OK);
        ASSERT_EQ(create_response.appid, 2);
        // first wait the table to create
        ASSERT_TRUE(state->spin_wait_staging(30));
        last_app_id = create_response.appid;
    }

    {
        test_logger("Test: create existing app, with option.sucess_if_exist=false");
        result = fake_create_app(state, create_request);
        fake_wait_rpc(result, create_response);
        ASSERT_TRUE(create_response.err == dsn::ERR_INVALID_PARAMETERS);
    }

    {
        test_logger("Test: create existing app, with different app options");
        create_request.options.partition_count += 10;
        create_request.options.app_type = "rrdb";
        create_request.options.success_if_exist = true;
        result = fake_create_app(state, create_request);
        fake_wait_rpc(result, create_response);
        ASSERT_EQ(create_response.err, dsn::ERR_INVALID_PARAMETERS);
    }

    // initialize the app
    std::shared_ptr<app_state> newly_app = (*apps)[2];
    ASSERT_TRUE(newly_app != nullptr);
    ASSERT_TRUE(newly_app->app_name == test_app2_name);
    generate_app(newly_app, server_nodes);
    for (auto &pc : newly_app->partitions) {
        ASSERT_FALSE(pc.primary.is_invalid());
        ASSERT_FALSE((pc.partition_flags & pc_flags::dropped));
    }

    // drop current app
    configuration_drop_app_request drop_request;
    configuration_drop_app_response drop_response;
    {
        drop_request.app_name = test_app2_name;
        drop_request.options.success_if_not_exist = false;

        std::vector<int64_t> old_ballots;
        for (auto &pc : newly_app->partitions) {
            old_ballots.push_back(pc.ballot);
        }

        result = fake_drop_app(state, drop_request);
        // a drop request immediately after the first request
        result2 = fake_drop_app(state, drop_request);
        // try to create a app with same name when it is dropping
        result3 = fake_create_app(state, create_request);

        test_logger("Test: normal drop table");
        fake_wait_rpc(result, drop_response);
        ASSERT_EQ(drop_response.err, dsn::ERR_OK);

        test_logger("Test: Another same drop response right after a previous one");
        fake_wait_rpc(result2, drop_response);
        ASSERT_TRUE(drop_response.err == dsn::ERR_BUSY_DROPPING);

        test_logger("Test: A create request for a dropping table");
        fake_wait_rpc(result3, create_response);
        ASSERT_TRUE(create_response.err == dsn::ERR_BUSY_DROPPING);

        ASSERT_TRUE(state->spin_wait_staging(20));
        test_logger("Test all rpelicas in every partitions is removed");
        for (auto &pc : newly_app->partitions) {
            ASSERT_EQ(0, replica_count(pc));
            ASSERT_TRUE((pc.partition_flags & pc_flags::dropped));
            ASSERT_EQ(old_ballots[pc.pid.get_partition_index()] + 1, pc.ballot);
        }
    }

    {
        test_logger("Test: drop a dropped table");
        drop_request.options.success_if_not_exist = false;
        result = fake_drop_app(state, drop_request);
        fake_wait_rpc(result, drop_response);
        ASSERT_EQ(dsn::ERR_APP_NOT_EXIST, drop_response.err);
    }

    {
        // now let's recreating the app with the same name
        result = fake_create_app(state, create_request);
        create_request.options.success_if_exist = true;
        result2 = fake_create_app(state, create_request);

        test_logger("Test: recreating the app with the same name");
        fake_wait_rpc(result, create_response);
        ASSERT_EQ(create_response.err, dsn::ERR_OK);
        ASSERT_EQ(create_response.appid, last_app_id + 1);
        last_app_id = create_response.appid;

        test_logger("Test: the same creating app right after a previous one");
        fake_wait_rpc(result2, create_response);
        ASSERT_EQ(create_response.err, dsn::ERR_BUSY_CREATING);

        ASSERT_TRUE(state->spin_wait_staging(30));

        newly_app = (*apps)[last_app_id];
        ASSERT_TRUE(newly_app != nullptr);
        ASSERT_TRUE(newly_app->app_name == test_app2_name);
        for (auto &pc : newly_app->partitions) {
            ASSERT_FALSE((pc.partition_flags & pc_flags::dropped));
        }
    }

    // here the app state is like this
    // app_id_1, simple_kv.instance0 <- created by default,
    // from 2 to last_app_id-1, test_app2_name, dropped
    // last_app_id, test_app2_name, created
    // then test recalling
    configuration_recall_app_request recall_request;
    configuration_recall_app_response recall_response;
    {
        test_logger("recall an app with non-exist id");
        recall_request.app_id = last_app_id + 10;
        recall_request.new_app_name = "";
        result = fake_recall_app(state, recall_request);
        fake_wait_rpc(result, recall_response);
        ASSERT_EQ(recall_response.err, dsn::ERR_APP_NOT_EXIST);
    }
    {
        test_logger("recall an available app");
        recall_request.app_id = 1;
        recall_request.new_app_name = "";
        result = fake_recall_app(state, recall_request);
        fake_wait_rpc(result, recall_response);
        ASSERT_EQ(recall_response.err, dsn::ERR_APP_EXIST);
    }
    {
        test_logger("recall an app but the name is occupied by other available apps");
        recall_request.app_id = 2;
        recall_request.new_app_name = "";
        result = fake_recall_app(state, recall_request);
        fake_wait_rpc(result, recall_response);
        ASSERT_EQ(recall_response.err, dsn::ERR_INVALID_PARAMETERS);
    }
    {
        test_logger("2 successive valid recall request");
        recall_request.app_id = 2;
        recall_request.new_app_name = test_app2_recalled_name;
        result = fake_recall_app(state, recall_request);
        result2 = fake_recall_app(state, recall_request);

        fake_wait_rpc(result, recall_response);
        ASSERT_EQ(dsn::ERR_OK, recall_response.err);
        ASSERT_EQ(2, recall_response.info.app_id);
        ASSERT_EQ(recall_request.new_app_name, recall_response.info.app_name);
        ASSERT_EQ(8, recall_response.info.partition_count);

        fake_wait_rpc(result2, recall_response);
        ASSERT_TRUE(recall_response.err == dsn::ERR_BUSY_CREATING);

        ASSERT_TRUE(state->spin_wait_staging(30));

        newly_app = (*apps)[2];
        ASSERT_TRUE(newly_app != nullptr);
        ASSERT_TRUE(newly_app->app_name == test_app2_recalled_name);
        for (auto &pc : newly_app->partitions) {
            ASSERT_FALSE((pc.partition_flags & pc_flags::dropped));
        }
    }
    {
        test_logger("create a recalled app");
        create_request.app_name = test_app2_recalled_name;
        result = fake_create_app(state, create_request);
        fake_wait_rpc(result, create_response);
        ASSERT_EQ(dsn::ERR_INVALID_PARAMETERS, create_response.err);
    }
    {
        test_logger("a recalling request right after the dropping request");
        drop_request.app_name = test_app2_recalled_name;
        drop_request.options.success_if_not_exist = false;

        recall_request.app_id = 2;
        recall_request.new_app_name = test_app2_recalled_name;

        result = fake_drop_app(state, drop_request);
        result2 = fake_recall_app(state, recall_request);

        fake_wait_rpc(result, drop_response);
        ASSERT_EQ(drop_response.err, dsn::ERR_OK);

        fake_wait_rpc(result2, recall_response);
        ASSERT_EQ(recall_response.err, dsn::ERR_BUSY_DROPPING);
    }
    {
        // after the previous test, the app with id 2 is still dropped
        // let's recall it again
        recall_request.app_id = 2;
        recall_request.new_app_name = "";

        create_request.app_name = test_app2_recalled_name;
        create_request.options.app_type = "simple_kv";
        create_request.options.partition_count = 10;
        create_request.options.replica_count = 3;
        create_request.options.success_if_exist = false;

        drop_request.app_name = test_app2_recalled_name;
        drop_request.options.success_if_not_exist = true;

        result = fake_recall_app(state, recall_request);
        result2 = fake_create_app(state, create_request);
        result3 = fake_drop_app(state, drop_request);

        fake_wait_rpc(result, recall_response);
        ASSERT_EQ(recall_response.err, dsn::ERR_OK);

        test_logger("a creating request right after a recall request");
        fake_wait_rpc(result2, create_response);
        ASSERT_EQ(create_response.err, dsn::ERR_BUSY_CREATING);

        test_logger("a dropping request right after a recall request");
        fake_wait_rpc(result3, drop_response);
        ASSERT_EQ(drop_response.err, dsn::ERR_BUSY_CREATING);
    }

    {
        test_logger("Test: list apps");
        configuration_list_apps_request list_request;
        configuration_list_apps_response list_response;

        list_request.status = dsn::app_status::AS_AVAILABLE;
        state->list_apps(list_request, list_response);
        ASSERT_EQ(list_response.err, dsn::ERR_OK);
        ASSERT_EQ(list_response.infos.size(), 3);

        for (dsn::app_info &info : list_response.infos) {
            if (info.app_id == 2) {
                ASSERT_EQ(info.app_name, test_app2_recalled_name);
                ASSERT_EQ(info.status, dsn::app_status::AS_AVAILABLE);
            } else if (info.app_id == last_app_id) {
                ASSERT_EQ(info.app_name, test_app2_name);
                ASSERT_EQ(info.status, dsn::app_status::AS_AVAILABLE);
            }
        }
    }

    {
        test_logger("Test: test recall an app after a dropped app expired");
        drop_request.app_name = test_app2_recalled_name;
        drop_request.options.success_if_not_exist = false;

        // we configuration the expire seconds of drop action to 30 seconds
        result = fake_drop_app(state, drop_request);
        fake_wait_rpc(result, drop_response);

        ASSERT_EQ(drop_response.err, dsn::ERR_OK);
        test_logger("wait 30 seconds for a dropped table to delete forever");
        for (int i = 0; i < 10; ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(3));
            test_logger("has slept for 3 seconds");
        }

        recall_request.app_id = 2;
        recall_request.new_app_name = test_app2_recalled_name;
        result = fake_recall_app(state, recall_request);
        fake_wait_rpc(result, recall_response);

        ASSERT_EQ(recall_response.err, dsn::ERR_APP_NOT_EXIST);
    }
}
