#include <cmath>
#include <fstream>
#include <vector>
#include <iostream>

#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>

#include "meta_service.h"
#include "server_state.h"
#include "meta_service_test_app.h"
#include "../misc/misc.h"

using namespace dsn::replication;

static void random_assign_partition_config(
    std::shared_ptr<app_state>& app,
    const std::vector<dsn::rpc_address>& server_list,
    int max_replica_count)
{
    auto get_server = [&server_list](int indice) {
        if (indice%2!=0)
            return dsn::rpc_address();
        return server_list[indice/2];
    };

    int max_servers = server_list.size()*2 - 1;
    for (dsn::partition_configuration& pc: app->partitions) {
        int start = 0;
        std::vector<int> indices;
        for (int i=0; i<max_replica_count && start<=max_servers; ++i) {
            indices.push_back( random32(start, max_servers) );
            start = indices.back() + 1;
        }
        pc.primary = get_server(indices[0]);
        for (int i=1; i<indices.size(); ++i) {
            dsn::rpc_address addr = get_server(indices[i]);
            if (!addr.is_invalid())
                pc.secondaries.push_back(addr);
        }
    }
}

static void file_data_compare(const char* fname1, const char* fname2)
{
    static const int length = 4096;
    std::shared_ptr<char> buffer(dsn::make_shared_array<char>(length*2));
    char* buf1 = buffer.get(), *buf2 = buffer.get()+length;

    std::ifstream ifile1(fname1, std::ios::in|std::ios::binary);
    std::ifstream ifile2(fname2, std::ios::in|std::ios::binary);

    auto file_length = [](std::ifstream& is) {
        is.seekg(0, is.end);
        int result = is.tellg();
        is.seekg(0, is.beg);
        return result;
    };

    int l = file_length(ifile1);
    ASSERT_EQ(l, file_length(ifile2));

    for (int i=0; i<l; i+=length) {
        int up_to_bytes = length<(l-i)?length:(l-i);
        ifile1.read(buf1, up_to_bytes);
        ifile2.read(buf2, up_to_bytes);
        ASSERT_TRUE( memcmp(buf1, buf2, up_to_bytes)==0 );
    }
}

void meta_service_test_app::state_sync_test()
{
    int apps_count = 15;
    int drop_ratio = 5;
    std::vector<dsn::rpc_address> server_list;
    std::vector<int> drop_set;
    generate_node_list(server_list, 10, 10);

    std::shared_ptr<meta_service> meta_svc = std::make_shared<meta_service>();
    meta_service* svc = meta_svc.get();
    meta_options& opt = svc->_meta_opts;
    opt.cluster_root = "/meta_test";
    opt.meta_state_service_type = "meta_state_service_simple";
    svc->remote_storage_initialize();

    std::string apps_root = "/meta_test/apps";
    std::shared_ptr<server_state> ss1 = svc->_state;
    // create apss randomly, and sync it to meta state service simple
    std::cerr << "testing create apps and sync to remote storage" << std::endl;
    {
        server_state* ss = ss1.get();
        ss->initialize(svc, apps_root);

        drop_set.clear();
        for (int i=1; i<=apps_count; ++i)
        {
            dsn::app_info info;
            info.is_stateful = true;
            info.app_id = i; info.app_type = "simple_kv";
            info.app_name = "test_app" + boost::lexical_cast<std::string>(i);
            info.max_replica_count = 3; info.partition_count = random32(100, 10000);
            info.status = dsn::app_status::AS_CREATING;
            std::shared_ptr<app_state> app = app_state::create(info);

            ss->_all_apps.emplace(app->app_id, app);
            if (i<apps_count && random32(1, apps_count)<=drop_ratio) {
                app->status = dsn::app_status::AS_DROPPED;
                drop_set.push_back(i);
                app->app_name = "test_app" + boost::lexical_cast<std::string>(apps_count);
            }
        }
        for (int i=1; i<=apps_count; ++i) {
            std::shared_ptr<app_state> app = ss->get_app(i);
            random_assign_partition_config(app, server_list, 3);
        }

        dsn::error_code ec = ss->sync_apps_to_remote_storage();
        ASSERT_EQ(ec, dsn::ERR_OK);
        ss->spin_wait_creating();
    }

    // then we sync from meta_state_service_simple, and dump to local file
    std::cerr << "testing sync from remote storage and dump to local file" << std::endl;
    {
        std::shared_ptr<server_state> ss2 = std::make_shared<server_state>();
        ss2->initialize(svc, apps_root);
        dsn::error_code ec = ss2->sync_apps_from_remote_storage();
        ASSERT_EQ(ec, dsn::ERR_OK);

        ec = ss2->dump_from_remote_storage("meta_state.dump1", false);
        ASSERT_EQ(ec, dsn::ERR_OK);
    }

    // dump another way
    std::cerr << "testing directly dump to local file" << std::endl;
    {
        std::shared_ptr<server_state> ss2 = std::make_shared<server_state>();
        ss2->initialize(svc, apps_root);
        dsn::error_code ec = ss2->dump_from_remote_storage("meta_state.dump2", true);

        ASSERT_EQ(ec, dsn::ERR_OK);
        file_data_compare("meta_state.dump1", "meta_state.dump2");
    }

    opt.meta_state_service_type = "meta_state_service_zookeeper";
    svc->remote_storage_initialize();
    std::cerr << "test sync to zookeeper's remote storage" << std::endl;
    // restore from the local file, and restore to zookeeper
    {
        std::shared_ptr<server_state> ss2 = std::make_shared<server_state>();

        ss2->initialize(svc, apps_root);
        dsn::error_code ec = ss2->restore_from_local_storage("meta_state.dump2");
        ASSERT_EQ(ec, dsn::ERR_OK);
    }

    //then sync from zookeeper
    std::cerr << "test sync from zookeeper's storage" << std::endl;
    {
        std::shared_ptr<server_state> ss2 = std::make_shared<server_state>();
        ss2->initialize(svc, apps_root);

        dsn::error_code ec = ss2->initialize_data_structure();
        ASSERT_EQ(ec, dsn::ERR_OK);

        app_mapper_compare(ss1->_all_apps, ss2->_all_apps);
        ASSERT_EQ(ss1->_exist_apps.size(), ss2->_exist_apps.size());
        for (const auto& iter: ss1->_exist_apps) {
            ASSERT_TRUE( ss2->_exist_apps.find(iter.first) != ss2->_exist_apps.end());
        }

        //then we dump the content to local file with binary format
        std::cerr << "test dump to local file from zookeeper's storage" << std::endl;
        ec = ss2->dump_from_remote_storage("meta_state.dump3", false);
        ASSERT_EQ(ec, dsn::ERR_OK);
    }

    //then we restore from local storage and restore to remote
    {
        std::shared_ptr<server_state> ss2 = std::make_shared<server_state>();

        ss2->initialize(svc, apps_root);
        dsn::error_code ec = ss2->restore_from_local_storage("meta_state.dump3");
        ASSERT_EQ(ec, dsn::ERR_OK);

        app_mapper_compare(ss1->_all_apps, ss2->_all_apps);
        ASSERT_TRUE(ss1->_exist_apps.size()==ss2->_exist_apps.size());
        for (const auto& iter: ss1->_exist_apps) {
            ASSERT_TRUE(ss2->_exist_apps.find(iter.first) != ss2->_exist_apps.end());
        }
        ss2->initialize_node_state();

        //then let's test the query configuration calls
        //1. by node
        configuration_query_by_node_request request;
        configuration_query_by_node_response response;

        //1.1 normal
        request.node = server_list[random32(0, 9)];
        ss2->query_configuration_by_node(request, response);
        ASSERT_EQ(dsn::ERR_OK, response.err);

        std::vector<dsn::partition_configuration> pc_list;
        for (auto& iter: ss1->_exist_apps) {
            std::shared_ptr<app_state>& app = iter.second;
            for (dsn::partition_configuration& pc: app->partitions)
                if ( is_member(pc, request.node) )
                    pc_list.push_back(pc);
        }

        auto cmp1 = [](const configuration_update_request& pc1, const configuration_update_request& pc2) {
            return pc1.config.pid < pc2.config.pid;
        };
        auto cmp2 = [](const dsn::partition_configuration& pc1, const dsn::partition_configuration& pc2) {
            return pc1.pid < pc2.pid;
        };

        std::sort(pc_list.begin(), pc_list.end(), cmp2);
        std::sort(response.partitions.begin(), response.partitions.end(), cmp1);
        ASSERT_EQ(pc_list.size(), response.partitions.size());
        for (unsigned i=0; i!=pc_list.size(); ++i)
            ASSERT_EQ(pc_list[i], response.partitions[i].config);

        //1.2 invalid address
        request.node.assign_ipv4("1.2.3.4", 1234);
        ss2->query_configuration_by_node(request, response);
        ASSERT_EQ(dsn::ERR_OBJECT_NOT_FOUND, response.err);

        //2.1. normal gpid
        dsn::gpid gpid = {15, 0};
        dsn::partition_configuration pc;
        ASSERT_TRUE(ss2->query_configuration_by_gpid(gpid, pc));
        ASSERT_EQ(ss1->_all_apps[15]->partitions[0], pc);
        //2.2 dropped app
        if (!drop_set.empty())
        {
            gpid.set_app_id(drop_set[0]);
            ASSERT_FALSE(ss2->query_configuration_by_gpid(gpid, pc));
        }

        //3.1 query configuration by index
        dsn::configuration_query_by_index_request req;
        dsn::configuration_query_by_index_response resp;
        req.app_name = "test_app15";
        req.partition_indices = {-1, 1, 2, 3, 0x7fffffff};

        std::shared_ptr<app_state> app_created = ss1->get_app(15);
        ss2->query_configuration_by_index(req, resp);
        ASSERT_EQ(dsn::ERR_OK, resp.err);
        ASSERT_EQ(15, resp.app_id);
        ASSERT_EQ(app_created->partition_count, resp.partition_count);
        ASSERT_EQ(resp.partitions.size(), 3);
        for (int i=1; i<=3; ++i)
            ASSERT_EQ(resp.partitions[i-1], app_created->partitions[i]);

        //3.2 no exist app
        req.app_name = "make_no_sense";
        ss2->query_configuration_by_index(req, resp);
        ASSERT_EQ(dsn::ERR_OBJECT_NOT_FOUND, resp.err);
    }

    //simulate the half creating
    std::cerr << "test some node for a app is not create on remote storage" << std::endl;
    {
        std::shared_ptr<server_state> ss2 = std::make_shared<server_state>();
        dsn::error_code ec;
        ss2->initialize(svc, apps_root);

        dsn::dist::meta_state_service* storage = svc->get_remote_storage();
        storage->delete_node(
            ss2->get_partition_path( dsn::gpid{apps_count, 0} ),
            false,
            LPC_META_CALLBACK,
            [&ec](dsn::error_code error) { ec = error; },
            nullptr
        )->wait();
        ASSERT_EQ(ec, dsn::ERR_OK);

        ec = ss2->sync_apps_from_remote_storage();
        ASSERT_EQ(ec, dsn::ERR_OK);
        ASSERT_TRUE(ss2->spin_wait_creating(30));
    }

    //clean up
    std::cerr << "start to clean up" << std::endl;
    {
        dsn::error_code ec;
        dsn::dist::meta_state_service* storage = svc->get_remote_storage();
        storage->delete_node(
            apps_root,
            true,
            LPC_META_CALLBACK,
            [&ec](dsn::error_code error) { ec = error; },
            nullptr
        )->wait();
        ASSERT_EQ(ec, dsn::ERR_OK);
    }
}
