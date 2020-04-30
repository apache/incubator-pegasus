// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>

#include "dist/replication/meta_server/server_load_balancer.h"
#include "dist/replication/meta_server/meta_server_failure_detector.h"
#include "dist/replication/meta_server/meta_split_service.h"
#include "dist/replication/meta_server/meta_bulk_load_service.h"
#include "dist/replication/test/meta_test/misc/misc.h"

#include "meta_service_test_app.h"

namespace dsn {
namespace replication {

class meta_test_base : public testing::Test
{
public:
    void SetUp() override
    {
        _ms = make_unique<fake_receiver_meta_service>();
        _ms->_failure_detector.reset(new meta_server_failure_detector(_ms.get()));
        _ms->_balancer.reset(utils::factory_store<server_load_balancer>::create(
            _ms->_meta_opts._lb_opts.server_load_balancer_type.c_str(),
            PROVIDER_TYPE_MAIN,
            _ms.get()));
        ASSERT_EQ(_ms->remote_storage_initialize(), ERR_OK);
        _ms->initialize_duplication_service();
        ASSERT_TRUE(_ms->_dup_svc);
        _ms->_split_svc = make_unique<meta_split_service>(_ms.get());
        ASSERT_TRUE(_ms->_split_svc);
        _ms->_bulk_load_svc = make_unique<bulk_load_service>(
            _ms.get(), meta_options::concat_path_unix_style(_ms->_cluster_root, "bulk_load"));
        ASSERT_TRUE(_ms->_bulk_load_svc);
        _ms->_bulk_load_svc->initialize_bulk_load_service();

        _ss = _ms->_state;
        _ss->initialize(_ms.get(), _ms->_cluster_root + "/apps");

        _ms->_started = true;
        _ms->set_function_level(meta_function_level::fl_steady);

        // recover apps from meta storage
        ASSERT_EQ(_ss->initialize_data_structure(), ERR_OK);
    }

    void TearDown() override
    {
        if (_ss && _ms) {
            delete_all_on_meta_storage();
        }

        _ss.reset();
        _ms.reset(nullptr);
    }

    void delete_all_on_meta_storage()
    {
        _ms->get_meta_storage()->get_children(
            {"/"}, [this](bool, const std::vector<std::string> &children) {
                for (const std::string &child : children) {
                    _ms->get_meta_storage()->delete_node_recursively("/" + child, []() {});
                }
            });
        wait_all();
    }

    void initialize_node_state() { _ss->initialize_node_state(); }

    void wait_all() { _ms->tracker()->wait_outstanding_tasks(); }

    // create an app for test with specified name and specified partition count
    void create_app(const std::string &name, uint32_t partition_count)
    {
        configuration_create_app_request req;
        configuration_create_app_response resp;
        req.app_name = name;
        req.options.app_type = "simple_kv";
        req.options.partition_count = partition_count;
        req.options.replica_count = 3;
        req.options.success_if_exist = false;
        req.options.is_stateful = true;
        req.options.envs["value_version"] = "1";

        auto result = fake_create_app(_ss.get(), req);
        fake_wait_rpc(result, resp);
        ASSERT_EQ(resp.err, ERR_OK) << resp.err.to_string() << " " << name;

        // wait for the table to create
        ASSERT_TRUE(_ss->spin_wait_staging(30));
    }

    void create_app(const std::string &name) { create_app(name, 8); }

    // drop an app for test.
    void drop_app(const std::string &name)
    {
        configuration_drop_app_request req;
        configuration_drop_app_response resp;
        req.app_name = name;
        req.options.success_if_not_exist = false;

        auto result = fake_drop_app(_ss.get(), req);
        fake_wait_rpc(result, resp);
        ASSERT_EQ(resp.err, ERR_OK) << resp.err.to_string() << " " << name;

        ASSERT_TRUE(_ss->spin_wait_staging(30));
    }

    configuration_update_app_env_response update_app_envs(const std::string &app_name,
                                                          const std::vector<std::string> &env_keys,
                                                          const std::vector<std::string> &env_vals)
    {
        auto req = make_unique<configuration_update_app_env_request>();
        req->__set_app_name(std::move(app_name));
        req->__set_op(std::move(app_env_operation::type::APP_ENV_OP_SET));
        req->__set_keys(env_keys);
        req->__set_values(env_vals);

        app_env_rpc rpc(std::move(req), RPC_CM_UPDATE_APP_ENV); // don't need reply
        _ss->set_app_envs(rpc);
        _ss->wait_all_task();
        return rpc.response();
    }

    void mock_node_state(const rpc_address &addr, const node_state &node)
    {
        _ss->_nodes[addr] = node;
    }

    std::shared_ptr<app_state> find_app(const std::string &name) { return _ss->get_app(name); }

    meta_duplication_service &dup_svc() { return *(_ms->_dup_svc); }

    meta_split_service &split_svc() { return *(_ms->_split_svc); }

    bulk_load_service &bulk_svc() { return *(_ms->_bulk_load_svc); }

    std::shared_ptr<server_state> _ss;
    std::unique_ptr<meta_service> _ms;
};

} // namespace replication
} // namespace dsn
