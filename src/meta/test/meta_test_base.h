// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>

#include <dsn/dist/replication/replication_types.h>

#include "meta/meta_data.h"

namespace dsn {
namespace replication {

class meta_split_service;
class meta_duplication_service;
class bulk_load_service;
class meta_service;
class server_state;

class meta_test_base : public testing::Test
{
public:
    ~meta_test_base();

    void SetUp() override;

    void TearDown() override;

    void delete_all_on_meta_storage();

    void initialize_node_state();

    void wait_all();

    // create an app for test with specified name and specified partition count
    void create_app(const std::string &name, uint32_t partition_count);

    void create_app(const std::string &name) { create_app(name, 8); }

    // drop an app for test.
    void drop_app(const std::string &name);

    configuration_update_app_env_response update_app_envs(const std::string &app_name,
                                                          const std::vector<std::string> &env_keys,
                                                          const std::vector<std::string> &env_vals);

    void mock_node_state(const rpc_address &addr, const node_state &node);

    std::shared_ptr<app_state> find_app(const std::string &name);

    meta_duplication_service &dup_svc();

    meta_split_service &split_svc();

    bulk_load_service &bulk_svc();

    std::shared_ptr<server_state> _ss;
    std::unique_ptr<meta_service> _ms;
    std::string _app_root;
};

} // namespace replication
} // namespace dsn
