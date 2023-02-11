// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "meta_admin_types.h"
#include "meta/meta_service.h" // IWYU pragma: keep

namespace dsn {
class rpc_address;

namespace replication {

class app_state;
class bulk_load_service;
class meta_duplication_service;
class meta_split_service;
class node_state;
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

    void set_min_live_node_count_for_unfreeze(uint64_t node_count);

    void set_node_live_percentage_threshold_for_update(uint64_t percentage_threshold);

    std::vector<rpc_address> ensure_enough_alive_nodes(int min_node_count);

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

private:
    std::vector<rpc_address> get_alive_nodes() const;
};

} // namespace replication
} // namespace dsn
