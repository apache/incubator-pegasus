/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

#include<vector>
#include<string>

#include <base/pegasus_const.h>
#include <dsn/service_api_c.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <gtest/gtest.h>
#include <pegasus/client.h>
#include <rrdb/rrdb_types.h>

using namespace ::pegasus;
using namespace ::dsn;
using namespace ::replication;

extern pegasus_client *client;
extern std::shared_ptr<replication_ddl_client> ddl_client;


/*
class batch_get_resolver : public partition_resolver {
public:
    batch_get_resolver(std::vector<rpc_address> meta_server_list, const char *app_name) : partition_resolver(meta_server_list[0], app_name) {
        inner_resolver = dsn::replication::partition_resolver::get_resolver("mycluster", meta_server_list, app_name);
    }

    void resolve(uint64_t partition_hash,
                 std::function<void(resolve_result &&)> &&callback,
                 int timeout_ms) override
    {
        partition_resolver *ptr = inner_resolver.get();
        ptr->resolve(); // this can not be done!
    }
    void on_access_failure(int partition_index, error_code err) override {}
    int get_partition_index(int partition_count, uint64_t partition_hash) override { return 0; }

private:
    dsn::replication::partition_resolver_ptr inner_resolver;
};
*/


TEST(batch_get, set_and_then_batch_get)
{
    std::vector<std::pair<std::string, std::string>> key_pair_list;
    std::vector<std::string> value_list;

    for (int i = 0; i < 100; ++i) {
        std::string hash_key = "hash_key_prefix_" + std::to_string(i);
        std::string sort_key = "sort_key_prefix_" + std::to_string(i);
        std::string value = "value_" + std::to_string(i);
        int ret = client->set(hash_key, sort_key, value);
        ASSERT_EQ(0, ret);

        key_pair_list.emplace_back(std::make_pair(hash_key, sort_key));
        value_list.push_back(value);
    }

    const std::string app_name(client->get_app_name());
    int app_id;
    int partition_count = 0;
    std::vector<partition_configuration> pc;
    auto list_app_error_code = ddl_client->list_app(app_name, app_id, partition_count, pc);
    ASSERT_EQ(ERR_OK, list_app_error_code);

    std::vector<apps::batch_get_request> v_batch_get(partition_count);
    std::vector<int> partition_to_error(partition_count, 0);

    std::vector<rpc_address> meta_list;
    replica_helper::load_meta_servers(meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "mycluster");

    // can not call inner_resolver->resolve & get_partition_index ......
    auto inner_resolver = dsn::replication::partition_resolver::get_resolver("mycluster", meta_list, app_name.c_str());

    std::vector<int> response_index;
    for (auto & i : key_pair_list) {
        auto &hash_key = i.first;
        auto &sort_key = i.second;
    }
}


