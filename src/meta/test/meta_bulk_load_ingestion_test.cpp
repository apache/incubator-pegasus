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

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/gpid.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "meta/meta_bulk_load_ingestion_context.h"
#include "meta/meta_data.h"
#include "meta_test_base.h"
#include "runtime/rpc/rpc_address.h"
#include "utils/fail_point.h"

namespace dsn {
namespace replication {

class node_context_test : public meta_test_base
{
public:
    void SetUp()
    {
        _context = ingestion_context::node_context();
        _context.node_ingesting_count = 0;
        _context.address = NODE;
        FLAGS_bulk_load_node_max_ingesting_count = 1;
        FLAGS_bulk_load_node_min_disk_count = 1;
    }

    void TearDown()
    {
        _context.disk_ingesting_counts.clear();
        _context.node_ingesting_count = 0;
    }

    void mock_context(const std::string &disk_tag,
                      const uint32_t disk_count = 0,
                      const uint32_t total_count = 0)
    {
        _context.node_ingesting_count = total_count;
        _context.disk_ingesting_counts[disk_tag] = disk_count;
    }

    void init_disk(const std::string &disk_tag) { _context.init_disk(disk_tag); }

    uint32_t get_disk_count(const std::string &disk_tag)
    {
        if (_context.disk_ingesting_counts.find(disk_tag) == _context.disk_ingesting_counts.end()) {
            return -1;
        }
        return _context.disk_ingesting_counts[disk_tag];
    }

    void mock_get_max_disk_ingestion_count(const uint32_t node_min_disk_count,
                                           const uint32_t current_disk_count)
    {
        FLAGS_bulk_load_node_min_disk_count = node_min_disk_count;
        _context.disk_ingesting_counts.clear();
        for (auto i = 0; i < current_disk_count; i++) {
            _context.init_disk(std::to_string(i));
        }
    }

    uint32_t get_max_disk_ingestion_count(const uint32_t max_node_count) const
    {
        return _context.get_max_disk_ingestion_count(max_node_count);
    }

    bool check_if_add() { return _context.check_if_add(TAG); }

public:
    ingestion_context::node_context _context;
    const rpc_address NODE = rpc_address("127.0.0.1", 10086);
    const std::string TAG = "default";
    const std::string TAG2 = "tag2";
};

TEST_F(node_context_test, init_disk_test)
{
    mock_context(TAG, 1, 1);
    struct init_disk_test
    {
        std::string disk_tag;
        uint32_t expected_disk_count;
    } tests[] = {{TAG, 1}, {TAG2, 0}};
    for (const auto &test : tests) {
        init_disk(test.disk_tag);
        ASSERT_EQ(get_disk_count(test.disk_tag), test.expected_disk_count);
    }
}

TEST_F(node_context_test, get_max_disk_ingestion_count_test)
{
    struct get_max_disk_ingestion_count_test
    {
        uint32_t max_node_count;
        uint32_t min_disk_count;
        uint32_t current_disk_count;
        uint32_t expected_count;
    } tests[] = {// min_disk_count = 1
                 {1, 1, 1, 1},
                 {2, 1, 1, 2},
                 // min_disk_count = 3
                 {1, 3, 1, 1},
                 {4, 3, 3, 2},
                 // min_disk_count = 7
                 {1, 7, 1, 1},
                 {1, 7, 11, 1},
                 {7, 7, 1, 1},
                 {7, 7, 11, 1},
                 {8, 7, 3, 2},
                 {8, 7, 8, 1},
                 {8, 7, 11, 1}};
    for (const auto &test : tests) {
        mock_get_max_disk_ingestion_count(test.min_disk_count, test.current_disk_count);
        ASSERT_EQ(get_max_disk_ingestion_count(test.max_node_count), test.expected_count);
    }
}

TEST_F(node_context_test, check_if_add_test)
{
    fail::setup();
    struct check_if_add_test
    {
        const uint32_t max_node_count;
        const uint32_t current_node_count;
        std::string max_disk_count_str;
        const uint32_t current_disk_count;
        bool expected_result;
    } tests[] = {{1, 1, "1", 1, false}, {3, 2, "2", 2, false}, {1, 0, "7", 0, true}};
    for (const auto &test : tests) {
        FLAGS_bulk_load_node_max_ingesting_count = test.max_node_count;
        mock_context(TAG, test.current_disk_count, test.current_node_count);
        auto str = "return(" + test.max_disk_count_str + ")";
        fail::cfg("ingestion_node_context_disk_count", str);
        ASSERT_EQ(check_if_add(), test.expected_result);
    }
    fail::teardown();
}

class ingestion_context_test : public meta_test_base
{
public:
    /// mock app and node info context
    ///  node1    node2    node3    node4
    /// p0(tag1) s0(tag1) s0(tag2)
    /// s1(tag1) s1(tag2)          p1(tag2)
    /// s2(tag2)          p2(tag1) s2(tag1)
    ///          p3(tag1) s3(tag1) s3(tag2)
    void SetUp()
    {
        _context = std::make_unique<ingestion_context>();
        add_node_context({NODE1, NODE2, NODE3, NODE4});
        mock_app();
        FLAGS_bulk_load_node_min_disk_count = MIN_DISK_COUNT;
        FLAGS_bulk_load_node_max_ingesting_count = MAX_NODE_COUNT;
    }

    void TearDown() { _context->reset_all(); }

    void update_max_node_count(const uint32_t max_node_count)
    {
        FLAGS_bulk_load_node_max_ingesting_count = max_node_count;
    }

    bool check_node_ingestion(const uint32_t max_node_count,
                              const rpc_address &node,
                              const std::string &tag)
    {
        _context->reset_all();
        update_max_node_count(max_node_count);
        _context->_nodes_context[NODE1] = ingestion_context::node_context(NODE1, TAG1);
        _context->_nodes_context[NODE1].add(TAG1);
        return _context->check_node_ingestion(node, tag);
    }

    void mock_app()
    {
        app_info ainfo;
        ainfo.app_id = APP_ID;
        ainfo.partition_count = PARTITION_COUNT;
        _app = std::make_shared<app_state>(ainfo);
        _app->partitions.reserve(PARTITION_COUNT);
        _app->helpers->contexts.reserve(PARTITION_COUNT);
        mock_partition(0,
                       {NODE1, NODE2, NODE3},
                       {TAG1, TAG1, TAG2},
                       _app->partitions[0],
                       _app->helpers->contexts[0]);
        mock_partition(1,
                       {NODE4, NODE1, NODE2},
                       {TAG2, TAG1, TAG2},
                       _app->partitions[1],
                       _app->helpers->contexts[1]);
        mock_partition(2,
                       {NODE3, NODE1, NODE4},
                       {TAG1, TAG2, TAG1},
                       _app->partitions[2],
                       _app->helpers->contexts[2]);
        mock_partition(3,
                       {NODE2, NODE3, NODE4},
                       {TAG1, TAG1, TAG2},
                       _app->partitions[3],
                       _app->helpers->contexts[3]);
    }

    void mock_partition(const uint32_t pidx,
                        std::vector<rpc_address> nodes,
                        const std::vector<std::string> tags,
                        partition_configuration &config,
                        config_context &cc)
    {
        config.pid = gpid(APP_ID, pidx);
        config.primary = nodes[0];
        config.secondaries.emplace_back(nodes[1]);
        config.secondaries.emplace_back(nodes[2]);

        auto count = nodes.size();
        for (auto i = 0; i < count; i++) {
            serving_replica r;
            r.node = nodes[i];
            r.disk_tag = tags[i];
            cc.serving.emplace_back(r);
        }
    }

    void add_node_context(std::vector<rpc_address> nodes)
    {
        for (const auto &address : nodes) {
            ingestion_context::node_context node(address, TAG1);
            node.init_disk(TAG2);
            _context->_nodes_context[address] = node;
        }
    }

    bool try_partition_ingestion(const uint32_t pidx)
    {
        return _context->try_partition_ingestion(_app->partitions[pidx],
                                                 _app->helpers->contexts[pidx]);
    }

    void add_partition(const uint32_t pidx)
    {
        auto pinfo = ingestion_context::partition_node_info(_app->partitions[pidx],
                                                            _app->helpers->contexts[pidx]);
        _context->add_partition(pinfo);
    }

    void remove_partition(const uint32_t pidx) { _context->remove_partition(gpid(APP_ID, pidx)); }

    bool is_partition_ingesting(const uint32_t pidx) const
    {
        return _context->_running_partitions.find(gpid(APP_ID, pidx)) !=
               _context->_running_partitions.end();
    }

    uint32_t get_app_ingesting_count() const { return _context->get_app_ingesting_count(APP_ID); }

    void reset_app() { return _context->reset_app(APP_ID); }

    int32_t get_node_running_count(const rpc_address &node)
    {
        if (_context->_nodes_context.find(node) == _context->_nodes_context.end()) {
            return 0;
        }
        return _context->_nodes_context[node].node_ingesting_count;
    }

    uint32_t get_disk_running_count(const rpc_address &node, const std::string &disk_tag)
    {
        if (_context->_nodes_context.find(node) == _context->_nodes_context.end()) {
            return 0;
        }
        auto node_cc = _context->_nodes_context[node];
        if (node_cc.disk_ingesting_counts.find(disk_tag) == node_cc.disk_ingesting_counts.end()) {
            return 0;
        }
        return node_cc.disk_ingesting_counts[disk_tag];
    }

    bool validate_count(const rpc_address &node,
                        const uint32_t expected_node_count,
                        const uint32_t expected_disk1_count,
                        const uint32_t expected_disk2_count)
    {
        return get_node_running_count(node) == expected_node_count &&
               get_disk_running_count(node, TAG1) == expected_disk1_count &&
               get_disk_running_count(node, TAG2) == expected_disk2_count;
    }

public:
    std::unique_ptr<ingestion_context> _context;
    std::shared_ptr<app_state> _app;
    const uint32_t APP_ID = 1;
    const uint32_t PARTITION_COUNT = 4;
    const uint32_t MAX_NODE_COUNT = 2;
    const uint32_t MIN_DISK_COUNT = 2;
    const rpc_address NODE1 = rpc_address("127.0.0.1", 10086);
    const rpc_address NODE2 = rpc_address("127.0.0.1", 10085);
    const rpc_address NODE3 = rpc_address("127.0.0.1", 10087);
    const rpc_address NODE4 = rpc_address("127.0.0.1", 10088);
    const std::string TAG1 = "tag1";
    const std::string TAG2 = "tag2";
};

TEST_F(ingestion_context_test, check_node_ingestion_test)
{
    struct check_node_ingestion_test
    {
        rpc_address node;
        std::string tag;
        uint32_t max_node_count;
        bool expected_result;
    } tests[] = {{NODE2, TAG1, 1, true}, {NODE1, TAG2, 2, true}, {NODE1, TAG2, 1, false}};
    for (const auto &test : tests) {
        ASSERT_EQ(check_node_ingestion(test.max_node_count, test.node, test.tag),
                  test.expected_result);
    }
}

TEST_F(ingestion_context_test, try_partition_ingestion_test)
{
    update_max_node_count(1);
    ASSERT_EQ(try_partition_ingestion(0), true);
    ASSERT_EQ(try_partition_ingestion(1), false);

    update_max_node_count(2);
    ASSERT_EQ(try_partition_ingestion(1), false);
    ASSERT_EQ(try_partition_ingestion(2), true);
    ASSERT_EQ(try_partition_ingestion(3), false);

    update_max_node_count(3);
    ASSERT_EQ(try_partition_ingestion(1), true);
    ASSERT_EQ(try_partition_ingestion(3), true);

    ASSERT_EQ(get_app_ingesting_count(), 4);
}

TEST_F(ingestion_context_test, operation_test)
{
    ASSERT_FALSE(is_partition_ingesting(0));
    add_partition(0);
    ASSERT_TRUE(is_partition_ingesting(0));
    ASSERT_TRUE(validate_count(NODE1, 1, 1, 0));
    ASSERT_TRUE(validate_count(NODE2, 1, 1, 0));
    ASSERT_TRUE(validate_count(NODE3, 1, 0, 1));
    ASSERT_TRUE(validate_count(NODE4, 0, 0, 0));
    ASSERT_EQ(get_app_ingesting_count(), 1);

    ASSERT_FALSE(is_partition_ingesting(1));
    add_partition(1);
    ASSERT_TRUE(is_partition_ingesting(1));
    ASSERT_TRUE(validate_count(NODE1, 2, 2, 0));
    ASSERT_TRUE(validate_count(NODE2, 2, 1, 1));
    ASSERT_TRUE(validate_count(NODE3, 1, 0, 1));
    ASSERT_TRUE(validate_count(NODE4, 1, 0, 1));
    ASSERT_EQ(get_app_ingesting_count(), 2);

    add_partition(2);
    remove_partition(0);
    ASSERT_TRUE(is_partition_ingesting(2));
    ASSERT_FALSE(is_partition_ingesting(0));
    ASSERT_TRUE(validate_count(NODE1, 2, 1, 1));
    ASSERT_TRUE(validate_count(NODE2, 1, 0, 1));
    ASSERT_TRUE(validate_count(NODE3, 1, 1, 0));
    ASSERT_TRUE(validate_count(NODE4, 2, 1, 1));
    ASSERT_EQ(get_app_ingesting_count(), 2);

    reset_app();
    ASSERT_TRUE(validate_count(NODE1, 0, 0, 0));
    ASSERT_TRUE(validate_count(NODE2, 0, 0, 0));
    ASSERT_TRUE(validate_count(NODE3, 0, 0, 0));
    ASSERT_TRUE(validate_count(NODE4, 0, 0, 0));
    ASSERT_EQ(get_app_ingesting_count(), 0);
}

} // namespace replication
} // namespace dsn
