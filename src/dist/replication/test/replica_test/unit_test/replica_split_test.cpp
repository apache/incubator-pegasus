// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>

#include <dsn/utility/fail_point.h>
#include "replica_test_base.h"

namespace dsn {
namespace replication {

class replica_split_test : public testing::Test
{
public:
    void SetUp()
    {
        _stub = make_unique<mock_replica_stub>();
        mock_app_info();
        _parent = _stub->generate_replica(
            _app_info, _parent_pid, partition_status::PS_PRIMARY, _init_ballot);
        mock_group_check_request();
    }

    void TearDown() {}

    void mock_app_info()
    {
        _app_info.app_id = 2;
        _app_info.app_name = "split_test";
        _app_info.app_type = "replica";
        _app_info.is_stateful = true;
        _app_info.max_replica_count = 3;
        _app_info.partition_count = 8;
    }

    void mock_group_check_request()
    {
        _req.child_gpid = _child_pid;
        _req.config.ballot = _init_ballot;
        _req.config.status = partition_status::PS_PRIMARY;
    }

    void generate_child(partition_status::type status)
    {
        _stub->generate_replica(_app_info, _child_pid, status, _init_ballot);
        _parent->set_child_gpid(_child_pid);
        _parent->set_init_child_ballot(_init_ballot);
    }

    void mock_parent_states()
    {
        // mock private log
        mock_log_file_ptr log_file_mock = new mock_log_file("log.1.0.txt", 0);
        log_file_mock->set_file_size(100);
        mock_mutation_log_private_ptr private_log_mock =
            new mock_mutation_log_private(_parent_pid, _parent);
        private_log_mock->add_log_file(log_file_mock);
        _parent->_private_log = private_log_mock;

        // mock prepare list
        prepare_list *plist_mock = new prepare_list(_parent, 0, 10, nullptr);
        for (int i = 0; i < 10; ++i) {
            mutation_ptr mu = new mutation();
            mu->data.header.decree = i;
            mu->data.header.ballot = _init_ballot;
            plist_mock->put(mu);
            _parent->_private_log->append(mu, LPC_WRITE_REPLICATION_LOG_PRIVATE, nullptr, nullptr);
        }
        _parent->_prepare_list = plist_mock;

        // mock stub shared log
        mock_mutation_log_shared_ptr shared_log_mock = new mock_mutation_log_shared("./");
        _stub->set_log(shared_log_mock);
    }

    void cleanup_parent_prepare_list() { _parent->_prepare_list->reset(0); }

    void test_on_add_child()
    {
        _parent->on_add_child(_req);
        _parent->tracker()->wait_outstanding_tasks();
    }

    bool test_parent_check_states() { return _parent->parent_check_states(); }

    void test_parent_prepare_states()
    {
        _parent->parent_prepare_states(_parent->_app->learn_dir());
        _parent->tracker()->wait_outstanding_tasks();
    }

public:
    std::unique_ptr<mock_replica_stub> _stub;

    mock_replica_ptr _parent;
    mock_replica_ptr _child;

    dsn::app_info _app_info;
    dsn::gpid _parent_pid = gpid(2, 1);
    dsn::gpid _child_pid = gpid(2, 9);
    uint32_t _old_partition_count = 8;
    ballot _init_ballot = 3;

    group_check_request _req;
};

TEST_F(replica_split_test, add_child_wrong_ballot)
{
    ballot wrong_ballot = 5;
    _req.config.ballot = wrong_ballot;
    test_on_add_child();
    ASSERT_EQ(_stub->get_replica(_child_pid), nullptr);
}

TEST_F(replica_split_test, add_child_with_child_existed)
{
    _parent->set_child_gpid(_child_pid);
    test_on_add_child();
    ASSERT_EQ(_stub->get_replica(_child_pid), nullptr);
}

TEST_F(replica_split_test, add_child_succeed)
{
    fail::setup();
    fail::cfg("replica_stub_create_child_replica_if_not_found", "return()");
    fail::cfg("replica_child_init_replica", "return()");

    test_on_add_child();
    ASSERT_NE(_stub->get_replica(_child_pid), nullptr);

    fail::teardown();
}

TEST_F(replica_split_test, parent_check_states_with_wrong_status)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    _parent->set_partition_status(partition_status::PS_POTENTIAL_SECONDARY);

    bool flag = test_parent_check_states();
    ASSERT_FALSE(flag);
}

TEST_F(replica_split_test, parent_check_states)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    bool flag = test_parent_check_states();
    ASSERT_TRUE(flag);
}

TEST_F(replica_split_test, parent_prepare_states_succeed)
{
    mock_parent_states();
    generate_child(partition_status::PS_PARTITION_SPLIT);

    fail::setup();
    fail::cfg("replica_parent_check_states", "return()");
    fail::cfg("replica_child_copy_states", "return()");
    test_parent_prepare_states();
    fail::teardown();

    cleanup_parent_prepare_list();
}

} // namespace replication
} // namespace dsn
