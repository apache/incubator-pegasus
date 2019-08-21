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

    void test_on_add_child()
    {
        _parent->on_add_child(_req);
        _parent->tracker()->wait_outstanding_tasks();
    }

public:
    std::unique_ptr<mock_replica_stub> _stub;

    std::unique_ptr<mock_replica> _parent;
    std::unique_ptr<mock_replica> _child;

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
    fail::cfg("replica_init_child_replica", "return()");

    test_on_add_child();
    ASSERT_NE(_stub->get_replica(_child_pid), nullptr);

    fail::teardown();
}

} // namespace replication
} // namespace dsn
