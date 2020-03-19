// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>

#include <dsn/utility/fail_point.h>
#include "replica_test_base.h"
#include <dsn/utility/defer.h>

namespace dsn {
namespace replication {

class replica_test : public replica_test_base
{
public:
    dsn::app_info _app_info;
    dsn::gpid pid = gpid(2, 1);
    mock_replica_ptr _mock_replica;

public:
    void SetUp() override
    {
        stub->install_perf_counters();
        mock_app_info();
        _mock_replica = stub->generate_replica(_app_info, pid, partition_status::PS_PRIMARY, 1);
    }

    int get_write_size_exceed_threshold_count()
    {
        return stub->_counter_recent_write_size_exceed_threshold_count->get_value();
    }

    int get_table_level_backup_request_qps()
    {
        return _mock_replica->_counter_backup_request_qps->get_integer_value();
    }

    void mock_app_info()
    {
        _app_info.app_id = 2;
        _app_info.app_name = "replica_test";
        _app_info.app_type = "replica";
        _app_info.is_stateful = true;
        _app_info.max_replica_count = 3;
        _app_info.partition_count = 8;
    }
};

TEST_F(replica_test, write_size_limited)
{
    int count = 100;
    task_code default_code;
    struct dsn::message_header header;
    header.body_length = 10000000;

    auto write_request = dsn::message_ex::create_request(default_code);
    auto cleanup = dsn::defer([=]() { delete write_request; });
    write_request->header = &header;

    for (int i = 0; i < count; i++) {
        stub->on_client_write(pid, write_request);
    }

    ASSERT_EQ(get_write_size_exceed_threshold_count(), count);
}

TEST_F(replica_test, backup_request_qps)
{
    // create backup request
    struct dsn::message_header header;
    header.context.u.is_backup_request = true;
    message_ptr backup_request = dsn::message_ex::create_request(task_code());
    backup_request->header = &header;

    _mock_replica->on_client_read(backup_request);

    // We have to sleep >= 0.1s, or the value this perf-counter will be 0, according to the
    // implementation of perf-counter which type is COUNTER_TYPE_RATE.
    usleep(1e5);
    ASSERT_GT(get_table_level_backup_request_qps(), 0);
}

} // namespace replication
} // namespace dsn
