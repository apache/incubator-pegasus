// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_server_test_base.h"

#include <dsn/dist/replication/replica_base.h>

namespace pegasus {
namespace server {

class mock_capacity_unit_calculator : public capacity_unit_calculator
{
public:
    int64_t add_read_cu(int64_t read_data_size) override
    {
        read_cu += capacity_unit_calculator::add_read_cu(read_data_size);
        return read_cu;
    }

    int64_t add_write_cu(int64_t write_data_size) override
    {
        write_cu += capacity_unit_calculator::add_write_cu(write_data_size);
        return write_cu;
    }

    explicit mock_capacity_unit_calculator(dsn::replication::replica_base *r)
        : capacity_unit_calculator(r)
    {
    }

    void reset()
    {
        write_cu = 0;
        read_cu = 0;
    }

    int64_t write_cu{0};
    int64_t read_cu{0};
};

class capacity_unit_calculator_test : public pegasus_server_test_base
{
protected:
    std::unique_ptr<mock_capacity_unit_calculator> _cal;

public:
    capacity_unit_calculator_test() : pegasus_server_test_base()
    {
        _cal = dsn::make_unique<mock_capacity_unit_calculator>(_server.get());
    }

    void test_init() {}
};

TEST_F(capacity_unit_calculator_test, init) { test_init(); }

TEST_F(capacity_unit_calculator_test, get)
{
    _cal->add_get_cu(rocksdb::Status::kOk, dsn::blob::create_from_bytes("abc"));
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    _cal->add_get_cu(rocksdb::Status::kCorruption, dsn::blob::create_from_bytes("abc"));
    ASSERT_EQ(_cal->read_cu, 0);
    _cal->reset();
}

TEST_F(capacity_unit_calculator_test, multi_get) {}

TEST_F(capacity_unit_calculator_test, scan) {}

TEST_F(capacity_unit_calculator_test, sortkey_count) {}

TEST_F(capacity_unit_calculator_test, ttl) {}

TEST_F(capacity_unit_calculator_test, put) {}

TEST_F(capacity_unit_calculator_test, remove) {}

TEST_F(capacity_unit_calculator_test, multi_put) {}

TEST_F(capacity_unit_calculator_test, multi_remove) {}

TEST_F(capacity_unit_calculator_test, incr) {}

TEST_F(capacity_unit_calculator_test, check_and_set) {}

TEST_F(capacity_unit_calculator_test, check_and_mutate) {}

} // namespace server
} // namespace pegasus
