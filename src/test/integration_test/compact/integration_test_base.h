// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <cstdlib>
#include <string>
#include <vector>
#include <climits>
#include <map>
#include <memory>
#include <boost/lexical_cast.hpp>

#include <dsn/service_api_c.h>
#include <dsn/dist/replication/replication_ddl_client.h>

#include <pegasus/client.h>
#include <gtest/gtest.h>

using namespace dsn::replication;

class integration_test_base : public testing::Test {
protected:
    virtual void SetUp();
    virtual void TearDown();

    void clear_onebox();
    void start_onebox(int meta_count, int replica_count);
    int32_t create_table(const std::string &table_name);
    void write_data(int count);
    void verify_data(int count);
    void restart_all_meta();
    void restart_all_replica();
    void wait_util_app_full_health(const std::string &table_name,
                                   int max_wait_seconds);

    void init_ddl_client();
    void init_pg_client(const std::string &table_name);
    std::vector<std::string> gen_key_value(int index);
    void operate_on_server(const std::string& server,
                           const std::string& operate,
                           int index);

public:
    std::shared_ptr<replication_ddl_client> ddl_client;
    pegasus::pegasus_client *pg_client = nullptr;

    static int32_t test_app_id;
    static int32_t meta_count;
    static int32_t replica_count;

    static const std::string pegasus_root;
    static const std::string test_app_name;
    static const std::string key_prefix;
    static const std::string value_prefix;
    static const int32_t partition_count = 4;
};