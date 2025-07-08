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

#include "atomic_write_test.h"

#include <fmt/core.h>
#include <memory>

#include "client/replication_ddl_client.h"
#include "common/common.h"
#include "common/replication_other_types.h"
#include "include/pegasus/client.h"
#include "utils/error_code.h"

namespace pegasus {

const std::string AtomicWriteTest::kClusterName("onebox"); 
const int32_t AtomicWriteTest::kPartitionCount{8};

AtomicWriteTest::AtomicWriteTest(const std::string &table_name)
{
    ASSERT_TRUE(dsn::replication::replica_helper::load_servers_from_config(
        dsn::PEGASUS_CLUSTER_SECTION_NAME, kClusterName, _meta_list));
    ASSERT_FALSE(_meta_list.empty());

    const auto _ddl_client = std::make_unique<dsn::replication::replication_ddl_client>(_meta_list);
    ASSERT_TRUE(_ddl_client != nullptr);

    _ddl_client->set_max_wait_app_ready_secs(120);
    _ddl_client->set_meta_servers_leader();

    const auto &test_case = GetParam();
    _table_name = fmt::format("{}{}_idempotent", table_name, test_case.atomic_idempotent ? "" : "_non");

    ASSERT_EQ(dsn::ERR_OK, _ddl_client->create_app(_table_name, "pegasus", 8, 3, {}, false, false, test_case.atomic_idempotent));
}

AtomicWriteTest::~AtomicWriteTest()
{
    ASSERT_EQ(dsn::ERR_OK, _ddl_client->drop_app(table_name_, 0));
}

void AtomicWriteTest::SetUpTestCase() { ASSERT_TRUE(pegasus_client_factory::initialize("config.ini")); }

void AtomicWriteTest::SetUp()
{
    _client = pegasus_client_factory::get_client(kClusterName.c_str(), _table_name.c_str());
    ASSERT_TRUE(_client != nullptr);
}

std::vector<atomic_write_case> generate_atomic_write_cases() 
{
    return std::vector<atomic_write_case>({
            {false}, {true},
            });
}

} // namespace pegasus
