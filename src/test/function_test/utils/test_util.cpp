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

#include "test_util.h"

#include <vector>

#include "base/pegasus_const.h"
#include "dsn/dist/replication/replication_ddl_client.h"
#include "dsn/dist/replication/replication_other_types.h"
#include "dsn/tool-api/rpc_address.h"
#include "include/pegasus/client.h"

using dsn::replication::replica_helper;
using dsn::replication::replication_ddl_client;
using dsn::rpc_address;
using std::vector;

namespace pegasus {

test_util::test_util() {}

test_util::~test_util() {}

void test_util::SetUpTestCase() { ASSERT_TRUE(pegasus_client_factory::initialize("config.ini")); }

void test_util::SetUp()
{
    client = pegasus_client_factory::get_client("mycluster", "temp");
    ASSERT_TRUE(client != nullptr);
    vector<rpc_address> meta_list;
    ASSERT_TRUE(replica_helper::load_meta_servers(
        meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "mycluster"));
    ASSERT_FALSE(meta_list.empty());
    ddl_client = std::make_shared<replication_ddl_client>(meta_list);
    ASSERT_TRUE(ddl_client != nullptr);
}

} // namespace pegasus
