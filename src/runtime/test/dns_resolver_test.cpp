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

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "rpc/dns_resolver.h"
#include "rpc/group_address.h"
#include "rpc/group_host_port.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"

namespace dsn {

TEST(host_port_test, dns_resolver)
{
    // Resolve HOST_TYPE_IPV4 type host_port.
    {
        host_port hp("localhost", 8080);
        const auto &addr = dns_resolver::instance().resolve_address(hp);
        ASSERT_TRUE(rpc_address::from_ip_port("127.0.0.1", 8080) == addr ||
                    rpc_address::from_ip_port("127.0.1.1", 8080) == addr);
    }

    // Resolve HOST_TYPE_GROUP type host_port.
    {
        host_port hp_grp;
        hp_grp.assign_group("test_group");
        auto g_hp = hp_grp.group_host_port();

        host_port hp1("localhost", 8080);
        ASSERT_TRUE(g_hp->add(hp1));
        host_port hp2("localhost", 8081);
        g_hp->set_leader(hp2);

        const auto &addr_grp = dns_resolver::instance().resolve_address(hp_grp);
        const auto *const g_addr = addr_grp.group_address();

        ASSERT_EQ(g_addr->is_update_leader_automatically(), g_hp->is_update_leader_automatically());
        ASSERT_STREQ(g_addr->name(), g_hp->name());
        ASSERT_EQ(g_addr->count(), g_hp->count());
        ASSERT_EQ(host_port::from_address(g_addr->leader()), g_hp->leader());
    }

    // Resolve host_port list.
    {
        struct host_port_list_test_case
        {
            std::string host_ports;
            std::string ip_ports;
        } test_cases[] = {{"localhost:8080", "127.0.0.1:8080"},
                          {"localhost:8080,localhost:8081,localhost:8082",
                           "127.0.0.1:8080,127.0.0.1:8081,127.0.0.1:8082"}};

        for (const auto &tc : test_cases) {
            ASSERT_EQ(tc.ip_ports, dns_resolver::ip_ports_from_host_ports(tc.host_ports));
        }
    }
}

} // namespace dsn
