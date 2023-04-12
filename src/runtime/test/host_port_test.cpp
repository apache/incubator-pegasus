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

#include <gtest/gtest.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <string>

#include "runtime/rpc/group_host_port.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_host_port.h"

namespace dsn {

TEST(host_port_test, host_port_to_string)
{
    {
        host_port hp = host_port("localhost", 8080);
        ASSERT_EQ(std::string("localhost:8080"), hp.to_string());
    }

    {
        host_port hp;
        ASSERT_EQ(std::string("invalid address"), hp.to_string());
    }
}

TEST(host_port_test, host_port_build)
{
    host_port hp = host_port("localhost", 8080);
    ASSERT_EQ(HOST_TYPE_IPV4, hp.type());
    ASSERT_EQ(8080, hp.port());
    ASSERT_EQ("localhost", hp.host());

    {
        rpc_address addr = rpc_address("localhost", 8080);
        host_port hp1(addr);
        ASSERT_EQ(hp, hp1);
    }
}

TEST(host_port_test, operators)
{
    host_port hp("localhost", 8080);
    ASSERT_EQ(hp, hp);

    {
        host_port new_hp(hp);
        ASSERT_EQ(hp, new_hp);
    }

    {
        host_port new_hp("localhost", 8081);
        ASSERT_NE(hp, new_hp);
    }

    host_port hp2;
    ASSERT_NE(hp, hp2);
    ASSERT_FALSE(hp.is_invalid());
    ASSERT_TRUE(hp2.is_invalid());
}

TEST(host_port_test, rpc_group_host_port)
{
    host_port hp("localhost", 8080);
    host_port hp2("localhost", 8081);
    host_port invalid_hp;

    host_port hp_grp;
    hp_grp.assign_group("test_group");
    ASSERT_EQ(HOST_TYPE_GROUP, hp_grp.type());
    rpc_group_host_port *g = hp_grp.group_host_port();
    ASSERT_EQ(std::string("test_group"), g->name());
    ASSERT_EQ(1, g->get_count());

    // invalid_hp
    ASSERT_FALSE(g->remove(hp));
    ASSERT_FALSE(g->contains(hp));
    ASSERT_EQ(0u, g->members().size());
    ASSERT_EQ(invalid_hp, g->random_member());
    ASSERT_EQ(invalid_hp, g->next(hp));
    ASSERT_EQ(invalid_hp, g->leader());
    ASSERT_EQ(invalid_hp, g->possible_leader());

    // hp
    g->set_leader(hp);
    ASSERT_TRUE(g->contains(hp));
    ASSERT_EQ(1u, g->members().size());
    ASSERT_EQ(hp, g->members().at(0));
    ASSERT_EQ(hp, g->leader());
    ASSERT_EQ(hp, g->possible_leader());

    // hp2
    g->set_leader(hp2);
    ASSERT_TRUE(g->contains(hp));
    ASSERT_TRUE(g->contains(hp2));
    ASSERT_EQ(2u, g->members().size());
    ASSERT_EQ(hp, g->members().at(0));
    ASSERT_EQ(hp2, g->members().at(1));
    ASSERT_EQ(hp2, g->leader());
    ASSERT_EQ(hp2, g->possible_leader());
    ASSERT_EQ(hp, g->next(hp2));
    ASSERT_EQ(hp2, g->next(hp));

    // change leader
    g->set_leader(hp);
    ASSERT_TRUE(g->contains(hp));
    ASSERT_TRUE(g->contains(hp2));
    ASSERT_EQ(2u, g->members().size());
    ASSERT_EQ(hp, g->members().at(0));
    ASSERT_EQ(hp2, g->members().at(1));
    ASSERT_EQ(hp, g->leader());
    g->leader_forward();
    ASSERT_EQ(hp2, g->leader());

    // del
    ASSERT_TRUE(g->remove(hp));
    ASSERT_FALSE(g->contains(hp));
    ASSERT_TRUE(g->contains(hp2));
    ASSERT_EQ(1u, g->members().size());
    ASSERT_EQ(hp2, g->members().at(0));
    ASSERT_EQ(invalid_hp, g->leader());

    ASSERT_TRUE(g->remove(hp2));
    ASSERT_FALSE(g->contains(hp2));
    ASSERT_EQ(0u, g->members().size());
    ASSERT_EQ(invalid_hp, g->leader());
}

} // namespace dsn
