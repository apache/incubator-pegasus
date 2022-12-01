/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <gtest/gtest.h>

#include "runtime/rpc/group_address.h"
#include "runtime/rpc/rpc_address.h"

namespace dsn {

static inline uint32_t host_ipv4(uint8_t sec1, uint8_t sec2, uint8_t sec3, uint8_t sec4)
{
    uint32_t ip = 0;
    ip |= (uint32_t)sec1 << 24;
    ip |= (uint32_t)sec2 << 16;
    ip |= (uint32_t)sec3 << 8;
    ip |= (uint32_t)sec4;
    return ip;
}

TEST(rpc_address_test, rpc_address_ipv4_from_host)
{
    // localhost --> 127.0.0.1
    // on some systems "localhost" could be "127.0.1.1" (debian)
    ASSERT_TRUE(host_ipv4(127, 0, 0, 1) == rpc_address::ipv4_from_host("localhost") ||
                host_ipv4(127, 0, 1, 1) == rpc_address::ipv4_from_host("localhost"));

    // 127.0.0.1 --> 127.0.0.1
    ASSERT_EQ(host_ipv4(127, 0, 0, 1), rpc_address::ipv4_from_host("127.0.0.1"));
}

TEST(rpc_address_test, rpc_address_ipv4_from_network_interface)
{
    ASSERT_EQ(host_ipv4(127, 0, 0, 1), rpc_address::ipv4_from_network_interface("lo"));
    ASSERT_EQ(host_ipv4(0, 0, 0, 0),
              rpc_address::ipv4_from_network_interface("not_exist_interface"));
}

TEST(rpc_address_test, is_site_local_address)
{
    ASSERT_FALSE(rpc_address::is_site_local_address(htonl(host_ipv4(1, 2, 3, 4))));
    ASSERT_TRUE(rpc_address::is_site_local_address(htonl(host_ipv4(10, 235, 111, 111))));
    ASSERT_FALSE(rpc_address::is_site_local_address(htonl(host_ipv4(171, 11, 11, 11))));
    ASSERT_TRUE(rpc_address::is_site_local_address(htonl(host_ipv4(172, 16, 2, 2))));
    ASSERT_TRUE(rpc_address::is_site_local_address(htonl(host_ipv4(172, 31, 234, 255))));
    ASSERT_FALSE(rpc_address::is_site_local_address(htonl(host_ipv4(191, 128, 1, 2))));
    ASSERT_TRUE(rpc_address::is_site_local_address(htonl(host_ipv4(192, 168, 3, 45))));
    ASSERT_FALSE(rpc_address::is_site_local_address(htonl(host_ipv4(201, 201, 201, 201))));
}

TEST(rpc_address_test, is_docker_netcard)
{
    ASSERT_TRUE(rpc_address::is_docker_netcard("docker0", htonl(host_ipv4(1, 2, 3, 4))));
    ASSERT_TRUE(rpc_address::is_docker_netcard("10docker5", htonl(host_ipv4(4, 5, 6, 8))));
    ASSERT_FALSE(rpc_address::is_docker_netcard("eth0", htonl(host_ipv4(192, 168, 123, 123))));
    ASSERT_TRUE(rpc_address::is_docker_netcard("eth0", htonl(host_ipv4(172, 17, 42, 1))));
}

TEST(rpc_address_test, rpc_address_to_string)
{
    {
        rpc_address addr;
        addr.assign_ipv4(host_ipv4(127, 0, 0, 1), 8080);
        ASSERT_EQ(std::string("127.0.0.1:8080"), addr.to_std_string());
    }

    {
        const char *name = "test_group";
        rpc_address addr;
        addr.assign_group(name);
        ASSERT_EQ(std::string(name), addr.to_std_string());
    }

    {
        rpc_address addr;
        ASSERT_EQ(std::string("invalid address"), addr.to_std_string());
    }
}

TEST(rpc_address_test, dsn_address_build)
{
    {
        rpc_address addr;
        addr.assign_ipv4(host_ipv4(127, 0, 0, 1), 8080);
        ASSERT_EQ(HOST_TYPE_IPV4, addr.type());
        ASSERT_EQ(host_ipv4(127, 0, 0, 1), addr.ip());
        ASSERT_EQ(8080, addr.port());

        ASSERT_TRUE(rpc_address("127.0.0.1", 8080) == rpc_address("localhost", 8080) ||
                    rpc_address("127.0.1.1", 8080) == rpc_address("localhost", 8080));
        ASSERT_EQ(addr, rpc_address("127.0.0.1", 8080));
        ASSERT_EQ(addr, rpc_address(host_ipv4(127, 0, 0, 1), 8080));
    }

    {
        const char *name = "test_group";
        rpc_address addr;
        addr.assign_group(name);

        ASSERT_EQ(HOST_TYPE_GROUP, addr.type());
        ASSERT_STREQ(name, addr.group_address()->name());
        ASSERT_EQ(1, addr.group_address()->get_count());
    }
}

TEST(rpc_address_test, operators)
{
    rpc_address addr(1234, 123);
    ASSERT_EQ(addr, addr);

    {
        rpc_address new_addr(addr);
        ASSERT_EQ(addr, new_addr);
    }

    {
        rpc_address new_addr(1234, 321);
        ASSERT_NE(addr, new_addr);
    }

    rpc_address addr_grp;
    ASSERT_EQ(addr_grp, addr_grp);
    ASSERT_NE(addr, addr_grp);

    addr_grp.assign_group("test_group");
    ASSERT_TRUE(addr_grp.group_address()->add(addr));
    ASSERT_NE(addr, addr_grp);

    {
        rpc_address new_addr_grp(addr_grp);
        ASSERT_EQ(addr_grp, new_addr_grp);
    }
}

TEST(rpc_address_test, rpc_group_address)
{
    rpc_address addr("127.0.0.1", 8080);
    rpc_address invalid_addr;
    rpc_address addr2("127.0.0.1", 8081);

    rpc_address t;
    t.assign_group("test_group");
    ASSERT_EQ(HOST_TYPE_GROUP, t.type());
    rpc_group_address *g = t.group_address();
    ASSERT_EQ(std::string("test_group"), g->name());
    ASSERT_EQ(1, g->get_count());

    // { }
    ASSERT_FALSE(g->remove(addr));
    ASSERT_FALSE(g->contains(addr));
    ASSERT_EQ(0u, g->members().size());
    ASSERT_EQ(invalid_addr, g->random_member());
    ASSERT_EQ(invalid_addr, g->next(addr));
    ASSERT_EQ(invalid_addr, g->leader());
    ASSERT_EQ(invalid_addr, g->possible_leader());

    // { addr }
    ASSERT_TRUE(g->add(addr));
    ASSERT_FALSE(g->add(addr));
    ASSERT_TRUE(g->contains(addr));
    ASSERT_EQ(1u, g->members().size());
    ASSERT_EQ(addr, g->members().at(0));
    ASSERT_EQ(addr, g->random_member());
    ASSERT_EQ(addr, g->next(addr));
    ASSERT_EQ(addr, g->next(invalid_addr));
    ASSERT_EQ(addr, g->next(addr2));
    ASSERT_EQ(invalid_addr, g->leader());
    ASSERT_EQ(addr, g->possible_leader());

    // { addr* }
    g->set_leader(addr);
    ASSERT_TRUE(g->contains(addr));
    ASSERT_EQ(1u, g->members().size());
    ASSERT_EQ(addr, g->members().at(0));
    ASSERT_EQ(addr, g->leader());
    ASSERT_EQ(addr, g->possible_leader());

    // { addr, addr2* }
    g->set_leader(addr2);
    ASSERT_TRUE(g->contains(addr));
    ASSERT_TRUE(g->contains(addr2));
    ASSERT_EQ(2u, g->members().size());
    ASSERT_EQ(addr, g->members().at(0));
    ASSERT_EQ(addr2, g->members().at(1));
    ASSERT_EQ(addr2, g->leader());
    ASSERT_EQ(addr2, g->possible_leader());
    ASSERT_EQ(addr, g->next(addr2));
    ASSERT_EQ(addr2, g->next(addr));

    // { addr, addr2 }
    g->set_leader(invalid_addr);
    ASSERT_TRUE(g->contains(addr));
    ASSERT_TRUE(g->contains(addr2));
    ASSERT_EQ(2u, g->members().size());
    ASSERT_EQ(addr, g->members().at(0));
    ASSERT_EQ(addr2, g->members().at(1));
    ASSERT_EQ(invalid_addr, g->leader());

    // { addr*, addr2 }
    g->set_leader(addr);
    ASSERT_TRUE(g->contains(addr));
    ASSERT_TRUE(g->contains(addr2));
    ASSERT_EQ(2u, g->members().size());
    ASSERT_EQ(addr, g->members().at(0));
    ASSERT_EQ(addr2, g->members().at(1));
    ASSERT_EQ(addr, g->leader());

    // { uri_addr }
    ASSERT_TRUE(g->remove(addr));
    ASSERT_FALSE(g->contains(addr));
    ASSERT_TRUE(g->contains(addr2));
    ASSERT_EQ(1u, g->members().size());
    ASSERT_EQ(addr2, g->members().at(0));
    ASSERT_EQ(invalid_addr, g->leader());

    // { }
    ASSERT_TRUE(g->remove(addr2));
    ASSERT_FALSE(g->contains(addr2));
    ASSERT_EQ(0u, g->members().size());
    ASSERT_EQ(invalid_addr, g->leader());
}

} // namespace dsn
