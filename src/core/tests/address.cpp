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

/*
 * Description:
 *     Unit-test for rpc_address.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include <dsn/cpp/address.h>
# include "../core/group_address.h"
# include <gtest/gtest.h>

using namespace ::dsn;

static inline uint32_t host_ipv4(uint8_t sec1, uint8_t sec2, uint8_t sec3, uint8_t sec4)
{
    uint32_t ip = 0;
    ip |= (uint32_t)sec1 << 24;
    ip |= (uint32_t)sec2 << 16;
    ip |= (uint32_t)sec3 << 8;
    ip |= (uint32_t)sec4;
    return ip;
}

static inline bool operator == (dsn_address_t l, dsn_address_t r)
{
    if (l.u.v4.type != r.u.v4.type)
        return false;

    switch (l.u.v4.type)
    {
        case HOST_TYPE_IPV4:
            return l.u.v4.ip == r.u.v4.ip && l.u.v4.port == r.u.v4.port;
        case HOST_TYPE_URI:
            return strcmp((const char*)(uintptr_t)l.u.uri.uri, (const char*)(uintptr_t)r.u.uri.uri) == 0;
        case HOST_TYPE_GROUP:
            return l.u.group.group == r.u.group.group;
        default:
            return true;
    }
}

TEST(core, dsn_ipv4_from_host)
{
    // localhost --> 127.0.0.1
    ASSERT_EQ(host_ipv4(127, 0, 0, 1), dsn_ipv4_from_host("localhost"));

    // 127.0.0.1 --> 127.0.0.1
    ASSERT_EQ(host_ipv4(127, 0, 0, 1), dsn_ipv4_from_host("127.0.0.1"));
}

TEST(core, dsn_ipv4_local)
{
#ifndef _WIN32
    ASSERT_EQ(host_ipv4(127, 0, 0, 1), dsn_ipv4_local("lo"));
    ASSERT_EQ(host_ipv4(0, 0, 0, 0), dsn_ipv4_local("not_exist_interface"));
#endif
}

TEST(core, dsn_address_to_string)
{
    {
        dsn_address_t addr;
        addr.u.v4.type = HOST_TYPE_IPV4;
        addr.u.v4.ip = host_ipv4(127, 0, 0, 1);
        addr.u.v4.port = 8080;
        ASSERT_EQ(std::string("127.0.0.1:8080"), dsn_address_to_string(addr));
    }

    {
        const char* uri = "http://localhost:8080/";
        dsn_address_t addr;
        addr.u.uri.type = HOST_TYPE_URI;
        addr.u.uri.uri = (uintptr_t)uri;
        ASSERT_EQ(std::string(uri), dsn_address_to_string(addr));
    }

    {
        const char* name = "test_group";
        dsn_group_t g = dsn_group_build(name);
        dsn_address_t addr;
        addr.u.group.type = HOST_TYPE_GROUP;
        addr.u.group.group = (uint64_t)g;
        ASSERT_EQ(std::string(name), dsn_address_to_string(addr));
        dsn_group_destroy(g);
    }

    {
        dsn_address_t addr;
        addr.u.uri.type = HOST_TYPE_INVALID;
        ASSERT_EQ(std::string("invalid address"), dsn_address_to_string(addr));
    }
}

TEST(core, dsn_address_build)
{
    {
        dsn_address_t addr;
        addr.u.v4.type = HOST_TYPE_IPV4;
        addr.u.v4.ip = host_ipv4(127, 0, 0, 1);
        addr.u.v4.port = 8080;

        ASSERT_EQ(addr, dsn_address_build("localhost", 8080));
        ASSERT_EQ(addr, dsn_address_build("127.0.0.1", 8080));
        ASSERT_EQ(addr, dsn_address_build_ipv4(host_ipv4(127, 0, 0, 1), 8080));
    }

    {
        const char* uri = "http://localhost:8080/";
        dsn_uri_t u = dsn_uri_build(uri);

        dsn_address_t addr;
        addr.u.uri.type = HOST_TYPE_URI;
        addr.u.uri.uri = (uintptr_t)u;

        
        ASSERT_EQ(addr, dsn_address_build_uri(u));
        dsn_uri_destroy(u);
    }

    {
        const char* name = "test_group";
        dsn_group_t g = dsn_group_build(name);
        dsn_address_t addr;
        addr.u.group.type = HOST_TYPE_GROUP;
        addr.u.group.group = (uint64_t)g;
        ASSERT_EQ(addr, dsn_address_build_group(g));
        dsn_group_destroy(g);
    }
}

TEST(core, rpc_group_address)
{
    rpc_group_address g("test_group");
    rpc_address addr("127.0.0.1", 8080);
    rpc_address invalid_addr;
    rpc_address uri_addr;
    uri_addr.assign_uri((dsn_uri_t)(uintptr_t)"http://localhost:8080/");

    ASSERT_EQ(std::string("test_group"), g.name());
    rpc_address t;
    t.assign_group((dsn_group_t)(uintptr_t)&g);
    ASSERT_EQ(t, g.address());

    // { }
    ASSERT_FALSE(g.remove(addr));
    ASSERT_FALSE(g.contains(addr));
    ASSERT_EQ(0u, g.members().size());
    ASSERT_EQ(invalid_addr, g.random_member());
    ASSERT_EQ(invalid_addr, g.next(addr));
    ASSERT_EQ(invalid_addr, g.leader());
    ASSERT_EQ(invalid_addr, g.possible_leader());

    // { addr }
    ASSERT_TRUE(g.add(addr));
    ASSERT_FALSE(g.add(addr));
    ASSERT_TRUE(g.contains(addr));
    ASSERT_EQ(1u, g.members().size());
    ASSERT_EQ(addr, g.members().at(0));
    ASSERT_EQ(addr, g.random_member());
    ASSERT_EQ(addr, g.next(addr));
    ASSERT_EQ(addr, g.next(invalid_addr));
    ASSERT_EQ(addr, g.next(uri_addr));
    ASSERT_EQ(invalid_addr, g.leader());
    ASSERT_EQ(addr, g.possible_leader());

    // { addr* }
    g.set_leader(addr);
    ASSERT_TRUE(g.contains(addr));
    ASSERT_EQ(1u, g.members().size());
    ASSERT_EQ(addr, g.members().at(0));
    ASSERT_EQ(addr, g.leader());
    ASSERT_EQ(addr, g.possible_leader());

    // { addr, uri_addr* }
    g.set_leader(uri_addr);
    ASSERT_TRUE(g.contains(addr));
    ASSERT_TRUE(g.contains(uri_addr));
    ASSERT_EQ(2u, g.members().size());
    ASSERT_EQ(addr, g.members().at(0));
    ASSERT_EQ(uri_addr, g.members().at(1));
    ASSERT_EQ(uri_addr, g.leader());
    ASSERT_EQ(uri_addr, g.possible_leader());
    ASSERT_EQ(addr, g.next(uri_addr));
    ASSERT_EQ(uri_addr, g.next(addr));

    // { addr, uri_addr }
    g.set_leader(invalid_addr);
    ASSERT_TRUE(g.contains(addr));
    ASSERT_TRUE(g.contains(uri_addr));
    ASSERT_EQ(2u, g.members().size());
    ASSERT_EQ(addr, g.members().at(0));
    ASSERT_EQ(uri_addr, g.members().at(1));
    ASSERT_EQ(invalid_addr, g.leader());

    // { addr*, uri_addr }
    g.set_leader(addr);
    ASSERT_TRUE(g.contains(addr));
    ASSERT_TRUE(g.contains(uri_addr));
    ASSERT_EQ(2u, g.members().size());
    ASSERT_EQ(addr, g.members().at(0));
    ASSERT_EQ(uri_addr, g.members().at(1));
    ASSERT_EQ(addr, g.leader());

    // { uri_addr }
    ASSERT_TRUE(g.remove(addr));
    ASSERT_FALSE(g.contains(addr));
    ASSERT_TRUE(g.contains(uri_addr));
    ASSERT_EQ(1u, g.members().size());
    ASSERT_EQ(uri_addr, g.members().at(0));
    ASSERT_EQ(invalid_addr, g.leader());

    // { }
    ASSERT_TRUE(g.remove(uri_addr));
    ASSERT_FALSE(g.contains(uri_addr));
    ASSERT_EQ(0u, g.members().size());
    ASSERT_EQ(invalid_addr, g.leader());
}

TEST(core, dsn_group)
{
    dsn_group_t g = dsn_group_build("test_group");
    rpc_address addr("127.0.0.1", 8080);
    rpc_address invalid_addr;
    rpc_address uri_addr;
    uri_addr.assign_uri((dsn_uri_t)(uintptr_t)"http://localhost:8080/");

    // { }
    ASSERT_EQ(invalid_addr.c_addr(), dsn_group_get_leader(g));
    ASSERT_EQ(invalid_addr.c_addr(), dsn_group_next(g, addr.c_addr()));

    // { addr }
    ASSERT_TRUE(dsn_group_add(g, addr.c_addr()));
    ASSERT_FALSE(dsn_group_add(g, addr.c_addr()));
    ASSERT_EQ(invalid_addr.c_addr(), dsn_group_get_leader(g));
    ASSERT_EQ(addr.c_addr(), dsn_group_next(g, addr.c_addr()));

    // { addr* }
    dsn_group_set_leader(g, addr.c_addr());
    ASSERT_EQ(addr.c_addr(), dsn_group_get_leader(g));
    ASSERT_TRUE(dsn_group_is_leader(g, addr.c_addr()));

    // { addr*, uri_addr }
    ASSERT_TRUE(dsn_group_add(g, uri_addr.c_addr()));
    ASSERT_EQ(uri_addr.c_addr(), dsn_group_next(g, addr.c_addr()));
    ASSERT_EQ(addr.c_addr(), dsn_group_next(g, uri_addr.c_addr()));

    // { uri_addr }
    ASSERT_TRUE(dsn_group_remove(g, addr.c_addr()));
    ASSERT_EQ(invalid_addr.c_addr(), dsn_group_get_leader(g));
    ASSERT_EQ(uri_addr.c_addr(), dsn_group_next(g, addr.c_addr()));

    dsn_group_destroy(g);
}
