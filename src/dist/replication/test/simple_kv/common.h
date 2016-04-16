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
 *     Replication testing framework.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/dist/replication.h>
# include "../../client_lib/replication_common.h"

namespace dsn { namespace replication { namespace test {

extern std::string g_case_input;
extern global_partition_id g_default_gpid;
extern bool g_done;
extern bool g_fail;

const char* partition_status_to_short_string(partition_status::type s);
partition_status::type partition_status_from_short_string(const std::string& str);

// transfer primary_address to node_name
// return "-" if addr.is_invalid()
// return "node@port" if not found
std::string address_to_node(rpc_address addr);
// transfer node_name to primary_address
// return invalid addr if not found
rpc_address node_to_address(const std::string& name);

std::string gpid_to_string(global_partition_id gpid);
bool gpid_from_string(const std::string& str, global_partition_id& gpid);

struct replica_id
{
    global_partition_id gpid;
    std::string node;
    replica_id() : gpid(g_default_gpid) {}
    replica_id(global_partition_id g, const std::string& n) : gpid(g), node(n) {}
    replica_id& operator= (const replica_id& o)
    {
        if (this == &o) return *this;
        gpid = o.gpid;
        node = o.node;
        return *this;
    }
    bool operator< (const replica_id& o) const
    {
        return (gpid < o.gpid) || (gpid == o.gpid && node < o.node);
    }
    bool operator== (const replica_id& o) const
    {
        return gpid == o.gpid && node == o.node;
    }
    bool operator!= (const replica_id& o) const
    {
        return !(*this == o);
    }
    std::string to_string() const;
    bool from_string(const std::string& str);
};

struct replica_state
{
    replica_id id;
    partition_status::type status;
    int64_t ballot;
    decree last_committed_decree;
    decree last_durable_decree; // -1 means not set
    replica_state() : status(partition_status::PS_INACTIVE), ballot(0), last_committed_decree(0),last_durable_decree(-1) {}
    replica_state& operator= (const replica_state& o)
    {
        if (this == &o) return *this;
        id = o.id;
        status = o.status;
        ballot = o.ballot;
        last_committed_decree = o.last_committed_decree;
        last_durable_decree = o.last_durable_decree;
        return *this;
    }
    bool operator== (const replica_state& o) const
    {
        return id == o.id &&
                status == o.status &&
                ballot == o.ballot &&
                last_committed_decree == o.last_committed_decree &&
                (last_durable_decree == -1 || o.last_durable_decree == -1 || last_durable_decree == o.last_durable_decree);
    }
    bool operator!= (const replica_state& o) const
    {
        return !(*this == o);
    }
    std::string to_string() const;
    bool from_string(const std::string& str);
};

struct state_snapshot
{
    std::map<replica_id, replica_state> state_map;
    state_snapshot& operator= (const state_snapshot& o)
    {
        if (this == &o) return *this;
        state_map = o.state_map;
        return *this;
    }
    bool operator== (const state_snapshot& o) const
    {
        return state_map == o.state_map;
    }
    bool operator!= (const state_snapshot& o) const
    {
        return !(*this == o);
    }
    bool operator< (const state_snapshot& o) const
    {
        for (auto& kv : state_map)
        {
            auto find = o.state_map.find(kv.first);
            if (find == o.state_map.end())
                continue;
            const replica_state& oth_state = find->second;
            const replica_state& cur_state = kv.second;
            if (cur_state.ballot > oth_state.ballot
                    || cur_state.last_committed_decree > oth_state.last_committed_decree)
                return false;
            if (cur_state.last_durable_decree != -1 && oth_state.last_durable_decree != -1
                    && cur_state.last_durable_decree > oth_state.last_durable_decree)
                return false;
        }
        return true;
    }
    std::string to_string() const;
    bool from_string(const std::string& str);
    std::string diff_string(const state_snapshot& other) const;
};

struct parti_config
{
    global_partition_id gpid;
    int64_t ballot;
    std::string primary;
    std::vector<std::string> secondaries;
    parti_config() : gpid(g_default_gpid), ballot(0) {}
    parti_config& operator= (const parti_config& o)
    {
        if (this == &o) return *this;
        gpid = o.gpid;
        ballot = o.ballot;
        primary = o.primary;
        secondaries = o.secondaries;
        return *this;
    }
    bool operator== (const parti_config& o) const
    {
        return gpid == o.gpid && ballot == o.ballot
                && primary == o.primary && secondaries == o.secondaries;
    }
    bool operator!= (const parti_config& o) const
    {
        return !(*this == o);
    }
    bool operator< (const parti_config& o) const
    {
        return gpid == o.gpid && ballot < o.ballot;
    }
    std::string to_string() const;
    bool from_string(const std::string& str);
    void convert_from(const partition_configuration& c);
};

}}}

