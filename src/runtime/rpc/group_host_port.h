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

#pragma once

#include <string>
#include <vector>

#include "runtime/rpc/group_host_port.h"
#include "runtime/rpc/rpc_host_port.h"
#include "utils/autoref_ptr.h"
#include "utils/fmt_logging.h"
#include "utils/rand.h"
#include "utils/synchronize.h"

namespace dsn {

static constexpr int kInvalidIndex = -1;


// Base on group_address, a group of host_post.
// Please use host_port like example if you want call group of host_port. 
//  e.g.
//
//  dsn::rpc_host_port group;
//  group.assign_group("test");
//  group.group_host_port()->add(host_port("test_fqdn", 34601));
//  group.group_host_port()->add(host_port("test_fqdn", 34602));
//  group.group_host_port()->add(host_port("test_fqdn", 34603));
//
class rpc_group_host_port : public ref_counter
{
public:
    rpc_group_host_port(const char *name);
    rpc_group_host_port(const rpc_group_address *g_addr);
    rpc_group_host_port(const rpc_group_host_port &other);
    rpc_group_host_port &operator=(const rpc_group_host_port &other);
    bool add(host_port hp) WARN_UNUSED_RESULT;
    void add_list(const std::vector<host_port> &hps)
    {
        for (const auto &hp : hps) {
            LOG_WARNING_IF(!add(hp), "duplicate adress {}", hp);
        }
    }
    void set_leader(host_port hp);
    bool remove(host_port hp) WARN_UNUSED_RESULT;
    bool contains(host_port hp) const WARN_UNUSED_RESULT;
    int count();

    const std::vector<host_port> &members() const { return _members; }
    host_port random_member() const
    {
        alr_t l(_lock);
        return _members.empty() ? host_port::s_invalid_host_port
                                : _members[rand::next_u32(0, static_cast<uint32_t>(_members.size() - 1))];
    }
    host_port next(host_port current) const;
    host_port leader() const
    {
        alr_t l(_lock);
        return _leader_index >= 0 ? _members[_leader_index] : host_port::s_invalid_host_port;
    }
    void leader_forward();
    host_port possible_leader();
    bool is_update_leader_automatically() const { return _update_leader_automatically; }
    void set_update_leader_automatically(bool value) { _update_leader_automatically = value; }
    const char *name() const { return _name.c_str(); }

private:
    typedef std::vector<host_port> members_t;
    typedef utils::auto_read_lock alr_t;
    typedef utils::auto_write_lock alw_t;

    mutable utils::rw_lock_nr _lock;
    members_t _members;
    int _leader_index;
    bool _update_leader_automatically;
    std::string _name;
};

// ------------------ inline implementation --------------------

inline rpc_group_host_port::rpc_group_host_port(const char *name)
{
    _name = name;
    _leader_index = kInvalidIndex;
    _update_leader_automatically = true;
}

inline rpc_group_host_port::rpc_group_host_port(const rpc_group_host_port &other)
{
    _name = other._name;
    _leader_index = other._leader_index;
    _update_leader_automatically = other._update_leader_automatically;
    _members = other._members;
}

inline rpc_group_host_port::rpc_group_host_port(const rpc_group_address *g_addr)
{
    _name = g_addr->name();
    for (const auto &addr : g_addr->members()) {
        CHECK_TRUE(add(host_port(addr)));
    }
    _update_leader_automatically = g_addr->is_update_leader_automatically(); 
    set_leader(host_port(g_addr->leader()));
}

inline rpc_group_host_port &rpc_group_host_port::operator=(const rpc_group_host_port &other)
{
    if (this == &other) {
        return *this;
    }
    _name = other._name;
    _leader_index = other._leader_index;
    _update_leader_automatically = other._update_leader_automatically;
    _members = other._members;
    return *this;
}

inline bool rpc_group_host_port::add(host_port hp)
{
    CHECK_EQ_MSG(hp.type(), HOST_TYPE_IPV4, "rpc group host_port member must be ipv4");

    alw_t l(_lock);
    if (_members.end() == std::find(_members.begin(), _members.end(), hp)) {
        _members.push_back(hp);
        return true;
    } else {
        return false;
    }
}

inline void rpc_group_host_port::leader_forward()
{
    alw_t l(_lock);
    if (_members.empty()) {
        return;
    }
    _leader_index = (_leader_index + 1) % _members.size();
}

inline void rpc_group_host_port::set_leader(host_port hp)
{
    alw_t l(_lock);
    if (hp.is_invalid()) {
        _leader_index = kInvalidIndex;
        return;
    }

    CHECK_EQ_MSG(hp.type(), HOST_TYPE_IPV4, "rpc group host_port member must be ipv4");
    for (int i = 0; i < _members.size(); i++) {
        if (_members[i] == hp) {
            _leader_index = i;
            return;
        }
    }

    _members.push_back(hp);
    _leader_index = static_cast<int>(_members.size() - 1);
}

inline host_port rpc_group_host_port::possible_leader()
{
    alw_t l(_lock);
    if (_members.empty()) {
        return host_port::s_invalid_host_port;
    }
    if (_leader_index == kInvalidIndex) {
        _leader_index = rand::next_u32(0, static_cast<uint32_t>(_members.size() - 1));
    }
    return _members[_leader_index];
}

inline bool rpc_group_host_port::remove(host_port hp)
{
    alw_t l(_lock);
    auto it = std::find(_members.begin(), _members.end(), hp);
    if (it == _members.end()) {
        return false;
    }

    if (kInvalidIndex != _leader_index && hp == _members[_leader_index]) {
        _leader_index = kInvalidIndex;
    }

    _members.erase(it);

    return true;
}

inline bool rpc_group_host_port::contains(host_port hp) const
{
    alr_t l(_lock);
    return _members.end() != std::find(_members.begin(), _members.end(), hp);
}

inline int rpc_group_host_port::count()
{
    alr_t l(_lock);
    return _members.size();
}

inline host_port rpc_group_host_port::next(host_port current) const
{
    alr_t l(_lock);
    if (_members.empty()) {
        return host_port::s_invalid_host_port;
    }

    if (current.is_invalid()) {
        return _members[rand::next_u32(0, static_cast<uint32_t>(_members.size() - 1))];
    }

    auto it = std::find(_members.begin(), _members.end(), current);
    if (it == _members.end()) {
        return _members[rand::next_u32(0, static_cast<uint32_t>(_members.size() - 1))];
    }

    it++;
    return it == _members.end() ? _members[0] : *it;
}

} // namespace dsn
