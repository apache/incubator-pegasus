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

#pragma once

#include <string>
#include <vector>

#include "runtime/rpc/rpc_host_port.h"
#include "utils/autoref_ptr.h"
#include "utils/fmt_logging.h"
#include "utils/rand.h"
#include "utils/synchronize.h"

namespace dsn {

class rpc_group_host_port : public ref_counter
{
public:
    rpc_group_host_port(const char *name);
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
                                : _members[rand::next_u32(0, (uint32_t)_members.size() - 1)];
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
    _leader_index = -1;
    _update_leader_automatically = true;
}

inline rpc_group_host_port::rpc_group_host_port(const rpc_group_host_port &other)
{
    _name = other._name;
    _leader_index = other._leader_index;
    _update_leader_automatically = other._update_leader_automatically;
    _members = other._members;
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
        _leader_index = -1;
        return;
    }

    CHECK_EQ_MSG(hp.type(), HOST_TYPE_IPV4, "rpc group host_port member must be ipv4");
    for (int i = 0; i < (int)_members.size(); i++) {
        if (_members[i] == hp) {
            _leader_index = i;
            return;
        }
    }

    _members.push_back(hp);
    _leader_index = (int)(_members.size() - 1);
}

inline host_port rpc_group_host_port::possible_leader()
{
    alw_t l(_lock);
    if (_members.empty()) {
        return host_port::s_invalid_host_port;
    }
    if (_leader_index == -1) {
        _leader_index = rand::next_u32(0, (uint32_t)_members.size() - 1);
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

    if (-1 != _leader_index && hp == _members[_leader_index]) {
        _leader_index = -1;
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
        return _members[rand::next_u32(0, (uint32_t)_members.size() - 1)];
    }

    auto it = std::find(_members.begin(), _members.end(), current);
    if (it == _members.end()) {
        return _members[rand::next_u32(0, (uint32_t)_members.size() - 1)];
    }

    it++;
    return it == _members.end() ? _members[0] : *it;
}

} // namespace dsn
