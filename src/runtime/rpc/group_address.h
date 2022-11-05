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
 *     group_address is a collection of rpc_addresses, usually used for replication
 *
 * Revision history:
 *     Sep., 2015, @imzhenyu, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <algorithm>
#include "utils/api_utilities.h"
#include "runtime/api_layer1.h"
#include "utils/synchronize.h"
#include "utils/autoref_ptr.h"
#include "utils/rand.h"
#include "utils/rpc_address.h"

namespace dsn {
class rpc_group_address : public dsn::ref_counter
{
public:
    rpc_group_address(const char *name);
    rpc_group_address(const rpc_group_address &other);
    rpc_group_address &operator=(const rpc_group_address &other);
    bool add(rpc_address addr);
    void add_list(const std::vector<rpc_address> &list)
    {
        for (const rpc_address &r : list) {
            add(r);
        }
    }
    void set_leader(rpc_address addr);
    bool remove(rpc_address addr);
    bool contains(rpc_address addr);
    int count();

    const std::vector<rpc_address> &members() const { return _members; }
    rpc_address random_member() const
    {
        alr_t l(_lock);
        return _members.empty() ? rpc_address::s_invalid_address
                                : _members[rand::next_u32(0, (uint32_t)_members.size() - 1)];
    }
    rpc_address next(rpc_address current) const;
    rpc_address leader() const
    {
        alr_t l(_lock);
        return _leader_index >= 0 ? _members[_leader_index] : rpc_address::s_invalid_address;
    }
    void leader_forward();
    rpc_address possible_leader();
    bool is_update_leader_automatically() const { return _update_leader_automatically; }
    void set_update_leader_automatically(bool value) { _update_leader_automatically = value; }
    const char *name() const { return _name.c_str(); }

private:
    typedef std::vector<rpc_address> members_t;
    typedef ::dsn::utils::auto_read_lock alr_t;
    typedef ::dsn::utils::auto_write_lock alw_t;

    mutable ::dsn::utils::rw_lock_nr _lock;
    members_t _members;
    int _leader_index;
    bool _update_leader_automatically;
    std::string _name;
};

// ------------------ inline implementation --------------------

inline rpc_group_address::rpc_group_address(const char *name)
{
    _name = name;
    _leader_index = -1;
    _update_leader_automatically = true;
}

inline rpc_group_address::rpc_group_address(const rpc_group_address &other)
{
    _name = other._name;
    _leader_index = other._leader_index;
    _update_leader_automatically = other._update_leader_automatically;
    _members = other._members;
}

inline rpc_group_address &rpc_group_address::operator=(const rpc_group_address &other)
{
    _name = other._name;
    _leader_index = other._leader_index;
    _update_leader_automatically = other._update_leader_automatically;
    _members = other._members;
    return *this;
}

inline bool rpc_group_address::add(rpc_address addr)
{
    CHECK_EQ_MSG(addr.type(), HOST_TYPE_IPV4, "rpc group address member must be ipv4");

    alw_t l(_lock);
    if (_members.end() == std::find(_members.begin(), _members.end(), addr)) {
        _members.push_back(addr);
        return true;
    } else {
        return false;
    }
}

inline void rpc_group_address::leader_forward()
{
    alw_t l(_lock);
    if (_members.empty())
        return;
    _leader_index = (_leader_index + 1) % _members.size();
}

inline void rpc_group_address::set_leader(rpc_address addr)
{
    alw_t l(_lock);
    if (addr.is_invalid()) {
        _leader_index = -1;
    } else {
        CHECK_EQ_MSG(addr.type(), HOST_TYPE_IPV4, "rpc group address member must be ipv4");
        for (int i = 0; i < (int)_members.size(); i++) {
            if (_members[i] == addr) {
                _leader_index = i;
                return;
            }
        }

        _members.push_back(addr);
        _leader_index = (int)(_members.size() - 1);
    }
}

inline rpc_address rpc_group_address::possible_leader()
{
    alr_t l(_lock);
    if (_members.empty())
        return rpc_address::s_invalid_address;
    if (_leader_index == -1)
        _leader_index = rand::next_u32(0, (uint32_t)_members.size() - 1);
    return _members[_leader_index];
}

inline bool rpc_group_address::remove(rpc_address addr)
{
    alw_t l(_lock);
    auto it = std::find(_members.begin(), _members.end(), addr);
    bool r = (it != _members.end());
    if (r) {
        if (-1 != _leader_index && addr == _members[_leader_index])
            _leader_index = -1;

        _members.erase(it);
    }
    return r;
}

inline bool rpc_group_address::contains(rpc_address addr)
{
    alr_t l(_lock);
    return _members.end() != std::find(_members.begin(), _members.end(), addr);
}

inline int rpc_group_address::count()
{
    alr_t l(_lock);
    return _members.size();
}

inline rpc_address rpc_group_address::next(rpc_address current) const
{
    alr_t l(_lock);
    if (_members.empty())
        return rpc_address::s_invalid_address;
    if (current.is_invalid())
        return _members[rand::next_u32(0, (uint32_t)_members.size() - 1)];
    else {
        auto it = std::find(_members.begin(), _members.end(), current);
        if (it == _members.end())
            return _members[rand::next_u32(0, (uint32_t)_members.size() - 1)];
        else {
            it++;
            return it == _members.end() ? _members[0] : *it;
        }
    }
}
} // namespace dsn
