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
# pragma once

# include <dsn/service_api_c.h>
# include <dsn/cpp/autoref_ptr.h>
# include <unordered_map>
# include <unordered_set>
# include <vector>

namespace dsn
{

    class rpc_group_address;
    typedef ref_ptr<rpc_group_address> address_group_ptr;

    class rpc_address
    {
    public:
        ~rpc_address() { clear(); }

        rpc_address(uint32_t ip, uint16_t port);
        rpc_address(uint32_t* ipv6, uint16_t port);
        rpc_address(const char* uri, uint16_t port);
        rpc_address(dsn_group_t g);
        rpc_address(dsn_host_type_t type, const char* name, uint16_t port);

        void assign(uint32_t ip, uint16_t port);
        void assign(uint32_t* ipv6, uint16_t port);
        void assign(const char* uri, uint16_t port);
        void assign(dsn_group_t g);
        void assign(dsn_host_type_t type, const char* name, uint16_t port);

        rpc_address();
        rpc_address(const rpc_address& addr);
        //rpc_address(rpc_address&& addr);
        rpc_address(dsn_address_t& addr);
        rpc_address& operator=(const dsn_address_t& addr);

        dsn_host_type_t type() const { return _addr.type; }
        const dsn_address_t& c_addr() const { return _addr; }
        dsn_address_t* c_addr_ptr() { return &_addr; }
        uint32_t ip() const { return _addr.ip; }
        uint16_t port() const { return _addr.port; }
        rpc_group_address* group_address() { return (rpc_group_address*)_addr.group; }
        const uint32_t* ipv6() const { return &_addr.ipv6[0]; }
        const char* uri() const { return _addr.uri; }
        const char* name() const;
        const std::string& str_name() const;
        bool is_invalid() const { return _addr.type == HOST_TYPE_INVALID; }
        void set_invalid() { clear(); _addr.type = HOST_TYPE_INVALID; }

        bool operator == (const ::dsn::rpc_address& r) const;
        bool operator != (const ::dsn::rpc_address& r) const;
        bool operator <  (const ::dsn::rpc_address& r) const;

    private:
        void clear();

    private:
        dsn_address_t       _addr;
        mutable std::string _name;
    };
}

namespace std
{
    template<>
    struct hash<::dsn::rpc_address> 
    {
        size_t operator()(const ::dsn::rpc_address &ep) const 
        {
            switch (ep.type())
            {
            case HOST_TYPE_IPV4:
                return std::hash<uint32_t>()(ep.ip()) ^ std::hash<uint16_t>()(ep.port());
            case HOST_TYPE_IPV6:
                return std::hash<uint32_t>()(ep.ipv6()[0]) 
                    ^ std::hash<uint32_t>()(ep.ipv6()[1])
                    ^ std::hash<uint32_t>()(ep.ipv6()[2])
                    ^ std::hash<uint32_t>()(ep.ipv6()[3])
                    ^ std::hash<uint16_t>()(ep.port());
            case HOST_TYPE_URI:
                return std::hash<string>()(ep.str_name()) ^ std::hash<uint16_t>()(ep.port());
            case HOST_TYPE_GROUP:
                return std::hash<void*>()(ep.c_addr().group);
            default:
                return 0;
            }
        }
    };
}

namespace dsn
{
    //
    // TODO: thread safety and dll interface support
    //
    class rpc_group_address : public ref_counter
    {
    public:
        rpc_group_address(const char* name);
        bool add(const rpc_address& addr);
        void set_leader(const rpc_address& addr);
        bool remove(const rpc_address& addr);
        bool contains(const rpc_address& addr);

        dsn_group_t handle() const { return (dsn_group_t)this; }
        const std::vector<rpc_address>& members() const { return _members; }
        const rpc_address& random_member() const { return _members[dsn_random32(0, (uint32_t)_members.size()-1)]; }
        const rpc_address& next(const rpc_address& current) const;
        const rpc_address& leader() const { return _leader_index >= 0 ? _members[_leader_index] : _invalid; };
        const rpc_address& leader_always_valid();
        const char* name() const { return _name.c_str(); }
        const rpc_address& address() const { return _group_address; }

    private:
        typedef std::vector<rpc_address> members_t;
        members_t _members;
        int         _leader_index;
        std::string _name;
        rpc_address _group_address;
        rpc_address _invalid;
    };

    // ------------- inline implementation -------------------
    inline rpc_address::rpc_address(uint32_t ip, uint16_t port)
    {
        _addr.type = HOST_TYPE_INVALID;
        assign(ip, port);
    }

    inline rpc_address::rpc_address(uint32_t* ipv6, uint16_t port)
    {
        _addr.type = HOST_TYPE_INVALID;
        assign(ipv6, port);
    }

    inline rpc_address::rpc_address(const char* uri, uint16_t port)
    {
        _addr.type = HOST_TYPE_INVALID;
        assign(uri, port);
    }

    inline rpc_address::rpc_address(dsn_group_t g)
    {
        _addr.type = HOST_TYPE_INVALID;
        assign(g);
    }

    inline rpc_address::rpc_address(dsn_host_type_t type, const char* name, uint16_t port)
    {
        _addr.type = HOST_TYPE_INVALID;
        assign(type, name, port);
    }

    inline void rpc_address::assign(uint32_t ip, uint16_t port)
    {
        clear();
        _addr.type = HOST_TYPE_IPV4;
        _addr.ip = ip;
        _addr.port = port;
    }

    inline void rpc_address::assign(uint32_t* ipv6, uint16_t port)
    {
        clear();
        _addr.type = HOST_TYPE_IPV6;
        memcpy((void*)_addr.ipv6, (const void*)ipv6, sizeof(_addr.ipv6));
        _addr.port = port;
    }

    inline void rpc_address::assign(const char* uri, uint16_t port)
    {
        clear();
        _name = uri;
        _addr.type = HOST_TYPE_URI;
        _addr.uri = _name.c_str();
        _addr.port = port;
    }

    inline void rpc_address::assign(dsn_group_t g)
    {
        clear();
        _addr.type = HOST_TYPE_GROUP;
        _addr.group = g;
        dsn_group_add_ref(g); // released on destruct
    }

    inline void rpc_address::assign(dsn_host_type_t type, const char* name, uint16_t port)
    {
        clear();
        _addr.type = type;
        _name = name;
        _addr.port = port;
        dsn_host_from_name(type, name, &_addr);
    }

    inline rpc_address::rpc_address()
    {
        _addr.type = HOST_TYPE_INVALID;
    }

    inline rpc_address::rpc_address(const rpc_address& addr)
    {
        _addr = addr._addr;
        _name = addr._name;

        if (_addr.type == HOST_TYPE_GROUP)
            dsn_group_add_ref(_addr.group); // released on destruct
    }

    inline rpc_address::rpc_address(dsn_address_t& addr)
    {
        _addr = addr;

        if (_addr.type == HOST_TYPE_GROUP)
            dsn_group_add_ref(_addr.group); // released on destruct
    }

    inline rpc_address& rpc_address::operator=(const dsn_address_t& addr)
    {
        clear();
        _addr = addr;

        if (_addr.type == HOST_TYPE_GROUP)
            dsn_group_add_ref(_addr.group); // released on destruct

        return *this;
    }

    inline const char* rpc_address::name() const
    {
        if (_name.length() == 0)
        {
            _name.resize(16);
            dsn_host_to_name(&_addr, (char*)_name.c_str(), 16);
        }
        return _name.c_str();
    }

    inline const std::string& rpc_address::str_name() const
    {
        if (_name.length() == 0)
        {
            _name.resize(16);
            dsn_host_to_name(&_addr, (char*)_name.c_str(), 16);
        }
        return _name;
    }

    inline bool rpc_address::operator == (const ::dsn::rpc_address& r) const
    {
        if (_addr.type != r.type())
            return false;

        switch (_addr.type)
        {
        case HOST_TYPE_IPV4:
            return _addr.ip == r.ip() && _addr.port == r.port();
        case HOST_TYPE_IPV6:
            return memcmp((const void*)_addr.ipv6, (const void*)r._addr.ipv6, sizeof(_addr.ipv6)) == 0 && _addr.port == r.port();
        case HOST_TYPE_URI:
            return strcmp(_addr.uri, r.uri()) == 0 && _addr.port == r.port();
        case HOST_TYPE_GROUP:
            return _addr.group == r.c_addr().group;
        default:
            return true;
        }
    }

    inline bool rpc_address::operator != (const ::dsn::rpc_address& r) const
    {
        return !(*this == r);
    }

    inline bool rpc_address::operator < (const ::dsn::rpc_address& r) const
    {
        if (_addr.type != r.type())
            return _addr.type < r.type();

        int c = 0;
        switch (_addr.type)
        {
        case HOST_TYPE_IPV4:
            return _addr.ip < r.ip() || (_addr.ip == r.ip() && _addr.port < r.port());
        case HOST_TYPE_IPV6:
            c = memcmp((const void*)_addr.ipv6, (const void*)r._addr.ipv6, sizeof(_addr.ipv6));
            return c < 0 || (c == 0 && _addr.port < r.port());
        case HOST_TYPE_URI:
            c = strcmp(_addr.uri, r.uri());
            return c < 0 || (c == 0 && _addr.port < r.port());
        case HOST_TYPE_GROUP:
            return _addr.group < r.c_addr().group;
        default:
            return true;
        }
    }

    inline void rpc_address::clear()
    {
        if (_addr.type == HOST_TYPE_GROUP)
        {
            dsn_group_release(_addr.group);
        }
        _addr.type = HOST_TYPE_INVALID;
        _name.clear();
    }

    // --------------- for rpc_group_address ----------------------

    inline rpc_group_address::rpc_group_address(const char* name)
    {
        _name = name;
        _leader_index = -1;
        _group_address.assign(handle());
    }

    inline bool rpc_group_address::add(const rpc_address& addr)
    {
        if (_members.end() == std::find(_members.begin(), _members.end(), addr))
        {
            _members.push_back(addr);
            return true;
        }
        else
            return false;
    }

    inline void rpc_group_address::set_leader(const rpc_address& addr)
    {
        if (addr.is_invalid())
        {
            _leader_index = -1;
        }
        else
        {
            for (int i = 0; i < (int)_members.size(); i++)
            {
                if (_members[i] == addr)
                {
                    _leader_index = i;
                    return;
                }
            }

            _members.push_back(addr);
            _leader_index = (int)(_members.size() - 1);
        }
    }

    inline const rpc_address& rpc_group_address::leader_always_valid()
    {
        if (_leader_index == -1)
            return random_member();
        else
            return _members[_leader_index];
    }

    inline bool rpc_group_address::remove(const rpc_address& addr)
    {
        auto it = std::find(_members.begin(), _members.end(), addr);
        bool r = (it != _members.end());
        if (r)
        {
            if (-1 != _leader_index && addr == _members[_leader_index])
                _leader_index = -1;

            _members.erase(it);
        }
        return r;
    }

    inline bool rpc_group_address::contains(const rpc_address& addr)
    {
        return _members.end() != std::find(_members.begin(), _members.end(), addr);
    }

    inline const rpc_address& rpc_group_address::next(const rpc_address& current) const
    {
        if (current.is_invalid())
            return random_member();
        else
        {
            auto it = std::find(_members.begin(), _members.end(), current);
            if (it == _members.end())
                return random_member();
            else
            {
                it++;
                return it == _members.end() ? _members[0] : *it;
            }
        }
    }
}
