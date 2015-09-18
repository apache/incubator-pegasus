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
        rpc_address(dsn_group_t g, bool add_ref = true);
        rpc_address(dsn_host_type_t type, const char* name, uint16_t port);

        void assign(uint32_t ip, uint16_t port);
        void assign(uint32_t* ipv6, uint16_t port);
        void assign(const char* uri, uint16_t port);
        void assign(dsn_group_t g, bool add_ref = true);
        void assign(dsn_host_type_t type, const char* name, uint16_t port);

        rpc_address();
        rpc_address(const rpc_address& addr);
        //rpc_address(rpc_address&& addr);
        rpc_address(const dsn_address_t& addr);
        rpc_address& operator=(const dsn_address_t& addr);

        dsn_host_type_t type() const { return _addr.type; }
        const dsn_address_t& c_addr() const { return _addr; }
        dsn_address_t* c_addr_ptr() { return &_addr; }
        uint32_t ip() const { return _addr.ip; }
        uint16_t port() const { return _addr.port; }
        rpc_group_address* group_address() const { return (rpc_group_address*)_addr.group; }
        dsn_group_t group_handle() const { return _addr.group; }
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

    inline rpc_address::rpc_address(dsn_group_t g, bool add_ref /*= true*/)
    {
        _addr.type = HOST_TYPE_INVALID;
        assign(g, add_ref);
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

    inline void rpc_address::assign(dsn_group_t g, bool add_ref)
    {
        clear();
        _addr.type = HOST_TYPE_GROUP;
        _addr.group = g;

        if (add_ref)
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

    inline rpc_address::rpc_address(const dsn_address_t& addr)
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
