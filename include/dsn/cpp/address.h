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
 *     define address helper routines around dsn_address_t structure
 *
 * Revision history:
 *     July, 2015, @imzhenyu (Zhenyu Guo), first version
 *     Aug., 2015, @imzhenyu (Zhenyu Guo), add group and uri address support
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/service_api_c.h>
# include <dsn/cpp/autoref_ptr.h>
# include <unordered_map>
# include <unordered_set>
# include <vector>
# include <cstring> // for strcmp()
# include <string>
# include <cstdlib>

#ifdef DSN_NOT_USE_DEFAULT_SERIALIZATION
# include <thrift/protocol/TProtocol.h>
#endif

namespace dsn
{
    /*!
    @addtogroup rpc-addr
    @{
    */
    class rpc_group_address;
    typedef ref_ptr<rpc_group_address> address_group_ptr;

    class rpc_uri_address;
    
    class rpc_address
    {
    public:
        ~rpc_address() { clear(); }

        rpc_address(uint32_t ip, uint16_t port);
        rpc_address(const char* host, uint16_t port);

        void assign_ipv4(uint32_t ip, uint16_t port);
        void assign_ipv4(const char* host, uint16_t port);
        void assign_ipv4_local_address(const char* card_interface, uint16_t port);
        void assign_uri(dsn_uri_t uri);
        void assign_group(dsn_group_t g);        

        rpc_address();
        rpc_address(const rpc_address& addr);
        rpc_address(dsn_address_t addr);
        rpc_address& operator=(dsn_address_t addr);

        const char* to_string() const;
        std::string to_std_string() const;
        bool from_string_ipv4(const char* s);
        dsn_host_type_t type() const { return (dsn_host_type_t)_addr.u.v4.type; }
        dsn_address_t c_addr() const { return _addr; }
        dsn_address_t* c_addr_ptr() { return &_addr; }
        uint32_t ip() const { return (uint32_t)_addr.u.v4.ip; }
        uint16_t port() const { return (uint16_t)_addr.u.v4.port; }
        rpc_group_address* group_address() const { return (rpc_group_address*)(uintptr_t)_addr.u.group.group; }
        dsn_group_t group_handle() const { return (dsn_group_t)(uintptr_t)_addr.u.group.group; }
        rpc_uri_address* uri_address() const { return (rpc_uri_address*)(uintptr_t)_addr.u.uri.uri; }
        dsn_uri_t uri_handle() const { return (dsn_group_t)(uintptr_t)_addr.u.uri.uri; }
        const char* uri() const { return to_string(); }        
        bool is_invalid() const { return _addr.u.v4.type == HOST_TYPE_INVALID; }
        void set_invalid() { clear(); }

        bool operator == (::dsn::rpc_address r) const;
        bool operator != (::dsn::rpc_address r) const;
        bool operator <  (::dsn::rpc_address r) const;

#ifdef DSN_NOT_USE_DEFAULT_SERIALIZATION
        uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
        uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;
#endif
    private:
        void clear();

    private:
        dsn_address_t       _addr;
    };
    
    // ------------- inline implementation -------------------
    inline rpc_address::rpc_address(uint32_t ip, uint16_t port)
    {
        _addr.u.v4.type = HOST_TYPE_INVALID;
        assign_ipv4(ip, port);

        static_assert (sizeof(rpc_address) == sizeof(dsn_address_t), 
            "make sure rpc_address does not add new payload to dsn_address_t to keep it sizeof(uint64_t)");
    }
    
    inline rpc_address::rpc_address(const char* host, uint16_t port)
    {
        _addr.u.v4.type = HOST_TYPE_INVALID;
        assign_ipv4(host, port);
    }

    inline void rpc_address::assign_ipv4(uint32_t ip, uint16_t port)
    {
        clear();
        _addr.u.v4.type = HOST_TYPE_IPV4;
        _addr.u.v4.ip = ip;
        _addr.u.v4.port = port;
    }

    inline void rpc_address::assign_ipv4(const char* host, uint16_t port)
    {
        clear();
        _addr.u.v4.type = HOST_TYPE_IPV4;
        _addr.u.v4.ip = dsn_ipv4_from_host(host);
        _addr.u.v4.port = port;
    }

    inline void rpc_address::assign_ipv4_local_address(const char* network_interface, uint16_t port)
    {
        clear();
        _addr.u.v4.type = HOST_TYPE_IPV4;
        _addr.u.v4.ip = dsn_ipv4_local(network_interface);
        _addr.u.v4.port = port;
    }

    inline void rpc_address::assign_uri(dsn_uri_t uri)
    {
        clear();
        _addr.u.v4.type = HOST_TYPE_URI;
        _addr.u.uri.uri = (uint64_t)uri;
    }

    inline void rpc_address::assign_group(dsn_group_t g)
    {
        clear();
        _addr.u.v4.type = HOST_TYPE_GROUP;
        _addr.u.group.group = (uint64_t)g;
    }

    inline rpc_address::rpc_address()
    {
        _addr.u.v4.type = HOST_TYPE_INVALID;
    }

    inline rpc_address::rpc_address(const rpc_address& addr)
    {
        _addr = addr._addr;
    }

    inline rpc_address::rpc_address(dsn_address_t addr)
    {
        _addr = addr;
    }

    inline rpc_address& rpc_address::operator=(dsn_address_t addr)
    {
        clear();
        _addr = addr;
        return *this;
    }

    inline bool rpc_address::operator == (::dsn::rpc_address r) const
    {
        if (_addr.u.v4.type != r.type())
            return false;

        switch (_addr.u.v4.type)
        {
        case HOST_TYPE_IPV4:
            return _addr.u.v4.ip == r.ip() && _addr.u.v4.port == r.port();
        case HOST_TYPE_URI:
            return strcmp(to_string(), r.to_string()) == 0;
        case HOST_TYPE_GROUP:
            return _addr.u.group.group == r.c_addr().u.group.group;
        default:
            return true;
        }
    }

    inline bool rpc_address::operator != (::dsn::rpc_address r) const
    {
        return !(*this == r);
    }

    inline bool rpc_address::operator < (::dsn::rpc_address r) const
    {
        if (_addr.u.v4.type != r.type())
            return _addr.u.v4.type < r.type();

        switch (_addr.u.v4.type)
        {
        case HOST_TYPE_IPV4:
            return _addr.u.v4.ip < r.ip() || (_addr.u.v4.ip == r.ip() && _addr.u.v4.port < r.port());
        case HOST_TYPE_URI:
            return strcmp(to_string(), r.to_string()) < 0;
        case HOST_TYPE_GROUP:
            return _addr.u.group.group < r.c_addr().u.group.group;
        default:
            return true;
        }
    }

    inline void rpc_address::clear()
    {
        _addr.u.value = 0;
    }

    inline const char* rpc_address::to_string() const
    {
        return dsn_address_to_string(_addr);
    }

    inline std::string rpc_address::to_std_string() const
    {
        return std::string(to_string());
    }

    inline bool rpc_address::from_string_ipv4(const char* s)
    {
        std::string str = std::string(s);
        auto pos = str.find_last_of(':');
        if (pos == std::string::npos)
            return false;
        else
        {
            auto host = str.substr(0, pos);
            auto port = atoi(str.substr(pos + 1).c_str());
            assign_ipv4(host.c_str(), (uint16_t)port);
            return true;
        }
    }
    /*@}*/
}

namespace std
{
    template<>
    struct hash< ::dsn::rpc_address> 
    {
        size_t operator()(const ::dsn::rpc_address &ep) const 
        {
            switch (ep.type())
            {
            case HOST_TYPE_IPV4:
                return std::hash<uint32_t>()(ep.ip()) ^ std::hash<uint16_t>()(ep.port());
            case HOST_TYPE_URI:
                return std::hash<std::string>()(std::string(ep.to_string()));
            case HOST_TYPE_GROUP:
                return std::hash<void*>()(ep.group_address());
            default:
                return 0;
            }
        }
    };
}
