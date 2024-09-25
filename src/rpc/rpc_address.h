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

#include <arpa/inet.h> // IWYU pragma: keep
#include <cstddef>
#include <cstdint>
// IWYU pragma: no_include <experimental/string_view>
#include <functional>
#include <memory>
#include <sstream>
#include <string_view>

#include "utils/errors.h"
#include "utils/fmt_utils.h"

struct addrinfo;

namespace apache {
namespace thrift {
namespace protocol {
class TProtocol;
} // namespace protocol
} // namespace thrift
} // namespace apache

typedef enum dsn_host_type_t
{
    HOST_TYPE_INVALID = 0,
    HOST_TYPE_IPV4 = 1,
    HOST_TYPE_GROUP = 2,
} dsn_host_type_t;
USER_DEFINED_ENUM_FORMATTER(dsn_host_type_t)

namespace dsn {

using AddrInfo = std::unique_ptr<addrinfo, std::function<void(addrinfo *)>>;

class rpc_group_address;

class rpc_address
{
public:
    // Convert IPv4:port to rpc_address, e.g. "192.168.0.1:12345" or "localhost:54321".
    // NOTE:
    //   - IP address without port (e.g. "127.0.0.1") is considered as invalid.
    static rpc_address from_ip_port(std::string_view ip_port);

    // Similar to the above, but specify the 'ip' and 'port' separately.
    static rpc_address from_ip_port(std::string_view ip, uint16_t port);

    // Convert hostname:port to rpc_address, e.g. "192.168.0.1:12345", "localhost:54321" or
    // "host1:12345".
    // NOTE:
    //   - Hostname without port (e.g. "host1") is considered as invalid.
    //   - It contains a hostname resolve produce, so typically it's slower than from_ip_port().
    //   - It requires 'hostname' is a null terminate string which only contains hostname.
    static rpc_address from_host_port(std::string_view host_port);

    // Similar to the above, but specify the 'host' and 'port' separately.
    static rpc_address from_host_port(std::string_view host, uint16_t port);

    static bool is_docker_netcard(const char *netcard_interface, uint32_t ip_net);
    static bool is_site_local_address(uint32_t ip_net);
    // TODO(yingchun): Use dsn_resolver to resolve hostname.
    static error_s ipv4_from_host(std::string_view hostname, uint32_t *ip);
    static uint32_t ipv4_from_network_interface(const char *network_interface);
    static error_s GetAddrInfo(std::string_view hostname, const addrinfo &hints, AddrInfo *info);

    constexpr rpc_address() = default;
    rpc_address(uint32_t ip, uint16_t port);
    explicit rpc_address(const struct sockaddr_in &addr);
    rpc_address(const rpc_address &another);
    ~rpc_address();

    rpc_address &operator=(const rpc_address &another);

    void assign_group(const char *name);

    const char *to_string() const;

    // return a.b.c.d if address is ipv4
    const char *ipv4_str() const;

    uint64_t &value() { return _addr.value; }

    dsn_host_type_t type() const { return (dsn_host_type_t)_addr.v4.type; }

    uint32_t ip() const { return (uint32_t)_addr.v4.ip; }

    uint16_t port() const { return (uint16_t)_addr.v4.port; }

    void set_port(uint16_t port) { _addr.v4.port = port; }

    rpc_group_address *group_address() const
    {
        return (rpc_group_address *)(uintptr_t)_addr.group.group;
    }

    operator bool() const { return _addr.v4.type != HOST_TYPE_INVALID; }

    // before you assign new value, must call set_invalid() to release original value
    // and you MUST ensure that _addr is INITIALIZED before you call this function
    void set_invalid();

    bool operator==(const rpc_address &r) const
    {
        if (this == &r) {
            return true;
        }

        if (type() != r.type()) {
            return false;
        }

        switch (type()) {
        case HOST_TYPE_IPV4:
            return ip() == r.ip() && port() == r.port();
        case HOST_TYPE_GROUP:
            return _addr.group.group == r._addr.group.group;
        default:
            return true;
        }
    }

    bool operator!=(const rpc_address &r) const { return !(*this == r); }

    bool operator<(const rpc_address &r) const
    {
        if (type() != r.type())
            return type() < r.type();

        switch (type()) {
        case HOST_TYPE_IPV4:
            return ip() < r.ip() || (ip() == r.ip() && port() < r.port());
        case HOST_TYPE_GROUP:
            return _addr.group.group < r._addr.group.group;
        default:
            return true;
        }
    }

    friend std::ostream &operator<<(std::ostream &os, const rpc_address &addr)
    {
        return os << addr.to_string();
    }

    // for serialization in thrift format
    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;

private:
    friend class rpc_group_address;
    friend class test_client;
    template <typename TResponse>
    friend class rpc_replier;

    static const rpc_address s_invalid_address;

    union
    {
        struct
        {
            unsigned long long type : 2;
            unsigned long long padding : 14;
            unsigned long long port : 16;
            unsigned long long ip : 32;
        } v4; ///< \ref HOST_TYPE_IPV4
        struct
        {
            unsigned long long type : 2;
            unsigned long long group : 62; ///< dsn_group_t
        } group;                           ///< \ref HOST_TYPE_GROUP
        uint64_t value;
    } _addr{.value = 0};
};

} // namespace dsn

USER_DEFINED_STRUCTURE_FORMATTER(::dsn::rpc_address);

namespace std {

template <>
struct hash<::dsn::rpc_address>
{
    size_t operator()(const ::dsn::rpc_address &ep) const
    {
        switch (ep.type()) {
        case HOST_TYPE_IPV4:
            return std::hash<uint32_t>()(ep.ip()) ^ std::hash<uint16_t>()(ep.port());
        case HOST_TYPE_GROUP:
            return std::hash<void *>()(ep.group_address());
        default:
            return 0;
        }
    }
};

} // namespace std
