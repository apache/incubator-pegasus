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

#include "rpc/rpc_address.h"

#include <arpa/inet.h>
#include <errno.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <string>
#include <string_view>

#include "rpc/group_address.h"
#include "utils/error_code.h"
#include "utils/fixed_size_buffer_pool.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/safe_strerror_posix.h"
#include "utils/string_conv.h"
#include "utils/strings.h"

namespace dsn {
/*static*/
error_s rpc_address::GetAddrInfo(std::string_view hostname, const addrinfo &hints, AddrInfo *info)
{
    addrinfo *res = nullptr;
    const int rc = ::getaddrinfo(hostname.data(), nullptr, &hints, &res);
    const int err = errno; // preserving the errno from the getaddrinfo() call
    AddrInfo result(res, ::freeaddrinfo);
    if (dsn_unlikely(rc != 0)) {
        if (rc == EAI_SYSTEM) {
            const auto &err_msg = utils::safe_strerror(err);
            LOG_ERROR("getaddrinfo failed, name = {}, err = {}", hostname, err_msg);
            return error_s::make(ERR_NETWORK_FAILURE, err_msg);
        }
        LOG_ERROR("getaddrinfo failed, name = {}, err = {}", hostname, gai_strerror(rc));
        return error_s::make(ERR_NETWORK_FAILURE, gai_strerror(rc));
    }

    if (info != nullptr) {
        info->swap(result);
    }
    return error_s::ok();
}

bool extract_host_port(const std::string_view &host_port, std::string_view &host, uint16_t &port)
{
    // Check the port field is present.
    const auto pos = host_port.find_last_of(':');
    if (dsn_unlikely(pos == std::string::npos)) {
        LOG_ERROR("bad format, should be in the form of <ip|host>:port, but got '{}'", host_port);
        return false;
    }

    // Check port.
    const auto port_str = host_port.substr(pos + 1);
    if (dsn_unlikely(!dsn::buf2uint16(port_str, port))) {
        LOG_ERROR("bad port, should be an uint16_t integer, but got '{}'", port_str);
        return false;
    }

    host = host_port.substr(0, pos);
    return true;
}

const rpc_address rpc_address::s_invalid_address;

rpc_address::rpc_address(uint32_t ip, uint16_t port)
{
    _addr.v4.type = HOST_TYPE_IPV4;
    _addr.v4.ip = ip;
    _addr.v4.port = port;
    static_assert(sizeof(rpc_address) == sizeof(uint64_t),
                  "make sure rpc_address does not add new payload to "
                  "rpc_address to keep it sizeof(uint64_t)");
}

rpc_address::rpc_address(const struct sockaddr_in &addr)
{
    _addr.v4.type = HOST_TYPE_IPV4;
    _addr.v4.ip = static_cast<uint32_t>(ntohl(addr.sin_addr.s_addr));
    _addr.v4.port = ntohs(addr.sin_port);
}

rpc_address::rpc_address(const rpc_address &another) { *this = another; }

rpc_address::~rpc_address() { set_invalid(); }

/*static*/
error_s rpc_address::ipv4_from_host(std::string_view hostname, uint32_t *ip)
{
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    AddrInfo result;
    RETURN_NOT_OK(GetAddrInfo(hostname, hints, &result));
    CHECK_EQ(result.get()->ai_family, AF_INET);
    auto *ipv4 = reinterpret_cast<struct sockaddr_in *>(result.get()->ai_addr);
    // converts from network byte order to host byte order
    if (ip != nullptr) {
        *ip = ntohl(ipv4->sin_addr.s_addr);
    }
    return error_s::ok();
}

/*static*/
bool rpc_address::is_site_local_address(uint32_t ip_net)
{
    uint32_t iphost = ntohl(ip_net);
    return (iphost >= 0x0A000000 && iphost <= 0x0AFFFFFF) || // 10.0.0.0-10.255.255.255
           (iphost >= 0xAC100000 && iphost <= 0xAC1FFFFF) || // 172.16.0.0-172.31.255.255
           (iphost >= 0xC0A80000 && iphost <= 0xC0A8FFFF) || // 192.168.0.0-192.168.255.255
           false;
}

/*static*/
bool rpc_address::is_docker_netcard(const char *netcard_interface, uint32_t ip_net)
{
    if (std::string_view(netcard_interface).find("docker") != std::string_view::npos) {
        return true;
    }
    uint32_t iphost = ntohl(ip_net);
    return iphost == 0xAC112A01; // 172.17.42.1
}

/*static*/
uint32_t rpc_address::ipv4_from_network_interface(const char *network_interface)
{
    // Get the network interface list.
    struct ifaddrs *ifa = nullptr;
    if (::getifaddrs(&ifa) != 0) {
        LOG_ERROR("{}: fail to getifaddrs", utils::safe_strerror(errno));
        return 0;
    }

    uint32_t ret = 0;
    struct ifaddrs *i = ifa;
    for (; i != nullptr; i = i->ifa_next) {
        // Skip non-IPv4 network interface.
        if (i->ifa_name == nullptr || i->ifa_addr == nullptr || i->ifa_addr->sa_family != AF_INET) {
            continue;
        }

        uint32_t ip_val = ((struct sockaddr_in *)i->ifa_addr)->sin_addr.s_addr;
        // Get the specified network interface, or the first internal network interface if not
        // specified.
        if (utils::equals(i->ifa_name, network_interface) ||
            (utils::is_empty(network_interface) && !is_docker_netcard(i->ifa_name, ip_val) &&
             is_site_local_address(ip_val))) {
            ret = static_cast<uint32_t>(ntohl(ip_val));
            break;
        }

        LOG_DEBUG(
            "skip interface({}), address({})", i->ifa_name, rpc_address(ip_val, 0).ipv4_str());
    }

    if (i == nullptr) {
        LOG_ERROR("get ip address from network interface({}) failed", network_interface);
    } else {
        LOG_INFO("get ip address from network interface({}), addr({}), input interface({})",
                 i->ifa_name,
                 rpc_address(ret, 0).ipv4_str(),
                 network_interface);
    }

    if (ifa != nullptr) {
        ::freeifaddrs(ifa);
    }

    return ret;
}

rpc_address &rpc_address::operator=(const rpc_address &another)
{
    if (this == &another) {
        // avoid memory leak
        return *this;
    }
    set_invalid();
    _addr = another._addr;
    switch (another.type()) {
    case HOST_TYPE_GROUP:
        group_address()->add_ref();
        break;
    default:
        break;
    }
    return *this;
}

void rpc_address::assign_group(const char *name)
{
    set_invalid();
    _addr.group.type = HOST_TYPE_GROUP;
    auto *addr = new rpc_group_address(name);
    // take the lifetime of rpc_uri_address, release_ref when change value or call destructor
    addr->add_ref();
    _addr.group.group = (uint64_t)addr;
}

void rpc_address::set_invalid()
{
    switch (type()) {
    case HOST_TYPE_GROUP:
        group_address()->release_ref();
        break;
    default:
        break;
    }
    _addr.value = 0;
}

static __thread fixed_size_buffer_pool<8, 256> bf;

const char *rpc_address::ipv4_str() const
{
    char *p = bf.next();
    auto sz = bf.get_chunk_size();
    struct in_addr net_addr;

    if (_addr.v4.type == HOST_TYPE_IPV4) {
        net_addr.s_addr = htonl(ip());
        ::inet_ntop(AF_INET, &net_addr, p, sz);
    } else {
        p = (char *)"invalid_ipv4";
    }
    return p;
}

rpc_address rpc_address::from_ip_port(std::string_view ip_port)
{
    std::string_view ip;
    uint16_t port;
    if (dsn_unlikely(!extract_host_port(ip_port, ip, port))) {
        return {};
    }

    // Use std::string(ip) to add a null terminator.
    return from_ip_port(std::string(ip), port);
}

rpc_address rpc_address::from_ip_port(std::string_view ip, uint16_t port)
{
    // Check is IPv4 integer.
    uint32_t ip_num;
    int ret = ::inet_pton(AF_INET, ip.data(), &ip_num);
    switch (ret) {
    case 1:
        // ::inet_pton() returns 1 on success (network address was successfully converted)
        return {ntohl(ip_num), port};
    case -1:
        LOG_ERROR(
            "{}: fail to convert '{}:{}' to rpc_address", utils::safe_strerror(errno), ip, port);
        break;
    default:
        LOG_ERROR("'{}' does not contain a character string representing a valid network address",
                  ip);
        break;
    }
    return {};
}

rpc_address rpc_address::from_host_port(std::string_view host_port)
{
    std::string_view host;
    uint16_t port;
    if (dsn_unlikely(!extract_host_port(host_port, host, port))) {
        return {};
    }

    // Use std::string(host) to add a null terminator.
    return from_host_port(std::string(host), port);
}

rpc_address rpc_address::from_host_port(std::string_view hostname, uint16_t port)
{
    uint32_t ip = 0;
    if (!ipv4_from_host(hostname, &ip)) {
        return {};
    }

    rpc_address addr;
    addr._addr.v4.type = HOST_TYPE_IPV4;
    addr._addr.v4.ip = ip;
    addr._addr.v4.port = port;
    return addr;
}

const char *rpc_address::to_string() const
{
    char *p = nullptr;
    switch (_addr.v4.type) {
    case HOST_TYPE_IPV4: {
        const auto sz = bf.get_chunk_size();
        struct in_addr net_addr;
        net_addr.s_addr = htonl(ip());
        p = bf.next();
        ::inet_ntop(AF_INET, &net_addr, p, sz);
        const auto ip_len = strlen(p);
        snprintf_p(p + ip_len, sz - ip_len, ":%hu", port());
        break;
    }
    case HOST_TYPE_GROUP:
        p = (char *)group_address()->name();
        break;
    default:
        p = (char *)"invalid address";
        break;
    }

    return (const char *)p;
}

} // namespace dsn
