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

#include "runtime/rpc/rpc_address.h"

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>

#include "runtime/rpc/group_address.h"
#include "utils/fixed_size_buffer_pool.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/string_conv.h"
#include "absl/strings/string_view.h"
#include "utils/strings.h"

namespace dsn {

const rpc_address rpc_address::s_invalid_address;

/*static*/
uint32_t rpc_address::ipv4_from_host(const char *name)
{
    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    if ((addr.sin_addr.s_addr = inet_addr(name)) == (unsigned int)(-1)) {
        // TODO(yingchun): use getaddrinfo instead
        hostent *hp = ::gethostbyname(name);
        if (dsn_unlikely(hp == nullptr)) {
            LOG_ERROR("gethostbyname failed, name = {}, err = {}", name, hstrerror(h_errno));
            return 0;
        }

        memcpy((void *)&(addr.sin_addr.s_addr), (const void *)hp->h_addr, (size_t)hp->h_length);
    }

    // converts from network byte order to host byte order
    return (uint32_t)ntohl(addr.sin_addr.s_addr);
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
    if (absl::string_view(netcard_interface).find("docker") != absl::string_view::npos) {
        return true;
    }
    uint32_t iphost = ntohl(ip_net);
    return iphost == 0xAC112A01; // 172.17.42.1
}

/*static*/
uint32_t rpc_address::ipv4_from_network_interface(const char *network_interface)
{
    uint32_t ret = 0;

    struct ifaddrs *ifa = nullptr;
    if (getifaddrs(&ifa) == 0) {
        struct ifaddrs *i = ifa;
        while (i != nullptr) {
            if (i->ifa_name != nullptr && i->ifa_addr != nullptr &&
                i->ifa_addr->sa_family == AF_INET) {
                uint32_t ip_val = ((struct sockaddr_in *)i->ifa_addr)->sin_addr.s_addr;
                if (utils::equals(i->ifa_name, network_interface) ||
                    (network_interface[0] == '\0' && !is_docker_netcard(i->ifa_name, ip_val) &&
                     is_site_local_address(ip_val))) {
                    ret = (uint32_t)ntohl(ip_val);
                    break;
                }
                LOG_DEBUG("skip interface({}), address({})",
                          i->ifa_name,
                          rpc_address(ip_val, 0).ipv4_str());
            }
            i = i->ifa_next;
        }

        if (i == nullptr) {
            LOG_ERROR("get local ip from network interfaces failed, network_interface = {}",
                      network_interface);
        } else {
            LOG_INFO("get ip address from network interface({}), addr({}), input interface({})",
                     i->ifa_name,
                     rpc_address(ret, 0).ipv4_str(),
                     network_interface);
        }

        if (ifa != nullptr) {
            // remember to free it
            freeifaddrs(ifa);
        }
    }

    return ret;
}

rpc_address::~rpc_address() { set_invalid(); }

rpc_address::rpc_address(const rpc_address &another) { *this = another; }

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
    rpc_group_address *addr = new rpc_group_address(name);
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
        inet_ntop(AF_INET, &net_addr, p, sz);
    } else {
        p = (char *)"invalid_ipv4";
    }
    return p;
}

bool rpc_address::from_string_ipv4(const char *s)
{
    set_invalid();
    std::string ip_port(s);
    auto pos = ip_port.find_last_of(':');
    if (pos == std::string::npos) {
        return false;
    }
    std::string ip = ip_port.substr(0, pos);
    std::string port = ip_port.substr(pos + 1);
    // check port
    unsigned int port_num;
    if (!internal::buf2unsigned(port, port_num) || port_num > UINT16_MAX) {
        return false;
    }
    // check localhost & IP
    uint32_t ip_addr;
    if (ip == "localhost" || inet_pton(AF_INET, ip.c_str(), &ip_addr)) {
        assign_ipv4(ip.c_str(), (uint16_t)port_num);
        return true;
    }
    return false;
}

const char *rpc_address::to_string() const
{
    char *p = bf.next();
    auto sz = bf.get_chunk_size();
    struct in_addr net_addr;
    int ip_len;

    switch (_addr.v4.type) {
    case HOST_TYPE_IPV4:
        net_addr.s_addr = htonl(ip());
        inet_ntop(AF_INET, &net_addr, p, sz);
        ip_len = strlen(p);
        snprintf_p(p + ip_len, sz - ip_len, ":%hu", port());
        break;
    case HOST_TYPE_GROUP:
        p = (char *)group_address()->name();
        break;
    default:
        p = (char *)"invalid address";
        break;
    }

    return (const char *)p;
}

rpc_address::rpc_address(const struct sockaddr_in &addr)
{
    set_invalid();
    _addr.v4.type = HOST_TYPE_IPV4;
    _addr.v4.ip = static_cast<uint32_t>(ntohl(addr.sin_addr.s_addr));
    _addr.v4.port = ntohs(addr.sin_port);
}

} // namespace dsn
