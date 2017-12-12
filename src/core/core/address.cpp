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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#ifdef _WIN32

#define _WINSOCK_DEPRECATED_NO_WARNINGS 1

#include <Winsock2.h>
#include <ws2tcpip.h>
#include <Windows.h>
#pragma comment(lib, "ws2_32.lib")

#else
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#if defined(__FreeBSD__)
#include <netinet/in.h>
#endif

#endif

#include <dsn/utility/ports.h>
#include <dsn/utility/fixed_size_buffer_pool.h>

#include <dsn/c/api_utilities.h>

#include <dsn/tool-api/rpc_address.h>
#include <dsn/tool-api/uri_address.h>
#include <dsn/tool-api/group_address.h>
#include <dsn/tool-api/task.h>

namespace dsn {
const rpc_address rpc_address::sInvalid;

#ifdef _WIN32
static void net_init()
{
    static std::once_flag flag;
    static bool flag_inited = false;
    if (!flag_inited) {
        std::call_once(flag, [&]() {
            WSADATA wsaData;
            WSAStartup(MAKEWORD(2, 2), &wsaData);
            flag_inited = true;
        });
    }
}
#endif

/*static*/
uint32_t rpc_address::ipv4_from_host(const char *name)
{
    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    if ((addr.sin_addr.s_addr = inet_addr(name)) == (unsigned int)(-1)) {
        hostent *hp = ::gethostbyname(name);
        int err =
#ifdef _WIN32
            (int)::WSAGetLastError()
#else
            h_errno
#endif
            ;

        if (hp == nullptr) {
            derror("gethostbyname failed, name = %s, err = %d.", name, err);
            return 0;
        } else {
            memcpy((void *)&(addr.sin_addr.s_addr), (const void *)hp->h_addr, (size_t)hp->h_length);
        }
    }

    // converts from network byte order to host byte order
    return (uint32_t)ntohl(addr.sin_addr.s_addr);
}

/*static*/
// if network_interface is "", then return the first
// site-local ipv4 address: 10.*.*.*, 172.16.*.*, 192.168.*.*
DSN_API uint32_t rpc_address::ipv4_from_network_interface(const char *network_interface)
{
    uint32_t ret = 0;

    static auto is_site_local = [&](uint32_t ip_net) {
        const char *addr = reinterpret_cast<const char *>(&ip_net);
        return addr[0] == 10 || (addr[0] == 172 && addr[1] == 16) ||
               (addr[0] == 192 && addr[1] == 168);
    };

    struct ifaddrs *ifa = nullptr;
    if (getifaddrs(&ifa) == 0) {
        struct ifaddrs *i = ifa;
        while (i != nullptr) {
            if (i->ifa_name != nullptr && i->ifa_addr != nullptr &&
                i->ifa_addr->sa_family == AF_INET) {
                uint32_t ip_val = ((struct sockaddr_in *)i->ifa_addr)->sin_addr.s_addr;
                if (strcmp(i->ifa_name, network_interface) == 0 ||
                    (network_interface[0] == '\0' && is_site_local(ip_val))) {
                    ret = (uint32_t)ntohl(ip_val);
                    break;
                }
            }
            i = i->ifa_next;
        }

        if (i == nullptr) {
            derror("get local ip from network interfaces failed, network_interface = %s",
                   network_interface);
        }

        if (ifa != nullptr) {
            // remember to free it
            freeifaddrs(ifa);
        }
    }

    return ret;
}

rpc_address::~rpc_address() { clear(); }

rpc_address::rpc_address(const rpc_address &another) { *this = another; }

rpc_address &rpc_address::operator=(const rpc_address &another)
{
    if (this == &another) {
        // avoid memory leak
        return *this;
    }
    clear();
    _addr = another._addr;
    switch (another.type()) {
    case HOST_TYPE_GROUP:
        group_address()->add_ref();
        break;
    case HOST_TYPE_URI:
        uri_address()->add_ref();
        break;
    default:
        break;
    }
    return *this;
}

void rpc_address::assign_uri(const char *host_uri)
{
    clear();
    _addr.uri.type = HOST_TYPE_URI;
    dsn::rpc_uri_address *addr = new dsn::rpc_uri_address(host_uri);
    // take the lifetime of rpc_uri_address, release_ref when change value or call deconstruct
    addr->add_ref();
    _addr.uri.uri = (uint64_t)addr;
}

void rpc_address::assign_group(const char *name)
{
    clear();
    _addr.group.type = HOST_TYPE_GROUP;
    dsn::rpc_group_address *addr = new dsn::rpc_group_address(name);
    // take the lifetime of rpc_uri_address, release_ref when change value or call deconstruct
    addr->add_ref();
    _addr.group.group = (uint64_t)addr;
}

rpc_address rpc_address::clone() const
{
    rpc_address new_address;

    if (type() == HOST_TYPE_IPV4) {
        new_address._addr = _addr;
    } else if (type() == HOST_TYPE_URI) {
        dsn::rpc_uri_address *addr = new dsn::rpc_uri_address(*uri_address());
        addr->add_ref();
        new_address._addr.uri.uri = (uint64_t)addr;
        new_address._addr.uri.type = HOST_TYPE_URI;
    } else if (type() == HOST_TYPE_GROUP) {
        dsn::rpc_group_address *addr = new dsn::rpc_group_address(*group_address());
        addr->add_ref();
        new_address._addr.group.group = (uint64_t)addr;
        new_address._addr.group.type = HOST_TYPE_GROUP;
    } else {
        new_address.clear();
    }

    return new_address;
}

void rpc_address::clear()
{
    switch (type()) {
    case HOST_TYPE_GROUP:
        group_address()->release_ref();
        break;
    case HOST_TYPE_URI:
        uri_address()->release_ref();
        break;
    default:
        break;
    }
    _addr.value = 0;
}

static __thread fixed_size_buffer_pool<8, 256> bf;
const char *rpc_address::to_string() const
{
    char *p = bf.next();
    auto sz = bf.get_chunk_size();
    struct in_addr net_addr;
#ifdef _WIN32
    char *ip_str;
#else
    int ip_len;
#endif

    switch (_addr.v4.type) {
    case HOST_TYPE_IPV4:
        net_addr.s_addr = htonl(ip());
#ifdef _WIN32
        ip_str = inet_ntoa(net_addr);
        snprintf_p(p, sz, "%s:%hu", ip_str, (uint16_t)addr.u.v4.port);
#else
        inet_ntop(AF_INET, &net_addr, p, sz);
        ip_len = strlen(p);
        snprintf_p(p + ip_len, sz - ip_len, ":%hu", port());
#endif
        break;
    case HOST_TYPE_URI:
        p = (char *)uri_address()->uri();
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
}
