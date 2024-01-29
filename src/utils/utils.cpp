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

#include "utils/utils.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <fstream>
#include <memory>
#include <vector>

#include "runtime/rpc/rpc_address.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"

#if defined(__linux__)
#elif defined(__FreeBSD__)
#include <sys/thr.h>
#elif defined(__APPLE__)
#include <pthread.h>
#endif

namespace dsn {
namespace utils {

bool hostname_from_ip(uint32_t ip, std::string *hostname_result)
{
    struct sockaddr_in addr_in;
    addr_in.sin_family = AF_INET;
    addr_in.sin_port = 0;
    addr_in.sin_addr.s_addr = ip;
    char hostname[256];
    int err = getnameinfo((struct sockaddr *)(&addr_in),
                          sizeof(struct sockaddr),
                          hostname,
                          sizeof(hostname),
                          nullptr,
                          0,
                          NI_NAMEREQD);
    if (err != 0) {
        struct in_addr net_addr;
        net_addr.s_addr = ip;
        char ip_str[256];
        inet_ntop(AF_INET, &net_addr, ip_str, sizeof(ip_str));
        if (err == EAI_SYSTEM) {
            LOG_WARNING("got error {} when try to resolve {}", strerror(errno), ip_str);
        } else {
            LOG_WARNING("return error({}) when try to resolve {}", gai_strerror(err), ip_str);
        }
        return false;
    } else {
        *hostname_result = std::string(hostname);
        return true;
    }
}

bool hostname_from_ip(const char *ip, std::string *hostname_result)
{
    uint32_t ip_addr;
    if (inet_pton(AF_INET, ip, &ip_addr) != 1) {
        // inet_pton() returns 1 on success (network address was successfully converted)
        *hostname_result = ip;
        return false;
    }
    if (!hostname_from_ip(ip_addr, hostname_result)) {
        *hostname_result = ip;
        return false;
    }
    return true;
}

bool hostname_from_ip_port(const char *ip_port, std::string *hostname_result)
{
    dsn::rpc_address addr;
    if (!addr.from_string_ipv4(ip_port)) {
        LOG_WARNING("invalid ip_port({})", ip_port);
        *hostname_result = ip_port;
        return false;
    }
    if (!hostname(addr, hostname_result)) {
        *hostname_result = ip_port;
        return false;
    }
    return true;
}

bool hostname(const rpc_address &address, std::string *hostname_result)
{
    if (address.type() != HOST_TYPE_IPV4) {
        return false;
    }
    if (hostname_from_ip(htonl(address.ip()), hostname_result)) {
        *hostname_result += ":" + std::to_string(address.port());
        return true;
    }
    return false;
}

bool list_hostname_from_ip(const char *ip_list, std::string *hostname_result_list)
{
    std::vector<std::string> splitted_ip;
    dsn::utils::split_args(ip_list, splitted_ip, ',');

    if (splitted_ip.empty()) {
        LOG_WARNING("invalid ip_list({})", ip_list);
        *hostname_result_list = *ip_list;
        return false;
    }

    std::string temp;
    std::stringstream result;
    bool all_ok = true;
    for (int i = 0; i < splitted_ip.size(); ++i) {
        result << (i ? "," : "");
        if (hostname_from_ip(splitted_ip[i].c_str(), &temp)) {
            result << temp;
        } else {
            result << splitted_ip[i].c_str();
            all_ok = false;
        }
    }
    *hostname_result_list = result.str();
    return all_ok;
}

bool list_hostname_from_ip_port(const char *ip_port_list, std::string *hostname_result_list)
{
    std::vector<std::string> splitted_ip_port;
    dsn::utils::split_args(ip_port_list, splitted_ip_port, ',');

    if (splitted_ip_port.empty()) {
        LOG_WARNING("invalid ip_list({})", ip_port_list);
        *hostname_result_list = *ip_port_list;
        return false;
    }

    std::string temp;
    std::stringstream result;
    bool all_ok = true;
    for (int i = 0; i < splitted_ip_port.size(); ++i) {
        result << (i ? "," : "");
        if (hostname_from_ip_port(splitted_ip_port[i].c_str(), &temp)) {
            result << temp;
        } else {
            result << splitted_ip_port[i].c_str();
            all_ok = false;
        }
    }
    *hostname_result_list = result.str();
    return all_ok;
}
} // namespace utils
} // namespace dsn
