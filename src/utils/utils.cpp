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
#include <memory>

#include "utils/fmt_logging.h"

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
} // namespace utils
} // namespace dsn
