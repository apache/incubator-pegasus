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

# include <dsn/ports.h>
# include <dsn/service_api_c.h>
# include <dsn/cpp/address.h>

# ifdef _WIN32


# else
# include <sys/socket.h>
# include <netdb.h>
# include <arpa/inet.h>

# if defined(__FreeBSD__)
# include <netinet/in.h>
# endif

# endif

# include <mutex>

DSN_API dsn_address_t dsn_address_invalid = {0, 0, "invalid"};

static void net_init()
{
    static std::once_flag flag;
    static bool flag_inited = false;
    if (!flag_inited)
    {
        std::call_once(flag, [&]()
        {
#ifdef _WIN32
            WSADATA wsaData;
            WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif
            flag_inited = true;
        });
    }
}

DSN_API void dsn_address_build(dsn_address_t* ep, const char* host, uint16_t port)
{
    net_init();
    
    ep->port = port;
    if (host != ep->name)
    {
        strncpy(ep->name, host, sizeof(ep->name));
    }
    
    sockaddr_in addr;
    memset(&addr,0,sizeof(addr));
    addr.sin_family = AF_INET;
    
    if ((addr.sin_addr.s_addr = inet_addr(host)) == (unsigned int)(-1))
    {
        hostent* hp = gethostbyname(host);
        if (hp != 0) 
        {
            memcpy((void*)&(addr.sin_addr.s_addr), (const void*)hp->h_addr, (size_t)hp->h_length);
        }
    }
    
    // network order
    ep->ip = (uint32_t)ntohl(addr.sin_addr.s_addr);
}


DSN_API void dsn_address_build_ipv4(
    /*out*/ dsn_address_t* ep,
    uint32_t ipv4,
    uint16_t port
    )
{
    net_init();
    ep->ip = ipv4;
    ep->port = port;

    uint32_t addr = htonl(ipv4);
    auto host = gethostbyaddr((char*)&addr, 4, AF_INET);
    if (host == nullptr)
    {
        sprintf(ep->name, "%u.%u.%u.%u", (ipv4 >> 24) & 0xff, (ipv4 >> 16) & 0xff, (ipv4 >> 8) & 0xff, ipv4 & 0xff);
    }
    else
    {
        strncpy(ep->name, host->h_name, sizeof(ep->name));
    }
}
