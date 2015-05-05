/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus(rDSN) -=- 
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
 
# include <dsn/internal/end_point.h>

# ifdef _WIN32


# else
# include <sys/socket.h>
# include <netdb.h>
# include <arpa/inet.h>
# endif

# include <mutex>

namespace dsn {

const end_point end_point::INVALID;

end_point::end_point(const char* str, uint16_t p)
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

    port = p;
    name = std::string(str);

    sockaddr_in addr;
    memset(&addr,0,sizeof(addr));
    addr.sin_family=AF_INET;

    if ((addr.sin_addr.s_addr = inet_addr(str)) == (unsigned int)(-1))
    {
        hostent* hp = gethostbyname(str);
        if (hp != 0) 
        {
            memcpy((void*)&(addr.sin_addr.s_addr), (const void*)hp->h_addr, (size_t)hp->h_length);
        }
    }

    // network order
    ip = (uint32_t)(addr.sin_addr.s_addr);
}

} // end namespace
