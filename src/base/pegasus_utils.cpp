/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "pegasus_utils.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdlib.h>
#include <errno.h>

#include "utils/api_utilities.h"
#include "utils/fmt_logging.h"

namespace pegasus {
namespace utils {

void addr2host(const ::dsn::rpc_address &addr, char *str, int len /* = 100*/)
{
    struct sockaddr_in addr2;
    addr2.sin_addr.s_addr = htonl(addr.ip());
    addr2.sin_family = AF_INET;
    if (getnameinfo((struct sockaddr *)&addr2,
                    sizeof(sockaddr),
                    str,
                    sizeof(char *) * len,
                    nullptr,
                    0,
                    NI_NAMEREQD)) {
        inet_ntop(AF_INET, &(addr2.sin_addr), str, 100);
    }
}

size_t
c_escape_string(const char *src, size_t src_len, char *dest, size_t dest_len, bool always_escape)
{
    const char *src_end = src + src_len;
    size_t used = 0;

    for (; src < src_end; src++) {
        unsigned char c = *src;
        if (always_escape) {
            if (dest_len - used < 5) // space for four-character escape + \0
                return (size_t)-1;
            snprintf(dest + used, 5, "\\x%02X", c);
            used += 4;
            continue;
        }
        if (dest_len - used < 2) // space for two-character escape
            return (size_t)-1;
        switch (c) {
        case '\n':
            dest[used++] = '\\';
            dest[used++] = 'n';
            break;
        case '\r':
            dest[used++] = '\\';
            dest[used++] = 'r';
            break;
        case '\t':
            dest[used++] = '\\';
            dest[used++] = 't';
            break;
        case '\"':
            dest[used++] = '\\';
            dest[used++] = '\"';
            break;
        case '\'':
            dest[used++] = '\\';
            dest[used++] = '\'';
            break;
        case '\\':
            dest[used++] = '\\';
            dest[used++] = '\\';
            break;
        default:
            // Note that if we emit \xNN and the src character after that is a hex
            // digit then that digit must be escaped too to prevent it being
            // interpreted as part of the character code by C.
            if (c < ' ' || c > '~') {
                if (dest_len - used < 5) // space for four-character escape + \0
                    return (size_t)-1;
                snprintf(dest + used, 5, "\\x%02X", c);
                used += 4;
            } else {
                dest[used++] = c;
                break;
            }
        }
    }

    if (dest_len - used < 1) // make sure that there is room for \0
        return (size_t)-1;

    dest[used] = '\0'; // doesn't count towards return value though
    return used;
}

inline unsigned int hex_digit_to_int(char c)
{
    /* Assume ASCII. */
    CHECK('0' == 0x30 && 'A' == 0x41 && 'a' == 0x61, "");
    CHECK(isxdigit(c), "");
    unsigned int x = static_cast<unsigned char>(c);
    if (x > '9') {
        x += 9;
    }
    return x & 0xf;
}

// return < 0 means failed
static int c_unescape_sequences(const char *source, char *dest)
{
    char *d = dest;
    const char *p = source;

    // Small optimization for case where source = dest and there's no escaping
    while (p == d && *p >= ' ' && *p <= '~' && *p != '\\') {
        p++;
        d++;
    }

    while (*p != '\0') {
        if (*p == '\\') {
            switch (*++p) { // skip past the '\\'
            case 'n':
                *d++ = '\n';
                break;
            case 'r':
                *d++ = '\r';
                break;
            case 't':
                *d++ = '\t';
                break;
            case '"':
                *d++ = '\"';
                break;
            case '\'':
                *d++ = '\'';
                break;
            case '\\':
                *d++ = '\\';
                break;
            case 'x':
            case 'X': {
                if (!isxdigit(p[1]) || !isxdigit(p[2])) {
                    return source - p - 1;
                }
                unsigned int ch = hex_digit_to_int(p[1]);
                ch = (ch << 4) + hex_digit_to_int(p[2]);
                *d++ = ch;
                p += 2;
                break;
            }
            default:
                return source - p - 1;
            }
            p++; // read past letter we escaped
        } else if (*p >= ' ' && *p <= '~') {
            *d++ = *p++;
        } else {
            return source - p - 1;
        }
    }
    *d = '\0';
    return d - dest;
}

int c_unescape_string(const std::string &src, std::string &dest)
{
    dest = src;
    int len = c_unescape_sequences(dest.c_str(), &dest[0]);
    if (len >= 0 && len < dest.length())
        dest.resize(len);
    return len;
}

} // namespace utils
} // namespace pegasus
