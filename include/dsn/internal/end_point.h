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
# pragma once

# include <dsn/internal/dsn_types.h>
# include <dsn/internal/utils.h>

namespace dsn {

#define MAX_NODE_NAME_LENGTH MAX_COMPUTERNAME_LENGTH

struct end_point
{
    uint32_t ip; // network order
    uint16_t port;
    std::string name;

    end_point()
    {
        ip = 0;
        port = 0;
    }
    
    end_point(uint32_t ip, uint16_t port, const char* n = "simulation")
        : name(n)
    {
        ip = ip;
        port = port;
    }

    end_point(const end_point& source)
    {
        ip = source.ip;
        port = source.port;
        name = source.name;
    }

    end_point(const char* str, uint16_t port);

    bool operator == (const end_point& r) const
    {
        return ip == r.ip && port == r.port;
    }

    bool operator < (const end_point& r) const
    {
        return (ip < r.ip) || (ip == r.ip && port < r.port);
    }

    bool operator != (const end_point& r) const
    {
        return !(*this == r);
    }

    std::string to_ip_string(bool dotted = true) const
    {
        char buffer[32];
        if (dotted)
        {
            sprintf(buffer, "%u.%u.%u.%u",
                ip & 0x000000ff,
                (ip & 0x0000ff00) >> 8,
                (ip & 0x00ff0000) >> 16,
                (ip & 0xff000000) >> 24
                );
        }
        else
        {
            sprintf(buffer, "%u", ip);
        }
        return buffer;
    }

    std::string to_port_string(uint16_t addMore = 0) const
    {
        char buffer[16];
        sprintf(buffer, "%u", (uint32_t)(port + addMore));
        return buffer;
    }

    bool operator()(const end_point& r) const
    {
        return port < r.port || (port == r.port && ip < r.ip);
    }

    static const end_point INVALID;
};

#ifndef ZION_NOT_USE_DEFAULT_SERIALIZATION

inline void unmarshall(::dsn::binary_reader& reader, __out_param end_point& val) 
{
    reader.read_pod(val.ip);
    reader.read_pod(val.port);
    reader.read(val.name);
}

inline void marshall(::dsn::binary_writer& writer, const end_point& val, uint16_t pos = 0xffff) 
{
    writer.write_pod(val.ip, pos);
    writer.write_pod(val.port, pos);
    writer.write(val.name, pos);
}

#endif

} // end namespace

namespace std
{
    template<>
    struct hash<::dsn::end_point> {
        size_t operator()(const ::dsn::end_point &ep) const {
            return std::hash<uint32_t>()(ep.ip) ^ std::hash<uint16_t>()(ep.port);
        }
    };
}


