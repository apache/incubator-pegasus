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

# include <dsn/service_api_c.h>
# include <dsn/cpp/utils.h>
# include <dsn/cpp/msg_binary_io.h>
# include <list>
# include <map>
# include <set>
# include <vector>

// pod types
#define DEFINE_POD_SERIALIZATION(T) \
    inline void marshall(::dsn::binary_writer& writer, const T& val)\
    {\
    writer.write((const char*)&val, static_cast<int>(sizeof(val))); \
    }\
    inline void unmarshall(::dsn::binary_reader& reader, __out_param T& val)\
    {\
    reader.read((char*)&val, static_cast<int>(sizeof(T))); \
    }

template<typename T>
inline void marshall(dsn_message_t msg, const T& val)
{
    ::dsn::msg_binary_writer writer(msg);
    marshall(writer, val);
}

template<typename T>
inline void unmarshall(dsn_message_t msg, __out_param T& val)
{
    ::dsn::msg_binary_reader reader(msg);
    unmarshall(reader, val);
}

namespace dsn {

    typedef ::dsn_address_t end_point;
    
#ifndef DSN_NOT_USE_DEFAULT_SERIALIZATION

        DEFINE_POD_SERIALIZATION(bool)
        DEFINE_POD_SERIALIZATION(char)
        //DEFINE_POD_SERIALIZATION(size_t)
        DEFINE_POD_SERIALIZATION(float)
        DEFINE_POD_SERIALIZATION(double)
        DEFINE_POD_SERIALIZATION(int8_t)
        DEFINE_POD_SERIALIZATION(uint8_t)
        DEFINE_POD_SERIALIZATION(int16_t)
        DEFINE_POD_SERIALIZATION(uint16_t)
        DEFINE_POD_SERIALIZATION(int32_t)
        DEFINE_POD_SERIALIZATION(uint32_t)
        DEFINE_POD_SERIALIZATION(int64_t)
        DEFINE_POD_SERIALIZATION(uint64_t)

    // error_code
    inline void marshall(::dsn::binary_writer& writer, const error_code& val)
    {
        int err = val.get();
        marshall(writer, err);
    }

    inline void unmarshall(::dsn::binary_reader& reader, __out_param error_code& val)
    {
        int err;
        unmarshall(reader, err);
        val = err;
    }

    // end point
    inline void unmarshall(::dsn::binary_reader& reader, __out_param dsn_address_t& val)
    {
        reader.read_pod(val.ip);
        reader.read_pod(val.port);
        reader.read((char*)val.name, (int)sizeof(val.name));
    }

    inline void marshall(::dsn::binary_writer& writer, const dsn_address_t& val)
    {
        writer.write_pod(val.ip);
        writer.write_pod(val.port);
        writer.write((const char*)val.name, (int)sizeof(val.name));
    }


    // std::string
    inline void marshall(::dsn::binary_writer& writer, const std::string& val)
    {
        writer.write(val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, __out_param std::string& val)
    {
        reader.read(val);
    }

    // end point
    //extern inline void marshall(::dsn::binary_writer& writer, const dsn_address_t& val);
    //extern inline void unmarshall(::dsn::binary_reader& reader, __out_param dsn_address_t& val);

    // blob
    inline void marshall(::dsn::binary_writer& writer, const blob& val)
    {
        writer.write(val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, __out_param blob& val)
    {
        reader.read(val);
    }

    // for generic list
    template<typename T>
    inline void marshall(::dsn::binary_writer& writer, const std::list<T>& val)
    {
        int sz = static_cast<int>(val.size());
        marshall(writer, sz);
        for (auto& v : val)
        {
            marshall(writer, v);
        }
    }

    template<typename T>
    inline void unmarshall(::dsn::binary_reader& reader, __out_param std::list<T>& val)
    {
        int sz;
        unmarshall(reader, sz);
        val.resize(sz);
        for (auto& v : val)
        {
            unmarshall(reader, v);
        }
    }

    // for generic vector
    template<typename T>
    inline void marshall(::dsn::binary_writer& writer, const std::vector<T>& val)
    {
        int sz = static_cast<int>(val.size());
        marshall(writer, sz);
        for (auto& v : val)
        {
            marshall(writer, v);
        }
    }

    template<typename T>
    inline void unmarshall(::dsn::binary_reader& reader, __out_param std::vector<T>& val)
    {
        int sz;
        unmarshall(reader, sz);
        val.resize(sz);
        for (auto& v : val)
        {
            unmarshall(reader, v);
        }
    }

    // for generic set
    template<typename T>
    inline void marshall(::dsn::binary_writer& writer, const std::set<T, std::less<T>, std::allocator<T>>& val)
    {
        int sz = static_cast<int>(val.size());
        marshall(writer, sz);
        for (auto& v : val)
        {
            marshall(writer, v);
        }
    }

    template<typename T>
    inline void unmarshall(::dsn::binary_reader& reader, __out_param std::set<T, std::less<T>, std::allocator<T>>& val)
    {
        int sz;
        unmarshall(reader, sz);
        val.resize(sz);
        for (auto& v : val)
        {
            unmarshall(reader, v);
        }
    }
#endif
}
