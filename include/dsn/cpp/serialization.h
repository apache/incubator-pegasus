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

# pragma once

# include <dsn/service_api_c.h>
# include <dsn/cpp/utils.h>
# include <dsn/cpp/address.h>
# include <dsn/cpp/rpc_stream.h>
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
    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ T& val)\
    {\
    reader.read((char*)&val, static_cast<int>(sizeof(T))); \
    }

template<typename T>
inline void marshall(dsn_message_t msg, const T& val)
{
    ::dsn::rpc_write_stream writer(msg);
    marshall(writer, val);
}

template<typename T>
inline void unmarshall(dsn_message_t msg, /*out*/ T& val)
{
    ::dsn::rpc_read_stream reader(msg);
    unmarshall(reader, val);
}

//template<typename T>
//inline void marshall(::dsn::rpc_write_stream writer, const T& val)
//{
//    marshall(writer, val);
//}
//
//template<typename T>
//inline void unmarshall(::dsn::rpc_read_stream reader, /*out*/ T& val)
//{
//    unmarshall(reader, val);
//}

namespace dsn {
    
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
        // avoid memory copy, equal to writer.write(std::string)
        const char* cstr = val.to_string();
        int len = static_cast<int>(strlen(cstr));
        writer.write((const char*)&len, sizeof(int));
        if (len > 0) writer.write(cstr, len);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ error_code& val)
    {
        std::string name;
        reader.read(name);
        error_code err(dsn_error_from_string(name.c_str(), ERR_UNKNOWN));
        val = err;
    }

    // task_code
    inline void marshall(::dsn::binary_writer& writer, const task_code& val)
    {
        // avoid memory copy, equal to writer.write(std::string)
        const char* cstr = val.to_string();
        int len = static_cast<int>(strlen(cstr));
        writer.write((const char*)&len, sizeof(int));
        if (len > 0) writer.write(cstr, len);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ task_code& val)
    {
        std::string name;
        reader.read(name);
        task_code code(dsn_task_code_from_string(name.c_str(), TASK_CODE_INVALID));
        val = code;
    }

    // dsn_address_t
    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ dsn_address_t& val)
    {
        reader.read_pod(val);
    }

    inline void marshall(::dsn::binary_writer& writer, const dsn_address_t& val)
    {
        writer.write_pod(val);
    }

    // rpc_address
    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ ::dsn::rpc_address& val)
    {
        unmarshall(reader, *val.c_addr_ptr());
    }

    inline void marshall(::dsn::binary_writer& writer, ::dsn::rpc_address val)
    {
        marshall(writer, val.c_addr());
    }
    
    // std::string
    inline void marshall(::dsn::binary_writer& writer, const std::string& val)
    {
        writer.write(val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ std::string& val)
    {
        reader.read(val);
    }

    // blob
    inline void marshall(::dsn::binary_writer& writer, const blob& val)
    {
        writer.write(val);
    }

    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ blob& val)
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
    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ std::list<T>& val)
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
    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ std::vector<T>& val)
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
    inline void unmarshall(::dsn::binary_reader& reader, /*out*/ std::set<T, std::less<T>, std::allocator<T>>& val)
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
