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

# include <dsn/cpp/serialization.h>

namespace dsn
{
    template<typename T>
    class serialize_manager
    {
    public:
        typedef void(*marshall_func)(binary_writer&, const T&);
        typedef void(*unmarshall_func)(binary_reader&, T&);

        static marshall_func marshallers[DSF_COUNT];
        static unmarshall_func unmarshallers[DSF_COUNT];
    };

    template<typename T>
    typename serialize_manager<T>::marshall_func serialize_manager<T>::marshallers[DSF_COUNT];
    
    template<typename T>
    typename serialize_manager<T>::unmarshall_func serialize_manager<T>::unmarshallers[DSF_COUNT];

    template<typename T>
    void marshall(binary_writer& writer, const T& value, dsn_msg_serialize_format fmt)
    {
        serialize_manager<T>::marshallers[fmt](writer, value);
    }

    template<typename T>
    void unmarshall(binary_reader& reader, T& value, dsn_msg_serialize_format fmt)
    {
        serialize_manager<T>::unmarshallers[fmt](reader, value);
    }

    template<typename T>
    inline void marshall(dsn_message_t msg, const T& val)
    {
        ::dsn::rpc_write_stream writer(msg);
        marshall(writer, val, dsn_msg_get_serialize_format(msg));
    }

    template<typename T>
    inline void marshall(dsn_message_t msg, const T& val, dsn_msg_serialize_format fmt)
    {
        ::dsn::rpc_write_stream writer(msg);
        marshall(writer, val, fmt);
    }

    template<typename T>
    inline void unmarshall(dsn_message_t msg, /*out*/ T& val)
    {
        ::dsn::rpc_read_stream reader(msg);
        unmarshall(reader, val, dsn_msg_get_serialize_format(msg));
    }

}