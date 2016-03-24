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

#ifdef DSN_USE_THRIFT_SERIALIZATION
# include <dsn/cpp/serialization_helper/thrift_helper.h>
#else
# include <dsn/cpp/serialization_helper/dsn_helper.h>
#endif

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

// marshall_struct_begin, marshall_struct_field and marshall_struct_end
// are useful when you want to marshall multiple values but
// you can't describe it in IDL/PROTO file. This is mainly because you don't know the
// actual types for each fields.
// A typical situation is rDSN's replication layer and replication_app layer.
template<typename T>
inline void marshall_struct_field(dsn_message_t msg, const T& val, int field_id)
{
    ::dsn::rpc_write_stream writer(msg);
    marshall_struct_field(writer, val, field_id);
}

inline void marshall_struct_begin(dsn_message_t msg)
{
    ::dsn::rpc_write_stream writer(msg);
    marshall_struct_begin(writer, dsn_msg_get_header_type(msg));
}

inline void marshall_struct_end(dsn_message_t msg)
{
    ::dsn::rpc_write_stream writer(msg);
    marshall_struct_end(writer, dsn_msg_get_header_type(msg));
}
