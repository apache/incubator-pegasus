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

#pragma once

#include <dsn/utility/utils.h>
#include <dsn/tool-api/rpc_address.h>
#include <dsn/cpp/rpc_stream.h>

#ifdef DSN_USE_THRIFT_SERIALIZATION
#include <dsn/cpp/serialization_helper/thrift_helper.h>
#endif

#ifdef DSN_USE_PROTOBUF_SERIALIZATION
#include <dsn/cpp/serialization_helper/protobuf_helper.h>
#endif
namespace dsn {
namespace serialization {
template <typename T>
std::string no_registered_function_error_notice(const T &t, dsn_msg_serialize_format fmt)
{
    std::stringstream ss;
    ss << "This error occurs because someone is trying to ";
    ss << "serialize/deserialize an object of the type ";
    ss << typeid(t).name();
    ss << " but has not registered corresponding serialization/deserialization function for the "
          "format of ";
    //           ss << enum_to_string(fmt) << ".";
    ss << fmt << ".";
    return ss.str();
}
}

#ifdef DSN_USE_THRIFT_SERIALIZATION

#define THRIFT_MARSHALLER                                                                          \
    case DSF_THRIFT_BINARY:                                                                        \
        marshall_thrift_binary(writer, value);                                                     \
        break;                                                                                     \
    case DSF_THRIFT_JSON:                                                                          \
        marshall_thrift_json(writer, value);                                                       \
        break;

#define THRIFT_UNMARSHALLER                                                                        \
    case DSF_THRIFT_BINARY:                                                                        \
        unmarshall_thrift_binary(reader, value);                                                   \
        break;                                                                                     \
    case DSF_THRIFT_JSON:                                                                          \
        unmarshall_thrift_json(reader, value);                                                     \
        break;

// the following 2 functions is for thrift basic type serialization
template <typename T>
inline void marshall(binary_writer &writer, const T &value, dsn_msg_serialize_format fmt)
{
    switch (fmt) {
        THRIFT_MARSHALLER
    default:
        dassert(false, serialization::no_registered_function_error_notice(value, fmt).c_str());
    }
}

template <typename T>
inline void unmarshall(binary_reader &reader, T &value, dsn_msg_serialize_format fmt)
{
    switch (fmt) {
        THRIFT_UNMARSHALLER
    default:
        dassert(false, serialization::no_registered_function_error_notice(value, fmt).c_str());
    }
}
#else
#define THRIFT_MARSHALLER                                                                          \
    {                                                                                              \
    }
#define THRIFT_UNMARSHALLER                                                                        \
    {                                                                                              \
    }
#endif

#ifdef DSN_USE_PROTOBUF_SERIALIZATION
#define PROTOBUF_MARSHALLER                                                                        \
    case DSF_PROTOC_BINARY:                                                                        \
        marshall_protobuf_binary(writer, value);                                                   \
        break;                                                                                     \
    case DSF_PROTOC_JSON:                                                                          \
        marshall_protobuf_json(writer, value);                                                     \
        break;

#define PROTOBUF_UNMARSHALLER                                                                      \
    case DSF_PROTOC_BINARY:                                                                        \
        unmarshall_protobuf_binary(reader, value);                                                 \
        break;                                                                                     \
    case DSF_PROTOC_JSON:                                                                          \
        unmarshall_protobuf_json(reader, value);                                                   \
        break;

#else
#define PROTOBUF_MARSHALLER                                                                        \
    {                                                                                              \
    }
#define PROTOBUF_UNMARSHALLER                                                                      \
    {                                                                                              \
    }
#endif

#define GENERATED_TYPE_SERIALIZATION(GType, SerializationType)                                     \
    inline void marshall(binary_writer &writer, const GType &value, dsn_msg_serialize_format fmt)  \
    {                                                                                              \
        switch (fmt) {                                                                             \
            SerializationType##_MARSHALLER default                                                 \
                : dassert(false,                                                                   \
                          serialization::no_registered_function_error_notice(value, fmt).c_str()); \
        }                                                                                          \
    }                                                                                              \
    inline void unmarshall(binary_reader &reader, GType &value, dsn_msg_serialize_format fmt)      \
    {                                                                                              \
        switch (fmt) {                                                                             \
            SerializationType##_UNMARSHALLER default                                               \
                : dassert(false,                                                                   \
                          serialization::no_registered_function_error_notice(value, fmt).c_str()); \
        }                                                                                          \
    }

template <typename T>
inline void marshall(dsn_message_t msg, const T &val)
{
    ::dsn::rpc_write_stream writer(msg);
    marshall(writer, val, dsn_msg_get_serialize_format(msg));
}

template <typename T>
inline void marshall(dsn_message_t msg, const T &val, dsn_msg_serialize_format fmt)
{
    ::dsn::rpc_write_stream writer(msg);
    marshall(writer, val, fmt);
}

template <typename T>
inline void unmarshall(dsn_message_t msg, /*out*/ T &val)
{
    ::dsn::rpc_read_stream reader(msg);
    unmarshall(reader, val, dsn_msg_get_serialize_format(msg));
}
}
