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

# include <dsn/cpp/utils.h>
# include <dsn/cpp/address.h>
# include <dsn/cpp/rpc_stream.h>

#ifdef DSN_USE_THRIFT_SERIALIZATION
# include <dsn/cpp/serialization_helper/thrift_helper.h>
#endif

#ifdef DSN_USE_PROTOBUF_SERIALIZATION
# include <dsn/cpp/serialization_helper/protobuf_helper.h>
#endif
namespace dsn
{
    namespace serialization
    {
        template<typename T>
        std::string no_registered_function_error_notice(const T& t, dsn_msg_serialize_format fmt)
        {
            std::stringstream ss;
            ss << "This error occurs because someone is trying to ";
            ss << "serialize/deserialize an object of the type ";
            ss << typeid(t).name();
            ss << " but has not registered corresponding serialization/deserialization function for the format of ";
            //           ss << enum_to_string(fmt) << ".";
            ss << fmt << ".";
            return ss.str();
        }
    }

#ifdef DSN_USE_THRIFT_SERIALIZATION

    // currently only support thrift binary serialization method for rpcaddress, blob, task_code and error_code
    inline void marshall(binary_writer& writer, const rpc_address& value, dsn_msg_serialize_format fmt)
    {
        marshall_thrift_binary(writer, value);
    }
    inline void unmarshall(binary_reader& reader, rpc_address& value, dsn_msg_serialize_format fmt)
    {
        unmarshall_thrift_binary(reader, value);
    }

    inline void marshall(binary_writer& writer, const blob& value, dsn_msg_serialize_format fmt)
    {
        marshall_thrift_binary(writer, value);
    }
    inline void unmarshall(binary_reader& reader, blob& value, dsn_msg_serialize_format fmt)
    {
        unmarshall_thrift_binary(reader, value);
    }

    inline void marshall(binary_writer& writer, const task_code& value, dsn_msg_serialize_format fmt)
    {
        marshall_thrift_binary(writer, value);
    }
    inline void unmarshall(binary_reader& reader, task_code& value, dsn_msg_serialize_format fmt)
    {
        unmarshall_thrift_binary(reader, value);
    }

    inline void marshall(binary_writer& writer, const error_code& value, dsn_msg_serialize_format fmt)
    {
        marshall_thrift_binary(writer, value);
    }
    inline void unmarshall(binary_reader& reader, error_code& value, dsn_msg_serialize_format fmt)
    {
        unmarshall_thrift_binary(reader, value);
    }

    inline void marshall(binary_writer& writer, const gpid& value, dsn_msg_serialize_format fmt)
    {
        marshall_thrift_binary(writer, value);
    }
    inline void unmarshall(binary_reader& reader, gpid& value, dsn_msg_serialize_format fmt)
    {
        unmarshall_thrift_binary(reader, value);
    }

    inline void marshall(binary_writer& writer, const atom_int& value, dsn_msg_serialize_format fmt)
    {
        marshall_thrift_binary(writer, value);
    }
    inline void unmarshall(binary_reader& reader, atom_int& value, dsn_msg_serialize_format fmt)
    {
        unmarshall_thrift_binary(reader, value);
    }

#define THRIFT_BASIC_TYPE_MARSHALLER \
        case DSF_THRIFT_BINARY: marshall_thrift_basic_Binary(writer, value); break;\
        case DSF_THRIFT_JSON: marshall_thrift_basic_JSON(writer, value); break;

#define THRIFT_GENERATED_TYPE_MARSHALLER \
        case DSF_THRIFT_BINARY: marshall_thrift_binary(writer, value); break; \
        case DSF_THRIFT_JSON: marshall_thrift_json(writer, value); break;

#define THRIFT_BASIC_TYPE_UNMARSHALLER \
        case DSF_THRIFT_BINARY: unmarshall_thrift_basic_Binary(reader, value); break; \
        case DSF_THRIFT_JSON: unmarshall_thrift_basic_JSON(reader, value); break;

#define THRIFT_GENERATED_TYPE_UNMARSHALLER \
        case DSF_THRIFT_BINARY: unmarshall_thrift_binary(reader, value); break; \
        case DSF_THRIFT_JSON: unmarshall_thrift_json(reader, value); break;
#else
#define THRIFT_BASIC_TYPE_MARSHALLER {}
#define THRIFT_GENERATED_TYPE_MARSHALLER {}
#define THRIFT_BASIC_TYPE_UNMARSHALLER {}
#define THRIFT_GENERATED_TYPE_UNMARSHALLER {}
#endif

#ifdef DSN_USE_PROTOBUF_SERIALIZATION
#define PROTOBUF_GENERATED_TYPE_MARSHALLER \
    case DSF_PROTOC_BINARY: marshall_protobuf_binary(writer, value); break; \
    case DSF_PROTOC_JSON: marshall_protobuf_json(writer, value); break;

#define PROTOBUF_GENERATED_TYPE_UNMARSHALLER \
    case DSF_PROTOC_BINARY: unmarshall_protobuf_binary(reader, value); break; \
    case DSF_PROTOC_JSON: unmarshall_protobuf_json(reader, value); break;

#else
#define PROTOBUF_GENERATED_TYPE_MARSHALLER {}
#define PROTOBUF_GENERATED_TYPE_UNMARSHALLER {}
#endif

#define BASIC_TYPE_SERIALIZATION(CXXType) \
    inline void marshall(binary_writer& writer, const CXXType &value, dsn_msg_serialize_format fmt) \
    { \
        switch (fmt) \
        { \
            THRIFT_BASIC_TYPE_MARSHALLER \
            default: dassert(false, serialization::no_registered_function_error_notice(value, fmt).c_str()); \
        } \
    } \
    inline void unmarshall(binary_reader& reader, CXXType &value, dsn_msg_serialize_format fmt) \
    { \
        switch (fmt) \
        { \
            THRIFT_BASIC_TYPE_UNMARSHALLER \
            default: dassert(false, serialization::no_registered_function_error_notice(value, fmt).c_str()); \
        } \
    }

    BASIC_TYPE_SERIALIZATION(bool)
    BASIC_TYPE_SERIALIZATION(int8_t)
    BASIC_TYPE_SERIALIZATION(int16_t)
    BASIC_TYPE_SERIALIZATION(int32_t)
    BASIC_TYPE_SERIALIZATION(int64_t)
    BASIC_TYPE_SERIALIZATION(double)
    BASIC_TYPE_SERIALIZATION(std::string)

#define GENERATED_TYPE_SERIALIZATION(GType, SerializationType) \
    inline void marshall(binary_writer& writer, const GType &value, dsn_msg_serialize_format fmt) \
    { \
        switch (fmt) \
        { \
            SerializationType##_GENERATED_TYPE_MARSHALLER \
            default: dassert(false, serialization::no_registered_function_error_notice(value, fmt).c_str()); \
        } \
    } \
    inline void unmarshall(binary_reader& reader, GType &value, dsn_msg_serialize_format fmt) \
    { \
        switch (fmt) \
        { \
            SerializationType##_GENERATED_TYPE_UNMARSHALLER \
            default: dassert(false, serialization::no_registered_function_error_notice(value, fmt).c_str()); \
        } \
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
