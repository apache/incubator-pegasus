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
 *     2016-02-24, Weijie Sun(sunweijie[at]xiaomi.com), add support for serialization in thrift
 *     2016-03-01, Weijie Sun(sunweijie[at]xiaomi.com), add support for rpc in thrift
 */

# pragma once

# include <dsn/internal/message_parser.h>
# include <dsn/tool_api.h>
# include <dsn/cpp/rpc_stream.h>

# include <thrift/Thrift.h>
# include <thrift/protocol/TBinaryProtocol.h>
# include <thrift/protocol/TVirtualProtocol.h>
# include <thrift/transport/TVirtualTransport.h>
# include <thrift/TApplicationException.h>

# include <type_traits>

using namespace ::apache::thrift::transport;

namespace dsn {

    class binary_reader_transport : public TVirtualTransport<binary_reader_transport>
    {
    public:
        binary_reader_transport(binary_reader& reader)
            : _reader(reader)
        {
        }

        bool isOpen() { return true; }

        void open() {}

        void close() {}

        uint32_t read(uint8_t* buf, uint32_t len)
        {
            int l = _reader.read((char*)buf, static_cast<int>(len));
            if (l == 0)
            {
                throw TTransportException(TTransportException::END_OF_FILE,
                    "no more data to read after end-of-buffer");
            }
            return (uint32_t)l;
        }
            
    private:
        binary_reader& _reader;
    };

    class binary_writer_transport : public TVirtualTransport<binary_writer_transport>
    {
    public:
        binary_writer_transport(binary_writer& writer)
            : _writer(writer)
        {
        }

        bool isOpen() { return true; }

        void open() {}

        void close() {}

        void write(const uint8_t* buf, uint32_t len)
        {
            _writer.write((const char*)buf, static_cast<int>(len));
        }

    private:
        binary_writer& _writer;
    };

    #define DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(TName, TTag, TMethod) \
        inline uint32_t write_base(::apache::thrift::protocol::TProtocol* proto, const TName& val)\
        {\
            return proto->write##TMethod(val); \
        }\
        inline uint32_t read_base(::apache::thrift::protocol::TProtocol* proto, /*out*/ TName& val)\
        {\
            return proto->read##TMethod(val); \
        }

    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(bool, BOOL, Bool)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int8_t, I08, Byte)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int16_t, I16, I16)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int32_t, I32, I32)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int64_t, I64, I64)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(double, DOUBLE, Double)        
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(std::string, STRING, String)

    template<typename T>
    uint32_t marshall_base(::apache::thrift::protocol::TProtocol* oproto, const T& val);
    template<typename T>
    uint32_t unmarshall_base(::apache::thrift::protocol::TProtocol* iproto, T& val);

    template<typename T>
    inline uint32_t write_base(::apache::thrift::protocol::TProtocol* proto, const T& value)
    {
        switch (sizeof(value))
        {
        case 1:
            return write_base(proto, (int8_t)value);
        case 2:
            return write_base(proto, (int16_t)value);
        case 4:
            return write_base(proto, (int32_t)value);
        case 8:
            return write_base(proto, (int64_t)value);
        default:
            assert(false);
            return 0;
        }
    }

    template <typename T>
    inline uint32_t read_base(::apache::thrift::protocol::TProtocol* proto, T& value)
    {
        uint32_t res = 0;
        switch (sizeof(value))
        {
        case 1: {
            int8_t val;
            res = read_base(proto, val);
            value = (T)val;
            return res;
        }
        case 2: {
            int16_t val;
            res = read_base(proto, val);
            value = (T)val;
            return res;
        }
        case 4: {
            int32_t val;
            res = read_base(proto, val);
            value = T(val);
            return res;
        }
        case 8: {
            int64_t val;
            res = read_base(proto, val);
            value = T(val);
            return res;
        }
        default:
            assert(false);
            return 0;
        }
    }

    template<typename T>
    inline uint32_t write_base(::apache::thrift::protocol::TProtocol* oprot, const std::vector<T>& val)
    {
        uint32_t xfer = oprot->writeFieldBegin("vector", ::apache::thrift::protocol::T_LIST, 1);
        xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT, static_cast<uint32_t>(val.size()));
        for (auto iter = val.begin(); iter!=val.end(); ++iter)
        {
            marshall_base(oprot, *iter);
        }
        xfer += oprot->writeListEnd();
        xfer += oprot->writeFieldEnd();
    }

    template <typename T>
    inline uint32_t read_base(::apache::thrift::protocol::TProtocol* iprot, std::vector<T>& val)
    {
        uint32_t xfer = 0;

        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;

        xfer += iprot->readFieldBegin(fname, ftype, fid);
        if (ftype == ::apache::thrift::protocol::T_LIST)
        {
            val.clear();
            uint32_t size;
            ::apache::thrift::protocol::TType element_type;
            xfer += iprot->readListBegin(element_type, size);
            val.resize(size);
            for (uint32_t i=0; i!=size; ++i)
            {
                xfer += unmarshall_base(iprot, val[i]);
            }
            xfer += iprot->readListEnd();
        }
        else
            xfer += iprot->skip(ftype);
        xfer += iprot->readFieldEnd();
        return xfer;
    }

    inline const char* to_string(const rpc_address& addr) { return addr.to_string(); }
    inline const char* to_string(const blob& blob) { return ""; }
    inline const char* to_string(const task_code& code) { return code.to_string(); }
    inline const char* to_string(const error_code& ec) { return ec.to_string(); }

    template<typename T>
    class serialization_forwarder
    {
    private:
        template<typename C>
        static constexpr auto check_method( C* ) ->
            typename std::is_same< decltype(std::declval<C>().write( std::declval< ::apache::thrift::protocol::TProtocol* >() ) ), uint32_t >::type;

        template<typename>
        static constexpr std::false_type check_method(...);

        typedef decltype(check_method<T>(nullptr)) has_read_write_method;

        static uint32_t marshall_internal(::apache::thrift::protocol::TProtocol* oproto, const T& value, std::false_type)
        {
            return write_base(oproto, value);
        }

        static uint32_t marshall_internal(::apache::thrift::protocol::TProtocol* oproto, const T& value, std::true_type)
        {
            return value.write(oproto);
        }

        static uint32_t unmarshall_internal(::apache::thrift::protocol::TProtocol* iproto, T& value, std::false_type)
        {
            return read_base(iproto, value);
        }

        static uint32_t unmarshall_internal(::apache::thrift::protocol::TProtocol* iproto, T& value, std::true_type)
        {
            return value.read(iproto);
        }
    public:
        static uint32_t marshall(::apache::thrift::protocol::TProtocol* oproto, const T& value)
        {
            return marshall_internal(oproto, value, has_read_write_method());
        }
        static uint32_t unmarshall(::apache::thrift::protocol::TProtocol* iproto, T& value)
        {
            return unmarshall_internal(iproto, value, has_read_write_method());
        }
    };

    template<typename TName>
    inline uint32_t marshall_base(::apache::thrift::protocol::TProtocol* oproto, const TName& val)
    {
        return serialization_forwarder<TName>::marshall(oproto, val);
    }

    template<typename TName>
    inline uint32_t unmarshall_base(::apache::thrift::protocol::TProtocol* iproto, /*out*/ TName& val)
    {
        //well, we assume read/write are in coupled
        return serialization_forwarder<TName>::unmarshall(iproto, val);
    }

#define GET_THRIFT_TYPE_MACRO(cpp_type, thrift_type) \
    inline ::apache::thrift::protocol::TType get_thrift_type(const cpp_type&)\
    {\
        return ::apache::thrift::protocol::thrift_type;\
    }\

    GET_THRIFT_TYPE_MACRO(bool, T_BOOL)
    GET_THRIFT_TYPE_MACRO(int8_t, T_BYTE)
    GET_THRIFT_TYPE_MACRO(uint8_t, T_BYTE)
    GET_THRIFT_TYPE_MACRO(int16_t, T_I16)
    GET_THRIFT_TYPE_MACRO(uint16_t, T_I16)
    GET_THRIFT_TYPE_MACRO(int32_t, T_I32)
    GET_THRIFT_TYPE_MACRO(uint32_t, T_I32)
    GET_THRIFT_TYPE_MACRO(int64_t, T_I64)
    GET_THRIFT_TYPE_MACRO(uint64_t, T_U64)
    GET_THRIFT_TYPE_MACRO(double, T_DOUBLE)
    GET_THRIFT_TYPE_MACRO(std::string, T_STRING)

    template<typename T>
    inline ::apache::thrift::protocol::TType get_thrift_type(const std::vector<T>&)
    {
        return ::apache::thrift::protocol::T_LIST;
    }

    template<typename T>
    inline ::apache::thrift::protocol::TType get_thrift_type(const T&)
    {
        return ::apache::thrift::protocol::T_STRUCT;
    }

    template<typename T>
    void marshall(binary_writer& writer, const T& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        proto.writeFieldBegin("args", get_thrift_type(val), (*writer._p_value_id)++);
        marshall_base<T>(&proto, val);
        proto.writeFieldEnd();
    }

    template<typename T>
    void unmarshall(binary_reader& reader, /*out*/ T& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);

        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;

        proto.readFieldBegin(fname, ftype, fid);
        if (ftype == get_thrift_type(val))
            unmarshall_base<T>(&proto, val);
        else
            proto.skip(ftype);
    }

    class char_ptr
    {
    private:
        const char* ptr;
        int length;
    public:
        char_ptr(const char* p, int len): ptr(p), length(len) {}
        std::size_t size() const { return length; }
        const char* data() const { return ptr; }
    };

    class blob_string
    {
    private:
        blob& _buffer;
    public:
        blob_string(blob& bb): _buffer(bb) {}

        void clear()
        {
            _buffer.assign(std::shared_ptr<char>(nullptr), 0, 0);
        }
        void resize(std::size_t new_size)
        {
            std::shared_ptr<char> b(new char[new_size], std::default_delete<char[]>());
            _buffer.assign(b, 0, new_size);
        }
        void assign(const char* ptr, std::size_t size)
        {
            std::shared_ptr<char> b(new char[size], std::default_delete<char[]>());
            memcpy(b.get(), ptr, size);
            _buffer.assign(b, 0, size);
        }
        const char* data() const
        {
            return _buffer.data();
        }
        size_t size() const
        {
            return _buffer.length();
        }

        char& operator [](int pos)
        {
            return const_cast<char*>(_buffer.data())[pos];
        }
    };

    inline uint32_t rpc_address::read(apache::thrift::protocol::TProtocol *iprot)
    {
        return iprot->readI64(reinterpret_cast<int64_t&>(_addr.u.value));
    }

    inline uint32_t rpc_address::write(apache::thrift::protocol::TProtocol *oprot) const
    {
        return oprot->writeI64((int64_t)_addr.u.value);
    }

    inline uint32_t task_code::read(apache::thrift::protocol::TProtocol *iprot)
    {
        std::string task_code_string;
        uint32_t xfer = iprot->readString(task_code_string);
        _internal_code = dsn_task_code_from_string(task_code_string.c_str(), TASK_CODE_INVALID);
        return xfer;
    }

    inline uint32_t task_code::write(apache::thrift::protocol::TProtocol *oprot) const
    {
        //for optimization, it is dangerous if the oprot is not a binary proto
        apache::thrift::protocol::TBinaryProtocol* binary_proto = static_cast<apache::thrift::protocol::TBinaryProtocol*>(oprot);
        const char* name = to_string();
        return binary_proto->writeString<char_ptr>(char_ptr(name, strlen(name)));
    }

    inline uint32_t blob::read(apache::thrift::protocol::TProtocol *iprot)
    {
        //for optimization, it is dangerous if the oprot is not a binary proto
        apache::thrift::protocol::TBinaryProtocol* binary_proto = static_cast<apache::thrift::protocol::TBinaryProtocol*>(iprot);
        blob_string str(*this);
        return binary_proto->readString<blob_string>(str);
    }

    inline uint32_t blob::write(apache::thrift::protocol::TProtocol *oprot) const
    {
        apache::thrift::protocol::TBinaryProtocol* binary_proto = static_cast<apache::thrift::protocol::TBinaryProtocol*>(oprot);
        return binary_proto->writeString<blob_string>(blob_string(const_cast<blob&>(*this)));
    }

    inline uint32_t error_code::read(apache::thrift::protocol::TProtocol *iprot)
    {
        std::string ec_string;
        uint32_t xfer = iprot->readString(ec_string);
        _internal_code = dsn_error_from_string(ec_string.c_str(), ERR_UNKNOWN);
        return xfer;
    }

    inline uint32_t error_code::write(apache::thrift::protocol::TProtocol *oprot) const
    {
        //for optimization, it is dangerous if the oprot is not a binary proto
        apache::thrift::protocol::TBinaryProtocol* binary_proto = static_cast<apache::thrift::protocol::TBinaryProtocol*>(oprot);
        const char* name = to_string();
        return binary_proto->writeString<char_ptr>(char_ptr(name, strlen(name)));
    }

    typedef union
    {
        struct {
            uint64_t is_forward_msg_disabled: 1;
            uint64_t is_replication_needed: 1;
            uint64_t unused: 63;
        }u;
        uint64_t o;
    }dsn_thrift_header_options;

    struct dsn_thrift_header
    {
        int32_t hdr_type;
        int32_t hdr_crc32;
        int32_t total_length;
        int32_t body_offset;
        int32_t request_hash;
        dsn_thrift_header_options opt;
    };

    class thrift_header_parser
    {
    public:
        static void read_thrift_header_from_buffer(/*out*/dsn_thrift_header& result, const char* buffer)
        {
            result.hdr_type = be32toh( *(int32_t*)(buffer) );
            buffer += sizeof(int32_t);
            result.hdr_crc32 = be32toh( *(int32_t*)(buffer) );
            buffer += sizeof(int32_t);
            result.total_length = be32toh( *(int32_t*)(buffer) );
            buffer += sizeof(int32_t);
            result.body_offset = be32toh( *(int32_t*)(buffer) );
            buffer += sizeof(int32_t);
            result.request_hash = be32toh( *(int32_t*)(buffer) );
            buffer += sizeof(int32_t);
            result.opt.o = be64toh( *(int64_t*)(buffer) );
        }

        static dsn::message_ex* parse_dsn_message(dsn_thrift_header* header, dsn::blob& message_data)
        {
            dsn::blob message_content = message_data.range(header->body_offset);
            dsn::message_ex* msg = message_ex::create_receive_message_with_standalone_header(message_content);
            dsn::message_header* dsn_hdr = msg->header;

            dsn::rpc_read_stream stream(msg);
            boost::shared_ptr< ::dsn::binary_reader_transport > input_trans(new ::dsn::binary_reader_transport( stream ));
            ::apache::thrift::protocol::TBinaryProtocol iprot(input_trans);

            std::string fname;
            ::apache::thrift::protocol::TMessageType mtype;
            int32_t seqid;
            iprot.readMessageBegin(fname, mtype, seqid);

            dinfo("rpc name: %s, type: %d, seqid: %d", fname.c_str(), mtype, seqid);
            memset(dsn_hdr, 0, sizeof(*dsn_hdr));
            dsn_hdr->hdr_type = hdr_dsn_thrift;
            dsn_hdr->body_length = header->total_length - header->body_offset;
            strncpy(dsn_hdr->rpc_name, fname.c_str(), DSN_MAX_TASK_CODE_NAME_LENGTH);

            if (mtype == ::apache::thrift::protocol::T_CALL || mtype == ::apache::thrift::protocol::T_ONEWAY)
                dsn_hdr->context.u.is_request = 1;

            dsn_hdr->id = seqid;
            dsn_hdr->context.u.is_replication_needed = header->opt.u.is_replication_needed;
            dsn_hdr->context.u.is_forward_disabled = header->opt.u.is_forward_msg_disabled;
            dsn_hdr->client.hash = header->request_hash;

            iprot.readStructBegin(fname);
            return msg;
        }

        static void add_prefix_for_thrift_response(message_ex* msg)
        {
            dsn::rpc_write_stream write_stream(msg);
            boost::shared_ptr< ::dsn::binary_writer_transport> msg_transport(new ::dsn::binary_writer_transport(write_stream));
            ::apache::thrift::protocol::TBinaryProtocol msg_proto(msg_transport);

            msg_proto.writeMessageBegin(msg->header->rpc_name, ::apache::thrift::protocol::T_REPLY, msg->header->id);
            msg_proto.writeStructBegin(""); //resp args
            if (msg->header->context.u.is_response_in_piece)
            {
                msg_proto.writeFieldBegin("", ::apache::thrift::protocol::T_STRUCT, 0);
                msg_proto.writeStructBegin("");
            }
        }

        static void add_postfix_for_thrift_response(message_ex* msg)
        {
            dsn::rpc_write_stream write_stream(msg);
            boost::shared_ptr< ::dsn::binary_writer_transport> msg_transport(new ::dsn::binary_writer_transport(write_stream));
            ::apache::thrift::protocol::TBinaryProtocol msg_proto(msg_transport);

            if (msg->header->context.u.is_response_in_piece)
            {
                msg_proto.writeFieldStop(); // for all pieces
                msg_proto.writeStructEnd();
            }
            //after all fields, write a field stop
            msg_proto.writeFieldStop();
            //writestruct end, this is because all thirft returning values are in a struct
            msg_proto.writeStructEnd();
            //write message end, which indicate the end of a thrift message
            msg_proto.writeMessageEnd();
        }

        static void adjust_thrift_response(message_ex* msg)
        {
            dassert(msg->_resp_adjusted==false, "we have adjusted this");

            msg->_resp_adjusted = true;
            add_postfix_for_thrift_response(msg);
        }

        static int prepare_buffers_on_send(message_ex* msg, int offset, /*out*/message_parser::send_buf* buffers)
        {
            if ( !msg->_resp_adjusted )
                adjust_thrift_response(msg);

            dassert(!msg->buffers.empty(), "buffers is not possible to be empty");

            // we ignore header for thrift resp
            offset += sizeof(message_header);

            int count=0;
            //ignore the msg header
            for (unsigned int i=0; i!=msg->buffers.size(); ++i)
            {
                blob& bb = msg->buffers[i];
                if (offset >= bb.length())
                {
                    offset -= bb.length();
                    continue;
                }

                buffers[count].buf = (void*)(bb.data() + offset);
                buffers[count].sz = (uint32_t)(bb.length() - offset);
                offset = 0;
                ++count;
            }
            return count;
        }

        static int get_send_buffers_count_and_total_length(message_ex* msg, /*out*/int* total_length)
        {
            if ( !msg->_resp_adjusted )
                adjust_thrift_response(msg);

            // when send thrift message, we ignore the header
            *total_length = msg->header->body_length;
            return msg->buffers.size();
        }
    };
}
