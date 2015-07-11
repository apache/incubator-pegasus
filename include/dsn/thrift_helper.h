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
# include <dsn/internal/message_parser.h>
# include <dsn/tool_api.h>

# include <thrift/Thrift.h>
# include <thrift/protocol/TBinaryProtocol.h>
# include <thrift/protocol/TVirtualProtocol.h>
# include <thrift/transport/TVirtualTransport.h>
# include <thrift/TApplicationException.h>

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
        inline int write_base(::apache::thrift::protocol::TProtocol* proto, const TName& val)\
        {\
            int xfer = proto->writeFieldBegin("val", ::apache::thrift::protocol::TType::T_##TTag, 0); \
            xfer += proto->write##TMethod(val); \
            xfer += proto->writeFieldEnd(); \
            return xfer;\
        }\
        inline int read_base(::apache::thrift::protocol::TProtocol* proto, __out_param TName& val, ::apache::thrift::protocol::TType ftype)\
        {\
            if (ftype == ::apache::thrift::protocol::TType::T_##TTag) return proto->read##TMethod(val); \
            else return proto->skip(ftype);\
        }
        
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(bool, BOOL, Bool)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int8_t, I08, Byte)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int16_t, I16, I16)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int32_t, I32, I32)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(int64_t, I64, I64)
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(double, DOUBLE, Double)        
    DEFINE_THRIFT_BASE_TYPE_SERIALIZATION(std::string, STRING, String)

    template<typename TName>
    inline uint32_t marshall_base(::apache::thrift::protocol::TProtocol* oproto, const TName& val)
    {
        uint32_t xfer = 0; 
        xfer += oproto->writeStructBegin("val");
        xfer += write_base(oproto, val);
        xfer += oproto->writeFieldStop();
        xfer += oproto->writeStructEnd();
        return xfer;
    }
        
    template<typename TName>
    inline uint32_t unmarshall_base(::apache::thrift::protocol::TProtocol* iproto, __out_param TName& val)
    {
        uint32_t xfer = 0;
        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;
        
        xfer += iproto->readStructBegin(fname);
        
        using ::apache::thrift::protocol::TProtocolException;
        
        while (true)
        {
            xfer += iproto->readFieldBegin(fname, ftype, fid);
            if (ftype == ::apache::thrift::protocol::T_STOP) {
                break;
            }

            switch (fid)
            {
            case 0:
                xfer += read_base(iproto, val, ftype);
                break;
            default:
                xfer += iproto->skip(ftype);
                break;
            }

            xfer += iproto->readFieldEnd();
        }
            
        xfer += iproto->readStructEnd();
        return xfer;
    }

    template<typename T>
    void marshall(binary_writer& writer, const T& val)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        marshall_base<T>(&proto, val);
    }

    template<typename T>
    void unmarshall(binary_reader& reader, __out_param T& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        unmarshall_base<T>(&proto, val);
    }

    template<typename T>
    uint32_t marshall_rpc_args(
        ::apache::thrift::protocol::TProtocol* oprot,
        const T& val,
        uint32_t(T::*writer)(::apache::thrift::protocol::TProtocol*) const
        )
    {
        uint32_t xfer = 0;
        oprot->incrementRecursionDepth();
        xfer += oprot->writeStructBegin("rpc_message");

        xfer += oprot->writeFieldBegin("msg", ::apache::thrift::protocol::T_STRUCT, 1);

        xfer += (val.*writer)(oprot);

        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        oprot->decrementRecursionDepth();
        return xfer;
    }

    template<typename T>
    uint32_t unmarshall_rpc_args(
        ::apache::thrift::protocol::TProtocol* iprot,
        __out_param T& val,
        uint32_t(T::*reader)(::apache::thrift::protocol::TProtocol*)
        )
    {
        uint32_t xfer = 0;
        std::string fname;
        ::apache::thrift::protocol::TType ftype;
        int16_t fid;

        xfer += iprot->readStructBegin(fname);

        using ::apache::thrift::protocol::TProtocolException;

        while (true)
        {
            xfer += iprot->readFieldBegin(fname, ftype, fid);
            if (ftype == ::apache::thrift::protocol::T_STOP) {
                break;
            }
            switch (fid)
            {
            case 1:
                if (ftype == ::apache::thrift::protocol::T_STRUCT) {
                    xfer += (val.*reader)(iprot);
                }
                else {
                    xfer += iprot->skip(ftype);
                }
                break;
            default:
                xfer += iprot->skip(ftype);
                break;
            }
            xfer += iprot->readFieldEnd();
        }

        xfer += iprot->readStructEnd();

        iprot->readMessageEnd();
        iprot->getTransport()->readEnd();
        return xfer;
    }

    DEFINE_CUSTOMIZED_ID(network_header_format, NET_HDR_THRIFT);

    class thrift_binary_message_parser : public message_parser
    {
    public:
        static void register_parser()
        {
            ::dsn::tools::register_component_provider<thrift_binary_message_parser>("thrift");
        }

    private:
        // only one concurrent write message for each parser, so
        char _write_buffer_for_header[512];

    public:
        thrift_binary_message_parser(int buffer_block_size)
            : message_parser(buffer_block_size)
        {
        }

        virtual void prepare_buffers_for_send(message_ptr& msg, __out_param std::vector<blob>& buffers)
        {
            // prepare head
            blob bb(_write_buffer_for_header, 0, 512);
            binary_writer writer(bb);
            boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
            ::apache::thrift::protocol::TBinaryProtocol proto(transport);

            auto sp = task_spec::get(msg->header().local_rpc_code);

            proto.writeMessageBegin(msg->header().rpc_name,
                sp->type == TASK_TYPE_RPC_REQUEST ?
                ::apache::thrift::protocol::T_CALL :
                ::apache::thrift::protocol::T_REPLY,
                (int32_t)msg->header().id
                );

            // patched end (writeMessageEnd)
            // no need for now as no data is written

            // finalize
            std::vector<blob> lbuffers;
            msg->writer().get_buffers(lbuffers);
            if (lbuffers[0].length() == MSG_HDR_SERIALIZED_SIZE)
            {
                lbuffers[0] = writer.get_buffer();
                buffers = lbuffers;
            }
            else
            {
                dassert(lbuffers[0].length() > MSG_HDR_SERIALIZED_SIZE, "");
                buffers.resize(lbuffers.size() + 1);
                buffers[0] = writer.get_buffer();

                for (int i = 0; i < static_cast<int>(lbuffers.size()); i++)
                {
                    if (i == 0)
                    {
                        buffers[1] = lbuffers[0].range(MSG_HDR_SERIALIZED_SIZE);
                    }
                    else
                    {
                        buffers[i + 1] = lbuffers[i];
                    }
                }
            }
        }

        virtual message_ptr get_message_on_receive(int read_length, __out_param int& read_next)
        {
            mark_read(read_length);

            if (_read_buffer_occupied < 10)
            {
                read_next = 128;
                return nullptr;
            }

            try
            {
                blob bb = _read_buffer.range(0, _read_buffer_occupied);
                binary_reader reader(bb);
                boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
                ::apache::thrift::protocol::TBinaryProtocol proto(transport);

                int32_t rseqid = 0;
                std::string fname;
                ::apache::thrift::protocol::TMessageType mtype;

                proto.readMessageBegin(fname, mtype, rseqid);
                int hdr_sz = _read_buffer_occupied - reader.get_remaining_size();

                if (mtype == ::apache::thrift::protocol::T_EXCEPTION)
                {
                    proto.skip(::apache::thrift::protocol::T_STRUCT);
                }
                else
                {
                    proto.skip(::apache::thrift::protocol::T_STRUCT);
                }

                proto.readMessageEnd();
                proto.getTransport()->readEnd();

                // msg done
                int msg_sz = _read_buffer_occupied - reader.get_remaining_size() - hdr_sz;
                auto msg_bb = _read_buffer.range(hdr_sz, msg_sz);
                message_ptr msg = new message(msg_bb, false);
                msg->header().id = msg->header().rpc_id = rseqid;
                strcpy(msg->header().rpc_name, fname.c_str());
                msg->header().body_length = msg_sz;

                _read_buffer = _read_buffer.range(msg_sz + hdr_sz);
                _read_buffer_occupied -= (msg_sz + hdr_sz);
                read_next = 128;
                return msg;
            }
            catch (TTransportException& ex)
            {
                ex;
                return nullptr;
            }
        }
    };

}

/*
    symbols defined in libthrift, putting here so we don't need to link :-)
*/
namespace apache {
    namespace thrift {
        namespace transport {
            inline const char* TTransportException::what() const throw() {
                if (message_.empty()) {
                    switch (type_) {
                    case UNKNOWN:
                        return "TTransportException: Unknown transport exception";
                    case NOT_OPEN:
                        return "TTransportException: Transport not open";
                    case TIMED_OUT:
                        return "TTransportException: Timed out";
                    case END_OF_FILE:
                        return "TTransportException: End of file";
                    case INTERRUPTED:
                        return "TTransportException: Interrupted";
                    case BAD_ARGS:
                        return "TTransportException: Invalid arguments";
                    case CORRUPTED_DATA:
                        return "TTransportException: Corrupted Data";
                    case INTERNAL_ERROR:
                        return "TTransportException: Internal error";
                    default:
                        return "TTransportException: (Invalid exception type)";
                    }
                }
                else {
                    return message_.c_str();
                }
            }
        }
    }
} // apache::thrift::transport
