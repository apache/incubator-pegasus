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

# include <dsn/internal/ports.h>
# include <dsn/cpp/utils.h>

# include <thrift/Thrift.h>
# include <thrift/protocol/TBinaryProtocol.h>
# include <thrift/protocol/TVirtualProtocol.h>
# include <thrift/protocol/TJSONProtocol.h>
# include <thrift/transport/TVirtualTransport.h>
# include <thrift/TApplicationException.h>

namespace dsn {
    class binary_reader_transport : public apache::thrift::transport::TVirtualTransport<binary_reader_transport>
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
                throw apache::thrift::transport::TTransportException(apache::thrift::transport::TTransportException::END_OF_FILE,
                    "no more data to read after end-of-buffer");
            }
            return (uint32_t)l;
        }

    private:
        binary_reader& _reader;
    };

    class binary_writer_transport : public apache::thrift::transport::TVirtualTransport<binary_writer_transport>
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

#ifndef DSN_IDL_JSON

    template<typename T>
    void marshall(binary_writer& writer, const T& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        val.write(&proto);
    }

    template<typename T>
    void unmarshall(binary_reader& reader, /*out*/ T& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        val.read(&proto);
    }
#else
    template<typename T>
    void marshall(binary_writer& writer, const T& val)
    {
        boost::shared_ptr< ::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TJSONProtocol proto(transport);
        val.write(&proto);
    }

    template<typename T>
    void unmarshall(binary_reader& reader, /*out*/ T& val)
    {
        boost::shared_ptr< ::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TJSONProtocol proto(transport);
        val.read(&proto);
    }
#endif // DSN_IDL_JSON
}