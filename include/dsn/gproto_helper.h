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

# include <dsn/ports.h>
# include <dsn/cpp/utils.h>

# include <google/protobuf/stubs/common.h>
# include <google/protobuf/io/zero_copy_stream.h>
# include <google/protobuf/io/coded_stream.h>

namespace dsn {

    class gproto_binary_reader : public ::google::protobuf::io::ZeroCopyInputStream
    {
    public:
        gproto_binary_reader(binary_reader& reader)
            : _reader(reader)
        {
        }

        virtual bool Next(const void** data, int* size)
        {
            return _reader.next(data, size);
        }

        virtual void BackUp(int count)
        {
            _reader.backup(count);
        }

        virtual bool Skip(int count)
        {
            return _reader.skip(count);
        }

        virtual int64_t ByteCount() const
        {
            return static_cast<int64_t>(_reader.total_size());
        }
            
    private:
        binary_reader& _reader;
    };

    class gproto_binary_writer : public ::google::protobuf::io::ZeroCopyOutputStream
    {
    public:
        gproto_binary_writer(binary_writer& writer)
            : _writer(writer)
        {
        }

        virtual bool Next(void** data, int* size)
        {
            return _writer.next(data, size);
        }

        virtual void BackUp(int count)
        {
            _writer.backup(count);
        }

        virtual int64_t ByteCount() const
        {
            return static_cast<int64_t>(_writer.total_size());
        }

        virtual bool WriteAliasedRaw(const void* data, int size)
        {
            return false;
        }

    private:
        binary_writer& _writer;
    };
    
    template<typename T>
    void marshall(binary_writer& writer, const T& val)
    {
        gproto_binary_writer wt2(writer);
        ::google::protobuf::io::CodedOutputStream os(&wt2);
        val.SerializeWithCachedSizes(&os);
    }

    template<typename T>
    void unmarshall(binary_reader& reader, __out_param T& val)
    {
        gproto_binary_reader rd2(reader);
        ::google::protobuf::io::CodedInputStream is(&rd2);
        val.MergePartialFromCodedStream(&is);
    }
    
}
