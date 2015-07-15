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
# include <dsn/internal/logging.h>
# include <dsn/internal/error_code.h>

namespace dsn {

    class blob
    {
    public:
        blob() { _buffer = _data = 0;  _length = 0; }

        blob(std::shared_ptr<char>& buffer, int length)
            : _holder(buffer), _buffer(buffer.get()), _data(buffer.get()), _length(length)
        {}

        blob(std::shared_ptr<char>& buffer, int offset, int length)
            : _holder(buffer), _buffer(buffer.get()), _data(buffer.get() + offset), _length(length)
        {}

        blob(const char* buffer, int offset, int length)
            : _buffer(buffer), _data(buffer + offset), _length(length)
        {}

        blob(const blob& source)
            : _holder(source._holder), _buffer(source._buffer), _data(source._data), _length(source._length)
        {}

        void assign(std::shared_ptr<char>& buffer, int offset, int length)
        {
            _holder = buffer;
            _buffer = (buffer.get());
            _data = (buffer.get() + offset);
            _length = (length);
        }

        const char* data() const { return _data; }

        int   length() const { return _length; }

        std::shared_ptr<char> buffer() { return _holder; }

        blob range(int offset) const
        {
            dassert(offset <= _length, "offset cannot exceed the current length value");

            blob temp = *this;
            temp._data += offset;
            temp._length -= offset;
            return temp;
        }

        blob range(int offset, int len) const
        {
            dassert(offset <= _length, "offset cannot exceed the current length value");

            blob temp = *this;
            temp._data += offset;
            temp._length -= offset;
            dassert(temp._length >= len, "buffer length must exceed the required length");
            temp._length = len;
            return temp;
        }

        bool operator == (const blob& r) const
        {
            dassert(false, "not implemented");
            return false;
        }

    private:
        friend class binary_writer;
        std::shared_ptr<char>  _holder;
        const char*            _buffer;
        const char*            _data;
        int                    _length; // data length
    };

    class binary_reader
    {
    public:
        binary_reader(blob& blob);

        template<typename T> int read_pod(__out_param T& val);
        template<typename T> int read(__out_param T& val) { dassert(false, "read of this type is not implemented"); return 0; }
        int read(__out_param int8_t& val) { return read_pod(val); }
        int read(__out_param uint8_t& val) { return read_pod(val); }
        int read(__out_param int16_t& val) { return read_pod(val); }
        int read(__out_param uint16_t& val) { return read_pod(val); }
        int read(__out_param int32_t& val) { return read_pod(val); }
        int read(__out_param uint32_t& val) { return read_pod(val); }
        int read(__out_param int64_t& val) { return read_pod(val); }
        int read(__out_param uint64_t& val) { return read_pod(val); }
        int read(__out_param bool& val) { return read_pod(val); }

        int read(__out_param error_code& err) { int val; int ret = read_pod(val); err.set(val); return ret; }
        int read(__out_param std::string& s);
        int read(char* buffer, int sz);
        int read(blob& blob);

        bool next(const void** data, int* size);
        bool skip(int count);
        bool backup(int count);

        blob get_buffer() const { return _blob; }
        blob get_remaining_buffer() const { return _blob.range(static_cast<int>(_ptr - _blob.data())); }
        bool is_eof() const { return _ptr >= _blob.data() + _size; }
        int  total_size() const { return _size; }
        int  get_remaining_size() const { return _remaining_size; }

    private:
        blob        _blob;
        int         _size;
        const char* _ptr;
        int         _remaining_size;
    };
    
    class binary_writer
    {
    public:
        binary_writer(int reservedBufferSize = 0);
        binary_writer(blob& buffer);
        ~binary_writer();

        uint16_t write_placeholder();
        template<typename T> void write_pod(const T& val, uint16_t pos = 0xffff);
        template<typename T> void write(const T& val, uint16_t pos = 0xffff) { dassert(false, "write of this type is not implemented"); }
        void write(const int8_t& val, uint16_t pos = 0xffff) { write_pod(val, pos); }
        void write(const uint8_t& val, uint16_t pos = 0xffff) { write_pod(val, pos); }
        void write(const int16_t& val, uint16_t pos = 0xffff) { write_pod(val, pos); }
        void write(const uint16_t& val, uint16_t pos = 0xffff) { write_pod(val, pos); }
        void write(const int32_t& val, uint16_t pos = 0xffff) { write_pod(val, pos); }
        void write(const uint32_t& val, uint16_t pos = 0xffff) { write_pod(val, pos); }
        void write(const int64_t& val, uint16_t pos = 0xffff) { write_pod(val, pos); }
        void write(const uint64_t& val, uint16_t pos = 0xffff) { write_pod(val, pos); }
        void write(const bool& val, uint16_t pos = 0xffff) { write_pod(val, pos); }

        void write(const error_code& val, uint16_t pos = 0xffff) { int err = val.get();  write_pod(err, pos); }
        void write(const std::string& val, uint16_t pos = 0xffff);
        void write(const char* buffer, int sz, uint16_t pos = 0xffff);
        void write(const blob& val, uint16_t pos = 0xffff);
        void write_empty(int sz, uint16_t pos = 0xffff);

        bool next(void** data, int* size);
        bool backup(int count);

        void get_buffers(__out_param std::vector<blob>& buffers) const;
        int  get_buffer_count() const { return static_cast<int>(_buffers.size()); }
        blob get_buffer() const;
        blob get_first_buffer() const;

        int total_size() const { return _total_size; }

    private:
        void create_buffer_and_writer(blob* pBuffer = nullptr);
        void sanity_check();

    private:
        std::vector<blob>  _buffers;
        std::vector<blob>  _data;
        bool               _cur_is_placeholder;
        int                _cur_pos;
        int                _total_size;
        int                _reserved_size_per_buffer;
        static int         _reserved_size_per_buffer_static;
    };

    //--------------- inline implementation -------------------
    template<typename T>
    inline int binary_reader::read_pod(__out_param T& val)
    {
        if (sizeof(T) <= get_remaining_size())
        {
            memcpy((void*)&val, _ptr, sizeof(T));
            _ptr += sizeof(T);
            _remaining_size -= sizeof(T);
            return static_cast<int>(sizeof(T));
        }
        else
        {
            dlog(::dsn::logging_level::log_level_WARNING, "dsn.utils", "read beyond the end of buffer");
            return 0;
        }
    }

    template<typename T>
    inline void binary_writer::write_pod(const T& val, uint16_t pos)
    {
        write((char*)&val, static_cast<int>(sizeof(T)), pos);
    }

    inline void binary_writer::get_buffers(__out_param std::vector<blob>& buffers) const
    {
        buffers = _data;
    }

    inline blob binary_writer::get_first_buffer() const
    {
        return _data[0];
    }

    inline void binary_writer::write(const std::string& val, uint16_t pos /*= 0xffff*/)
    {
        int len = static_cast<int>(val.length());
        write((const char*)&len, sizeof(int), pos);
        if (len > 0) write((const char*)&val[0], len, pos);
    }

    inline void binary_writer::write(const blob& val, uint16_t pos /*= 0xffff*/)
    {
        // TODO: optimization by not memcpy
        int len = val.length();
        write((const char*)&len, sizeof(int), pos);
        if (len > 0) write((const char*)val.data(), len, pos);
    }
}

namespace dsn {
    namespace utils {

        extern void split_args(const char* args, __out_param std::vector<std::string>& sargs, char splitter = ' ');
        extern void split_args(const char* args, __out_param std::list<std::string>& sargs, char splitter = ' ');
        extern std::string get_last_component(const std::string& input, char splitters[]);

        extern char* trim_string(char* s);

        extern uint64_t get_random64();

        extern uint64_t get_random64_pseudo();

        extern uint64_t get_current_physical_time_ns();

        extern void time_ms_to_string(uint64_t ts_ms, char* str);

        extern int get_current_tid();

        inline int get_invalid_tid() { return -1; }
    }
} // end namespace dsn::utils

