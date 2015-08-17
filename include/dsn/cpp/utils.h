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
# include <dsn/cpp/auto_codes.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "utils"

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

        void assign(const char* buffer, int offset, int length)
        {
            _holder = nullptr;
            _buffer = buffer;
            _data = buffer + offset;
            _length = (length);
        }

        const char* data() const { return _data; }

        int   length() const { return _length; }

        std::shared_ptr<char> buffer() { return _holder; }

        const char* buffer_ptr() { return _holder.get(); }

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
        // given bb on ctor
        binary_reader(blob& blob);

        // or delayed init
        binary_reader() {}
        void init(blob& bb);

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

        int read(__out_param error_code& err) { int val; int ret = read_pod(val); err = val; return ret; }
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
        binary_writer(int reserved_buffer_size = 0);
        binary_writer(blob& buffer);
        ~binary_writer();

        template<typename T> void write_pod(const T& val);
        template<typename T> void write(const T& val) { dassert(false, "write of this type is not implemented"); }
        void write(const int8_t& val) { write_pod(val); }
        void write(const uint8_t& val) { write_pod(val); }
        void write(const int16_t& val) { write_pod(val); }
        void write(const uint16_t& val) { write_pod(val); }
        void write(const int32_t& val) { write_pod(val); }
        void write(const uint32_t& val) { write_pod(val); }
        void write(const int64_t& val) { write_pod(val); }
        void write(const uint64_t& val) { write_pod(val); }
        void write(const bool& val) { write_pod(val); }

        void write(const error_code& val) { int err = val.get();  write_pod(err); }
        void write(const std::string& val);
        void write(const char* buffer, int sz);
        void write(const blob& val);
        void write_empty(int sz);

        bool next(void** data, int* size);
        bool backup(int count);

        void get_buffers(__out_param std::vector<blob>& buffers);
        int  get_buffer_count() const { return static_cast<int>(_buffers.size()); }
        blob get_buffer();
        blob get_first_buffer() const;

        int total_size() const { return _total_size; }

    protected:
        // bb may have large space than size
        void create_buffer(size_t size);
        void commit();
        virtual void create_new_buffer(size_t size, /*out*/blob& bb);

    private:
        std::vector<blob>  _buffers;
        
        char*              _current_buffer;
        int                _current_offset;
        int                _current_buffer_length;

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
            dassert(false, "read beyond the end of buffer");
            return 0;
        }
    }

    template<typename T>
    inline void binary_writer::write_pod(const T& val)
    {
        write((char*)&val, static_cast<int>(sizeof(T)));
    }

    inline void binary_writer::get_buffers(__out_param std::vector<blob>& buffers)
    {
        commit();
        buffers = _buffers;
    }

    inline blob binary_writer::get_first_buffer() const
    {
        return _buffers[0];
    }

    inline void binary_writer::write(const std::string& val)
    {
        int len = static_cast<int>(val.length());
        write((const char*)&len, sizeof(int));
        if (len > 0) write((const char*)&val[0], len);
    }

    inline void binary_writer::write(const blob& val)
    {
        // TODO: optimization by not memcpy
        int len = val.length();
        write((const char*)&len, sizeof(int));
        if (len > 0) write((const char*)val.data(), len);
    }
}

namespace dsn {
    namespace utils {

        extern void split_args(const char* args, __out_param std::vector<std::string>& sargs, char splitter = ' ');
        extern void split_args(const char* args, __out_param std::list<std::string>& sargs, char splitter = ' ');
        extern std::string replace_string(std::string subject, const std::string& search, const std::string& replace);
        extern std::string get_last_component(const std::string& input, char splitters[]);

        extern char* trim_string(char* s);

        extern uint64_t get_random64();

        extern uint64_t get_random64_pseudo();

        extern uint64_t get_current_physical_time_ns();

        extern void time_ms_to_string(uint64_t ts_ms, char* str);

        extern int get_current_tid_internal();

        typedef struct _tls_tid
        {
            int magic;
            int local_tid;
        } tls_tid;
        extern __thread tls_tid s_tid;

        inline int get_current_tid()
        {
            if (s_tid.magic == 0xdeadbeef)
                return s_tid.local_tid;
            else
            {
                s_tid.magic = 0xdeadbeef;
                s_tid.local_tid = get_current_tid_internal();                
                return s_tid.local_tid;
            }
        }

        inline int get_invalid_tid() { return -1; }

        extern bool is_file_or_dir_exist(const char* path);

        extern std::string get_absolute_path(const char* path);

        extern std::string remove_file_name(const char* path);

        extern bool remove_dir(const char* path, bool recursive);
    }
} // end namespace dsn::utils

