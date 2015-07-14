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
# include <dsn/internal/utils.h>
# include <dsn/internal/env_provider.h>
# include <random>
# include <dsn/internal/singleton.h>
# include <sys/types.h>

# if defined(__linux__)
# include <sys/syscall.h>
# elif defined(__FreeBSD__)
# include <sys/thr.h>
# elif defined(__APPLE__)
# include <pthread.h>
# endif

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "dsn.utils"

namespace dsn {
    namespace utils {

        int get_current_tid()
        {
# if defined(_WIN32)
            return static_cast<int>(::GetCurrentThreadId());
# elif defined(__linux__)
            //return static_cast<int>(gettid());
            return static_cast<int>(syscall(SYS_gettid));
# elif defined(__FreeBSD__)
            long lwpid;
            thr_self(&lwpid);
            return static_cast<int>(lwpid);
# elif defined(__APPLE__)
            return static_cast<int>(pthread_mach_thread_np(pthread_self()));
# else
# error not implemented yet
# endif 
        }

        std::string get_last_component(const std::string& input, char splitters[])
        {
            int index = -1;
            char* s = splitters;

            while (*s != 0)
            {
                auto pos = input.find_last_of(*s);
                if (pos != std::string::npos && (static_cast<int>(pos) > index))
                    index = static_cast<int>(pos);
                s++;
            }

            if (index != -1)
                return input.substr(index + 1);
            else
                return "";
        }

        void split_args(const char* args, __out_param std::vector<std::string>& sargs, char splitter)
        {
            sargs.clear();

            std::string v(args);

            int lastPos = 0;
            while (true)
            {
                auto pos = v.find(splitter, lastPos);
                if (pos != std::string::npos)
                {
                    std::string s = v.substr(lastPos, pos - lastPos);
                    if (s.length() > 0)
                    {
                        std::string s2 = trim_string((char*)s.c_str());
                        if (s2.length() > 0)
                            sargs.push_back(s2);
                    }
                    lastPos = static_cast<int>(pos + 1);
                }
                else
                {
                    std::string s = v.substr(lastPos);
                    if (s.length() > 0)
                    {
                        std::string s2 = trim_string((char*)s.c_str());
                        if (s2.length() > 0)
                            sargs.push_back(s2);
                    }
                    break;
                }
            }
        }
        void split_args(const char* args, __out_param std::list<std::string>& sargs, char splitter)
        {
            sargs.clear();

            std::string v(args);

            int lastPos = 0;
            while (true)
            {
                auto pos = v.find(splitter, lastPos);
                if (pos != std::string::npos)
                {
                    std::string s = v.substr(lastPos, pos - lastPos);
                    if (s.length() > 0)
                    {
                        std::string s2 = trim_string((char*)s.c_str());
                        if (s2.length() > 0)
                            sargs.push_back(s2);
                    }
                    lastPos = static_cast<int>(pos + 1);
                }
                else
                {
                    std::string s = v.substr(lastPos);
                    if (s.length() > 0)
                    {
                        std::string s2 = trim_string((char*)s.c_str());
                        if (s2.length() > 0)
                            sargs.push_back(s2);
                    }
                    break;
                }
            }
        }

        char* trim_string(char* s)
        {
            while (*s != '\0' && (*s == ' ' || *s == '\t')) { s++; }
            char* r = s;
            s += strlen(s);
            while (s >= r && (*s == '\0' || *s == ' ' || *s == '\t' || *s == '\r' || *s == '\n')) { *s = '\0';  s--; }
            return r;
        }

        class random64_generator : public singleton<random64_generator>
        {
        public:
            random64_generator()
                : _rng(std::random_device()())
            {
            }

            uint64_t next()
            {
                return _dist(_rng);
            }

        private:
            std::default_random_engine _rng;
            std::uniform_int_distribution<uint64_t> _dist;
        };


        uint64_t get_random64()
        {
            return random64_generator::instance().next();
        }

        uint64_t get_random64_pseudo()
        {
            uint64_t v = ((uint64_t)std::rand());
            v *= ((uint64_t)std::rand());
            v *= ((uint64_t)std::rand());
            v *= ((uint64_t)std::rand());
            v *= ((uint64_t)std::rand());
            v ^= ((uint64_t)std::rand());
            return v;
        }


        uint64_t get_current_physical_time_ns()
        {
            return env_provider::get_current_physical_time_ns();
        }

        void time_ms_to_string(uint64_t ts_ms, char* str)
        {
            auto hr = static_cast<uint32_t>(ts_ms / (60ULL * 60ULL * 1000ULL) % 24);
            auto min = static_cast<uint32_t>(ts_ms / (60ULL * 1000ULL) % 60);
            auto sc = static_cast<uint32_t>(ts_ms / (1000ULL) % 60);
            auto ms = static_cast<uint32_t>(ts_ms % 1000);

            sprintf(str, "%02u:%02u:%02u.%03u", hr, min, sc, ms);
        }
    }
}

namespace  dsn 
{

    binary_reader::binary_reader(blob& blob)
    {
        _blob = blob;
        _size = blob.length();
        _ptr = blob.data();
        _remaining_size = _size;
    }

    int binary_reader::read(__out_param std::string& s)
    {
        int len;
        if (0 == read(len))
            return 0;
    
        s.resize(len, 0);

        if (len > 0)
        {
            int x = read((char*)&s[0], len);
            return x == 0 ? x : (x + sizeof(len));
        }
        else
        {
            return static_cast<int>(sizeof(len));
        }        
    }

    int binary_reader::read(blob& blob)
    {
        int len;
        if (0 == read(len))
            return 0;

        if (len <= get_remaining_size())
        {
            blob = _blob.range(static_cast<int>(_ptr - _blob.data()), len);
            _ptr += len;
            _remaining_size -= len;
            return len + sizeof(len);
        }
        else
        {
            dwarn("read beyond the end of buffer");
            return 0;
        }
    }

    int binary_reader::read(char* buffer, int sz)
    {
        if (sz <= get_remaining_size())
        {
            memcpy((void*)buffer, _ptr, sz);
            _ptr += sz;
            _remaining_size -= sz;
            return sz;
        }
        else
        {
            dwarn("read beyond the end of buffer");
            return 0;
        }
    }

    bool binary_reader::next(const void** data, int* size)
    {
        if (get_remaining_size() > 0)
        {
            *data = (const void*)_ptr;
            *size = _remaining_size;

            _ptr += _remaining_size;
            _remaining_size = 0;
            return true;
        }
        else
            return false;
    }

    bool binary_reader::backup(int count)
    {
        if (count <= static_cast<int>(_ptr - _blob.data()))
        {
            _ptr -= count;
            _remaining_size += count;
            return true;
        }
        else
            return false;
    }

    bool binary_reader::skip(int count)
    {
        if (count <= get_remaining_size())
        {
            _ptr += count;
            _remaining_size -= count;
            return true;
        }
        else
        {
            dwarn("read beyond the end of buffer");
            return false;
        }
    }

    int binary_writer::_reserved_size_per_buffer_static = 256;                

    binary_writer::binary_writer(int reserveBufferSize)
    {
        _total_size = 0;

        _buffers.reserve(1);
        _data.reserve(1);

        _cur_pos = -1;
        _cur_is_placeholder = false;

        _reserved_size_per_buffer = (reserveBufferSize == 0) ? _reserved_size_per_buffer_static : reserveBufferSize;

        create_buffer_and_writer();
    }

    binary_writer::binary_writer(blob& buffer)
    {
        _total_size = 0;

        _buffers.reserve(1);
        _data.reserve(1);

        _cur_pos = -1;
        _cur_is_placeholder = false;

        _reserved_size_per_buffer = _reserved_size_per_buffer_static;

        create_buffer_and_writer(&buffer);
    }

    binary_writer::~binary_writer()
    {
    }

    void binary_writer::create_buffer_and_writer(blob* pBuffer)
    {
        if (pBuffer == nullptr)
        {
            std::shared_ptr<char> ptr((char*)malloc(_reserved_size_per_buffer));
            blob bb(ptr, _reserved_size_per_buffer);
            _buffers.push_back(bb);

            bb._length = 0;
            _data.push_back(bb);
        }
        else
        {
            _buffers.push_back(*pBuffer);

            pBuffer->_length = 0;
            _data.push_back(*pBuffer);
        }

        ++_cur_pos;
    }

    void binary_writer::sanity_check()
    {
        dassert(_cur_pos < static_cast<int>(_buffers.size()), 
            "current position must be within the buffer array: %d vs %d", 
            static_cast<int>(_cur_pos), 
            static_cast<int>(_buffers.size())
            );

        dassert(_data.size() == _buffers.size(),
            "data and buffer array must be of the same size: %d vs %d",
            static_cast<int>(_data.size()),
            static_cast<int>(_buffers.size())
            );

        for (size_t i = 0; i < _data.size(); i++)
        {
            dassert(_data[i].length() <= _buffers[i].length(),
                "data size must not be greater than the buffer size: %d vs %d",
                static_cast<int>(_data[i].length()),
                static_cast<int>(_buffers[i].length())
                );
        }
    }

    uint16_t binary_writer::write_placeholder()
    {
        if (_cur_is_placeholder)
        {
            create_buffer_and_writer();
        }
        _cur_is_placeholder = true;
        dassert(_cur_pos <= 0x0000ffff, "placeholder do not support index exceed uint16.max right now: %d", _cur_pos);
        return (uint16_t)_cur_pos;
    }

    blob binary_writer::get_buffer() const
    {
        if (_data.size() == 1)
        {
            return _data[0];
        }
        else
        {
            std::shared_ptr<char> bptr((char*)malloc(_total_size));
            blob bb(bptr, _total_size);
            const char* ptr = bb.data();

            for (int i = 0; i < static_cast<int>(_data.size()); i++)
            {
                memcpy((void*)ptr, (const void*)_data[i].data(), (size_t)_data[i].length());
                ptr += _data[i].length();
            }
            return bb;
        }
    }

    void binary_writer::write_empty(int sz, uint16_t pos /*= 0xffff*/)
    {
        int sz0 = sz;
# ifdef _DEBUG
        sanity_check();
# endif
        if (pos != 0xffff)
        {
            int rem_size = _buffers[pos].length() - _data[pos].length();
            if (sz > rem_size)
            {
                int allocSize = _data[pos].length() + sz;
                std::shared_ptr<char> ptr((char*)malloc(allocSize));
                blob bb(ptr, allocSize);

                memcpy((void*)bb.data(), (const void*)_data[pos].data(), (size_t)_data[pos].length());

                _buffers[pos] = bb;
                _data[pos] = bb;
            }
            else
            {
                _data[pos]._length += sz;
            }
        }
        else
        {
            if (_cur_is_placeholder)
            {
                create_buffer_and_writer();
                _cur_is_placeholder = false;
            }

            int pos2 = _cur_pos;

            int rem_size = _buffers[pos2].length() - _data[pos2].length();
            if (rem_size >= sz)
            {
                _data[pos2]._length += sz;
            }
            else
            {
                _data[pos2]._length += rem_size;

                sz -= rem_size;

                int allocSize = _reserved_size_per_buffer;
                if (sz > allocSize)
                    allocSize = sz;

                std::shared_ptr<char> ptr((char*)malloc(allocSize));
                blob bb(ptr, allocSize);
                _buffers.push_back(bb);

                bb._length = 0;
                _data.push_back(bb);

                pos2 = (++_cur_pos);

                _data[pos2]._length += sz;
            }
        }
# ifdef _DEBUG
        sanity_check();
# endif

        _total_size += sz0;
    }

    void binary_writer::write(const char* buffer, int sz, uint16_t pos /*= 0xffff*/)
    {
        int sz0 = sz;

# ifdef _DEBUG
        sanity_check();
# endif

        if (pos != 0xffff)
        {
            int rem_size = _buffers[pos].length() - _data[pos].length();
            if (sz > rem_size)
            {
                int allocSize = _data[pos].length() + sz;
                std::shared_ptr<char> ptr((char*)malloc(allocSize));
                blob bb(ptr, allocSize);

                memcpy((void*)bb.data(), (const void*)_data[pos].data(), (size_t)_data[pos].length());
                memcpy((void*)(bb.data() + _data[pos].length()), (const void*)buffer, (size_t)sz);

                _buffers[pos] = bb;
                _data[pos] = bb;
            }
            else
            {
                memcpy((void*)(_data[pos].data() + _data[pos].length()), buffer, (size_t)sz);
                _data[pos]._length += sz;
            }
        }
        else
        {
            if (_cur_is_placeholder)
            {
                create_buffer_and_writer();
                _cur_is_placeholder = false;
            }

            int pos2 = _cur_pos;

            int rem_size = _buffers[pos2].length() - _data[pos2].length();
            if (rem_size >= sz)
            {
                memcpy((void*)(_data[pos2].data() + _data[pos2].length()), buffer, (size_t)sz);
                _data[pos2]._length += sz;
            }
            else
            {
                if (rem_size > 0)
                {
                    memcpy((void*)(_data[pos2].data() + _data[pos2].length()), buffer, (size_t)rem_size);
                    _data[pos2]._length += rem_size;

                    sz -= rem_size;
                    buffer += rem_size;
                }
                else
                {
                    dbg_dassert(rem_size == 0, "remaining size must be zero in this case: %d", rem_size);
                }

                int allocSize = _reserved_size_per_buffer;
                if (sz > allocSize)
                    allocSize = sz;

                std::shared_ptr<char> ptr((char*)malloc(allocSize));
                blob bb(ptr, allocSize);
                _buffers.push_back(bb);

                bb._length = 0;
                _data.push_back(bb);

                pos2 = (++_cur_pos);

                memcpy((void*)(_data[pos2].data() + _data[pos2].length()), buffer, (size_t)sz);
                _data[pos2]._length += sz;
            }
        }

# ifdef _DEBUG
        sanity_check();
# endif

        _total_size += sz0;
    }

    bool binary_writer::next(void** data, int* size)
    {
        int sz = _buffers[_cur_pos].length() - _data[_cur_pos].length();
        if (sz == 0)
        {
            std::shared_ptr<char> ptr((char*)malloc(_reserved_size_per_buffer));
            blob bb(ptr, _reserved_size_per_buffer);
            _buffers.push_back(bb);

            bb._length = 0;
            _data.push_back(bb);
            ++_cur_pos;

            sz = _reserved_size_per_buffer;
        }

        *size = sz;
        *data = (void*)(_data[_cur_pos].data() + _data[_cur_pos].length());
        _data[_cur_pos]._length += sz;
        _total_size += sz;
        return true;
    }

    bool binary_writer::backup(int count)
    {
        dassert(count <= _data[_cur_pos].length(), "currently we don't support backup before the last buffer's header");
        _data[_cur_pos]._length -= count;
        _total_size -= count;
        return true;
    }
} // end namespace dsn





