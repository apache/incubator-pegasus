#ifndef DUMP_FILE_H
#define DUMP_FILE_H

#include <dsn/service_api_c.h>
#include <dsn/service_api_cpp.h>
#include <cstdio>
#include <cerrno>
#include <iostream>

inline void error_msg(int err_number, /*out*/char* buffer, int buflen)
{
#ifdef _WIN32
    int result = strerror_s(buffer, buflen, err_number);
    if (result != 0)
        fprintf(stderr, "maybe unknown err number(%s)", err_number);
#else
    char* result = strerror_r(err_number, buffer, buflen);
    if (result != buffer) {
        fprintf(stderr, "%s\n", result);
    }
#endif
}

#define log_error_and_return(buffer, length) do {\
    error_msg(errno, buffer, length);\
    derror("append file failed, reason(%s)", buffer);\
    return -1;\
} while (0)

struct block_header{
    uint32_t length;
    uint32_t crc32;
};

class dump_file {
public:
    ~dump_file() { fclose(_file_handle); }

    static std::shared_ptr<dump_file> open_file(const char* filename, bool is_write)
    {
        std::shared_ptr<dump_file> res(new dump_file());
        res->_filename = filename;
        if ( is_write )
            res->_file_handle = fopen(filename, "wb");
        else
            res->_file_handle = fopen(filename, "rb");
        res->_is_write = is_write;

        if ( res->_file_handle == nullptr)
            return nullptr;
        return res;
    }

    int append_buffer(const char* data, uint32_t data_length)
    {
        static __thread char msg_buffer[128];

        dassert(_is_write, "call append when open file with read mode");

        block_header hdr = {data_length, 0};
        hdr.crc32 = dsn_crc32_compute(data, data_length, _crc);
        _crc = hdr.crc32;
        size_t len = fwrite(&hdr, sizeof(hdr), 1, _file_handle);
        if (len < 1)
        {
            log_error_and_return(msg_buffer, 128);
        }

        len = 0;
        while (len < data_length)
        {
            size_t cnt = fwrite(data+len, 1, data_length-len, _file_handle);
            if (len+cnt<data_length && errno!=EINTR)
            {
                log_error_and_return(msg_buffer, 128);
            }
            len += cnt;
        }
        return 0;
    }
    int append_buffer(const dsn::blob& data)
    {
        return append_buffer(data.data(), data.length());
    }
    int append_buffer(const std::string& data)
    {
        return append_buffer(data.c_str(), data.size());
    }
    int read_next_buffer(/*out*/dsn::blob& output)
    {
        static __thread char msg_buffer[128];
        dassert(!_is_write, "call read next buffer when open file with write mode");

        block_header hdr;
        size_t len = fread(&hdr, sizeof(hdr), 1, _file_handle);
        if (len < 1 )
        {
            if ( feof(_file_handle) )
                return 0;
            else {
                log_error_and_return(msg_buffer, 128);
            }
        }

        std::shared_ptr<char> ptr(new char[hdr.length], [](char* raw){ delete []raw; });
        char* raw_mem = ptr.get();
        len = 0;
        while (len < hdr.length)
        {
            size_t cnt = fread(raw_mem+len, 1, hdr.length-len, _file_handle);
            if (len+cnt<hdr.length)
            {
                if ( feof(_file_handle) )
                {
                    derror("unexpected file end, start offset of this block (%u)", ftell(_file_handle)-len-sizeof(hdr));
                    return -1;
                }
                else if (errno != EINTR)
                {
                    log_error_and_return(msg_buffer, 128);
                }
            }
            len += cnt;
        }
        _crc = dsn_crc32_compute(raw_mem, len, _crc);
        if (_crc != hdr.crc32)
        {
            derror("file %s data error, block offset(%ld)", _filename.c_str(), ftell(_file_handle)-hdr.length-sizeof(hdr));
            return -1;
        }

        output.assign(ptr, 0, hdr.length);
        return 1;
    }

private:
    dump_file(): _file_handle(nullptr), _crc(0) {}
    bool _is_write;//true for write, false for read
    FILE* _file_handle;
    std::string _filename;
    uint32_t _crc;
};
#endif // DUMP_FILE_H
