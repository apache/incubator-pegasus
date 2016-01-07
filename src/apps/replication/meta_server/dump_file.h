#ifndef DUMP_FILE_H
#define DUMP_FILE_H

#include <dsn/service_api_c.h>
#include <dsn/service_api_cpp.h>
#include <cstdio>

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
        dassert(_is_write, "call append when open file with read mode");
        block_header hdr = {data_length, 0};
        hdr.crc32 = dsn_crc32_compute(data, data_length, 0);
        int len = fwrite(&hdr, sizeof(hdr), 1, _file_handle);
        if (len < 1) {
            derror("append file failed");
            return -1;
        }
        len = 0;
        while (len < data_length)
        {
            len += fwrite(data+len, 1, data_length-len, _file_handle);
        }
        return 0;
    }
    int append_buffer(const dsn::blob& data)
    {
        return append_buffer(data.data(), data.length());
    }
    int read_next_buffer(/*out*/dsn::blob& output)
    {
        dassert(!_is_write, "call read next buffer when open file with write mode");
        block_header hdr;
        int len = fread(&hdr, sizeof(hdr), 1, _file_handle);
        if (len < 1 ) {
            if ( feof(_file_handle) )
                return 0;
            else {
                derror("read file failed, offset(%ld)", ftell(_file_handle));
                return -1;
            }
        }

        std::shared_ptr<char> ptr(new char[hdr.length], [](char* raw){ delete []raw; });
        char* raw_mem = ptr.get();
        len = 0;
        while (len < hdr.length)
        {
            len += fread(raw_mem+len, 1, hdr.length-len, _file_handle);
        }
        uint32_t crc_res = dsn_crc32_compute(raw_mem, len, 0);
        if (crc_res != hdr.crc32)
        {
            derror("file %s data error, block offset(%ld)", ftell(_file_handle)-hdr.length-sizeof(hdr));
            return -1;
        }

        output.assign(ptr, 0, hdr.length);
        return 1;
    }

private:
    dump_file(): _file_handle(nullptr) {}
    bool _is_write;//true for write, false for read
    FILE* _file_handle;
    std::string _filename;
};
#endif // DUMP_FILE_H
