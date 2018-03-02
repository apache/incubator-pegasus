#pragma once

#include <cstring>
#include <dsn/utility/blob.h>

namespace dsn {
class binary_reader
{
public:
    // given bb on ctor
    binary_reader(const blob &blob);
    binary_reader(blob &&blob);

    // or delayed init
    binary_reader() {}

    virtual ~binary_reader() {}

    void init(const blob &bb);
    void init(blob &&bb);

    template <typename T>
    int read_pod(/*out*/ T &val);
    template <typename T>
    int read(/*out*/ T &val)
    {
        // read of this type is not implemented
        assert(false);
        return 0;
    }
    int read(/*out*/ int8_t &val) { return read_pod(val); }
    int read(/*out*/ uint8_t &val) { return read_pod(val); }
    int read(/*out*/ int16_t &val) { return read_pod(val); }
    int read(/*out*/ uint16_t &val) { return read_pod(val); }
    int read(/*out*/ int32_t &val) { return read_pod(val); }
    int read(/*out*/ uint32_t &val) { return read_pod(val); }
    int read(/*out*/ int64_t &val) { return read_pod(val); }
    int read(/*out*/ uint64_t &val) { return read_pod(val); }
    int read(/*out*/ bool &val) { return read_pod(val); }

    int read(/*out*/ std::string &s);
    int read(char *buffer, int sz);
    int read(blob &blob);
    int read(blob &blob, int len);

    bool next(const void **data, int *size);
    bool skip(int count);
    bool backup(int count);

    blob get_buffer() const { return _blob; }
    blob get_remaining_buffer() const { return _blob.range(static_cast<int>(_ptr - _blob.data())); }
    bool is_eof() const { return _ptr >= _blob.data() + _size; }
    int total_size() const { return _size; }
    int get_remaining_size() const { return _remaining_size; }

private:
    blob _blob;
    int _size;
    const char *_ptr;
    int _remaining_size;
};

template <typename T>
inline int binary_reader::read_pod(/*out*/ T &val)
{
    if (sizeof(T) <= get_remaining_size()) {
        memcpy((void *)&val, _ptr, sizeof(T));
        _ptr += sizeof(T);
        _remaining_size -= sizeof(T);
        return static_cast<int>(sizeof(T));
    } else {
        // read beyond the end of buffer
        assert(false);
        return 0;
    }
}
}
