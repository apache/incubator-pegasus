#pragma once

#include <dsn/utility/blob.h>
#include <cstring>

namespace dsn {

class binary_writer
{
public:
    binary_writer(int reserved_buffer_size = 0);
    binary_writer(blob &buffer);
    virtual ~binary_writer();

    virtual void flush();

    template <typename T>
    void write_pod(const T &val);
    template <typename T>
    void write(const T &val)
    {
        // write of this type is not implemented
        assert(false);
    }
    void write(const int8_t &val) { write_pod(val); }
    void write(const uint8_t &val) { write_pod(val); }
    void write(const int16_t &val) { write_pod(val); }
    void write(const uint16_t &val) { write_pod(val); }
    void write(const int32_t &val) { write_pod(val); }
    void write(const uint32_t &val) { write_pod(val); }
    void write(const int64_t &val) { write_pod(val); }
    void write(const uint64_t &val) { write_pod(val); }
    void write(const bool &val) { write_pod(val); }

    void write(const std::string &val);
    void write(const char *buffer, int sz);
    void write(const blob &val);
    void write_empty(int sz);

    bool next(void **data, int *size);
    bool backup(int count);

    void get_buffers(/*out*/ std::vector<blob> &buffers);
    int get_buffer_count() const { return static_cast<int>(_buffers.size()); }
    blob get_buffer();
    blob get_current_buffer(); // without commit, write can be continued on the last buffer
    blob get_first_buffer() const;

    int total_size() const { return _total_size; }

protected:
    // bb may have large space than size
    void create_buffer(size_t size);
    void commit();
    virtual void create_new_buffer(size_t size, /*out*/ blob &bb);

private:
    std::vector<blob> _buffers;

    char *_current_buffer;
    int _current_offset;
    int _current_buffer_length;

    int _total_size;
    int _reserved_size_per_buffer;
    static int _reserved_size_per_buffer_static;
};

//--------------- inline implementation -------------------
template <typename T>
inline void binary_writer::write_pod(const T &val)
{
    write((char *)&val, static_cast<int>(sizeof(T)));
}

inline void binary_writer::get_buffers(/*out*/ std::vector<blob> &buffers)
{
    commit();
    buffers = _buffers;
}

inline blob binary_writer::get_first_buffer() const { return _buffers[0]; }

inline void binary_writer::write(const std::string &val)
{
    int len = static_cast<int>(val.length());
    write((const char *)&len, sizeof(int));
    if (len > 0)
        write((const char *)&val[0], len);
}

inline void binary_writer::write(const blob &val)
{
    // TODO: optimization by not memcpy
    int len = val.length();
    write((const char *)&len, sizeof(int));
    if (len > 0)
        write((const char *)val.data(), len);
}
}
