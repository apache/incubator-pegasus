#pragma once

#include <memory>
#ifdef DSN_USE_THRIFT_SERIALIZATION
#include <thrift/protocol/TProtocol.h>
#endif

namespace dsn {

class blob
{
public:
    blob() : _buffer(nullptr), _data(nullptr), _length(0) {}

    blob(const std::shared_ptr<char> &buffer, unsigned int length)
        : _holder(buffer), _buffer(_holder.get()), _data(_holder.get()), _length(length)
    {
    }

    blob(std::shared_ptr<char> &&buffer, unsigned int length)
        : _holder(std::move(buffer)), _buffer(_holder.get()), _data(_holder.get()), _length(length)
    {
    }

    blob(const std::shared_ptr<char> &buffer, int offset, unsigned int length)
        : _holder(buffer), _buffer(_holder.get()), _data(_holder.get() + offset), _length(length)
    {
    }

    blob(std::shared_ptr<char> &&buffer, int offset, unsigned int length)
        : _holder(std::move(buffer)),
          _buffer(_holder.get()),
          _data(_holder.get() + offset),
          _length(length)
    {
    }

    blob(const char *buffer, int offset, unsigned int length)
        : _buffer(buffer), _data(buffer + offset), _length(length)
    {
    }

    blob(const blob &source)
        : _holder(source._holder),
          _buffer(source._buffer),
          _data(source._data),
          _length(source._length)
    {
    }

    blob(blob &&source)
        : _holder(std::move(source._holder)),
          _buffer(source._buffer),
          _data(source._data),
          _length(source._length)
    {
        source._buffer = nullptr;
        source._data = nullptr;
        source._length = 0;
    }

    blob &operator=(const blob &that)
    {
        _holder = that._holder;
        _buffer = that._buffer;
        _data = that._data;
        _length = that._length;
        return *this;
    }

    blob &operator=(blob &&that)
    {
        _holder = std::move(that._holder);
        _buffer = that._buffer;
        _data = that._data;
        _length = that._length;
        that._buffer = nullptr;
        that._data = nullptr;
        that._length = 0;
        return *this;
    }

    void assign(const std::shared_ptr<char> &buffer, int offset, unsigned int length)
    {
        _holder = buffer;
        _buffer = _holder.get();
        _data = _holder.get() + offset;
        _length = length;
    }

    void assign(std::shared_ptr<char> &&buffer, int offset, unsigned int length)
    {
        _holder = std::move(buffer);
        _buffer = (_holder.get());
        _data = (_holder.get() + offset);
        _length = length;
    }

    void assign(const char *buffer, int offset, unsigned int length)
    {
        _holder = nullptr;
        _buffer = buffer;
        _data = buffer + offset;
        _length = length;
    }

    const char *data() const { return _data; }

    unsigned int length() const { return _length; }

    std::shared_ptr<char> buffer() const { return _holder; }

    bool has_holder() const { return _holder.get() != nullptr; }

    const char *buffer_ptr() const { return _holder.get(); }

    // offset can be negative for buffer dereference
    blob range(int offset) const
    {
        // offset cannot exceed the current length value
        assert(offset <= static_cast<int>(_length));

        blob temp = *this;
        temp._data += offset;
        temp._length -= offset;
        return temp;
    }

    blob range(int offset, unsigned int len) const
    {
        // offset cannot exceed the current length value
        assert(offset <= static_cast<int>(_length));

        blob temp = *this;
        temp._data += offset;
        temp._length -= offset;

        // buffer length must exceed the required length
        assert(temp._length >= len);
        temp._length = len;
        return temp;
    }

    bool operator==(const blob &r) const
    {
        // not implemented
        assert(false);
        return false;
    }

#ifdef DSN_USE_THRIFT_SERIALIZATION
    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;
#endif
private:
    friend class binary_writer;
    std::shared_ptr<char> _holder;
    const char *_buffer;
    const char *_data;
    unsigned int _length; // data length
};
}
