#include <dsn/utility/utils.h>
#include <dsn/utility/binary_reader.h>
#include <dsn/c/api_utilities.h>

namespace dsn {

binary_reader::binary_reader(const blob &bb) { init(bb); }
binary_reader::binary_reader(blob &&bb) { init(std::move(bb)); }

void binary_reader::init(const blob &bb)
{
    _blob = bb;
    _size = bb.length();
    _ptr = bb.data();
    _remaining_size = _size;
}

void binary_reader::init(blob &&bb)
{
    _blob = std::move(bb);
    _size = _blob.length();
    _ptr = _blob.data();
    _remaining_size = _size;
}

int binary_reader::read(/*out*/ std::string &s)
{
    int len;
    if (0 == read(len))
        return 0;

    s.resize(len, 0);

    if (len > 0) {
        int x = read((char *)&s[0], len);
        return x == 0 ? x : (x + sizeof(len));
    } else {
        return static_cast<int>(sizeof(len));
    }
}

int binary_reader::read(blob &blob)
{
    int len;
    if (0 == read(len))
        return 0;

    return read(blob, len);
}

int binary_reader::read(blob &blob, int len)
{
    if (len <= get_remaining_size()) {
        blob = _blob.range(static_cast<int>(_ptr - _blob.data()), len);

        // optimization: zero-copy
        if (!blob.buffer_ptr()) {
            std::shared_ptr<char> buffer(::dsn::utils::make_shared_array<char>(len));
            memcpy(buffer.get(), blob.data(), blob.length());
            blob = ::dsn::blob(buffer, 0, blob.length());
        }

        _ptr += len;
        _remaining_size -= len;
        return len + sizeof(len);
    } else {
        assert(false);
        return 0;
    }
}

int binary_reader::read(char *buffer, int sz)
{
    if (sz <= get_remaining_size()) {
        memcpy((void *)buffer, _ptr, sz);
        _ptr += sz;
        _remaining_size -= sz;
        return sz;
    } else {
        assert(false);
        return 0;
    }
}

bool binary_reader::next(const void **data, int *size)
{
    if (get_remaining_size() > 0) {
        *data = (const void *)_ptr;
        *size = _remaining_size;

        _ptr += _remaining_size;
        _remaining_size = 0;
        return true;
    } else
        return false;
}

bool binary_reader::backup(int count)
{
    if (count <= static_cast<int>(_ptr - _blob.data())) {
        _ptr -= count;
        _remaining_size += count;
        return true;
    } else
        return false;
}

bool binary_reader::skip(int count)
{
    if (count <= get_remaining_size()) {
        _ptr += count;
        _remaining_size -= count;
        return true;
    } else {
        assert(false);
        return false;
    }
}
}
