//
// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// File: string_view.h
// -----------------------------------------------------------------------------
//
// This file contains the definition of the `dsn::string_view` class. A
// `string_view` points to a contiguous span of characters, often part or all of
// another `std::string`, double-quoted std::string literal, character array, or even
// another `string_view`.
//
// This `dsn::string_view` abstraction is designed to be a drop-in
// replacement for the C++17 `std::string_view` abstraction.
//
// --- Update(wutao1) ---
//
// This file is copied from abseil, though in order to maintain minimum
// dependencies, abseil is not an requirement. The dsn::string_view consists of only
// a subset of functions that std::string_view and absl::string_view provide, so that
// we can keep this module lightweight, but reducing the generality.
//
// dsn::string_view also supports view of dsn::blob, which can also function as a constant
// view. However, dsn::blob is not designed to be as lightweight as dsn::string_view
// since it requires at least one atomic operation to copy the internal std::shared_ptr.
// So in most cases where data is immutable, using dsn::string_view over dsn::blob will
// be a more proper choice.

#pragma once

#include <algorithm>
#include <cassert>
#include <cstring>
#include <stdexcept>
#include <string>

#include "ports.h"

namespace dsn {

class blob;

// dsn::string_view
//
// A `string_view` provides a lightweight view into the std::string data provided by
// a `std::string`, double-quoted std::string literal, character array, or even
// another `string_view`. A `string_view` does *not* own the std::string to which it
// points, and that data cannot be modified through the view.
//
// You can use `string_view` as a function or method parameter anywhere a
// parameter can receive a double-quoted std::string literal, `const char*`,
// `std::string`, or another `absl::string_view` argument with no need to copy
// the std::string data. Systematic use of `string_view` within function arguments
// reduces data copies and `strlen()` calls.
//
// Because of its small size, prefer passing `string_view` by value:
//
//   void MyFunction(dsn::string_view arg);
//
// If circumstances require, you may also pass one by const reference:
//
//   void MyFunction(const dsn::string_view& arg);  // not preferred
//
// Passing by value generates slightly smaller code for many architectures.
//
// In either case, the source data of the `string_view` must outlive the
// `string_view` itself.
//
// A `string_view` is also suitable for local variables if you know that the
// lifetime of the underlying object is longer than the lifetime of your
// `string_view` variable. However, beware of binding a `string_view` to a
// temporary value:
//
//   // BAD use of string_view: lifetime problem
//   dsn::string_view sv = obj.ReturnAString();
//
//   // GOOD use of string_view: str outlives sv
//   std::string str = obj.ReturnAString();
//   dsn::string_view sv = str;
//
// Due to lifetime issues, a `string_view` is sometimes a poor choice for a
// return value and usually a poor choice for a data member. If you do use a
// `string_view` this way, it is your responsibility to ensure that the object
// pointed to by the `string_view` outlives the `string_view`.
//
// A `string_view` may represent a whole std::string or just part of a std::string. For
// example, when splitting a std::string, `std::vector<dsn::string_view>` is a
// natural data type for the output.
//
//
// When constructed from a source which is nul-terminated, the `string_view`
// itself will not include the nul-terminator unless a specific size (including
// the nul) is passed to the constructor. As a result, common idioms that work
// on nul-terminated strings do not work on `string_view` objects. If you write
// code that scans a `string_view`, you must check its length rather than test
// for nul, for example. Note, however, that nuls may still be embedded within
// a `string_view` explicitly.
//
// You may create a null `string_view` in two ways:
//
//   dsn::string_view sv();
//   dsn::string_view sv(nullptr, 0);
//
// For the above, `sv.data() == nullptr`, `sv.length() == 0`, and
// `sv.empty() == true`. Also, if you create a `string_view` with a non-null
// pointer then `sv.data() != nullptr`. Thus, you can use `string_view()` to
// signal an undefined value that is different from other `string_view` values
// in a similar fashion to how `const char* p1 = nullptr;` is different from
// `const char* p2 = "";`. However, in practice, it is not recommended to rely
// on this behavior.
//
// Be careful not to confuse a null `string_view` with an empty one. A null
// `string_view` is an empty `string_view`, but some empty `string_view`s are
// not null. Prefer checking for emptiness over checking for null.
//
// There are many ways to create an empty string_view:
//
//   const char* nullcp = nullptr;
//   // string_view.size() will return 0 in all cases.
//   dsn::string_view();
//   dsn::string_view(nullcp, 0);
//   dsn::string_view("");
//   dsn::string_view("", 0);
//   dsn::string_view("abcdef", 0);
//   dsn::string_view("abcdef" + 6, 0);
//
// All empty `string_view` objects whether null or not, are equal:
//
//   dsn::string_view() == dsn::string_view("", 0)
//   dsn::string_view(nullptr, 0) == dsn::string_view("abcdef"+6, 0)
class string_view
{
public:
    using traits_type = std::char_traits<char>;
    using value_type = char;
    using pointer = char *;
    using const_pointer = const char *;
    using reference = char &;
    using const_reference = const char &;
    using const_iterator = const char *;
    using iterator = const_iterator;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;
    using reverse_iterator = const_reverse_iterator;
    using size_type = size_t;
    using difference_type = std::ptrdiff_t;

    static constexpr size_type npos = static_cast<size_type>(-1);

    // Null `string_view` constructor
    constexpr string_view() noexcept : ptr_(nullptr), length_(0) {}

    // Implicit constructors

    template <typename Allocator>
    string_view( // NOLINT(runtime/explicit)
        const std::basic_string<char, std::char_traits<char>, Allocator> &str) noexcept
        : ptr_(str.data()), length_(str.size())
    {
    }

    string_view(const blob &buf) noexcept; // NOLINT(runtime/explicit)

    constexpr string_view(const char *str) // NOLINT(runtime/explicit)
        : ptr_(str),
          length_(str == nullptr ? 0 : traits_type::length(str))
    {
    }

    // Implicit constructor of a `string_view` from a `const char*` and length.
    constexpr string_view(const char *data, size_type len) : ptr_(data), length_(len) {}

    // NOTE: Harmlessly omitted to work around gdb bug.
    //   constexpr string_view(const string_view&) noexcept = default;
    //   string_view& operator=(const string_view&) noexcept = default;

    // Iterators

    // string_view::begin()
    //
    // Returns an iterator pointing to the first character at the beginning of the
    // `string_view`, or `end()` if the `string_view` is empty.
    constexpr const_iterator begin() const noexcept { return ptr_; }

    // string_view::end()
    //
    // Returns an iterator pointing just beyond the last character at the end of
    // the `string_view`. This iterator acts as a placeholder; attempting to
    // access it results in undefined behavior.
    constexpr const_iterator end() const noexcept { return ptr_ + length_; }

    // string_view::cbegin()
    //
    // Returns a const iterator pointing to the first character at the beginning
    // of the `string_view`, or `end()` if the `string_view` is empty.
    constexpr const_iterator cbegin() const noexcept { return begin(); }

    // string_view::cend()
    //
    // Returns a const iterator pointing just beyond the last character at the end
    // of the `string_view`. This pointer acts as a placeholder; attempting to
    // access its element results in undefined behavior.
    constexpr const_iterator cend() const noexcept { return end(); }

    // string_view::rbegin()
    //
    // Returns a reverse iterator pointing to the last character at the end of the
    // `string_view`, or `rend()` if the `string_view` is empty.
    const_reverse_iterator rbegin() const noexcept { return const_reverse_iterator(end()); }

    // string_view::rend()
    //
    // Returns a reverse iterator pointing just before the first character at the
    // beginning of the `string_view`. This pointer acts as a placeholder;
    // attempting to access its element results in undefined behavior.
    const_reverse_iterator rend() const noexcept { return const_reverse_iterator(begin()); }

    // string_view::crbegin()
    //
    // Returns a const reverse iterator pointing to the last character at the end
    // of the `string_view`, or `crend()` if the `string_view` is empty.
    const_reverse_iterator crbegin() const noexcept { return rbegin(); }

    // string_view::crend()
    //
    // Returns a const reverse iterator pointing just before the first character
    // at the beginning of the `string_view`. This pointer acts as a placeholder;
    // attempting to access its element results in undefined behavior.
    const_reverse_iterator crend() const noexcept { return rend(); }

    // Capacity Utilities

    // string_view::size()
    //
    // Returns the number of characters in the `string_view`.
    constexpr size_type size() const noexcept { return length_; }

    // string_view::length()
    //
    // Returns the number of characters in the `string_view`. Alias for `size()`.
    constexpr size_type length() const noexcept { return size(); }

    // string_view::empty()
    //
    // Checks if the `string_view` is empty (refers to no characters).
    constexpr bool empty() const noexcept { return length_ == 0; }

    // std::string:view::operator[]
    //
    // Returns the ith element of an `string_view` using the array operator.
    // Note that this operator does not perform any bounds checking.
    constexpr const_reference operator[](size_type i) const { return ptr_[i]; }

    // string_view::front()
    //
    // Returns the first element of a `string_view`.
    constexpr const_reference front() const { return ptr_[0]; }

    // string_view::back()
    //
    // Returns the last element of a `string_view`.
    constexpr const_reference back() const { return ptr_[size() - 1]; }

    // string_view::data()
    //
    // Returns a pointer to the underlying character array (which is of course
    // stored elsewhere). Note that `string_view::data()` may contain embedded nul
    // characters, but the returned buffer may or may not be nul-terminated;
    // therefore, do not pass `data()` to a routine that expects a nul-terminated
    // std::string.
    constexpr const_pointer data() const noexcept { return ptr_; }

    // Modifiers

    // string_view::remove_prefix()
    //
    // Removes the first `n` characters from the `string_view`. Note that the
    // underlying std::string is not changed, only the view.
    void remove_prefix(size_type n)
    {
        assert(n <= length_);
        ptr_ += n;
        length_ -= n;
    }

    // string_view::remove_suffix()
    //
    // Removes the last `n` characters from the `string_view`. Note that the
    // underlying std::string is not changed, only the view.
    void remove_suffix(size_type n)
    {
        assert(n <= length_);
        length_ -= n;
    }

    // string_view::swap()
    //
    // Swaps this `string_view` with another `string_view`.
    void swap(string_view &s) noexcept
    {
        auto t = *this;
        *this = s;
        s = t;
    }

    // Explicit conversion operators

    // Converts to `std::basic_string`.
    template <typename A>
    explicit operator std::basic_string<char, traits_type, A>() const
    {
        if (!data())
            return {};
        return std::basic_string<char, traits_type, A>(data(), size());
    }

    // string_view::substr()
    //
    // Returns a "substring" of the `string_view` (at offset `pos` and length
    // `n`) as another string_view. This function throws `std::out_of_bounds` if
    // `pos > size'.
    string_view substr(size_type pos, size_type n = npos) const
    {
        if (dsn_unlikely(pos > length_))
            throw std::out_of_range("absl::string_view::substr");
        n = std::min(n, length_ - pos);
        return string_view(ptr_ + pos, n);
    }

    // string_view::compare()
    //
    // Performs a lexicographical comparison between the `string_view` and
    // another `dsn::string_view), returning -1 if `this` is less than, 0 if
    // `this` is equal to, and 1 if `this` is greater than the passed std::string
    // view. Note that in the case of data equality, a further comparison is made
    // on the respective sizes of the two `string_view`s to determine which is
    // smaller, equal, or greater.
    int compare(string_view x) const noexcept
    {
        auto min_length = std::min(length_, x.length_);
        if (min_length > 0) {
            int r = std::memcmp(ptr_, x.ptr_, min_length);
            if (r < 0)
                return -1;
            if (r > 0)
                return 1;
        }
        if (length_ < x.length_)
            return -1;
        if (length_ > x.length_)
            return 1;
        return 0;
    }

    // Overload of `string_view::compare()` for comparing a substring of the
    // 'string_view` and another `absl::string_view`.
    int compare(size_type pos1, size_type count1, string_view v) const
    {
        return substr(pos1, count1).compare(v);
    }

    // Overload of `string_view::compare()` for comparing a substring of the
    // `string_view` and a substring of another `absl::string_view`.
    int
    compare(size_type pos1, size_type count1, string_view v, size_type pos2, size_type count2) const
    {
        return substr(pos1, count1).compare(v.substr(pos2, count2));
    }

    // Overload of `string_view::compare()` for comparing a `string_view` and a
    // a different  C-style std::string `s`.
    int compare(const char *s) const { return compare(string_view(s)); }

    // Overload of `string_view::compare()` for comparing a substring of the
    // `string_view` and a different std::string C-style std::string `s`.
    int compare(size_type pos1, size_type count1, const char *s) const
    {
        return substr(pos1, count1).compare(string_view(s));
    }

    // Overload of `string_view::compare()` for comparing a substring of the
    // `string_view` and a substring of a different C-style std::string `s`.
    int compare(size_type pos1, size_type count1, const char *s, size_type count2) const
    {
        return substr(pos1, count1).compare(string_view(s, count2));
    }

    // string_view::find()
    //
    // Finds the first occurrence of the substring `s` within the `string_view`,
    // returning the position of the first character's match, or `npos` if no
    // match was found.
    size_type find(string_view s, size_type pos = 0) const noexcept;

private:
    const char *ptr_;
    size_type length_;
};

// This large function is defined inline so that in a fairly common case where
// one of the arguments is a literal, the compiler can elide a lot of the
// following comparisons.
inline bool operator==(string_view x, string_view y) noexcept
{
    auto len = x.size();
    if (len != y.size()) {
        return false;
    }
    return x.data() == y.data() || len <= 0 || std::memcmp(x.data(), y.data(), len) == 0;
}

inline bool operator!=(string_view x, string_view y) noexcept { return !(x == y); }

// IO Insertion Operator
std::ostream &operator<<(std::ostream &o, string_view piece);

} // namespace dsn
