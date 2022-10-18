// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <stdlib.h>
#include <stdint.h>

namespace dsn {

enum empty_field_action
{
    SKIP_EMPTY_FIELD,
    ALLOW_EMPTY_FIELD
};

// Split a string with one character
class string_splitter
{
public:
    // Split `input' with `separator'. If `action' is SKIP_EMPTY_FIELD, zero-
    // length() field() will be skipped.
    inline string_splitter(const char *input,
                           char separator,
                           empty_field_action action = SKIP_EMPTY_FIELD);

    // Allows containing embedded '\0' characters and separator can be '\0',
    // if str_end is not NULL.
    inline string_splitter(const char *str_begin,
                           const char *str_end,
                           char separator,
                           empty_field_action = SKIP_EMPTY_FIELD);

    // Move splitter forward.
    inline string_splitter &operator++();

    inline string_splitter operator++(int);

    // True iff field() is valid.
    inline operator const void *() const;

    // Beginning address and length of the field. *(field() + length()) may
    // not be '\0' because we don't modify `input'.
    inline const char *field() const;

    inline size_t length() const;

    // Cast field to specific type, and write the value into `pv'.
    // Returns 0 on success, -1 otherwise.
    // NOTE: If separator is a digit, casting functions always return -1.
    inline int to_int8(int8_t *pv) const;

    inline int to_uint8(uint8_t *pv) const;

    inline int to_int(int *pv) const;

    inline int to_uint(unsigned int *pv) const;

    inline int to_long(long *pv) const;

    inline int to_ulong(unsigned long *pv) const;

    inline int to_longlong(long long *pv) const;

    inline int to_ulonglong(unsigned long long *pv) const;

    inline int to_float(float *pv) const;

    inline int to_double(double *pv) const;

private:
    inline bool not_end(const char *p) const;

    inline void init();

    const char *_head;
    const char *_tail;
    const char *_str_tail;
    const char _sep;
    const empty_field_action _empty_field_action;
};

string_splitter::string_splitter(const char *str, char sep, empty_field_action action)
    : _head(str), _str_tail(NULL), _sep(sep), _empty_field_action(action)
{
    init();
}

string_splitter::string_splitter(const char *str_begin,
                                 const char *str_end,
                                 const char sep,
                                 empty_field_action action)
    : _head(str_begin), _str_tail(str_end), _sep(sep), _empty_field_action(action)
{
    init();
}

void string_splitter::init()
{
    // Find the starting _head and _tail.
    if (__builtin_expect(_head != NULL, 1)) {
        if (_empty_field_action == SKIP_EMPTY_FIELD) {
            for (; _sep == *_head && not_end(_head); ++_head) {
            }
        }
        for (_tail = _head; *_tail != _sep && not_end(_tail); ++_tail) {
        }
    } else {
        _tail = NULL;
    }
}

string_splitter &string_splitter::operator++()
{
    if (__builtin_expect(_tail != NULL, 1)) {
        if (not_end(_tail)) {
            ++_tail;
            if (_empty_field_action == SKIP_EMPTY_FIELD) {
                for (; _sep == *_tail && not_end(_tail); ++_tail) {
                }
            }
        }
        _head = _tail;
        for (; *_tail != _sep && not_end(_tail); ++_tail) {
        }
    }
    return *this;
}

string_splitter string_splitter::operator++(int)
{
    string_splitter tmp = *this;
    operator++();
    return tmp;
}

string_splitter::operator const void *() const
{
    return (_head != NULL && not_end(_head)) ? _head : NULL;
}

const char *string_splitter::field() const { return _head; }

size_t string_splitter::length() const { return static_cast<size_t>(_tail - _head); }

bool string_splitter::not_end(const char *p) const
{
    return (_str_tail == NULL) ? *p : (p != _str_tail);
}

int string_splitter::to_int8(int8_t *pv) const
{
    long v = 0;
    if (to_long(&v) == 0 && v >= -128 && v <= 127) {
        *pv = (int8_t)v;
        return 0;
    }
    return -1;
}

int string_splitter::to_uint8(uint8_t *pv) const
{
    unsigned long v = 0;
    if (to_ulong(&v) == 0 && v <= 255) {
        *pv = (uint8_t)v;
        return 0;
    }
    return -1;
}

int string_splitter::to_int(int *pv) const
{
    long v = 0;
    if (to_long(&v) == 0 && v >= INT_MIN && v <= INT_MAX) {
        *pv = (int)v;
        return 0;
    }
    return -1;
}

int string_splitter::to_uint(unsigned int *pv) const
{
    unsigned long v = 0;
    if (to_ulong(&v) == 0 && v <= UINT_MAX) {
        *pv = (unsigned int)v;
        return 0;
    }
    return -1;
}

int string_splitter::to_long(long *pv) const
{
    char *endptr = NULL;
    *pv = strtol(field(), &endptr, 10);
    return (endptr == field() + length()) ? 0 : -1;
}

int string_splitter::to_ulong(unsigned long *pv) const
{
    char *endptr = NULL;
    *pv = strtoul(field(), &endptr, 10);
    return (endptr == field() + length()) ? 0 : -1;
}

int string_splitter::to_longlong(long long *pv) const
{
    char *endptr = NULL;
    *pv = strtoll(field(), &endptr, 10);
    return (endptr == field() + length()) ? 0 : -1;
}

int string_splitter::to_ulonglong(unsigned long long *pv) const
{
    char *endptr = NULL;
    *pv = strtoull(field(), &endptr, 10);
    return (endptr == field() + length()) ? 0 : -1;
}

int string_splitter::to_float(float *pv) const
{
    char *endptr = NULL;
    *pv = strtof(field(), &endptr);
    return (endptr == field() + length()) ? 0 : -1;
}

int string_splitter::to_double(double *pv) const
{
    char *endptr = NULL;
    *pv = strtod(field(), &endptr);
    return (endptr == field() + length()) ? 0 : -1;
}

} // namespace dsn
