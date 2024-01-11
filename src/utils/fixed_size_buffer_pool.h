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

#pragma once

///
/// A simple buffer pool designed for efficiently formatting
/// frequently used types (like gpid, rpc_address) into string,
/// without dynamic memory allocation.
///
/// It's not suitable to be used in multi-threaded environment,
/// unless when it's declared as thread local.
///
/// \see dsn_address_to_string
/// \see dsn::gpid::to_string
///
template <unsigned int PoolCapacity, unsigned int ChunkSize>
class fixed_size_buffer_pool
{
private:
    char buffer[PoolCapacity][ChunkSize];
    unsigned int index;

public:
    constexpr unsigned int get_chunk_size() const { return ChunkSize; }
    char *next()
    {
        // we must update index first, coz the index may be uninitialized
        // the reason we don't initialize the buffer/index in constructor
        // is that the round_buffer may be declared as thread_local variable
        index = (index + 1) % PoolCapacity;
        return buffer[index];
    }
};
