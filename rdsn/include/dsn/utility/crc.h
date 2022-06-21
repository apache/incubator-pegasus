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

#include <cstdint>
#include <cstddef>

#define CRC_INVALID 0x0

namespace dsn {
namespace utils {

uint32_t crc32_calc(const void *ptr, size_t size, uint32_t init_crc);

//
// Given
//      x_final = crc32_calc(x_ptr, x_size, x_init);
// and
//      y_final = crc32_calc(y_ptr, y_size, y_init);
// compute CRC of concatenation of A and B
//      x##y_crc = crc32_calc(x##y, x_size + y_size, xy_init);
// without touching A and B
//
uint32_t crc32_concat(uint32_t xy_init,
                      uint32_t x_init,
                      uint32_t x_final,
                      size_t x_size,
                      uint32_t y_init,
                      uint32_t y_final,
                      size_t y_size);

uint64_t crc64_calc(const void *ptr, size_t size, uint64_t init_crc);

//
// Given
//      x_final = crc64_calc(x_ptr, x_size, x_init);
// and
//      y_final = crc64_calc(y_ptr, y_size, y_init);
// compute CRC of concatenation of A and B
//      x##y_crc = crc64_calc(x##y, x_size + y_size, xy_init);
// without touching A and B
//
uint64_t crc64_concat(uint32_t xy_init,
                      uint64_t x_init,
                      uint64_t x_final,
                      size_t x_size,
                      uint64_t y_init,
                      uint64_t y_final,
                      size_t y_size);
}
}
