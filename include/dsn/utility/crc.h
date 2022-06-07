/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
