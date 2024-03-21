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

#include <string.h>
#include <string>

#include "gtest/gtest.h"
#include "runtime/rpc/message_parser.h"
#include "utils/blob.h"

namespace dsn {

class message_reader_test : public testing::Test
{
public:
    void test_init()
    {
        message_reader reader(4096);
        ASSERT_EQ(reader._buffer_block_size, 4096);
        ASSERT_EQ(reader._buffer_occupied, 0);
        ASSERT_EQ(reader._buffer.length(), 0);
    }

    void test_read_buffer()
    {
        message_reader reader(4096);

        const char *p1 = reader.read_buffer_ptr(10);
        ASSERT_EQ(reader._buffer_occupied, 0);
        ASSERT_EQ(reader._buffer.length(), 4096);
        reader.mark_read(10);
        ASSERT_EQ(reader._buffer_occupied, 10);

        const char *p2 = reader.read_buffer_ptr(10);
        ASSERT_EQ(reader._buffer_occupied, 10);
        ASSERT_EQ(reader._buffer.length(), 4096);
        reader.mark_read(10);
        ASSERT_EQ(reader._buffer_occupied, 20);
        ASSERT_EQ(p2 - p1, 10); // p1, p2 reside on the same allocated memory buffer.

        reader.read_buffer_ptr(4076);
        ASSERT_EQ(reader._buffer_occupied, 20);
        ASSERT_EQ(reader._buffer.length(), 4096);
        reader.mark_read(4076);
        ASSERT_EQ(reader._buffer_occupied, 4096);

        // buffer capacity extends
        p1 = reader.read_buffer_ptr(1);
        ASSERT_EQ(reader._buffer_occupied, 4096);
        ASSERT_EQ(reader._buffer.length(), 4097);
        reader.mark_read(1);
        ASSERT_EQ(reader._buffer_occupied, 4097);

        // if buffer is not consumed in time,
        // each read will cause one data copy
        p2 = reader.read_buffer_ptr(3);
        reader.mark_read(3);
        ASSERT_EQ(reader._buffer.length(), 4100);
        ASSERT_EQ(reader._buffer_occupied, 4100);
        ASSERT_NE(p2 - p1, 3);
    }

    void test_read_data()
    {
        message_reader reader(4096);

        std::string data = std::string("THFT") + std::string(44, '\0'); // 48 bytes
        data[7] = data[9] = '\1';

        char *buf = reader.read_buffer_ptr(data.length());
        memcpy(buf, data.data(), data.size());
        reader.mark_read(data.length());
        ASSERT_EQ(reader.buffer().size(), data.length());
        ASSERT_EQ(reader.buffer().to_string(), data);
    }

    void test_consume_buffer()
    {
        message_reader reader(5000);

        reader.read_buffer_ptr(1000);
        reader.mark_read(1000);
        ASSERT_EQ(reader._buffer_occupied, 1000);
        ASSERT_EQ(reader._buffer.length(), 5000);
        ASSERT_EQ(reader.buffer().size(), 1000);

        reader.consume_buffer(500);
        ASSERT_EQ(reader._buffer.length(), 4500);
        ASSERT_EQ(reader._buffer_occupied, 500);
    }
};

TEST_F(message_reader_test, init) { test_init(); }

TEST_F(message_reader_test, read_buffer) { test_read_buffer(); }

TEST_F(message_reader_test, read_data) { test_read_data(); }

TEST_F(message_reader_test, consume_buffer) { test_consume_buffer(); }

} // namespace dsn
