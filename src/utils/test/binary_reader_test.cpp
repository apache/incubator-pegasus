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

#include "utils/binary_reader.h"

#include "gtest/gtest.h"
#include "utils/defer.h"

namespace dsn {

TEST(binary_reader_test, inner_read)
{

    {
        blob input = blob::create_from_bytes(std::string("test10086"));
        binary_reader reader(input);

        blob output;
        int size = 4;
        auto res = reader.inner_read(output, size);
        ASSERT_EQ(res, size + sizeof(size));
        ASSERT_EQ(output.to_string(), "test");
    }

    {
        blob input = blob::create_from_bytes(std::string("test10086"));
        binary_reader reader(input);

        blob output;
        int size = 10;
        auto res = reader.inner_read(output, size);
        ASSERT_EQ(res, -1);
    }

    {

        blob input = blob::create_from_bytes(std::string("test10086"));
        binary_reader reader(input);

        int size = 4;
        char *output_str = new char[size + 1];
        auto cleanup = dsn::defer([&output_str]() { delete[] output_str; });
        auto res = reader.inner_read(output_str, size);
        output_str[size] = '\0';
        ASSERT_EQ(res, size);
        ASSERT_EQ(std::string(output_str), "test");
    }

    {
        blob input = blob::create_from_bytes(std::string("test10086"));
        binary_reader reader(input);

        int size = 10;
        char *output_str = new char[size];
        auto cleanup = dsn::defer([&output_str]() { delete[] output_str; });
        auto res = reader.inner_read(output_str, size);
        ASSERT_EQ(res, -1);
    }
}
} // namespace dsn
