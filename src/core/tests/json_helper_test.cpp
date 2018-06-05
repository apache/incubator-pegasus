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

#include <gtest/gtest.h>
#include <dsn/cpp/json_helper.h>

namespace dsn {

class test_entity
{
public:
    int field = 1;
    const int const_field = 2;

private:
    int private_field = 3;
    const int private_const_field = 4;

public:
    DEFINE_JSON_SERIALIZATION(field, const_field, private_field, private_const_field);

    bool operator==(const test_entity &rhs) const
    {
        return field == rhs.field && const_field == rhs.const_field &&
               private_field == rhs.private_field && private_const_field == rhs.private_const_field;
    }
};

// This test verifies that json_forwarder can correctly encode an object with private
// and const fields.
TEST(json_helper, encode_and_decode)
{
    test_entity entity;
    entity.field =
        5; // ensures that `entity` doesn't equal to the default value of `decoded_entity`

    blob encoded_entity = dsn::json::json_forwarder<test_entity>::encode(entity);

    test_entity decoded_entity;
    dsn::json::json_forwarder<test_entity>::decode(encoded_entity, decoded_entity);

    ASSERT_EQ(entity, decoded_entity);
}

} // namespace dsn
