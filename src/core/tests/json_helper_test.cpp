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
    DEFINE_JSON_SERIALIZATION(field, const_field, private_field, private_const_field)

    bool operator==(const test_entity &rhs) const
    {
        return field == rhs.field && const_field == rhs.const_field &&
               private_field == rhs.private_field && private_const_field == rhs.private_const_field;
    }
};

struct older_struct
{
    int a;
    std::string b;
    std::map<std::string, std::string> c;
    DEFINE_JSON_SERIALIZATION(a, b, c)
};

struct newer_struct
{
    int a;
    std::string b;
    std::map<std::string, std::string> c;
    std::vector<int> d;
    std::map<int, int> e;
    DEFINE_JSON_SERIALIZATION(a, b, c, d, e)
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

TEST(json_helper, struct_add_fields)
{
    // newer version of json cann't be decoded with older version of struct
    newer_struct n;
    n.a = 1;
    n.b = "hehe";
    n.c = {{"aa", "bb"}, {"cc", "dd"}};
    n.d = {1, 3, 4};
    n.e = {{1, 4}, {2, 3}};
    blob bb = dsn::json::json_forwarder<newer_struct>::encode(n);

    older_struct o;
    o.a = -1;
    o.b = "xixi";
    dsn::json::string_tokenizer tok(bb.data(), 0, bb.length());
    bool result = o.decode_json_state(tok);
    ASSERT_FALSE(result);
}

TEST(json_helper, decode_invalid_json)
{
    // decode from invalid json
    const char *json[] = {
        "{\"a\":1,\"b\":\"hehe\",\"c\":{\"aa\":\"bb\",\"cc\":\"dd\"},\"c\":[1,3,4],"
        "\"d\":{\"1\":4,\"2\":3},\"hehe\"}",
        "{\"c\":1}",
        "{\"a\":1,\"xxx\":\"hehe\"}",
        "{\"a\":1,\"b\":\"hehe\"",
        "{\"a\":1,\"b\":\"hehe\",",
        "{\"a\":1,\"b\":\"hehe\",}",
        "{\"a\":1,\"b\":\"hehe\",{\"x\":\"y\"}}}",
        nullptr};

    older_struct o;
    for (int i = 0; json[i]; ++i) {
        dsn::json::string_tokenizer in(json[i], 0, strlen(json[i]));
        ASSERT_FALSE(o.decode_json_state(in));
    }
}

} // namespace dsn
