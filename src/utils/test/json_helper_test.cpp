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

#include <string.h>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/json_helper.h"
#include "gtest/gtest.h"
#include "utils/blob.h"

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

struct struct_type1
{
    bool b;
    std::string s;
    double d;

    bool operator==(const struct_type1 &another) const
    {
        return b == another.b && s == another.s && d == another.d;
    }
};
NON_MEMBER_JSON_SERIALIZATION(struct_type1, b, s, d)

struct struct_type2
{
    int8_t i8;
    int16_t i16;
    int32_t i32;
    int64_t i64;
    bool operator==(const struct_type2 &another) const
    {
        return i8 == another.i8 && i16 == another.i16 && i32 == another.i32 && i64 == another.i64;
    }
};
NON_MEMBER_JSON_SERIALIZATION(struct_type2, i8, i16, i32, i64)

struct struct_type3
{
    uint8_t u8;
    uint16_t u16;
    uint32_t u32;
    uint64_t u64;
    bool operator==(const struct_type3 &another) const
    {
        return u8 == another.u8 && u16 == another.u16 && u32 == another.u32 && u64 == another.u64;
    }
};
NON_MEMBER_JSON_SERIALIZATION(struct_type3, u8, u16, u32, u64)

struct nested_type
{
    std::shared_ptr<std::string> str;
    struct_type1 t1;
    std::vector<struct_type2> t2_vec;
    std::map<std::string, struct_type3> t3_map;
    std::unordered_map<int, double> t4_umap;
    std::set<uint32_t> t5_set;
    bool operator==(const nested_type &another) const
    {
        return *(str.get()) == *(another.str.get()) && t1 == another.t1 &&
               t2_vec == another.t2_vec && t3_map == another.t3_map && t4_umap == another.t4_umap &&
               t5_set == another.t5_set;
    }
};
NON_MEMBER_JSON_SERIALIZATION(nested_type, str, t1, t2_vec, t3_map, t4_umap, t5_set)

struct older_struct
{
    int a;
    std::string b;
    struct_type1 t1;
    std::map<std::string, std::string> c;
    DEFINE_JSON_SERIALIZATION(a, b, t1, c)
};

struct new_struct_type1 : struct_type1
{
    std::vector<std::string> new_vecs;
    DEFINE_JSON_SERIALIZATION(b, s, d, new_vecs)
};

struct newer_struct
{
    int a;
    std::string b;
    new_struct_type1 t1;
    std::map<std::string, std::string> c;
    std::vector<int> d;
    std::map<int, int> e;
    DEFINE_JSON_SERIALIZATION(a, b, t1, c, d, e)
};

// This test verifies that json_forwarder can correctly encode an object with private
// and const fields.
TEST(json_helper, encode_and_decode)
{
    test_entity entity;
    // ensures that `entity` doesn't equal to the default value of `decoded_entity`
    entity.field = 5;

    blob encoded_entity = dsn::json::json_forwarder<test_entity>::encode(entity);

    test_entity decoded_entity;
    dsn::json::json_forwarder<test_entity>::decode(encoded_entity, decoded_entity);

    ASSERT_EQ(entity, decoded_entity);
}

TEST(json_helper, simple_type_encode_decode)
{
    struct_type1 t1_in, t1_out;
    t1_in.b = true;
    t1_in.d = -0.00;
    t1_in.s = "hahaha";

    t1_out.b = false;
    t1_out.d = -0.0;
    t1_out.s = "";

    dsn::blob bb = dsn::json::json_forwarder<struct_type1>::encode(t1_in);
    t1_in.s = "";
    ASSERT_TRUE(dsn::json::json_forwarder<struct_type1>::decode(bb, t1_out));

    ASSERT_EQ(t1_in.b, t1_out.b);
    ASSERT_EQ(t1_in.d, t1_out.d);
    ASSERT_EQ("hahaha", t1_out.s);

    t1_in.b = false;
    t1_in.d = 99.999;
    t1_in.s = "";

    bb = dsn::json::json_forwarder<struct_type1>::encode(t1_in);
    ASSERT_TRUE(dsn::json::json_forwarder<struct_type1>::decode(bb, t1_out));

    ASSERT_EQ(t1_in, t1_out);

    t1_in.s = "string with escape: \\\\, \\\"";
    bb = dsn::json::json_forwarder<struct_type1>::encode(t1_in);
    ASSERT_TRUE(dsn::json::json_forwarder<struct_type1>::decode(bb, t1_out));
    std::cout << bb.data() << std::endl;
    ASSERT_EQ(t1_in.s, t1_out.s);
}

TEST(json_helper, int_type_encode_decode)
{
    struct_type2 t_in, t_out;
    t_in.i8 = 0x80;
    t_in.i16 = 0x8000;
    t_in.i32 = 0x80000000;
    t_in.i64 = 0x8000000000000000;

    t_out.i8 = 0;
    t_out.i16 = 0;
    t_out.i32 = 0;
    t_out.i64 = 0;

    dsn::blob bb = dsn::json::json_forwarder<struct_type2>::encode(t_in);
    ASSERT_TRUE(dsn::json::json_forwarder<struct_type2>::decode(bb, t_out));

    ASSERT_EQ(t_in, t_out);

    t_in.i8 = 0x7f;
    t_in.i16 = 0x7fff;
    t_in.i32 = 0x7fffffff;
    t_in.i64 = 0x7fffffffffffffff;

    bb = dsn::json::json_forwarder<struct_type2>::encode(t_in);
    ASSERT_TRUE(dsn::json::json_forwarder<struct_type2>::decode(bb, t_out));

    ASSERT_EQ(t_in, t_out);
}

TEST(json_helper, int_overflow_underflow)
{
    const char *abnormal_targets[] = {
        "{\"i8\":128,\"i16\":32767,\"i32\":2147483647,\"i64\":9223372036854775807}",
        "{\"i8\":127,\"i16\":32768,\"i32\":2147483647,\"i64\":9223372036854775807}",
        "{\"i8\":127,\"i16\":32767,\"i32\":2147483648,\"i64\":9223372036854775807}",
        "{\"i8\":127,\"i16\":32767,\"i32\":2147483647,\"i64\":9223372036854775808}",
        "{\"i8\":-129,\"i16\":-32768,\"i32\":-2147483648,\"i64\":-9223372036854775808}",
        "{\"i8\":-128,\"i16\":-32769,\"i32\":-2147483648,\"i64\":-9223372036854775808}",
        "{\"i8\":-128,\"i16\":-32768,\"i32\":-2147483649,\"i64\":-9223372036854775808}",
        "{\"i8\":-128,\"i16\":-32768,\"i32\":-2147483648,\"i64\":-9223372036854775809}"};

    struct_type2 t;

    for (int i = 0; i < 8; ++i) {
        dsn::blob bb(abnormal_targets[i], 0, strlen(abnormal_targets[i]));
        bool result = dsn::json::json_forwarder<struct_type2>::decode(bb, t);
        ASSERT_FALSE(result);
    }

    const char *normal_targets[] = {
        "{\"i8\":-128,\"i16\":-32768,\"i32\":-2147483648,\"i64\":-9223372036854775808}",
        "{\"i8\":127,\"i16\":32767,\"i32\":2147483647,\"i64\":9223372036854775807}"};

    struct_type2 normal_results[2];
    normal_results[0].i8 = 0x80;
    normal_results[0].i16 = 0x8000;
    normal_results[0].i32 = 0x80000000;
    normal_results[0].i64 = 0x8000000000000000;

    normal_results[1].i8 = 0x7f;
    normal_results[1].i16 = 0x7fff;
    normal_results[1].i32 = 0x7fffffff;
    normal_results[1].i64 = 0x7fffffffffffffff;

    for (int i = 0; i < 2; ++i) {
        dsn::blob bb(normal_targets[i], 0, strlen(normal_targets[i]));
        bool result = dsn::json::json_forwarder<struct_type2>::decode(bb, t);
        ASSERT_TRUE(result);

        ASSERT_EQ(normal_results[i], t);
    }
}

TEST(json_helper, uint_encode_decode)
{
    struct_type3 t_in, t_out;
    t_in.u8 = 0xff;
    t_in.u16 = 0xffff;
    t_in.u32 = 0xffffffff;
    t_in.u64 = 0xffffffffffffffff;

    dsn::blob bb = dsn::json::json_forwarder<struct_type3>::encode(t_in);

    t_out.u8 = 0;
    t_out.u16 = 0;
    t_out.u32 = 0;
    t_out.u64 = 0;
    dsn::json::json_forwarder<struct_type3>::decode(bb, t_out);

    ASSERT_EQ(t_in, t_out);
}

TEST(json_helper, uint_overflow_underflow)
{
    const char *abnormal_cases[] = {
        "{\"u8\":256,\"u16\":65535,\"u32\":4294967295,\"u64\":18446744073709551615}",
        "{\"u8\":255,\"u16\":65536,\"u32\":4294967295,\"u64\":18446744073709551615}",
        "{\"u8\":255,\"u16\":65535,\"u32\":4294967296,\"u64\":18446744073709551615}",
        "{\"u8\":255,\"u16\":65535,\"u32\":4294967295,\"u64\":18446744073709551616}",
        "{\"u8\":-1,\"u16\":65535,\"u32\":4294967295,\"u64\":18446744073709551615}",
        "{\"u8\":255,\"u16\":-1,\"u32\":4294967295,\"u64\":18446744073709551615}",
        "{\"u8\":255,\"u16\":65535,\"u32\":-1,\"u64\":18446744073709551615}",
        "{\"u8\":255,\"u16\":65535,\"u32\":4294967295,\"u64\":-1}",
    };

    struct_type3 t;
    for (int i = 0; i < 8; ++i) {
        dsn::blob bb(abnormal_cases[i], 0, strlen(abnormal_cases[i]));
        bool ans = dsn::json::json_forwarder<struct_type3>::decode(bb, t);
        ASSERT_FALSE(ans);
    }

    const char *normal_cases[] = {
        "{\"u8\":255,\"u16\":65535,\"u32\":4294967295,\"u64\":18446744073709551615}",
        "{\"u8\":0,\"u16\":0,\"u32\":0,\"u64\":0}",
    };

    struct_type3 normal_results[2];
    normal_results[0].u8 = 0xff;
    normal_results[0].u16 = 0xffff;
    normal_results[0].u32 = 0xffffffff;
    normal_results[0].u64 = 0xffffffffffffffff;

    normal_results[1].u8 = 0;
    normal_results[1].u16 = 0;
    normal_results[1].u32 = 0;
    normal_results[1].u64 = 0;

    for (int i = 0; i < 2; ++i) {
        dsn::blob bb(normal_cases[i], 0, strlen(normal_cases[i]));
        bool result = dsn::json::json_forwarder<struct_type3>::decode(bb, t);
        ASSERT_TRUE(result);

        ASSERT_EQ(normal_results[i], t);
    }
}

TEST(json_helper, nested_type_encode_decode)
{
    nested_type nt;
    nt.str = std::make_shared<std::string>("this is a shared ptr string");
    nt.t1 = struct_type1{false, "simple", 99.99999};
    nt.t2_vec = {struct_type2{2, 4, 6, 8}, struct_type2{-3, -5, -7, -9}};
    nt.t3_map = {{"string1", struct_type3{1, 3, 4, 6}}, {"string2", struct_type3{0, 0, 0, 0}}};
    nt.t4_umap = {{123, 0.23333354}, {-123, 99.99999}};
    nt.t5_set = {1, 3, 5, 7, 9};

    dsn::blob bb = dsn::json::json_forwarder<decltype(nt)>::encode(nt);

    nested_type nt2;
    dsn::json::json_forwarder<decltype(nt)>::decode(bb, nt2);

    ASSERT_EQ(nt, nt2);
}

TEST(json_helper, decode_invalid_json)
{
    // decode from invalid json
    const char *json[] = {
        "{\"a\":1,\"b\":\"hehe\",\"c\":{\"aa\":\"bb\",\"cc\":\"dd\"},\"c\":[1,3,4],"
        "\"d\":{\"1\":4,\"2\":3},\"hehe\"}",
        "{\"a\":[]}",
        "{\"c\":1}",
        "{\"a\":1,\"xxx\":\"hehe\"}",
        "{\"a\":1,\"b\":\"hehe\"",
        "{\"a\":1,\"b\":\"hehe\",",
        "{\"a\":1,\"b\":\"hehe\",}",
        "{\"a\":1,\"b\":\"hehe\",{\"x\":\"y\"}}}",
        nullptr};

    older_struct o;
    for (int i = 0; json[i]; ++i) {
        dsn::blob in(json[i], 0, strlen(json[i]));
        ASSERT_FALSE(dsn::json::json_forwarder<older_struct>::decode(in, o));
    }
}

TEST(json_helper, type_mismatch)
{
    struct_type1 t1;
    const char *type_mismatch1[] = {
        "{\"b\":\"heheda\",\"s\":\"heheda\",\"d\":12.345}",
        "{\"b\":1,\"s\":[1, 2, 3],\"d\":12.345}",
        "{\"b\":1,\"s\":\"heheda\",\"d\":12.3.4.5}",
        "{\"b\":1,\"s\":\"heheda\",\"d\":{}}",
        "{\"b\":t,\"s\":\"heheda\",\"d\":{}}",
        "{\"b\":1,\"s\":\"heheda\",\"d\":12.345}",
        "{\"b\":1,\"s\":\"heheda\",\"d\":2345}",
        "{\"b\":1,\"s\":\"heheda\",\"d\":-0}",
        "{\"b\":true,\"s\":\"heheda\",\"d\":-0}",
        "{\"b\":false,\"s\":\"heheda\",\"d\":-0}",
        nullptr,
    };
    bool expected1[] = {false, false, false, false, false, true, true, true, true, true};
    for (int i = 0; type_mismatch1[i]; ++i) {
        dsn::blob bb(type_mismatch1[i], 0, strlen(type_mismatch1[i]));
        bool result = dsn::json::json_forwarder<struct_type1>::decode(bb, t1);
        ASSERT_EQ(expected1[i], result) << "case " << i << " failed: " << type_mismatch1[i];
    }

    struct_type2 t2;
    const char *int_mismatch[] = {
        "{\"i8\":\"aa\",\"i16\":32767,\"i32\":2147483647,\"i64\":9223372036854775807}",
        "{\"i8\":127,\"i16\":[1, 3, 5],\"i32\":2147483647,\"i64\":9223372036854775807}",
        "{\"i8\":127,\"i16\":32767,\"i32\":{},\"i64\":9223372036854775807}",
        "{\"i8\":127,\"i16\":32767,\"i32\":2147483647,\"i64\":0.256}",
        "{\"i8\":127,\"i16\":32767,\"i32\":2147483647,\"i64\":0}",
        nullptr,
    };
    bool expected[] = {false, false, false, false, true};
    for (int i = 0; int_mismatch[i]; ++i) {
        dsn::blob bb(int_mismatch[i], 0, strlen(int_mismatch[i]));
        bool result = dsn::json::json_forwarder<struct_type2>::decode(bb, t2);
        ASSERT_EQ(expected[i], result) << "case " << i << " failed";
    }

    struct_type3 t3;
    const char *uint_mismatch[] = {
        "{\"u8\":[],\"u16\":65535,\"u32\":4294967295,\"u64\":18446744073709551615}",
        "{\"u8\":255,\"u16\":{\"a\":1, \"b\":2},\"u32\":4294967295,\"u64\":18446744073709551615}",
        "{\"u8\":255,\"u16\":65535,\"u32\":23.456,\"u64\":18446744073709551615}",
        "{\"u8\":255,\"u16\":65535,\"u32\":4294967295,\"u64\":\"hehe\"}",
        "{\"u8\":255,\"u16\":65535,\"u32\":4294967295,\"u64\":18446744073709551615}",
        nullptr,
    };
    bool expected_uint[] = {false, false, false, false, true};
    for (int i = 0; uint_mismatch[i]; ++i) {
        dsn::blob bb(uint_mismatch[i], 0, strlen(uint_mismatch[i]));
        bool result = dsn::json::json_forwarder<struct_type3>::decode(bb, t3);
        ASSERT_EQ(expected_uint[i], result);
    }

    nested_type nt;
    const char *nt_mismatch[] = {
        /// str is not string
        "{\"str\":12,\"t1\":{\"b\":232,\"s\":\"\",\"d\":3.26e-322},\"t2_vec\":[],\"t3_map\":{},"
        "\"t4_umap\":{},\"t5_set\":[]}",
        /// t1 is not object
        "{\"str\":\"a\",\"t1\":[],\"t2_vec\":[],\"t3_map\":{},\"t4_umap\":{},\"t5_set\":[]}",
        /// t2 is not vector
        "{\"str\":\"a\",\"t1\":{\"b\":232,\"s\":\"\",\"d\":3.26e-322},\"t2_vec\":\"heheda\",\"t3_"
        "map\":{},\"t4_umap\":{},\"t5_set\":[]}",
        /// t3 is not map
        "{\"str\":\"a\",\"t1\":{\"b\":232,\"s\":\"\",\"d\":3.26e-322},\"t2_vec\":[],\"t3_map\":[],"
        "\"t4_umap\":{},\"t5_set\":[]}",
        /// t4 is not unordered_map
        "{\"str\":\"a\",\"t1\":{\"b\":232,\"s\":\"\",\"d\":3.26e-322},\"t2_vec\":[],\"t3_map\":{},"
        "\"t4_umap\":234.5,\"t5_set\":[]}",
        /// t5 is not set
        "{\"str\":\"a\",\"t1\":{\"b\":232,\"s\":\"\",\"d\":3.26e-322},\"t2_vec\":[],\"t3_map\":{},"
        "\"t4_umap\":{},\"t5_set\":1}",
        /// t1.s is not string
        "{\"str\":\"a\",\"t1\":{\"b\":232,\"s\":12,\"d\":3.26e-322},\"t2_vec\":[],\"t3_map\":{},"
        "\"t4_umap\":{},\"t5_set\":[]}",
        /// t2_vec is vector of integer
        "{\"str\":\"a\",\"t1\":{\"b\":232,\"s\":\"\",\"d\":3.26e-322},\"t2_vec\":[1, 3, "
        "5],\"t3_map\":{},\"t4_umap\":{},\"t5_set\":[]}",
        /// normal case
        "{\"str\":\"a\",\"t1\":{\"b\":232,\"s\":\"\",\"d\":3.26e-322},\"t2_vec\":[],\"t3_map\":{},"
        "\"t4_umap\":{},\"t5_set\":[]}",
        nullptr,
    };
    bool expected_nt[] = {false, false, false, false, false, false, false, false, true};
    for (int i = 0; nt_mismatch[i]; ++i) {
        dsn::blob bb(nt_mismatch[i], 0, strlen(nt_mismatch[i]));
        bool result = dsn::json::json_forwarder<nested_type>::decode(bb, nt);
        ASSERT_EQ(expected_nt[i], result) << "case " << i << " failed";
    }
}

TEST(json_helper, upgrade_downgrade)
{
    // newer version of json can be decoded with older version of struct
    newer_struct n;
    n.a = 1;
    n.b = "hehe";
    n.t1.b = false;
    n.t1.d = 23.445;
    n.t1.new_vecs = {"t1", "t2", "t3"};
    n.t1.s = "haha";
    n.c = {{"aa", "bb"}, {"cc", "dd"}};
    n.d = {1, 3, 4};
    n.e = {{1, 4}, {2, 3}};
    blob bb = dsn::json::json_forwarder<newer_struct>::encode(n);

    older_struct o;
    o.a = -1;
    o.b = "xixi";
    bool result = dsn::json::json_forwarder<older_struct>::decode(bb, o);
    ASSERT_TRUE(result);

    ASSERT_EQ(n.a, o.a);
    ASSERT_EQ(n.b, o.b);
    ASSERT_EQ(n.t1.b, o.t1.b);
    ASSERT_EQ(n.t1.d, o.t1.d);
    ASSERT_EQ(n.t1.s, o.t1.s);
    ASSERT_EQ(n.c, o.c);

    // older version of json can be decoded by newer version of struct
    newer_struct n2;
    blob bb2 = dsn::json::json_forwarder<older_struct>::encode(o);
    result = dsn::json::json_forwarder<newer_struct>::decode(bb2, n2);
    ASSERT_TRUE(result);

    ASSERT_EQ(n2.a, o.a);
    ASSERT_EQ(n2.b, o.b);
    ASSERT_EQ(n2.t1.b, o.t1.b);
    ASSERT_EQ(n2.t1.d, o.t1.d);
    ASSERT_EQ(n2.t1.s, o.t1.s);
    ASSERT_EQ(n2.c, o.c);
}

} // namespace dsn
