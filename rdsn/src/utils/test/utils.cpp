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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/utility/utils.h>
#include <dsn/utility/strings.h>
#include <dsn/utility/binary_reader.h>
#include <dsn/utility/binary_writer.h>
#include <dsn/utility/link.h>
#include <dsn/utility/crc.h>
#include <dsn/utility/autoref_ptr.h>
#include <dsn/c/api_layer1.h>
#include <gtest/gtest.h>
#include <dsn/utility/rand.h>

using namespace ::dsn;
using namespace ::dsn::utils;

TEST(core, get_last_component)
{
    ASSERT_EQ("a", get_last_component("a", "/"));
    ASSERT_EQ("b", get_last_component("a/b", "/"));
    ASSERT_EQ("b", get_last_component("a//b", "/"));
    ASSERT_EQ("", get_last_component("a/", "/"));
    ASSERT_EQ("c", get_last_component("a/b_c", "/_"));
}

TEST(core, crc)
{
    char buffer[24];
    for (int i = 0; i < sizeof(buffer) / sizeof(char); i++) {
        buffer[i] = rand::next_u32(0, 200);
    }

    auto c1 = dsn::utils::crc32_calc(buffer, 12, 0);
    auto c2 = dsn::utils::crc32_calc(buffer + 12, 12, c1);
    auto c3 = dsn::utils::crc32_calc(buffer, 24, 0);
    auto c4 = dsn::utils::crc32_concat(0, 0, c1, 12, c1, c2, 12);
    EXPECT_TRUE(c3 == c4);
}

TEST(core, binary_io)
{
    int value = 0xdeadbeef;
    binary_writer writer;
    writer.write(value);

    auto buf = writer.get_buffer();
    binary_reader reader(buf);
    int value3;
    reader.read(value3);

    EXPECT_TRUE(value3 == value);
}

TEST(core, split_args)
{
    std::string value = "a ,b, c ";
    std::vector<std::string> sargs;
    std::list<std::string> sargs2;
    ::dsn::utils::split_args(value.c_str(), sargs, ',');
    ::dsn::utils::split_args(value.c_str(), sargs2, ',');

    EXPECT_EQ(sargs.size(), 3);
    EXPECT_EQ(sargs[0], "a");
    EXPECT_EQ(sargs[1], "b");
    EXPECT_EQ(sargs[2], "c");

    EXPECT_EQ(sargs2.size(), 3);
    auto it = sargs2.begin();
    EXPECT_EQ(*it++, "a");
    EXPECT_EQ(*it++, "b");
    EXPECT_EQ(*it++, "c");

    std::unordered_set<std::string> sargs_set;
    dsn::utils::split_args(value.c_str(), sargs_set, ',');
    EXPECT_EQ(sargs_set.size(), 3);

    // test value = ""
    value = "";
    sargs.clear();
    dsn::utils::split_args(value.c_str(), sargs, ',');
    EXPECT_EQ(sargs.size(), 0);

    sargs2.clear();
    dsn::utils::split_args(value.c_str(), sargs2, ',');
    EXPECT_EQ(sargs2.size(), 0);

    sargs_set.clear();
    dsn::utils::split_args(value.c_str(), sargs_set, ',');
    EXPECT_EQ(sargs_set.size(), 0);
}

TEST(core, split_args_keep_place_holder)
{
    std::string value = "a ,b, c ";
    std::vector<std::string> sargs;
    ::dsn::utils::split_args(value.c_str(), sargs, ',', true);

    EXPECT_EQ(sargs.size(), 3);
    EXPECT_EQ(sargs[0], "a");
    EXPECT_EQ(sargs[1], "b");
    EXPECT_EQ(sargs[2], "c");

    value = " ,  a ,b, c ";
    sargs.clear();
    ::dsn::utils::split_args(value.c_str(), sargs, ',', true);

    EXPECT_EQ(sargs.size(), 4);
    EXPECT_EQ(sargs[0], "");
    EXPECT_EQ(sargs[1], "a");
    EXPECT_EQ(sargs[2], "b");
    EXPECT_EQ(sargs[3], "c");

    value = "a ,b, , c";
    sargs.clear();
    ::dsn::utils::split_args(value.c_str(), sargs, ',', true);

    EXPECT_EQ(sargs.size(), 4);
    EXPECT_EQ(sargs[0], "a");
    EXPECT_EQ(sargs[1], "b");
    EXPECT_EQ(sargs[2], "");
    EXPECT_EQ(sargs[3], "c");

    value = "a ,b, c , ";
    sargs.clear();
    ::dsn::utils::split_args(value.c_str(), sargs, ',', true);

    EXPECT_EQ(sargs.size(), 4);
    EXPECT_EQ(sargs[0], "a");
    EXPECT_EQ(sargs[1], "b");
    EXPECT_EQ(sargs[2], "c");
    EXPECT_EQ(sargs[3], "");

    value = ", a ,b, ,c , ";
    sargs.clear();
    ::dsn::utils::split_args(value.c_str(), sargs, ',', true);

    EXPECT_EQ(sargs.size(), 6);
    EXPECT_EQ(sargs[0], "");
    EXPECT_EQ(sargs[1], "a");
    EXPECT_EQ(sargs[2], "b");
    EXPECT_EQ(sargs[3], "");
    EXPECT_EQ(sargs[4], "c");
    EXPECT_EQ(sargs[5], "");
}

TEST(core, trim_string)
{
    std::string value = " x x x x ";
    auto r = trim_string((char *)value.c_str());
    EXPECT_EQ(std::string(r), "x x x x");
}

TEST(core, dlink)
{
    dlink links[10];
    dlink hdr;

    for (int i = 0; i < 10; i++)
        links[i].insert_before(&hdr);

    int count = 0;
    dlink *p = hdr.next();
    while (p != &hdr) {
        count++;
        p = p->next();
    }

    EXPECT_EQ(count, 10);

    p = hdr.next();
    while (p != &hdr) {
        auto p1 = p;
        p = p->next();
        p1->remove();
        count--;
    }

    EXPECT_TRUE(hdr.is_alone());
    EXPECT_TRUE(count == 0);
}

class foo : public ::dsn::ref_counter
{
public:
    foo(int &count) : _count(count) { _count++; }

    ~foo() { _count--; }

private:
    int &_count;
};

typedef ::dsn::ref_ptr<foo> foo_ptr;

TEST(core, ref_ptr)
{
    int count = 0;
    foo_ptr x = nullptr;
    auto y = new foo(count);
    x = y;
    EXPECT_TRUE(x->get_count() == 1);
    EXPECT_TRUE(count == 1);
    x = new foo(count);
    EXPECT_TRUE(x->get_count() == 1);
    EXPECT_TRUE(count == 1);
    x = nullptr;
    EXPECT_TRUE(count == 0);

    std::map<int, foo_ptr> xs;
    x = new foo(count);
    EXPECT_TRUE(x->get_count() == 1);
    EXPECT_TRUE(count == 1);
    xs.insert(std::make_pair(1, x));
    EXPECT_TRUE(x->get_count() == 2);
    EXPECT_TRUE(count == 1);
    x = nullptr;
    EXPECT_TRUE(count == 1);
    xs.clear();
    EXPECT_TRUE(count == 0);

    x = new foo(count);
    EXPECT_TRUE(count == 1);
    xs[2] = x;
    EXPECT_TRUE(x->get_count() == 2);
    x = nullptr;
    EXPECT_TRUE(count == 1);
    xs.clear();
    EXPECT_TRUE(count == 0);

    y = new foo(count);
    EXPECT_TRUE(count == 1);
    xs.insert(std::make_pair(1, y));
    EXPECT_TRUE(count == 1);
    EXPECT_TRUE(y->get_count() == 1);
    xs.clear();
    EXPECT_TRUE(count == 0);

    y = new foo(count);
    EXPECT_TRUE(count == 1);
    xs[2] = y;
    EXPECT_TRUE(count == 1);
    EXPECT_TRUE(y->get_count() == 1);
    xs.clear();
    EXPECT_TRUE(count == 0);

    foo_ptr z = new foo(count);
    EXPECT_TRUE(count == 1);
    z = foo_ptr();
    EXPECT_TRUE(count == 0);
}

TEST(core, flip_map)
{
    std::map<int, int> source;
    source.emplace(3, 1);
    source.emplace(2, 1);
    source.emplace(1, 1);

    auto target = flip_map(source);
    ASSERT_EQ(target.size(), 3);
    ASSERT_EQ(target.count(1), 3);
    ASSERT_EQ(target.count(2), 0);
    ASSERT_EQ(target.count(3), 0);
    std::string values;
    for (auto it = target.equal_range(1); it.first != it.second; it.first++) {
        values += std::to_string(it.first->second);
    }
    ASSERT_EQ(values, "123");
}

TEST(core, get_intersection)
{
    std::set<int> set1;
    set1.insert(1);
    set1.insert(2);
    set1.insert(3);

    std::set<int> set2;
    set2.insert(3);
    set2.insert(4);
    set2.insert(5);

    auto intersection = utils::get_intersection(set1, set2);
    ASSERT_EQ(intersection.size(), 1);
    ASSERT_EQ(*intersection.begin(), 3);
}
