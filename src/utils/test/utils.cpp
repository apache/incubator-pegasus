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

#include <gtest/gtest.h>

#include "runtime/api_layer1.h"
#include "utils/autoref_ptr.h"
#include "utils/binary_reader.h"
#include "utils/binary_writer.h"
#include "utils/crc.h"
#include "utils/link.h"
#include "utils/rand.h"
#include "utils/strings.h"
#include "utils/utils.h"

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

void check_empty(const char *str) { EXPECT_TRUE(dsn::utils::is_empty(str)); }

void check_nonempty(const char *str) { EXPECT_FALSE(dsn::utils::is_empty(str)); }

TEST(core, check_c_string_empty)
{
    const char *empty_strings[] = {nullptr, "", "\0", "\0\0", "\0\0\0", "\0a", "\0ab", "\0abc"};
    for (const auto &p : empty_strings) {
        check_empty(p);
    }

    const char *nonempty_strings[] = {"\\",
                                      "\\\\",
                                      "0",
                                      "00",
                                      "\\0",
                                      "\\0a",
                                      "\\\\00",
                                      "a",
                                      "a\0",
                                      "a\\0",
                                      "a\0b",
                                      "ab\0c",
                                      "abc\0",
                                      "abc"};
    for (const auto &p : nonempty_strings) {
        check_nonempty(p);
    }
}

// For containers such as std::unordered_set, the expected result will be deduplicated
// at initialization. Therefore, it can be used to compare with actual result safely.
template <typename Container>
void test_split_args()
{
    // Test cases:
    // - split empty string by ' ' without place holder
    // - split empty string by ' ' with place holder
    // - split empty string by ',' without place holder
    // - split empty string by ',' with place holder
    // - split a space (' ') by ' ' without place holder
    // - split a space (' ') by ' ' with place holder
    // - split a space (' ') by ',' without place holder
    // - split a space (' ') by ',' with place holder
    // - split a comma (',') by ' ' without place holder
    // - split a comma (',') by ' ' with place holder
    // - split a comma (',') by ',' without place holder
    // - split a comma (',') by ',' with place holder
    // - split 2 leading spaces by ' ' without place holder
    // - split 2 leading spaces by ' ' with place holder
    // - split 2 leading spaces by ',' without place holder
    // - split 2 leading spaces by ',' with place holder
    // - split 3 leading spaces by ' ' without place holder
    // - split 3 leading spaces by ' ' with place holder
    // - split 3 leading spaces by ',' without place holder
    // - split 3 leading spaces by ',' with place holder
    // - split 2 trailing spaces by ' ' without place holder
    // - split 2 trailing spaces by ' ' with place holder
    // - split 2 trailing spaces by ',' without place holder
    // - split 2 trailing spaces by ',' with place holder
    // - split 3 trailing spaces by ' ' without place holder
    // - split 3 trailing spaces by ' ' with place holder
    // - split 3 trailing spaces by ',' without place holder
    // - split 3 trailing spaces by ',' with place holder
    // - split a string including "\\t", "\\r" and "\\n" by ' ' without place holder
    // - split a string including "\\t", "\\r" and "\\n" by ' ' with place holder
    // - split a string including "\\t", "\\r" and "\\n" by ',' without place holder
    // - split a string including "\\t", "\\r" and "\\n" by ',' with place holder
    // - split a single letter by ' ' without place holder
    // - split a single letter by ' ' with place holder
    // - split a single letter by ',' without place holder
    // - split a single letter by ',' with place holder
    // - split a single word by ' ' without place holder
    // - split a single word by ' ' with place holder
    // - split a single word by ',' without place holder
    // - split a single word by ',' with place holder
    // - split a string including letters and words by ' ' without place holder
    // - split a string including letters and words by ' ' with place holder
    // - split a string including letters and words by ',' without place holder
    // - split a string including letters and words by ',' with place holder
    // - split a string that includes multiple letters by ' ' without place holder
    // - split a string that includes multiple letters by ' ' with place holder
    // - split a string that includes multiple letters by ',' without place holder
    // - split a string that includes multiple letters by ',' with place holder
    // - split a string that includes multiple words by ' ' without place holder
    // - split a string that includes multiple words by ' ' with place holder
    // - split a string that includes multiple words by ',' without place holder
    // - split a string that includes multiple words by ',' with place holder
    struct test_case
    {
        const char *input;
        char separator;
        bool keep_place_holder;
        Container expected_output;
    } tests[] = {{"", ' ', false, {}},
                 {"", ' ', true, {""}},
                 {"", ',', false, {}},
                 {"", ',', true, {""}},
                 {" ", ' ', false, {}},
                 {" ", ' ', true, {"", ""}},
                 {" ", ',', false, {}},
                 {" ", ',', true, {""}},
                 {",", ' ', false, {","}},
                 {",", ' ', true, {","}},
                 {",", ',', false, {}},
                 {",", ',', true, {"", ""}},
                 {"  ", ' ', false, {}},
                 {"\t ", ' ', true, {"", ""}},
                 {" \t", ',', false, {}},
                 {"\t\t", ',', true, {""}},
                 {" \t ", ' ', false, {}},
                 {"\t  ", ' ', true, {"", "", ""}},
                 {"\t\t ", ',', false, {}},
                 {"  \t", ',', true, {""}},
                 {"\r ", ' ', false, {}},
                 {"\t\n", ' ', true, {""}},
                 {"\r\t", ',', false, {}},
                 {" \n", ',', true, {""}},
                 {"\n\t\r", ' ', false, {}},
                 {" \r ", ' ', true, {"", "", ""}},
                 {"\r \n", ',', false, {}},
                 {"\t\n\r", ',', true, {""}},
                 {" \\n,,\\t \\r ", ' ', false, {"\\n,,\\t", "\\r"}},
                 {" \\n,,\\t \\r ", ' ', true, {"", "\\n,,\\t", "\\r", ""}},
                 {" \\n,,\\t \\r ", ',', false, {"\\n", "\\t \\r"}},
                 {" \\n,,\\t \\r ", ',', true, {"\\n", "", "\\t \\r"}},
                 {"a", ' ', false, {"a"}},
                 {"a", ' ', true, {"a"}},
                 {"a", ',', false, {"a"}},
                 {"a", ',', true, {"a"}},
                 {"dinner", ' ', false, {"dinner"}},
                 {"dinner", ' ', true, {"dinner"}},
                 {"dinner", ',', false, {"dinner"}},
                 {"dinner", ',', true, {"dinner"}},
                 {"\t\r\na\t\tdog,\t\r\n  \t\r\nand\t\r\n \t\ta\t\tcat",
                  ' ',
                  false,
                  {"\r\na\t\tdog,", "\r\nand", "a\t\tcat"}},
                 {"\t\r\na\t\tdog,\t\r\n  \t\r\nand\t\r\n \t\ta\t\tcat",
                  ' ',
                  true,
                  {"\r\na\t\tdog,", "", "\r\nand", "a\t\tcat"}},
                 {"\t\r\na\t\tdog,\t\r\n  \t\r\nand\t\r\n \t\ta\t\tcat",
                  ',',
                  false,
                  {"\r\na\t\tdog", "\r\n  \t\r\nand\t\r\n \t\ta\t\tcat"}},
                 {"\t\r\na\t\tdog,\t\r\n  \t\r\nand\t\r\n \t\ta\t\tcat",
                  ',',
                  true,
                  {"\r\na\t\tdog", "\r\n  \t\r\nand\t\r\n \t\ta\t\tcat"}},
                 {"a ,b, ,c ", ' ', false, {"a", ",b,", ",c"}},
                 {"a ,b, ,c ", ' ', true, {"a", ",b,", ",c", ""}},
                 {"a ,b, ,c ", ',', false, {"a", "b", "c"}},
                 {"a ,b, ,c ", ',', true, {"a", "b", "", "c"}},
                 {" in  early 2000s ,  too, ", ' ', false, {"in", "early", "2000s", ",", "too,"}},
                 {" in  early 2000s ,  too, ",
                  ' ',
                  true,
                  {"", "in", "", "early", "2000s", ",", "", "too,", ""}},
                 {" in  early 2000s ,  too, ", ',', false, {"in  early 2000s", "too"}},
                 {" in  early 2000s ,  too, ", ',', true, {"in  early 2000s", "too", ""}}};

    for (const auto &test : tests) {
        Container actual_output;
        split_args(test.input, actual_output, test.separator, test.keep_place_holder);
        EXPECT_EQ(actual_output, test.expected_output);

        // Test default value (i.e. false) for keep_place_holder.
        if (!test.keep_place_holder) {
            split_args(test.input, actual_output, test.separator);
            EXPECT_EQ(actual_output, test.expected_output);

            // Test default value (i.e. ' ') for separator.
            if (test.separator == ' ') {
                split_args(test.input, actual_output);
                EXPECT_EQ(actual_output, test.expected_output);
            }
        }
    }
}

TEST(core, split_args)
{
    test_split_args<std::vector<std::string>>();
    test_split_args<std::list<std::string>>();
    test_split_args<std::unordered_set<std::string>>();
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
