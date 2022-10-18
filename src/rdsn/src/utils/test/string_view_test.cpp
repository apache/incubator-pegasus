// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "utils/string_view.h"

#include <gtest/gtest.h>

namespace {

// Separated from STL1() because some compilers produce an overly
// large stack frame for the combined function.
TEST(StringViewTest, STL2)
{
    const dsn::string_view a("abcdefghijklmnopqrstuvwxyz");
    const dsn::string_view b("abc");
    const dsn::string_view c("xyz");
    dsn::string_view d("foobar");
    const dsn::string_view e;
    const dsn::string_view f("123"
                             "\0"
                             "456",
                             7);

    d = dsn::string_view();
    EXPECT_EQ(d.size(), 0);
    EXPECT_TRUE(d.empty());
    EXPECT_TRUE(d.data() == nullptr);
    EXPECT_TRUE(d.begin() == d.end());

    EXPECT_EQ(a.find(b), 0);
    EXPECT_EQ(a.find(b, 1), dsn::string_view::npos);
    EXPECT_EQ(a.find(c), 23);
    EXPECT_EQ(a.find(c, 9), 23);
    EXPECT_EQ(a.find(c, dsn::string_view::npos), dsn::string_view::npos);
    EXPECT_EQ(b.find(c), dsn::string_view::npos);
    EXPECT_EQ(b.find(c, dsn::string_view::npos), dsn::string_view::npos);
    EXPECT_EQ(a.find(d), 0);
    EXPECT_EQ(a.find(e), 0);
    EXPECT_EQ(a.find(d, 12), 12);
    EXPECT_EQ(a.find(e, 17), 17);
    dsn::string_view g("xx not found bb");
    EXPECT_EQ(a.find(g), dsn::string_view::npos);
    // empty std::string nonsense
    EXPECT_EQ(d.find(b), dsn::string_view::npos);
    EXPECT_EQ(e.find(b), dsn::string_view::npos);
    EXPECT_EQ(d.find(b, 4), dsn::string_view::npos);
    EXPECT_EQ(e.find(b, 7), dsn::string_view::npos);

    size_t empty_search_pos = std::string().find(std::string());
    EXPECT_EQ(d.find(d), empty_search_pos);
    EXPECT_EQ(d.find(e), empty_search_pos);
    EXPECT_EQ(e.find(d), empty_search_pos);
    EXPECT_EQ(e.find(e), empty_search_pos);
    EXPECT_EQ(d.find(d, 4), std::string().find(std::string(), 4));
    EXPECT_EQ(d.find(e, 4), std::string().find(std::string(), 4));
    EXPECT_EQ(e.find(d, 4), std::string().find(std::string(), 4));
    EXPECT_EQ(e.find(e, 4), std::string().find(std::string(), 4));
}

// Continued from STL2
TEST(StringViewTest, STL2Substr)
{
    const dsn::string_view a("abcdefghijklmnopqrstuvwxyz");
    const dsn::string_view b("abc");
    const dsn::string_view c("xyz");
    dsn::string_view d("foobar");
    const dsn::string_view e;

    d = dsn::string_view();
    EXPECT_EQ(a.substr(0, 3), b);
    EXPECT_EQ(a.substr(23), c);
    EXPECT_EQ(a.substr(23, 3), c);
    EXPECT_EQ(a.substr(23, 99), c);
    EXPECT_EQ(a.substr(0), a);
    EXPECT_EQ(a.substr(3, 2), "de");
    // empty std::string nonsense
    EXPECT_EQ(d.substr(0, 99), e);
    // use of npos
    EXPECT_EQ(a.substr(0, dsn::string_view::npos), a);
    EXPECT_EQ(a.substr(23, dsn::string_view::npos), c);

    EXPECT_THROW(a.substr(99, 2), std::out_of_range);
}

TEST(StringViewTest, Ctor)
{
    {
        // Null.
        dsn::string_view s10;
        EXPECT_TRUE(s10.data() == nullptr);
        EXPECT_EQ(0, s10.length());
    }

    {
        // const char* without length.
        const char *hello = "hello";
        dsn::string_view s20(hello);
        EXPECT_TRUE(s20.data() == hello);
        EXPECT_EQ(5, s20.length());

        // const char* with length.
        dsn::string_view s21(hello, 4);
        EXPECT_TRUE(s21.data() == hello);
        EXPECT_EQ(4, s21.length());

        // Not recommended, but valid C++
        dsn::string_view s22(hello, 6);
        EXPECT_TRUE(s22.data() == hello);
        EXPECT_EQ(6, s22.length());
    }

    {
        // std::string.
        std::string hola = "hola";
        dsn::string_view s30(hola);
        EXPECT_TRUE(s30.data() == hola.data());
        EXPECT_EQ(4, s30.length());

        // std::string with embedded '\0'.
        hola.push_back('\0');
        hola.append("h2");
        hola.push_back('\0');
        dsn::string_view s31(hola);
        EXPECT_TRUE(s31.data() == hola.data());
        EXPECT_EQ(8, s31.length());
    }
}

TEST(StringViewTest, Swap)
{
    dsn::string_view a("a");
    dsn::string_view b("bbb");
    EXPECT_TRUE(noexcept(a.swap(b)));
    a.swap(b);
    EXPECT_EQ(a, "bbb");
    EXPECT_EQ(b, "a");
    a.swap(b);
    EXPECT_EQ(a, "a");
    EXPECT_EQ(b, "bbb");
}

#define EXPECT_COMPARE_TRUE(op, x, y)                                                              \
    EXPECT_TRUE(dsn::string_view((x)) op dsn::string_view((y)));                                   \
    EXPECT_TRUE(dsn::string_view((x)).compare(dsn::string_view((y))) op 0)

#define EXPECT_COMPARE_FALSE(op, x, y)                                                             \
    EXPECT_FALSE(dsn::string_view((x)) op dsn::string_view((y)));                                  \
    EXPECT_FALSE(dsn::string_view((x)).compare(dsn::string_view((y))) op 0)

TEST(StringViewTest, ComparisonOperators)
{
    EXPECT_COMPARE_FALSE(==, "a", "");
    EXPECT_COMPARE_FALSE(==, "", "a");
    EXPECT_COMPARE_FALSE(==, "a", "b");
    EXPECT_COMPARE_FALSE(==, "a", "aa");
    EXPECT_COMPARE_FALSE(==, "aa", "a");

    EXPECT_COMPARE_TRUE(==, "", "");
    EXPECT_COMPARE_TRUE(==, "", dsn::string_view());
    EXPECT_COMPARE_TRUE(==, dsn::string_view(), "");
    EXPECT_COMPARE_TRUE(==, "a", "a");
    EXPECT_COMPARE_TRUE(==, "aa", "aa");

    EXPECT_COMPARE_FALSE(!=, "", "");
    EXPECT_COMPARE_FALSE(!=, "a", "a");
    EXPECT_COMPARE_FALSE(!=, "aa", "aa");

    EXPECT_COMPARE_TRUE(!=, "a", "");
    EXPECT_COMPARE_TRUE(!=, "", "a");
    EXPECT_COMPARE_TRUE(!=, "a", "b");
    EXPECT_COMPARE_TRUE(!=, "a", "aa");
    EXPECT_COMPARE_TRUE(!=, "aa", "a");
}

TEST(StringViewTest, STL1)
{
    const dsn::string_view a("abcdefghijklmnopqrstuvwxyz");
    const dsn::string_view b("abc");
    const dsn::string_view c("xyz");
    const dsn::string_view d("foobar");
    const dsn::string_view e;
    std::string temp("123");
    temp += '\0';
    temp += "456";
    const dsn::string_view f(temp);

    EXPECT_EQ(a[6], 'g');
    EXPECT_EQ(b[0], 'a');
    EXPECT_EQ(c[2], 'z');
    EXPECT_EQ(f[3], '\0');
    EXPECT_EQ(f[5], '5');

    EXPECT_EQ(*d.data(), 'f');
    EXPECT_EQ(d.data()[5], 'r');
    EXPECT_TRUE(e.data() == nullptr);

    EXPECT_EQ(*a.begin(), 'a');
    EXPECT_EQ(*(b.begin() + 2), 'c');
    EXPECT_EQ(*(c.end() - 1), 'z');

    EXPECT_EQ(*a.rbegin(), 'z');
    EXPECT_EQ(*(b.rbegin() + 2), 'a');
    EXPECT_EQ(*(c.rend() - 1), 'x');
    EXPECT_TRUE(a.rbegin() + 26 == a.rend());

    EXPECT_EQ(a.size(), 26);
    EXPECT_EQ(b.size(), 3);
    EXPECT_EQ(c.size(), 3);
    EXPECT_EQ(d.size(), 6);
    EXPECT_EQ(e.size(), 0);
    EXPECT_EQ(f.size(), 7);

    EXPECT_TRUE(!d.empty());
    EXPECT_TRUE(d.begin() != d.end());
    EXPECT_TRUE(d.begin() + 6 == d.end());

    EXPECT_TRUE(e.empty());
    EXPECT_TRUE(e.begin() == e.end());
}

TEST(StringViewTest, Remove)
{
    dsn::string_view a("foobar");
    std::string s1("123");
    s1 += '\0';
    s1 += "456";
    dsn::string_view b(s1);
    dsn::string_view e;
    std::string s2;

    // remove_prefix
    dsn::string_view c(a);
    c.remove_prefix(3);
    EXPECT_EQ(c, "bar");
    c = a;
    c.remove_prefix(0);
    EXPECT_EQ(c, a);
    c.remove_prefix(c.size());
    EXPECT_EQ(c, e);

    // remove_suffix
    c = a;
    c.remove_suffix(3);
    EXPECT_EQ(c, "foo");
    c = a;
    c.remove_suffix(0);
    EXPECT_EQ(c, a);
    c.remove_suffix(c.size());
    EXPECT_EQ(c, e);
}

TEST(StringViewTest, Set)
{
    dsn::string_view a("foobar");
    dsn::string_view empty;
    dsn::string_view b;

    // set
    b = dsn::string_view("foobar", 6);
    EXPECT_EQ(b, a);
    b = dsn::string_view("foobar", 0);
    EXPECT_EQ(b, empty);
    b = dsn::string_view("foobar", 7);
    EXPECT_NE(b, a);

    b = dsn::string_view("foobar");
    EXPECT_EQ(b, a);
}

TEST(StringViewTest, FrontBack)
{
    static const char arr[] = "abcd";
    const dsn::string_view csp(arr, 4);
    EXPECT_EQ(&arr[0], &csp.front());
    EXPECT_EQ(&arr[3], &csp.back());
}

TEST(StringViewTest, FrontBackSingleChar)
{
    static const char c = 'a';
    const dsn::string_view csp(&c, 1);
    EXPECT_EQ(&c, &csp.front());
    EXPECT_EQ(&c, &csp.back());
}

TEST(StringViewTest, NULLInput)
{
    dsn::string_view s;
    EXPECT_EQ(s.data(), nullptr);
    EXPECT_EQ(s.size(), 0);

    s = dsn::string_view(nullptr);
    EXPECT_EQ(s.data(), nullptr);
    EXPECT_EQ(s.size(), 0);

    EXPECT_EQ("", std::string(s));
}

TEST(StringViewTest, ExplicitConversionOperator)
{
    dsn::string_view sp = "hi";
    EXPECT_EQ(sp, std::string(sp));
}

TEST(StringViewTest, Noexcept)
{
    EXPECT_TRUE((std::is_nothrow_constructible<dsn::string_view, const std::string &>::value));
    EXPECT_TRUE((std::is_nothrow_constructible<dsn::string_view, const std::string &>::value));
    EXPECT_TRUE(std::is_nothrow_constructible<dsn::string_view>::value);
    constexpr dsn::string_view sp;
    EXPECT_TRUE(noexcept(sp.begin()));
    EXPECT_TRUE(noexcept(sp.end()));
    EXPECT_TRUE(noexcept(sp.cbegin()));
    EXPECT_TRUE(noexcept(sp.cend()));
    EXPECT_TRUE(noexcept(sp.rbegin()));
    EXPECT_TRUE(noexcept(sp.rend()));
    EXPECT_TRUE(noexcept(sp.crbegin()));
    EXPECT_TRUE(noexcept(sp.crend()));
    EXPECT_TRUE(noexcept(sp.size()));
    EXPECT_TRUE(noexcept(sp.length()));
    EXPECT_TRUE(noexcept(sp.empty()));
    EXPECT_TRUE(noexcept(sp.data()));
    EXPECT_TRUE(noexcept(sp.compare(sp)));
    EXPECT_TRUE(noexcept(sp.find(sp)));
}

TEST(StringViewTest, HeterogenousStringViewEquals)
{
    EXPECT_EQ(dsn::string_view("hello"), std::string("hello"));
    EXPECT_EQ("hello", dsn::string_view("hello"));
}

TEST(StringViewTest, FindConformance)
{
    struct
    {
        std::string haystack;
        std::string needle;
    } specs[] = {
        {"", ""},
        {"", "a"},
        {"a", ""},
        {"a", "a"},
        {"a", "b"},
        {"aa", ""},
        {"aa", "a"},
        {"aa", "b"},
        {"ab", "a"},
        {"ab", "b"},
        {"abcd", ""},
        {"abcd", "a"},
        {"abcd", "d"},
        {"abcd", "ab"},
        {"abcd", "bc"},
        {"abcd", "cd"},
        {"abcd", "abcd"},
    };
    for (const auto &s : specs) {
        SCOPED_TRACE(s.haystack);
        SCOPED_TRACE(s.needle);
        std::string st = s.haystack;
        dsn::string_view sp = s.haystack;
        for (size_t i = 0; i <= sp.size(); ++i) {
            size_t pos = (i == sp.size()) ? dsn::string_view::npos : i;
            SCOPED_TRACE(pos);
            EXPECT_EQ(sp.find(s.needle, pos), st.find(s.needle, pos));
        }
    }
}

class StringViewStreamTest : public ::testing::Test
{
public:
    // Set negative 'width' for right justification.
    template <typename T>
    std::string Pad(const T &s, int width, char fill = 0)
    {
        std::ostringstream oss;
        if (fill != 0) {
            oss << std::setfill(fill);
        }
        if (width < 0) {
            width = -width;
            oss << std::right;
        }
        oss << std::setw(width) << s;
        return oss.str();
    }
};

TEST_F(StringViewStreamTest, Padding)
{
    std::string s("hello");
    dsn::string_view sp(s);
    for (int w = -64; w < 64; ++w) {
        SCOPED_TRACE(w);
        EXPECT_EQ(Pad(s, w), Pad(sp, w));
    }
    for (int w = -64; w < 64; ++w) {
        SCOPED_TRACE(w);
        EXPECT_EQ(Pad(s, w, '#'), Pad(sp, w, '#'));
    }
}

TEST_F(StringViewStreamTest, ResetsWidth)
{
    // Width should reset after one formatted write.
    // If we weren't resetting width after formatting the string_view,
    // we'd have width=5 carrying over to the printing of the "]",
    // creating "[###hi####]".
    std::string s = "hi";
    dsn::string_view sp = s;
    {
        std::ostringstream oss;
        oss << "[" << std::setfill('#') << std::setw(5) << s << "]";
        ASSERT_EQ("[###hi]", oss.str());
    }
    {
        std::ostringstream oss;
        oss << "[" << std::setfill('#') << std::setw(5) << sp << "]";
        EXPECT_EQ("[###hi]", oss.str());
    }
}

} // namespace
