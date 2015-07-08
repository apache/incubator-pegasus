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

# include <dsn/internal/utils.h>
# include <dsn/internal/link.h>
# include <gtest/gtest.h>

using namespace ::dsn;
using namespace ::dsn::utils;

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


TEST(core, binary_io_with_pos)
{
    int value = 0xdeadbeef;
    int value2 = 0xdeadbeef + 100;
    binary_writer writer;
    auto pos = writer.write_placeholder();
    
    writer.write(value);
    writer.write(value2, pos);
    
    auto buf = writer.get_buffer();
    binary_reader reader(buf);
    int value3, value4;
    reader.read(value3);
    reader.read(value4);

    EXPECT_TRUE(value3 == value2);
    EXPECT_TRUE(value4 == value);
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
}

TEST(core, trim_string)
{
    std::string value = " x x x x ";
    auto r = trim_string((char*)value.c_str());
    EXPECT_EQ(std::string(r), "x x x x");
}

TEST(core, dlink)
{
    dlink links[10];
    dlink hdr;

    for (int i = 0; i < 10; i++)
        links[i].insert_before(&hdr);

    int count = 0;
    dlink* p = hdr.next();
    while (p != &hdr)
    {
        count++;
        p = p->next();
    }

    EXPECT_EQ(count, 10);

    p = hdr.next();
    while (p != &hdr)
    {
        auto p1 = p;
        p = p->next();
        p1->remove();
        count--;
    }

    EXPECT_TRUE(hdr.is_alone());
    EXPECT_TRUE(count == 0);
}

