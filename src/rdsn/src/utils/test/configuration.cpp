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

#include <algorithm>
#include <fstream>
#include <gtest/gtest.h>

#include "utils/configuration.h"

using namespace ::dsn;

TEST(configuration, load)
{
    std::shared_ptr<configuration> c;

    c.reset(new configuration());
    ASSERT_FALSE(c->load("not_exist_config_file"));

    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-empty.ini"));

    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-sample.ini", "a="));

    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-no-section.ini"));

    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-null-section.ini"));

    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-dup-section.ini"));

    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-unmatch-section.ini"));

    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-bad-section.ini"));

    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-no-key.ini"));

    c.reset(new configuration());
    ASSERT_TRUE(c->load("config-dup-key.ini"));

    c.reset(new configuration());
    ASSERT_TRUE(c->load("config-sample.ini"));
}

TEST(configuration, tool)
{
    std::shared_ptr<configuration> c(new configuration());
    ASSERT_TRUE(c->load("config-sample.ini"));

    bool old = c->set_warning(true);
    ASSERT_FALSE(old);

    ASSERT_STREQ("config-sample.ini", c->get_file_name());
}

TEST(configuration, special_char)
{
    std::shared_ptr<configuration> c(new configuration());
    ASSERT_TRUE(c->load("config-sample.ini", "replace=replace_value"));

    // %xx%
    auto v = c->get_string_value("apps.server", "replace_data", "unknown", "for test replace");
    ASSERT_STREQ("replace_value", v);

    // ^x
    v = c->get_string_value("apps.server", "shift_data", "unknown", "for test shift");
    ASSERT_STREQ("head#middle;tail", v);
}

TEST(configuration, get_section_and_key)
{
    std::shared_ptr<configuration> c(new configuration());
    ASSERT_TRUE(c->load("config-sample.ini"));

    std::vector<std::string> sections;
    c->get_all_sections(sections);
    ASSERT_EQ(4u, sections.size());
    std::sort(sections.begin(), sections.end());
    ASSERT_EQ("apps..default", sections[0]);
    ASSERT_EQ("apps.client", sections[1]);
    ASSERT_EQ("apps.server", sections[2]);
    ASSERT_EQ("test", sections[3]);

    std::vector<const char *> keys;
    c->get_all_keys("apps..default", keys);
    ASSERT_EQ(2u, keys.size());
    std::sort(
        keys.begin(), keys.end(), [](const char *l, const char *r) { return strcmp(l, r) < 0; });
    ASSERT_STREQ("count", keys[0]);
    ASSERT_STREQ("run", keys[1]);

    c->get_all_keys("test", keys);
    ASSERT_EQ(0u, keys.size());
}

TEST(configuration, add_section_and_key)
{
    std::shared_ptr<configuration> c(new configuration());
    ASSERT_TRUE(c->load("config-sample.ini"));

    // add [my_section] my_key
    auto v = c->get_string_value("my_section", "my_key", "my_value", "my key and value");
    ASSERT_STREQ("my_value", v);

    std::vector<std::string> sections;
    c->get_all_sections(sections);
    ASSERT_EQ(5u, sections.size());
    std::sort(sections.begin(), sections.end());
    ASSERT_EQ("apps..default", sections[0]);
    ASSERT_EQ("apps.client", sections[1]);
    ASSERT_EQ("apps.server", sections[2]);
    ASSERT_EQ("my_section", sections[3]);
    ASSERT_EQ("test", sections[4]);

    std::vector<const char *> keys;
    c->get_all_keys("my_section", keys);
    ASSERT_EQ(1u, keys.size());
    ASSERT_STREQ("my_key", keys[0]);

    // add [apps..default] my_key
    v = c->get_string_value("apps..default", "my_key", "my_value", "my key and value");
    ASSERT_STREQ("my_value", v);
    v = c->get_string_value("apps..default", "my_key", "my_value", "my key and value again");
    ASSERT_STREQ("my_value", v);

    c->get_all_keys("apps..default", keys);
    ASSERT_EQ(3u, keys.size());
    std::sort(
        keys.begin(), keys.end(), [](const char *l, const char *r) { return strcmp(l, r) < 0; });
    ASSERT_STREQ("count", keys[0]);
    ASSERT_STREQ("my_key", keys[1]);
    ASSERT_STREQ("run", keys[2]);
}

TEST(configuration, has_section_and_key)
{
    std::shared_ptr<configuration> c(new configuration());
    ASSERT_TRUE(c->load("config-sample.ini"));

    ASSERT_TRUE(c->has_section("test"));
    ASSERT_FALSE(c->has_section("unexist_section"));

    ASSERT_TRUE(c->has_key("apps..default", "run"));
    ASSERT_FALSE(c->has_key("apps..default", "unexist_key"));
    ASSERT_FALSE(c->has_key("unexist_section", "unexist_key"));
}

TEST(configuration, bool_value)
{
    std::shared_ptr<configuration> c(new configuration());
    ASSERT_TRUE(c->load("config-sample.ini"));

    ASSERT_TRUE(c->get_value<bool>("apps.client", "run", false, "client run"));
    ASSERT_TRUE(c->get_value<bool>("apps.client", "RUN", false, "client run"));
    ASSERT_TRUE(c->get_value<bool>("apps.client", "Run", false, "client run"));
    ASSERT_FALSE(c->get_value<bool>("apps.client", "notrun", true, "client not run"));
    ASSERT_FALSE(c->get_value<bool>("apps.client", "unexist_bool_key", false, ""));
}

TEST(configuration, string_value)
{
    std::shared_ptr<configuration> c(new configuration());
    ASSERT_TRUE(c->load("config-sample.ini"));

    ASSERT_EQ("test", c->get_value<std::string>("apps.client", "type", "", ""));
    ASSERT_EQ("unexist_value",
              c->get_value<std::string>("apps.client", "unexist_key", "unexist_value", ""));
}

TEST(configuration, list_value)
{
    std::shared_ptr<configuration> c(new configuration());
    ASSERT_TRUE(c->load("config-sample.ini"));

    std::list<std::string> l =
        c->get_string_value_list("apps.client", "pools", ',', "thread pools");
    ASSERT_EQ(2u, l.size());
    ASSERT_STREQ("THREAD_POOL_DEFAULT", l.begin()->c_str());
    ASSERT_STREQ("THREAD_POOL_TEST_SERVER", (++l.begin())->c_str());

    l = c->get_string_value_list("apps.client", "my_list", ',', "my list");
    ASSERT_EQ(0u, l.size());
}

TEST(configuration, double_value)
{
    std::shared_ptr<configuration> c(new configuration());
    ASSERT_TRUE(c->load("config-sample.ini"));

    ASSERT_EQ(1.0, c->get_value<double>("apps.client", "count1", 2.0, ""));
    ASSERT_EQ(1.2345678, c->get_value<double>("apps.client", "count2", 2.0, ""));
    ASSERT_EQ(2.0, c->get_value<double>("apps.client", "unexist_double_key", 2.0, ""));
}

TEST(configuration, int64_value)
{
    std::shared_ptr<configuration> c(new configuration());
    ASSERT_TRUE(c->load("config-sample.ini"));

    int64_t dft = 1LL << 60;
    ASSERT_EQ(123, c->get_value<int64_t>("apps.client", "int64_t_ok1", dft, ""));
    ASSERT_EQ(0xdeadbeef, c->get_value<int64_t>("apps.client", "int64_t_ok2", dft, ""));
    ASSERT_EQ(std::numeric_limits<int64_t>::max(),
              c->get_value<int64_t>("apps.client", "int64_t_ok3", dft, ""));
    ASSERT_EQ(std::numeric_limits<int64_t>::min(),
              c->get_value<int64_t>("apps.client", "int64_t_ok4", dft, ""));
    ASSERT_EQ(std::numeric_limits<int64_t>::max(),
              c->get_value<int64_t>("apps.client", "int64_t_ok5", dft, ""));

    ASSERT_EQ(dft, c->get_value<int64_t>("apps.client", "unexist_int64_t_key", dft, ""));
    ASSERT_EQ(dft, c->get_value<int64_t>("apps.client", "int64_t_bad1", dft, ""));
    ASSERT_EQ(dft, c->get_value<int64_t>("apps.client", "int64_t_bad2", dft, ""));
    ASSERT_EQ(dft, c->get_value<int64_t>("apps.client", "int64_t_bad3", dft, ""));
    ASSERT_EQ(dft, c->get_value<int64_t>("apps.client", "int64_t_bad4", dft, ""));
    ASSERT_EQ(dft, c->get_value<int64_t>("apps.client", "int64_t_bad5", dft, ""));
}

TEST(configuration, uint64_value)
{
    std::shared_ptr<configuration> c(new configuration());
    ASSERT_TRUE(c->load("config-sample.ini"));

    uint64_t dft = 1ULL << 60;
    ASSERT_EQ(123, c->get_value<uint64_t>("apps.client", "uint64_t_ok1", dft, ""));
    ASSERT_EQ(0xdeadbeef, c->get_value<uint64_t>("apps.client", "uint64_t_ok2", dft, ""));
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              c->get_value<uint64_t>("apps.client", "uint64_t_ok3", dft, ""));
    ASSERT_EQ(std::numeric_limits<uint64_t>::min(),
              c->get_value<uint64_t>("apps.client", "uint64_t_ok4", dft, ""));
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
              c->get_value<uint64_t>("apps.client", "uint64_t_ok5", dft, ""));
    ASSERT_EQ(std::numeric_limits<uint64_t>::min(),
              c->get_value<uint64_t>("apps.client", "uint64_t_ok6", dft, ""));

    ASSERT_EQ(dft, c->get_value<uint64_t>("apps.client", "unexist_uint64_t_key", dft, ""));
    ASSERT_EQ(dft, c->get_value<uint64_t>("apps.client", "uint64_t_bad1", dft, ""));
    ASSERT_EQ(dft, c->get_value<uint64_t>("apps.client", "uint64_t_bad2", dft, ""));
    ASSERT_EQ(dft, c->get_value<uint64_t>("apps.client", "uint64_t_bad3", dft, ""));
    ASSERT_EQ(dft, c->get_value<uint64_t>("apps.client", "uint64_t_bad4", dft, ""));
    ASSERT_EQ(dft, c->get_value<uint64_t>("apps.client", "uint64_t_bad5", dft, ""));
}

TEST(configuration, dump)
{
    // load old config
    std::shared_ptr<configuration> c(new configuration());
    ASSERT_TRUE(c->load("config-sample.ini"));

    // add [my_section] my_key
    auto v = c->get_string_value("my_section", "my_key", "my_value", "my key and value");
    ASSERT_STREQ("my_value", v);

    // add [apps..default] my_key
    v = c->get_string_value("apps..default", "my_key", "my_value", "my key and value");
    ASSERT_STREQ("my_value", v);

    // dump
    std::fstream out;
    out.open("config-sample-dump.ini", std::ios::out);
    c->dump(out);
    out.close();

    // load new config
    c.reset(new configuration());
    ASSERT_TRUE(c->load("config-sample-dump.ini"));

    std::vector<std::string> sections;
    c->get_all_sections(sections);
    ASSERT_EQ(5u, sections.size());
    std::sort(sections.begin(), sections.end());
    ASSERT_EQ("apps..default", sections[0]);
    ASSERT_EQ("apps.client", sections[1]);
    ASSERT_EQ("apps.server", sections[2]);
    ASSERT_EQ("my_section", sections[3]);
    ASSERT_EQ("test", sections[4]);

    ASSERT_TRUE(!c->has_key("not-exsit", "not-exsit"));
    c->set("not-exsit", "not-exsit", "exsit", "kaka");
    ASSERT_EQ(std::string("exsit"),
              std::string(c->get_string_value("not-exsit", "not-exsit", "", "")));
    c->set("not-exsit", "not-exsit", "exsit2", "kaka");
    ASSERT_EQ(std::string("exsit2"),
              std::string(c->get_string_value("not-exsit", "not-exsit", "", "")));
}
