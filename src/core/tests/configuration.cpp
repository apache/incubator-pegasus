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
 *     Unit-test for configuration.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <algorithm>
#include <fstream>
#include <gtest/gtest.h>

#include <dsn/utility/configuration.h>

using namespace ::dsn;

TEST(core, configuration)
{
    std::shared_ptr<configuration> c;

    printf("load not_exist_config_file\n");
    c.reset(new configuration());
    ASSERT_FALSE(c->load("not_exist_config_file"));

    printf("load config-empty.ini\n");
    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-empty.ini"));

    printf("load config-sample.ini with bad arguments\n");
    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-sample.ini", "a="));

    printf("load config-no-section.ini\n");
    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-no-section.ini"));

    printf("load config-null-section.ini\n");
    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-null-section.ini"));

    printf("load config-dup-section.ini\n");
    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-dup-section.ini"));

    printf("load config-unmatch-section.ini\n");
    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-unmatch-section.ini"));

    printf("load config-bad-section.ini\n");
    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-bad-section.ini"));

    printf("load config-no-key.ini\n");
    c.reset(new configuration());
    ASSERT_FALSE(c->load("config-no-key.ini"));

    printf("load config-dup-key.ini\n");
    c.reset(new configuration());
    ASSERT_TRUE(c->load("config-dup-key.ini"));

    printf("load config-sample.ini\n");
    c.reset(new configuration());
    ASSERT_TRUE(c->load("config-sample.ini", "replace=replace_value"));
    bool old = c->set_warning(true);
    ASSERT_FALSE(old);

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

    auto v = c->get_string_value("apps.server", "replace_data", "unknown", "for test replace");
    ASSERT_STREQ("replace_value", v);

    v = c->get_string_value("apps.server", "shift_data", "unknown", "for test shift");
    ASSERT_STREQ("head#middle;tail", v);

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

    // add [my_section] my_key
    v = c->get_string_value("my_section", "my_key", "my_value", "my key and value");
    ASSERT_STREQ("my_value", v);
    c->get_all_sections(sections);
    ASSERT_EQ(5u, sections.size());
    std::sort(sections.begin(), sections.end());
    ASSERT_EQ("apps..default", sections[0]);
    ASSERT_EQ("apps.client", sections[1]);
    ASSERT_EQ("apps.server", sections[2]);
    ASSERT_EQ("my_section", sections[3]);
    ASSERT_EQ("test", sections[4]);
    c->get_all_keys("my_section", keys);
    ASSERT_EQ(1u, keys.size());
    ASSERT_STREQ("my_key", keys[0]);

    std::list<std::string> l =
        c->get_string_value_list("apps.client", "pools", ',', "thread pools");
    ASSERT_EQ(2u, l.size());
    ASSERT_STREQ("THREAD_POOL_DEFAULT", l.begin()->c_str());
    ASSERT_STREQ("THREAD_POOL_TEST_SERVER", (++l.begin())->c_str());

    l = c->get_string_value_list("apps.client", "my_list", ',', "my list");
    ASSERT_EQ(0u, l.size());

    ASSERT_TRUE(c->has_section("test"));
    ASSERT_FALSE(c->has_section("unexist_section"));

    ASSERT_TRUE(c->has_key("apps..default", "run"));
    ASSERT_FALSE(c->has_key("apps..default", "unexist_key"));
    ASSERT_FALSE(c->has_key("unexist_section", "unexist_key"));

    ASSERT_STREQ("config-sample.ini", c->get_file_name());

    ASSERT_EQ("unexist_value",
              c->get_value<std::string>("apps.client", "unexist_key", "unexist_value", ""));
    ASSERT_EQ(1.0, c->get_value<double>("apps.client", "count", 2.0, "client count"));
    ASSERT_EQ(2.0, c->get_value<double>("apps.client", "unexist_double_key", 2.0, ""));
    ASSERT_EQ(1, c->get_value<long long>("apps.client", "count", 100, "client count"));
    ASSERT_EQ(100, c->get_value<long long>("apps.client", "unexist_long_long_key", 100, ""));
    ASSERT_EQ(0xdeadbeef, c->get_value<long long>("apps.server", "hex_data", 100, ""));
    ASSERT_EQ(1u, c->get_value<unsigned long long>("apps.client", "count", 100, "client count"));
    ASSERT_EQ(
        100u,
        c->get_value<unsigned long long>("apps.client", "unexist_unsigned_long_long_key", 100, ""));
    ASSERT_EQ(1, c->get_value<long>("apps.client", "count", 100, "client count"));
    ASSERT_EQ(100, c->get_value<long>("apps.client", "unexist_long_key", 100, ""));
    ASSERT_EQ(0xdeadbeef, c->get_value<long>("apps.server", "hex_data", 100, ""));
    ASSERT_EQ(1u, c->get_value<unsigned long>("apps.client", "count", 100, "client count"));
    ASSERT_EQ(100u,
              c->get_value<unsigned long>("apps.client", "unexist_unsigned_long_key", 100, ""));
    ASSERT_EQ(1, c->get_value<int>("apps.client", "count", 100, "client count"));
    ASSERT_EQ(100, c->get_value<int>("apps.client", "unexist_int_key", 100, ""));
    ASSERT_EQ(1u, c->get_value<unsigned int>("apps.client", "count", 100, "client count"));
    ASSERT_EQ(100u, c->get_value<unsigned int>("apps.client", "unexist_unsigned_int_key", 100, ""));
    ASSERT_EQ(1, c->get_value<short>("apps.client", "count", 100, "client count"));
    ASSERT_EQ(100, c->get_value<short>("apps.client", "unexist_short_key", 100, ""));
    ASSERT_EQ(1u, c->get_value<unsigned short>("apps.client", "count", 100, "client count"));
    ASSERT_EQ(100u,
              c->get_value<unsigned short>("apps.client", "unexist_unsigned_short_key", 100, ""));
    ASSERT_TRUE(c->get_value<bool>("apps.client", "run", false, "client run"));
    ASSERT_FALSE(c->get_value<bool>("apps.client", "unexist_bool_key", false, ""));

    std::fstream out;
    out.open("config-sample-dump.ini", std::ios::out);
    c->dump(out);
    out.close();

    printf("load config-sample-dump.ini\n");
    c.reset(new configuration());
    ASSERT_TRUE(c->load("config-sample-dump.ini"));
    c->get_all_sections(sections);
    ASSERT_EQ(5u, sections.size());
    std::sort(sections.begin(), sections.end());
    ASSERT_EQ("apps..default", sections[0]);
    ASSERT_EQ("apps.client", sections[1]);
    ASSERT_EQ("apps.server", sections[2]);
    ASSERT_EQ("my_section", sections[3]);
    ASSERT_EQ("test", sections[4]);

    // configuration set test
    ASSERT_TRUE(!c->has_key("not-exsit", "not-exsit"));
    c->set("not-exsit", "not-exsit", "exsit", "kaka");
    ASSERT_EQ(std::string("exsit"),
              std::string(c->get_string_value("not-exsit", "not-exsit", "", "")));
    c->set("not-exsit", "not-exsit", "exsit2", "kaka");
    ASSERT_EQ(std::string("exsit2"),
              std::string(c->get_string_value("not-exsit", "not-exsit", "", "")));
}
