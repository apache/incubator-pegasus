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
 *     Unit-test for join point.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/utility/join_point.h>
#include <gtest/gtest.h>

using namespace ::dsn;

struct entry
{
    std::string name;
    void *func;
    bool is_native;
    entry(const std::string &name_, void *func_, bool is_native_)
        : name(name_), func(func_), is_native(is_native_)
    {
    }
    bool operator==(const entry &e) const
    {
        return name == e.name && func == e.func && is_native == e.is_native;
    }
};

class join_point_for_test : public join_point_base
{
public:
    join_point_for_test() : join_point_base("join_point_for_test") {}
    void get_all(std::vector<entry> &vec) const
    {
        vec.clear();
        advice_entry *p = _hdr.next;
        while (p != &_hdr) {
            vec.push_back(entry(p->name, p->func, p->is_native));
            p = p->next;
        }
    }
};

TEST(core, join_point)
{
    join_point_for_test jp;
    std::vector<entry> check_vec, jp_vec;

    jp.get_all(jp_vec);
    ASSERT_EQ(check_vec, jp_vec);

    { // [1]
        entry e("1", (void *)1, false);
        bool r = jp.put_front(e.func, e.name.c_str(), e.is_native);
        ASSERT_TRUE(r);
        check_vec.push_back(e);
        jp.get_all(jp_vec);
        ASSERT_EQ(check_vec, jp_vec);
    }

    { // [2,1]
        entry e("2", (void *)2, true);
        bool r = jp.put_front(e.func, e.name.c_str(), e.is_native);
        ASSERT_TRUE(r);
        check_vec.push_back(e);
        std::swap(check_vec[0], check_vec[1]);
        jp.get_all(jp_vec);
        ASSERT_EQ(check_vec, jp_vec);
    }

    { // [2,1,3]
        entry e("3", (void *)3, false);
        bool r = jp.put_back(e.func, e.name.c_str(), e.is_native);
        ASSERT_TRUE(r);
        check_vec.push_back(e);
        jp.get_all(jp_vec);
        ASSERT_EQ(check_vec, jp_vec);
    }

    { // [2,1,3,4]
        entry e("4", (void *)4, true);
        bool r = jp.put_back(e.func, e.name.c_str(), e.is_native);
        ASSERT_TRUE(r);
        check_vec.push_back(e);
        jp.get_all(jp_vec);
        ASSERT_EQ(check_vec, jp_vec);
    }

    { // [2,1,3,5,4]
        entry e("5", (void *)5, false);
        bool r = jp.put_before("4", e.func, e.name.c_str(), e.is_native);
        ASSERT_TRUE(r);
        check_vec.push_back(e);
        std::swap(check_vec[3], check_vec[4]);
        jp.get_all(jp_vec);
        ASSERT_EQ(check_vec, jp_vec);
    }

    { // [2,1,3,5,4,6]
        entry e("6", (void *)6, true);
        bool r = jp.put_after("4", e.func, e.name.c_str(), e.is_native);
        ASSERT_TRUE(r);
        check_vec.push_back(e);
        jp.get_all(jp_vec);
        ASSERT_EQ(check_vec, jp_vec);
    }

    { // [2,1,3,5,4]
        bool r = jp.remove("6");
        ASSERT_TRUE(r);
        check_vec.pop_back();
        jp.get_all(jp_vec);
        ASSERT_EQ(check_vec, jp_vec);
    }

    { // [2,1,3,4]
        bool r = jp.remove("5");
        ASSERT_TRUE(r);
        std::swap(check_vec[3], check_vec[4]);
        check_vec.pop_back();
        jp.get_all(jp_vec);
        ASSERT_EQ(check_vec, jp_vec);
    }

    { // [2,1,7,4]
        entry e("7", (void *)7, false);
        bool r = jp.put_replace("3", e.func, e.name.c_str());
        ASSERT_TRUE(r);
        std::swap(check_vec[2], e);
        jp.get_all(jp_vec);
        ASSERT_EQ(check_vec, jp_vec);
    }
}
