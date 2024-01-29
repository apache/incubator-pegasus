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

#include "utils/priority_queue.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "utils/utils.h"

using namespace ::dsn::utils;

struct queue_data
{
    int32_t priority;
    int32_t queue_index;

    queue_data(int32_t pri, int32_t idx) : priority(pri), queue_index(idx) {}
};

typedef priority_queue<queue_data *, 3> my_priority_queue;
TEST(core, priority_queue)
{
    my_priority_queue q("my_priority_queue_name");
    ASSERT_EQ("my_priority_queue_name", q.get_name());
    ASSERT_EQ(0, q.count());
    ASSERT_EQ(nullptr, q.dequeue());

    std::vector<queue_data> datas;
    datas.push_back(queue_data(0, 1));
    datas.push_back(queue_data(2, 1));
    datas.push_back(queue_data(1, 1));
    datas.push_back(queue_data(1, 2));
    datas.push_back(queue_data(2, 2));
    datas.push_back(queue_data(0, 2));
    datas.push_back(queue_data(1, 3));
    datas.push_back(queue_data(0, 3));
    datas.push_back(queue_data(2, 3));

    for (int i = 0; i < datas.size(); ++i) {
        ASSERT_EQ(i, q.count());
        queue_data *d = &datas[i];
        ASSERT_EQ(i + 1, q.enqueue(d, d->priority));
        ASSERT_EQ(i + 1, q.count());
    }

    std::vector<queue_data> sort_datas(datas);
    std::sort(sort_datas.begin(), sort_datas.end(), [](const queue_data &l, const queue_data &r) {
        return l.priority > r.priority ||
               (l.priority == r.priority && l.queue_index < r.queue_index);
    });

    int count = sort_datas.size();
    for (int i = 0; i < count; ++i) {
        ASSERT_EQ(count, q.count());
        queue_data *d = nullptr;
        if (i % 2 == 0) {
            d = q.dequeue();
        } else {
            long ct;
            d = q.dequeue(ct);
            ASSERT_EQ(count - 1, ct);
        }
        ASSERT_EQ(sort_datas[i].priority, d->priority);
        ASSERT_EQ(sort_datas[i].queue_index, d->queue_index);
        ASSERT_EQ(count - 1, q.count());
        count--;
    }
}

typedef blocking_priority_queue<queue_data *, 3> my_blocking_priority_queue;
TEST(core, blocking_priority_queue)
{
    my_blocking_priority_queue q("my_blocking_priority_queue_name");
    ASSERT_EQ("my_blocking_priority_queue_name", q.get_name());
    ASSERT_EQ(0, q.count());

    long ct;
    ASSERT_EQ(nullptr, q.dequeue_with_timeout(ct, 10));
    ASSERT_EQ(0, ct);

    ASSERT_EQ(1, q.enqueue(new queue_data(0, 10), 0));
    queue_data *d = q.dequeue_with_timeout(ct, 10);
    ASSERT_NE(nullptr, d);
    ASSERT_EQ(0, ct);
    ASSERT_EQ(0, d->priority);
    ASSERT_EQ(10, d->queue_index);
    delete d;

    bool flag = false;

    std::thread t1([&q, &flag]() {
        long ct;
        queue_data *d = nullptr;

        d = q.dequeue_with_timeout(ct, 10);
        ASSERT_EQ(nullptr, d);
        ASSERT_EQ(0, ct);

        flag = true;

        d = q.dequeue(ct);
        ASSERT_NE(nullptr, d);
        ASSERT_EQ(0, ct);
        ASSERT_EQ(1, d->priority);
        ASSERT_EQ(20, d->queue_index);
        delete d;

        d = q.dequeue_with_timeout(ct, 10);
        ASSERT_EQ(nullptr, d);
        ASSERT_EQ(0, ct);
    });

    std::thread t2([&q, &flag]() {
        while (!flag)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        ASSERT_EQ(1, q.enqueue(new queue_data(1, 20), 1));
    });

    t1.join();
    t2.join();
}
