// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <boost/asio/deadline_timer.hpp>
#include <atomic>
#include <cstdint>
#include <memory>
#include <utility>

#include "boost/asio/basic_deadline_timer.hpp"
#include "perf_counter/perf_counter.h"
#include "utils/fmt_logging.h"
#include "utils/process_utils.h"
#include "utils/time_utils.h"

namespace boost {
namespace system {
class error_code;
} // namespace system
} // namespace boost

namespace dsn {

#pragma pack(push)
#pragma pack(8)

// -----------   NUMBER perf counter ---------------------------------

#define DIVIDE_CONTAINER 107
class perf_counter_number_atomic : public perf_counter
{
public:
    perf_counter_number_atomic(const char *app,
                               const char *section,
                               const char *name,
                               dsn_perf_counter_type_t type,
                               const char *dsptr)
        : perf_counter(app, section, name, type, dsptr)
    {
        for (int i = 0; i < DIVIDE_CONTAINER; i++) {
            _val[i].store(0);
        }
    }
    ~perf_counter_number_atomic(void) {}

    virtual void increment()
    {
        uint64_t task_id = static_cast<int>(utils::get_current_tid());
        _val[task_id % DIVIDE_CONTAINER].fetch_add(1, std::memory_order_relaxed);
    }
    virtual void decrement()
    {
        uint64_t task_id = static_cast<int>(utils::get_current_tid());
        _val[task_id % DIVIDE_CONTAINER].fetch_sub(1, std::memory_order_relaxed);
    }
    virtual void add(int64_t val)
    {
        uint64_t task_id = static_cast<int>(utils::get_current_tid());
        _val[task_id % DIVIDE_CONTAINER].fetch_add(val, std::memory_order_relaxed);
    }
    virtual void set(int64_t val)
    {
        // the set-op of number is reset the number to zero.
        // for simplicity, only set other zero, not add the lock to protect, if needed, should add
        // lock.
        for (int i = 0; i < DIVIDE_CONTAINER; i++)
            _val[i].store(0, std::memory_order_relaxed);
        _val[0].store(val, std::memory_order_relaxed);
    }
    virtual double get_value()
    {
        double val = 0;
        for (int i = 0; i < DIVIDE_CONTAINER; i++) {
            val += static_cast<double>(_val[i].load(std::memory_order_relaxed));
        }
        return val;
    }
    virtual int64_t get_integer_value()
    {
        int64_t val = 0;
        for (int i = 0; i < DIVIDE_CONTAINER; i++) {
            val += _val[i].load(std::memory_order_relaxed);
        }
        return val;
    }
    virtual double get_percentile(dsn_perf_counter_percentile_type_t type)
    {
        CHECK(false, "invalid execution flow");
        return 0.0;
    }

protected:
    std::atomic<int64_t> _val[DIVIDE_CONTAINER];
};

// -----------   VOLATILE_NUMBER perf counter ---------------------------------

class perf_counter_volatile_number_atomic : public perf_counter_number_atomic
{
public:
    perf_counter_volatile_number_atomic(const char *app,
                                        const char *section,
                                        const char *name,
                                        dsn_perf_counter_type_t type,
                                        const char *dsptr)
        : perf_counter_number_atomic(app, section, name, type, dsptr)
    {
    }
    ~perf_counter_volatile_number_atomic(void) {}

    virtual double get_value()
    {
        double val = 0;
        for (int i = 0; i < DIVIDE_CONTAINER; i++) {
            val += static_cast<double>(_val[i].exchange(0, std::memory_order_relaxed));
        }
        return val;
    }
    virtual int64_t get_integer_value()
    {
        int64_t val = 0;
        for (int i = 0; i < DIVIDE_CONTAINER; i++) {
            val += _val[i].exchange(0, std::memory_order_relaxed);
        }
        return val;
    }
};

// -----------   RATE perf counter ---------------------------------

class perf_counter_rate_atomic : public perf_counter
{
public:
    perf_counter_rate_atomic(const char *app,
                             const char *section,
                             const char *name,
                             dsn_perf_counter_type_t type,
                             const char *dsptr)
        : perf_counter(app, section, name, type, dsptr), _rate(0)
    {
        _last_time = utils::get_current_physical_time_ns();
        for (int i = 0; i < DIVIDE_CONTAINER; i++) {
            _val[i].store(0, std::memory_order_relaxed);
        }
    }
    ~perf_counter_rate_atomic(void) {}

    virtual void increment()
    {
        uint64_t task_id = static_cast<int>(utils::get_current_tid());
        _val[task_id % DIVIDE_CONTAINER].fetch_add(1, std::memory_order_relaxed);
    }
    virtual void decrement()
    {
        uint64_t task_id = static_cast<int>(utils::get_current_tid());
        _val[task_id % DIVIDE_CONTAINER].fetch_sub(1, std::memory_order_relaxed);
    }
    virtual void add(int64_t val)
    {
        uint64_t task_id = static_cast<int>(utils::get_current_tid());
        _val[task_id % DIVIDE_CONTAINER].fetch_add(val, std::memory_order_relaxed);
    }
    virtual void set(int64_t val) { CHECK(false, "invalid execution flow"); }
    virtual double get_value()
    {
        uint64_t now = utils::get_current_physical_time_ns();
        double interval = (now - _last_time) / 1e9;
        if (interval <= 0.1)
            return _rate;

        double val = 0;
        for (int i = 0; i < DIVIDE_CONTAINER; i++) {
            val += _val[i].fetch_and(0, std::memory_order_relaxed);
        }

        _rate = val / interval;
        _last_time = now;
        return _rate;
    }
    virtual int64_t get_integer_value() { return (int64_t)get_value(); }
    virtual double get_percentile(dsn_perf_counter_percentile_type_t type)
    {
        CHECK(false, "invalid execution flow");
        return 0.0;
    }

private:
    std::atomic<double> _rate;
    std::atomic<uint64_t> _last_time;
    std::atomic<int64_t> _val[DIVIDE_CONTAINER];
};

// -----------   NUMBER_PERCENTILE perf counter ---------------------------------

#define MAX_QUEUE_LENGTH 5000
#define _LEFT 0
#define _RIGHT 1
#define _QLEFT 2
#define _QRIGHT 3

class perf_counter_number_percentile_atomic : public perf_counter
{
public:
    perf_counter_number_percentile_atomic(const char *app,
                                          const char *section,
                                          const char *name,
                                          dsn_perf_counter_type_t type,
                                          const char *dsptr,
                                          bool use_timer = true);

    ~perf_counter_number_percentile_atomic(void)
    {
        if (_timer) {
            _timer->cancel();
        }
    }

    virtual void increment() { CHECK(false, "invalid execution flow"); }
    virtual void decrement() { CHECK(false, "invalid execution flow"); }
    virtual void add(int64_t val) { CHECK(false, "invalid execution flow"); }
    virtual void set(int64_t val)
    {
        uint64_t idx = _tail.fetch_add(1, std::memory_order_relaxed);
        _samples[idx % MAX_QUEUE_LENGTH] = val;
    }

    virtual double get_value()
    {
        CHECK(false, "invalid execution flow");
        return 0.0;
    }
    virtual int64_t get_integer_value() { return (int64_t)get_value(); }

    virtual double get_percentile(dsn_perf_counter_percentile_type_t type)
    {
        if ((type < 0) || (type >= COUNTER_PERCENTILE_COUNT)) {
            CHECK(false, "send a wrong counter percentile type");
            return 0.0;
        }
        return (double)_results[type];
    }

    virtual int get_latest_samples(int required_sample_count,
                                   /*out*/ samples_t &samples) const override
    {
        CHECK_LE(required_sample_count, MAX_QUEUE_LENGTH);

        uint64_t count = _tail.load();
        int return_count = count >= (uint64_t)required_sample_count ? required_sample_count : count;

        samples.clear();
        int end_index = (count + MAX_QUEUE_LENGTH - 1) % MAX_QUEUE_LENGTH;
        int start_index = (end_index + MAX_QUEUE_LENGTH - return_count) % MAX_QUEUE_LENGTH;

        if (end_index >= start_index) {
            samples.push_back(std::make_pair((int64_t *)_samples + start_index, return_count));
        } else {
            samples.push_back(
                std::make_pair((int64_t *)_samples + start_index, MAX_QUEUE_LENGTH - start_index));
            samples.push_back(std::make_pair((int64_t *)_samples,
                                             return_count - (MAX_QUEUE_LENGTH - start_index)));
        }

        return return_count;
    }

    virtual int64_t get_latest_sample() const override
    {
        int idx = (_tail.load() + MAX_QUEUE_LENGTH - 1) % MAX_QUEUE_LENGTH;
        return _samples[idx];
    }

private:
    friend class perf_counter_nth_element_finder;

    struct compute_context
    {
        int64_t ask[COUNTER_PERCENTILE_COUNT];
        int64_t tmp[MAX_QUEUE_LENGTH];
        int64_t mid_tmp[MAX_QUEUE_LENGTH];
        int calc_queue[MAX_QUEUE_LENGTH][4];
    };

    void insert_calc_queue(const std::shared_ptr<compute_context> &ctx,
                           int left,
                           int right,
                           int qleft,
                           int qright,
                           int &calc_tail)
    {
        calc_tail++;
        ctx->calc_queue[calc_tail][_LEFT] = left;
        ctx->calc_queue[calc_tail][_RIGHT] = right;
        ctx->calc_queue[calc_tail][_QLEFT] = qleft;
        ctx->calc_queue[calc_tail][_QRIGHT] = qright;
        return;
    }

    int64_t find_mid(const std::shared_ptr<compute_context> &ctx, int left, int right)
    {
        if (left == right)
            return ctx->mid_tmp[left];

        for (int index = left; index < right; index += 5) {
            int remain_num = index + 5 >= right ? right - index + 1 : 5;
            for (int i = index; i < index + remain_num; i++) {
                int j;
                int64_t k = ctx->mid_tmp[i];
                for (j = i - 1; (j >= index) && (ctx->mid_tmp[j] > k); j--)
                    ctx->mid_tmp[j + 1] = ctx->mid_tmp[j];
                ctx->mid_tmp[j + 1] = k;
            }
            ctx->mid_tmp[(index - left) / 5] = ctx->mid_tmp[index + remain_num / 2];
        }

        return find_mid(ctx, 0, (right - left - 1) / 5);
    }

    void select(const std::shared_ptr<compute_context> &ctx,
                int left,
                int right,
                int qleft,
                int qright,
                int &calc_tail)
    {
        int i, j, index, now;
        int64_t mid;

        if (qleft > qright)
            return;

        if (left == right) {
            for (i = qleft; i <= qright; i++)
                if (ctx->ask[i] == 1) {
                    _results[i] = ctx->tmp[left];
                } else
                    CHECK(false, "select percentail wrong!!!");
            return;
        }

        for (i = left; i <= right; i++)
            ctx->mid_tmp[i] = ctx->tmp[i];
        mid = find_mid(ctx, left, right);

        for (index = left; index <= right; index++)
            if (ctx->tmp[index] == mid)
                break;

        ctx->tmp[index] = ctx->tmp[left];
        index = left;
        for (i = left, j = right; i <= j;) {
            while ((i <= j) && (ctx->tmp[j] > mid))
                j--;
            if (i <= j)
                ctx->tmp[index] = ctx->tmp[j], index = j--;
            while ((i <= j) && (ctx->tmp[i] < mid))
                i++;
            if (i <= j)
                ctx->tmp[index] = ctx->tmp[i], index = i++;
        }
        ctx->tmp[index] = mid;

        now = index - left + 1;
        for (i = qleft; (i <= qright) && (ctx->ask[i] < now); i++)
            ;
        for (j = i; j <= qright; j++)
            ctx->ask[j] -= now;
        for (j = i; (j <= qright) && (ctx->ask[j] == 0); j++)
            ctx->ask[j]++;
        insert_calc_queue(ctx, left, index - 1, qleft, i - 1, calc_tail);
        insert_calc_queue(ctx, index, index, i, j - 1, calc_tail);
        insert_calc_queue(ctx, index + 1, right, j, qright, calc_tail);
        return;
    }

    void calc(const std::shared_ptr<compute_context> &ctx)
    {
        uint64_t _num = _tail.load();
        if (_num > MAX_QUEUE_LENGTH)
            _num = MAX_QUEUE_LENGTH;

        if (_num == 0)
            return;
        for (int i = 0; i < _num; i++)
            ctx->tmp[i] = _samples[i];

        ctx->ask[COUNTER_PERCENTILE_50] = (int)(_num * 0.5) + 1;
        ctx->ask[COUNTER_PERCENTILE_90] = (int)(_num * 0.90) + 1;
        ctx->ask[COUNTER_PERCENTILE_95] = (int)(_num * 0.95) + 1;
        ctx->ask[COUNTER_PERCENTILE_99] = (int)(_num * 0.99) + 1;
        ctx->ask[COUNTER_PERCENTILE_999] = (int)(_num * 0.999) + 1;
        // must be sorted
        // std::sort(ctx->ask, ctx->ask + MAX_TYPE_NUMBER);

        int l, r = 0;

        insert_calc_queue(ctx, 0, _num - 1, 0, COUNTER_PERCENTILE_COUNT - 1, r);
        for (l = 1; l <= r; l++)
            select(ctx,
                   ctx->calc_queue[l][_LEFT],
                   ctx->calc_queue[l][_RIGHT],
                   ctx->calc_queue[l][_QLEFT],
                   ctx->calc_queue[l][_QRIGHT],
                   r);

        return;
    }

    void on_timer(std::shared_ptr<boost::asio::deadline_timer> timer,
                  const boost::system::error_code &ec);

    std::shared_ptr<boost::asio::deadline_timer> _timer;
    std::atomic<uint64_t> _tail; // should use unsigned int to avoid out of bound
    int64_t _samples[MAX_QUEUE_LENGTH];
    int64_t _results[COUNTER_PERCENTILE_COUNT];
};

#pragma pack(pop)
} // namespace dsn
