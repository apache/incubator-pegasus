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
 *     Performance counter ver.atomic
 *     Using devided container to improve efficiency
 *     Using atomic to store values in Number and Rate type counters
 *     This version has higher precision but lower efficiency than ver.faster
 *
 * Revision history:
 *     2015-08-17, zjc95, first version
 *     2015-11-24, zjc95, revised the decription
 *
 */

#include "simple_perf_counter_v2_atomic.h"
#include "shared_io_service.h"

namespace dsn {
namespace tools {

#pragma pack(push)
#pragma pack(8)

// -----------   NUMBER perf counter ---------------------------------
//# define PERFORMANCE_TEST
#define DIVIDE_CONTAINER 107
class perf_counter_number_v2_atomic : public perf_counter
{
public:
    perf_counter_number_v2_atomic(const char *app,
                                  const char *section,
                                  const char *name,
                                  dsn_perf_counter_type_t type,
                                  const char *dsptr)
        : perf_counter(app, section, name, type, dsptr)
    {
        for (int i = 0; i < DIVIDE_CONTAINER; i++) {
            _val[i].store(0, std::memory_order_relaxed);
        }
    }
    ~perf_counter_number_v2_atomic(void) {}

    virtual void increment()
    {
        uint64_t task_id = static_cast<int>(::dsn::utils::get_current_tid());
        _val[task_id % DIVIDE_CONTAINER].fetch_add(1, std::memory_order_relaxed);
    }
    virtual void decrement()
    {
        uint64_t task_id = static_cast<int>(::dsn::utils::get_current_tid());
        _val[task_id % DIVIDE_CONTAINER].fetch_sub(1, std::memory_order_relaxed);
    }
    virtual void add(uint64_t val)
    {
        uint64_t task_id = static_cast<int>(::dsn::utils::get_current_tid());
        _val[task_id % DIVIDE_CONTAINER].fetch_add(val, std::memory_order_relaxed);
    }
    virtual void set(uint64_t val)
    {
        uint64_t task_id = static_cast<int>(::dsn::utils::get_current_tid());
        _val[task_id % DIVIDE_CONTAINER] = val;
    }
    virtual double get_value()
    {
        double val = 0;
        for (int i = 0; i < DIVIDE_CONTAINER; i++) {
            val += static_cast<double>(_val[i].load(std::memory_order_relaxed));
        }
        return val;
    }
    virtual uint64_t get_integer_value()
    {
        uint64_t val = 0;
        for (int i = 0; i < DIVIDE_CONTAINER; i++) {
            val += _val[i].load(std::memory_order_relaxed);
        }
        return val;
    }
    virtual double get_percentile(dsn_perf_counter_percentile_type_t type)
    {
        dassert(false, "invalid execution flow");
        return 0.0;
    }

protected:
    std::atomic<uint64_t> _val[DIVIDE_CONTAINER];
};

// -----------   VOLATILE_NUMBER perf counter ---------------------------------
class perf_counter_volatile_number_v2_atomic : public perf_counter_number_v2_atomic
{
public:
    perf_counter_volatile_number_v2_atomic(const char *app,
                                           const char *section,
                                           const char *name,
                                           dsn_perf_counter_type_t type,
                                           const char *dsptr)
        : perf_counter_number_v2_atomic(app, section, name, type, dsptr)
    {
    }
    ~perf_counter_volatile_number_v2_atomic(void) {}

    virtual double get_value()
    {
        double val = 0;
        for (int i = 0; i < DIVIDE_CONTAINER; i++) {
            val += static_cast<double>(_val[i].exchange(0, std::memory_order_relaxed));
        }
        return val;
    }
    virtual uint64_t get_integer_value()
    {
        uint64_t val = 0;
        for (int i = 0; i < DIVIDE_CONTAINER; i++) {
            val += _val[i].exchange(0, std::memory_order_relaxed);
        }
        return val;
    }
};

// -----------   RATE perf counter ---------------------------------

class perf_counter_rate_v2_atomic : public perf_counter
{
public:
    perf_counter_rate_v2_atomic(const char *app,
                                const char *section,
                                const char *name,
                                dsn_perf_counter_type_t type,
                                const char *dsptr)
        : perf_counter(app, section, name, type, dsptr), _rate(0)
    {
        _last_time = ::dsn::utils::get_current_physical_time_ns();
        for (int i = 0; i < DIVIDE_CONTAINER; i++) {
            _val[i].store(0, std::memory_order_relaxed);
        }
    }
    ~perf_counter_rate_v2_atomic(void) {}

    virtual void increment()
    {
        uint64_t task_id = static_cast<int>(::dsn::utils::get_current_tid());
        _val[task_id % DIVIDE_CONTAINER].fetch_add(1, std::memory_order_relaxed);
    }
    virtual void decrement()
    {
        uint64_t task_id = static_cast<int>(::dsn::utils::get_current_tid());
        _val[task_id % DIVIDE_CONTAINER].fetch_sub(1, std::memory_order_relaxed);
    }
    virtual void add(uint64_t val)
    {
        uint64_t task_id = static_cast<int>(::dsn::utils::get_current_tid());
        _val[task_id % DIVIDE_CONTAINER].fetch_add(val, std::memory_order_relaxed);
    }
    virtual void set(uint64_t val) { dassert(false, "invalid execution flow"); }
    virtual double get_value()
    {
        uint64_t now = ::dsn::utils::get_current_physical_time_ns();
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
    virtual uint64_t get_integer_value() { return (uint64_t)get_value(); }
    virtual double get_percentile(dsn_perf_counter_percentile_type_t type)
    {
        dassert(false, "invalid execution flow");
        return 0.0;
    }

private:
    std::atomic<double> _rate;
    std::atomic<uint64_t> _last_time;
    std::atomic<uint64_t> _val[DIVIDE_CONTAINER];
};

// -----------   NUMBER_PERCENTILE perf counter ---------------------------------

#define MAX_QUEUE_LENGTH 10000
#define _LEFT 0
#define _RIGHT 1
#define _QLEFT 2
#define _QRIGHT 3

class perf_counter_number_percentile_v2_atomic : public perf_counter
{
public:
    perf_counter_number_percentile_v2_atomic(const char *app,
                                             const char *section,
                                             const char *name,
                                             dsn_perf_counter_type_t type,
                                             const char *dsptr)
        : perf_counter(app, section, name, type, dsptr), _tail(0)
    {
        _results[COUNTER_PERCENTILE_50] = 0;
        _results[COUNTER_PERCENTILE_90] = 0;
        _results[COUNTER_PERCENTILE_95] = 0;
        _results[COUNTER_PERCENTILE_99] = 0;
        _results[COUNTER_PERCENTILE_999] = 0;

        _counter_computation_interval_seconds = (int)dsn_config_get_value_uint64(
            "components.simple_perf_counter_v2_atomic",
            "counter_computation_interval_seconds",
            30,
            "period (seconds) the system computes the percentiles of the counters");
        _timer.reset(new boost::asio::deadline_timer(shared_io_service::instance().ios));
        _timer->expires_from_now(
            boost::posix_time::seconds(rand() % _counter_computation_interval_seconds + 1));
        this->add_ref();
        _timer->async_wait(std::bind(&perf_counter_number_percentile_v2_atomic::on_timer,
                                     this,
                                     _timer,
                                     std::placeholders::_1));
    }

    ~perf_counter_number_percentile_v2_atomic(void) { _timer->cancel(); }

    virtual void increment() { dassert(false, "invalid execution flow"); }
    virtual void decrement() { dassert(false, "invalid execution flow"); }
    virtual void add(uint64_t val) { dassert(false, "invalid execution flow"); }
    virtual void set(uint64_t val)
    {
        uint64_t idx = _tail++;
        _samples[idx % MAX_QUEUE_LENGTH] = val;
    }

    virtual double get_value()
    {
        dassert(false, "invalid execution flow");
        return 0.0;
    }
    virtual uint64_t get_integer_value() { return (uint64_t)get_value(); }
    virtual double get_percentile(dsn_perf_counter_percentile_type_t type)
    {
        if ((type < 0) || (type >= COUNTER_PERCENTILE_COUNT)) {
            dassert(false, "send a wrong counter percentile type");
            return 0.0;
        }
        return (double)_results[type];
    }

    virtual int get_latest_samples(int required_sample_count,
                                   /*out*/ samples_t &samples) const override
    {
        dassert(required_sample_count <= MAX_QUEUE_LENGTH, "");

        uint64_t count = _tail;
        int return_count = count >= required_sample_count ? required_sample_count : count;

        samples.clear();
        int end_index = (count + MAX_QUEUE_LENGTH - 1) % MAX_QUEUE_LENGTH;
        int start_index = (end_index + MAX_QUEUE_LENGTH - return_count) % MAX_QUEUE_LENGTH;

        if (end_index >= start_index) {
            samples.push_back(std::make_pair((uint64_t *)_samples + start_index, return_count));
        } else {
            samples.push_back(
                std::make_pair((uint64_t *)_samples + start_index, MAX_QUEUE_LENGTH - start_index));
            samples.push_back(std::make_pair((uint64_t *)_samples,
                                             return_count - (MAX_QUEUE_LENGTH - start_index)));
        }

        return return_count;
    }

    virtual uint64_t get_latest_sample() const override
    {
        int idx = (_tail + MAX_QUEUE_LENGTH - 1) % MAX_QUEUE_LENGTH;
        return _samples[idx];
    }

private:
    struct compute_context
    {
        uint64_t ask[COUNTER_PERCENTILE_COUNT];
        uint64_t tmp[MAX_QUEUE_LENGTH];
        uint64_t mid_tmp[MAX_QUEUE_LENGTH];
        int calc_queue[MAX_QUEUE_LENGTH][4];
    };

    inline void insert_calc_queue(boost::shared_ptr<compute_context> &ctx,
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

    uint64_t find_mid(boost::shared_ptr<compute_context> &ctx, int left, int right)
    {
        if (left == right)
            return ctx->mid_tmp[left];

        for (int index = left; index < right; index += 5) {
            int remain_num = index + 5 >= right ? right - index + 1 : 5;
            for (int i = index; i < index + remain_num; i++) {
                int j;
                uint64_t k = ctx->mid_tmp[i];
                for (j = i - 1; (j >= index) && (ctx->mid_tmp[j] > k); j--)
                    ctx->mid_tmp[j + 1] = ctx->mid_tmp[j];
                ctx->mid_tmp[j + 1] = k;
            }
            ctx->mid_tmp[(index - left) / 5] = ctx->mid_tmp[index + remain_num / 2];
        }

        return find_mid(ctx, 0, (right - left - 1) / 5);
    }

    inline void select(boost::shared_ptr<compute_context> &ctx,
                       int left,
                       int right,
                       int qleft,
                       int qright,
                       int &calc_tail)
    {
        int i, j, index, now;
        uint64_t mid;

        if (qleft > qright)
            return;

        if (left == right) {
            for (i = qleft; i <= qright; i++)
                if (ctx->ask[i] == 1)
                    _results[i] = ctx->tmp[left];
                else
                    dassert(false, "select percentail wrong!!!");
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
        for (i = qleft; (i <= qright) && (ctx->ask[i] < (uint64_t)now); i++)
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

    void calc(boost::shared_ptr<compute_context> &ctx)
    {
        int _num = _tail > MAX_QUEUE_LENGTH ? MAX_QUEUE_LENGTH : _tail;

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
                  const boost::system::error_code &ec)
    {
        // as the callback is not in tls context, so the log system calls like ddebug, dassert will
        // cause a lock
        if (!ec) {
            // only when others also hold the reference
            if (this->get_count() > 1) {
                boost::shared_ptr<compute_context> ctx(new compute_context());
                calc(ctx);

                timer->expires_from_now(
                    boost::posix_time::seconds(_counter_computation_interval_seconds));
                this->add_ref();
                timer->async_wait(std::bind(&perf_counter_number_percentile_v2_atomic::on_timer,
                                            this,
                                            timer,
                                            std::placeholders::_1));
            }
        } else if (boost::system::errc::operation_canceled != ec) {
            dassert(false, "on_timer error!!!");
        }

        this->release_ref();
    }

    std::shared_ptr<boost::asio::deadline_timer> _timer;
    uint64_t _tail;
    uint64_t _samples[MAX_QUEUE_LENGTH];
    uint64_t _results[COUNTER_PERCENTILE_COUNT];
    int _counter_computation_interval_seconds;
};

// ---------------------- perf counter dispatcher ---------------------

perf_counter *simple_perf_counter_v2_atomic_factory(const char *app,
                                                    const char *section,
                                                    const char *name,
                                                    dsn_perf_counter_type_t type,
                                                    const char *dsptr)
{
    if (type == dsn_perf_counter_type_t::COUNTER_TYPE_NUMBER)
        return new perf_counter_number_v2_atomic(app, section, name, type, dsptr);
    else if (type == dsn_perf_counter_type_t::COUNTER_TYPE_VOLATILE_NUMBER)
        return new perf_counter_volatile_number_v2_atomic(app, section, name, type, dsptr);
    else if (type == dsn_perf_counter_type_t::COUNTER_TYPE_RATE)
        return new perf_counter_rate_v2_atomic(app, section, name, type, dsptr);
    else if (type == dsn_perf_counter_type_t::COUNTER_TYPE_NUMBER_PERCENTILES)
        return new perf_counter_number_percentile_v2_atomic(app, section, name, type, dsptr);
    else {
        dassert(false, "invalid type(%d)", type);
        return nullptr;
    }
}

#pragma pack(pop)
}
}
