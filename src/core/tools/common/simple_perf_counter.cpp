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
# include "simple_perf_counter.h"
# include "shared_io_service.h"

namespace dsn {
    namespace tools {

        // -----------   NUMBER perf counter ---------------------------------

        class perf_counter_number : public perf_counter
        {
        public:
            perf_counter_number(const char *section, const char *name, perf_counter_type type)
                : perf_counter(section, name, type), _val(0){}
            ~perf_counter_number(void) {}

            virtual void   increment() { _val++; }
            virtual void   decrement() { _val--; }
            virtual void   add(uint64_t val) { _val += val; }
            virtual void   set(uint64_t val) { dassert(false, "invalid execution flow"); }
            virtual double get_value() { return static_cast<double>(_val.load()); }
            virtual double get_percentile(counter_percentile_type type) { dassert(false, "invalid execution flow"); return 0.0; }

        private:
            std::atomic<uint64_t> _val;
        };

        // -----------   RATE perf counter ---------------------------------

        class perf_counter_rate : public perf_counter
        {
        public:
            perf_counter_rate(const char *section, const char *name, perf_counter_type type)
                : perf_counter(section, name, type), _val(0)
            {
                qts = 0;
            }
            ~perf_counter_rate(void) {}

            virtual void   increment() { _val++; }
            virtual void   decrement() { _val--; }
            virtual void   add(uint64_t val) { _val += val; }
            virtual void   set(uint64_t val) { dassert(false, "invalid execution flow"); }
            virtual double get_value()
            {
                uint64_t now = dsn_now_ns();
                uint64_t interval = now - qts;
                double val = static_cast<double>(_val.load());
                qts = now;
                _val = 0;
                return val / interval * 1000 * 1000 * 1000;
            }
            virtual double get_percentile(counter_percentile_type type) { dassert(false, "invalid execution flow"); return 0.0; }

        private:
            std::atomic<uint64_t> _val;
            std::atomic<uint64_t> qts;
        };

        // -----------   NUMBER_PERCENTILE perf counter ---------------------------------

        # define MAX_QUEUE_LENGTH 50000
        # define _LEFT 0
        # define _RIGHT 1
        # define _QLEFT 2
        # define _QRIGHT 3

        class perf_counter_number_percentile : public perf_counter
        {
        public:
            perf_counter_number_percentile(const char *section, const char *name, perf_counter_type type)
                : perf_counter(section, name, type), _tail(0)
            {
                _counter_computation_interval_seconds = config()->get_value<int>(
                    "components.simple_perf_counter", 
                    "counter_computation_interval_seconds",
                    30,
                    "period (seconds) the system computes the percentiles of the counters");

                _timer.reset(new boost::asio::deadline_timer(shared_io_service::instance().ios));
                _timer->expires_from_now(boost::posix_time::seconds(rand() % _counter_computation_interval_seconds + 1));
                _timer->async_wait(std::bind(&perf_counter_number_percentile::on_timer, this, std::placeholders::_1));
            }
            
            ~perf_counter_number_percentile(void) 
            {
                _timer->cancel();
            }

            virtual void   increment() { dassert(false, "invalid execution flow"); }
            virtual void   decrement() { dassert(false, "invalid execution flow"); }
            virtual void   add(uint64_t val) { dassert(false, "invalid execution flow"); }
            virtual void   set(uint64_t val)
            {
                auto idx = _tail++;
                _samples[idx % MAX_QUEUE_LENGTH] = val;
            }

            virtual double get_value() { dassert(false, "invalid execution flow");  return 0.0; }

            virtual double get_percentile(counter_percentile_type type)
            {
                if (_tail == 0)
                    return -1.0;
                if ((type < 0) || (type >= COUNTER_PERCENTILE_COUNT))
                {
                    dassert(false, "send a wrong counter percentile type");
                    return -1;
                }
                return (double)_results[type];
            }

        private:
            struct compute_context
            {
                uint64_t ask[COUNTER_PERCENTILE_COUNT];
                uint64_t tmp[MAX_QUEUE_LENGTH];
                uint64_t mid_tmp[MAX_QUEUE_LENGTH];
                int      calc_queue[MAX_QUEUE_LENGTH][4];
            };

        private:
            inline void insert_calc_queue(boost::shared_ptr<compute_context>& ctx, int left, int right, int qleft, int qright, int &calc_tail)
            {
                calc_tail++;
                ctx->calc_queue[calc_tail][_LEFT] = left;
                ctx->calc_queue[calc_tail][_RIGHT] = right;
                ctx->calc_queue[calc_tail][_QLEFT] = qleft;
                ctx->calc_queue[calc_tail][_QRIGHT] = qright;
                return;
            }

            uint64_t find_mid(boost::shared_ptr<compute_context>& ctx, int left, int right)
            {
                if (left == right)
                    return ctx->mid_tmp[left];

                int index;
                for (index = left; index < right; index += 5)
                {
                    int remain_num = index + 5 >= right ? right - index + 1 : 5;
                    for (int i = index; i < index + remain_num; i++)
                    {
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

            inline void select(boost::shared_ptr<compute_context>& ctx, int left, int right, int qleft, int qright, int &calc_tail)
            {
                int i, j, index, now;
                uint64_t mid;

                if (qleft > qright)
                    return;

                if (left == right)
                {
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
                for (i = left, j = right; i <= j;)
                {
                    while ((i <= j) && (ctx->tmp[j] > mid)) j--;
                    if (i <= j) ctx->tmp[index] = ctx->tmp[j], index = j--;
                    while ((i <= j) && (ctx->tmp[i] < mid)) i++;
                    if (i <= j) ctx->tmp[index] = ctx->tmp[i], index = i++;
                }
                ctx->tmp[index] = mid;

                now = index - left + 1;
                for (i = qleft; (i <= qright) && (ctx->ask[i] < now); i++);
                for (j = i; j <= qright; j++) ctx->ask[j] -= now;
                for (j = i; (j <= qright) && (ctx->ask[j] == 0); j++) ctx->ask[j]++;
                insert_calc_queue(ctx, left, index - 1, qleft, i - 1, calc_tail);
                insert_calc_queue(ctx, index, index, i, j - 1, calc_tail);
                insert_calc_queue(ctx, index + 1, right, j, qright, calc_tail);
                return;
            }

            void   calc(boost::shared_ptr<compute_context>& ctx)
            {
                if (_tail == 0)
                    return;

                int tmp_num = _tail > MAX_QUEUE_LENGTH ? MAX_QUEUE_LENGTH : _tail.load();
                for (int i = 0; i < tmp_num; i++)
                    ctx->tmp[i] = _samples[i];

                ctx->ask[COUNTER_PERCENTILE_50] = (int)(tmp_num * 0.5) + 1;
                ctx->ask[COUNTER_PERCENTILE_90] = (int)(tmp_num * 0.90) + 1;
                ctx->ask[COUNTER_PERCENTILE_95] = (int)(tmp_num * 0.95) + 1;
                ctx->ask[COUNTER_PERCENTILE_99] = (int)(tmp_num * 0.99) + 1;
                ctx->ask[COUNTER_PERCENTILE_999] = (int)(tmp_num * 0.999) + 1;
                // must be sorted
                // std::sort(ctx->ask, ctx->ask + MAX_TYPE_NUMBER);

                int l, r = 0;

                insert_calc_queue(ctx, 0, tmp_num - 1, 0, COUNTER_PERCENTILE_COUNT - 1, r);
                for (l = 1; l <= r; l++)
                    select(ctx, ctx->calc_queue[l][_LEFT], ctx->calc_queue[l][_RIGHT], ctx->calc_queue[l][_QLEFT], ctx->calc_queue[l][_QRIGHT], r);

                return;
            }

            void on_timer(const boost::system::error_code& ec)
            {
                if (!ec)
                {
                    boost::shared_ptr<compute_context> ctx(new compute_context());
                    calc(ctx);

                    _timer.reset(new boost::asio::deadline_timer(shared_io_service::instance().ios));
                    _timer->expires_from_now(boost::posix_time::seconds(_counter_computation_interval_seconds));
                    _timer->async_wait(std::bind(&perf_counter_number_percentile::on_timer, this, std::placeholders::_1));
                }
                else
                {
                    dassert(false, "on _timer error!!!");
                }
            }

            std::shared_ptr<boost::asio::deadline_timer> _timer;
            std::atomic<int> _tail;
            uint64_t _samples[MAX_QUEUE_LENGTH];
            uint64_t _results[COUNTER_PERCENTILE_COUNT];
            int      _counter_computation_interval_seconds;
        };

        // ---------------------- perf counter dispatcher ---------------------

        simple_perf_counter::simple_perf_counter(const char *section, const char *name, perf_counter_type type)
            : perf_counter(section, name, type)
        {
            if (type == perf_counter_type::COUNTER_TYPE_NUMBER)
                _counter_impl = new perf_counter_number(section, name, type);
            else if (type == perf_counter_type::COUNTER_TYPE_RATE)
                _counter_impl = new perf_counter_rate(section, name, type);
            else
                _counter_impl = new perf_counter_number_percentile(section, name, type);
        }

        simple_perf_counter::~simple_perf_counter(void) 
        {
            delete _counter_impl;
        }
    }
}
