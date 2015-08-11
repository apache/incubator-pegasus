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
# include <dsn\internal\env_provider.h>

namespace dsn {
    namespace tools {

#pragma pack(push)
#pragma pack(8)

        // -----------   NUMBER perf counter ---------------------------------
//# define PERFORMANCE_TEST
# define DIVIDE_CONTAINER 107
        class perf_counter_number : public perf_counter
        {
        public:
            perf_counter_number(const char *section, const char *name, perf_counter_type type)
                : perf_counter(section, name, type)
            {
                for (int i = 0; i < DIVIDE_CONTAINER; i++)
                {
                    #ifdef PERFORMANCE_TEST
                    _val[i] = 0;
                    #else
                    _val[i].store(0, std::memory_order_relaxed);
                    #endif
                }
            }
            ~perf_counter_number(void) {}

            virtual void   increment() 
            {
                uint64_t task_id = static_cast<int>(::GetCurrentThreadId());
                #ifdef PERFORMANCE_TEST
                _val[task_id % DIVIDE_CONTAINER]++;
                #else
                _val[task_id % DIVIDE_CONTAINER].fetch_add(1, std::memory_order_consume);
                #endif
            }
            virtual void   decrement()
            {
                uint64_t task_id = static_cast<int>(::GetCurrentThreadId());
                #ifdef PERFORMANCE_TEST
                _val[task_id % DIVIDE_CONTAINER]--;
                #else
                _val[task_id % DIVIDE_CONTAINER].fetch_sub(1, std::memory_order_consume);
                #endif
            }
            virtual void   add(uint64_t val) 
            {
                uint64_t task_id = static_cast<int>(::GetCurrentThreadId());
                #ifdef PERFORMANCE_TEST
                _val[task_id % DIVIDE_CONTAINER] += val;
                #else
                _val[task_id % DIVIDE_CONTAINER].fetch_add(val, std::memory_order_consume);
                #endif
            }
            virtual void   set(uint64_t val) { dassert(false, "invalid execution flow"); }
            virtual double get_value() 
            { 
                double val = 0;
                for (int i = 0; i < DIVIDE_CONTAINER; i++)
                {
                    #ifdef PERFORMANCE_TEST
                    val += _val[i];
                    #else
                    val += static_cast<double>(_val[i].load(std::memory_order_relaxed));
                    #endif
                }
                return val; 
            }
            virtual double get_percentile(counter_percentile_type type) { dassert(false, "invalid execution flow"); return 0.0; }

        private:
            #ifdef PERFORMANCE_TEST
            uint64_t              _val[DIVIDE_CONTAINER];
            #else
            std::atomic<uint64_t> _val[DIVIDE_CONTAINER];
            #endif
        };

        // -----------   RATE perf counter ---------------------------------

        class perf_counter_rate : public perf_counter
        {
        public:
            perf_counter_rate(const char *section, const char *name, perf_counter_type type)
                : perf_counter(section, name, type), _rate(0) 
            {
                _last_time = ::dsn::env_provider::get_current_physical_time_ns();
                for (int i = 0; i < DIVIDE_CONTAINER; i++)
                {
                    #ifdef PERFORMANCE_TEST
                    _val[i] = 0;
                    #else
                    _val[i].store(0, std::memory_order_relaxed);
                    #endif
                }
            }
            ~perf_counter_rate(void) {}

            virtual void   increment()
            {
                uint64_t task_id = static_cast<int>(::GetCurrentThreadId());
                #ifdef PERFORMANCE_TEST
                _val[task_id % DIVIDE_CONTAINER]++;
                #else
                _val[task_id % DIVIDE_CONTAINER].fetch_add(1, std::memory_order_consume);
                #endif
            }
            virtual void   decrement()
            {
                uint64_t task_id = static_cast<int>(::GetCurrentThreadId());
                #ifdef PERFORMANCE_TEST
                _val[task_id % DIVIDE_CONTAINER]--;
                #else
                _val[task_id % DIVIDE_CONTAINER].fetch_sub(1, std::memory_order_consume);
                #endif
            }
            virtual void   add(uint64_t val)
            {
                uint64_t task_id = static_cast<int>(::GetCurrentThreadId());
                #ifdef PERFORMANCE_TEST
                _val[task_id % DIVIDE_CONTAINER] += val;
                #else
                _val[task_id % DIVIDE_CONTAINER].fetch_add(val, std::memory_order_consume);
                #endif
            }
            virtual void   set(uint64_t val) { dassert(false, "invalid execution flow"); }
            virtual double get_value()
            {
                double val = 0;
                for (int i = 0; i < DIVIDE_CONTAINER; i++)
                {
                    #ifdef PERFORMANCE_TEST
                    val += _val[i];
                    #else
                    val += static_cast<double>(_val[i].load(std::memory_order_relaxed));
                    #endif
                }

                uint64_t now = ::dsn::env_provider::get_current_physical_time_ns();
                double interval = (now - _last_time) / 1e9;
                if (interval <= 0.1) 
                    return _rate;
                
                _last_time = now;
                for (int i = 0; i < DIVIDE_CONTAINER; i++)
                {
                    #ifdef PERFORMANCE_TEST
                    _val[i] = 0;
                    #else
                    _val[i].store(0, std::memory_order_relaxed);
                    #endif
                }

                _rate = val / interval;
                return _rate;
            }
            virtual double get_percentile(counter_percentile_type type) { dassert(false, "invalid execution flow"); return 0.0; }

        private:
            std::atomic<double> _rate;
            std::atomic<uint64_t> _last_time;
            #ifdef PERFORMANCE_TEST
            uint64_t              _val[DIVIDE_CONTAINER];
            #else
            std::atomic<uint64_t> _val[DIVIDE_CONTAINER];
            #endif
        };

        // -----------   NUMBER_PERCENTILE perf counter ---------------------------------

# define MAX_QUEUE_LENGTH 10000
# define _LEFT 0
# define _RIGHT 1
# define _QLEFT 2
# define _QRIGHT 3

        class perf_counter_number_percentile : public perf_counter
        {
        public:
            perf_counter_number_percentile(const char *section, const char *name, perf_counter_type type)
                : perf_counter(section, name, type)
            {
                _results[COUNTER_PERCENTILE_50] = 0;
                _results[COUNTER_PERCENTILE_90] = 0;
                _results[COUNTER_PERCENTILE_95] = 0;
                _results[COUNTER_PERCENTILE_99] = 0;
                _results[COUNTER_PERCENTILE_999] = 0;
                _tail = 0;

                _counter_computation_interval_seconds = config()->get_value<int>("components.simple_perf_counter", "counter_computation_interval_seconds", 30);
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
                if ((type < 0) || (type >= COUNTER_PERCENTILE_COUNT))
                {
                    dassert(false, "send a wrong counter percentile type");
                    return 0.0;
                }
                return (double)_results[type];
            }

        private:
            inline void insert_calc_queue(int left, int right, int qleft, int qright, int &calc_tail)
            {
                calc_tail++;
                _calc_queue[calc_tail][_LEFT] = left;
                _calc_queue[calc_tail][_RIGHT] = right;
                _calc_queue[calc_tail][_QLEFT] = qleft;
                _calc_queue[calc_tail][_QRIGHT] = qright;
                return;
            }

            uint64_t find_mid(int left, int right)
            {
                if (left == right)
                    return _mid_tmp[left];

                for (int index = left; index < right; index += 5)
                {
                    int remain_num = index + 5 >= right ? right - index + 1 : 5;
                    for (int i = index; i < index + remain_num; i++)
                    {
                        int j;
                        uint64_t k = _mid_tmp[i];
                        for (j = i - 1; (j >= index) && (_mid_tmp[j] > k); j--)
                            _mid_tmp[j + 1] = _mid_tmp[j];
                        _mid_tmp[j + 1] = k;
                    }
                    _mid_tmp[(index - left) / 5] = _mid_tmp[index + remain_num / 2];
                }

                return find_mid(0, (right - left - 1) / 5);
            }

            inline void select(int left, int right, int qleft, int qright, int &calc_tail)
            {
                int i, j, index, now;
                uint64_t mid;

                if (qleft > qright)
                    return;

                if (left == right)
                {
                    for (i = qleft; i <= qright; i++)
                    if (_ask[i] == 1)
                        _results[i] = _tmp[left];
                    else
                        dassert(false, "select percentail wrong!!!");
                    return;
                }

                for (i = left; i <= right; i++)
                    _mid_tmp[i] = _tmp[i];
                mid = find_mid(left, right);

                for (index = left; index <= right; index++)
                if (_tmp[index] == mid)
                    break;

                _tmp[index] = _tmp[left];
                index = left;
                for (i = left, j = right; i <= j;)
                {
                    while ((i <= j) && (_tmp[j] > mid)) j--;
                    if (i <= j) _tmp[index] = _tmp[j], index = j--;
                    while ((i <= j) && (_tmp[i] < mid)) i++;
                    if (i <= j) _tmp[index] = _tmp[i], index = i++;
                }
                _tmp[index] = mid;

                now = index - left + 1;
                for (i = qleft; (i <= qright) && (_ask[i] < now); i++);
                for (j = i; j <= qright; j++) _ask[j] -= now;
                for (j = i; (j <= qright) && (_ask[j] == 0); j++) _ask[j]++;
                insert_calc_queue(left, index - 1, qleft, i - 1, calc_tail);
                insert_calc_queue(index, index, i, j - 1, calc_tail);
                insert_calc_queue(index + 1, right, j, qright, calc_tail);
                return;
            }

            void   calc()
            {
                uint64_t _num = _tail > MAX_QUEUE_LENGTH ? MAX_QUEUE_LENGTH : _tail;

                if (_num == 0)
                    return;
                for (int i = 0; i < _num; i++)
                    _tmp[i] = _samples[i];

                _ask[COUNTER_PERCENTILE_50] = (int)(_num * 0.5) + 1;
                _ask[COUNTER_PERCENTILE_90] = (int)(_num * 0.90) + 1;
                _ask[COUNTER_PERCENTILE_95] = (int)(_num * 0.95) + 1;
                _ask[COUNTER_PERCENTILE_99] = (int)(_num * 0.99) + 1;
                _ask[COUNTER_PERCENTILE_999] = (int)(_num * 0.999) + 1;
                // must be sorted
                // std::sort(ctx->ask, ctx->ask + MAX_TYPE_NUMBER);

                int l, r = 0;

                insert_calc_queue(0, _num - 1, 0, COUNTER_PERCENTILE_COUNT - 1, r);
                for (l = 1; l <= r; l++)
                    select(_calc_queue[l][_LEFT], _calc_queue[l][_RIGHT], _calc_queue[l][_QLEFT], _calc_queue[l][_QRIGHT], r);

                return;
            }

            void on_timer(const boost::system::error_code& ec)
            {
                if (!ec)
                {
                    calc();

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
            int _tail;
            uint64_t _samples[MAX_QUEUE_LENGTH];
            uint64_t _results[COUNTER_PERCENTILE_COUNT];
            int      _counter_computation_interval_seconds;

            uint64_t _ask[COUNTER_PERCENTILE_COUNT];
            uint64_t _tmp[MAX_QUEUE_LENGTH];
            uint64_t _mid_tmp[MAX_QUEUE_LENGTH];
            int      _calc_queue[MAX_QUEUE_LENGTH / 20][4];
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

#pragma pack(pop)

    }
}
