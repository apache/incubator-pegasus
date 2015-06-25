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
# pragma once

# include <dsn/service_api.h>
# include <dsn/internal/logging.h>
# include <sstream>
# include <atomic>
# include <vector>

# ifndef __TITLE__
# define __TITLE__ "perf.test.helper"
# endif

namespace dsn {
    namespace service {

        template<typename T>
        class perf_client_helper
        {
        protected:
            struct perf_test_case
            {
                int rounds;
                int timeout_ms;
            };

            struct perf_test_suite
            {
                const char* name;
                std::function<void()> start;
                std::vector<perf_test_case> cases;
            };

            void load_suite_config(perf_test_suite& s)
            {
                // TODO: load from configuration files
                int timeouts_ms[] = { 1, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000 };
                int rounds = 1000;

                for (size_t i = 0; i < sizeof(timeouts_ms) / sizeof(int); i++)
                {
                    perf_test_case c;
                    c.rounds = rounds;
                    c.timeout_ms = timeouts_ms[i];
                    s.cases.push_back(c);
                }
            }

            void start(const std::vector<perf_test_suite>& suits)
            {
                _suits = suits;
                _current_suit_index = -1;
                _current_case_index = 0xffffff;

                start_next_case();
            }

            void* prepare_send_one()
            {
                int id = ++_rounds_req;
                if (id > _rounds)
                    return nullptr;

                ++_live_rpc_count;
                _rounds_latency_us[id - 1] = env::now_us();
                return (void*)(size_t)(id);
            }

            void end_send_one(void* context, error_code err, std::function<void()> send_one)
            {
                int id = (int)(size_t)(context);
                int lr = --_live_rpc_count;
                int next = 2;
                if (err == ERR_TIMEOUT)
                {
                    _rounds_latency_us[id - 1] = 0;
                    next = (lr == 0 ? 1 : 0);
                }
                else
                {
                    _rounds_latency_us[id - 1] = env::now_us() - _rounds_latency_us[id - 1];
                }

                // if completed
                if (++_rounds_resp == _rounds)
                {
                    finalize_case();
                    return;
                }

                // conincontinue further waves
                for (int i = 0; i < next; i++)
                {
                    send_one();
                }
            }
            
        private:
            void finalize_case()
            {
                int non_timeouts = 0;
                double sum = 0.0;
                for (auto& t : _rounds_latency_us)
                {
                    if (t != 0)
                    {
                        non_timeouts++;
                        sum += static_cast<double>(t);
                    }
                }

                std::stringstream ss;
                ss << "rpc test " << _name << " w/ target timeout " << _timeout_ms << " ms for " << _rounds
                    << " rounds, timeout: " << (_rounds - non_timeouts)
                    << ", non-timeout avg latency: " << sum / 1000.0 / (double)non_timeouts << " ms"
                    << ", qps = " << (double)non_timeouts/((double)(env::now_us() - _case_start_ts_us) / 1000.0 / 1000.0) << "#/s";

                dwarn(ss.str().c_str());

                start_next_case();
            }


            void start_next_case()
            {
                ++_current_case_index;

                // switch to next suit
                if (_current_suit_index == -1
                    || _current_case_index >= (int)_suits[_current_suit_index].cases.size())
                {
                    _current_suit_index++;
                    _current_case_index = 0;

                    if (_current_suit_index >= (int)_suits.size())
                        return;
                }

                // get next case
                auto& suit = _suits[_current_suit_index];
                auto& cs = suit.cases[_current_case_index];

                // setup for the case
                _name = suit.name;
                _timeout_ms = cs.timeout_ms;
                _rounds = cs.rounds;
                _case_start_ts_us = env::now_us();

                _live_rpc_count = 0;
                _rounds_req = 0;
                _rounds_resp = 0;
                _rounds_latency_us.resize(_rounds, 0);

                // start
                suit.start();
            }

            
        protected:
            int              _timeout_ms;

        private:
            std::string      _name;            
            int              _rounds;
            std::atomic<int> _live_rpc_count;
            std::atomic<int> _rounds_req;
            std::atomic<int> _rounds_resp;
            std::vector<uint64_t> _rounds_latency_us;

            uint64_t         _case_start_ts_us;

            std::vector<perf_test_suite> _suits;
            int                         _current_suit_index;
            int                         _current_case_index;
        };
    }
}

