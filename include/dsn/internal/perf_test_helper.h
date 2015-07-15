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

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "perf.test.helper"

namespace dsn {
    namespace service {

        struct perf_test_opts
        {
            int  perf_test_rounds;            
            bool perf_test_concurrent;            
            std::vector<int> perf_test_payload_bytes;
            std::vector<int> perf_test_timeouts_ms;
        };

        CONFIG_BEGIN(perf_test_opts)
            CONFIG_FLD(int, perf_test_rounds, 10000)            
            CONFIG_FLD(bool, perf_test_concurrent, true)
            CONFIG_FLD_INT_LIST(perf_test_payload_bytes)
            CONFIG_FLD_INT_LIST(perf_test_timeouts_ms)
        CONFIG_END
        
        template<typename T>
        class perf_client_helper
        {
        protected:
            perf_client_helper()
            {
                auto cf = system::config();
                if (!read_config(cf, "task.default", _default_opts))
                {
                    dassert(false, "read configuration failed for section [task.default]");
                }

                if (_default_opts.perf_test_timeouts_ms.size() == 0)
                {
                    const int timeouts_ms[] = { 1, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000 };
                    for (size_t i = 0; i < sizeof(timeouts_ms) / sizeof(int); i++)
                        _default_opts.perf_test_timeouts_ms.push_back(timeouts_ms[i]);
                }

                if (_default_opts.perf_test_payload_bytes.size() == 0)
                {
                    const int payload_bytes[] = { 1, 1024, 1024*1024 };
                    for (size_t i = 0; i < sizeof(payload_bytes) / sizeof(int); i++)
                        _default_opts.perf_test_payload_bytes.push_back(payload_bytes[i]);
                }
            }

            struct perf_test_case
            {
                int  rounds;
                int  payload_bytes;
                bool concurrent;
                int  timeout_ms;

                // statistics 
                std::atomic<int> timeout_rounds;
                std::atomic<int> error_rounds;
                int    succ_rounds;                
                double succ_latency_avg_us;
                double succ_qps;
                int    min_latency_us;
                int    max_latency_us;

                perf_test_case& operator = (const perf_test_case& r)
                {
                    rounds = r.rounds;
                    payload_bytes = r.payload_bytes;
                    timeout_ms = r.timeout_ms;
                    concurrent = r.concurrent;

                    timeout_rounds.store(r.timeout_rounds.load());
                    error_rounds.store(r.error_rounds.load());
                    succ_rounds = r.succ_rounds;
                    succ_latency_avg_us = r.succ_latency_avg_us;
                    succ_qps = r.succ_qps;
                    min_latency_us = r.min_latency_us;
                    max_latency_us = r.max_latency_us;
                    return *this;
                }

                perf_test_case(const perf_test_case& r)
                {
                    *this = r;
                }

                perf_test_case()
                {}
            };

            struct perf_test_suite
            {
                const char* name;
                const char* config_section;
                std::function<void(int)> send_one;
                std::vector<perf_test_case> cases;
            };

            void load_suite_config(perf_test_suite& s)
            {
                perf_test_opts opt;
                auto cf = system::config();
                if (!read_config(cf, s.config_section, opt, &_default_opts))
                {
                    dassert(false, "read configuration failed for section [%s]", s.config_section);
                }

                s.cases.clear();
                for (auto& bytes : opt.perf_test_payload_bytes)
                {
                    int last_index = static_cast<int>(opt.perf_test_timeouts_ms.size()) - 1;                    
                    for (int i = last_index; i >= 0; i--)
                    {
                        perf_test_case c;
                        c.rounds = opt.perf_test_rounds;
                        c.payload_bytes = bytes;
                        c.timeout_ms = opt.perf_test_timeouts_ms[i];
                        c.concurrent = opt.perf_test_concurrent;
                        s.cases.push_back(c);
                    }
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
                if (id > _current_case->rounds)
                    return nullptr;

                ++_live_rpc_count;
                _rounds_latency_us[id - 1] = env::now_us();
                return (void*)(size_t)(id);
            }

            void end_send_one(void* context, error_code err)
            {
                int id = (int)(size_t)(context);
                int lr = --_live_rpc_count;
                int next = _current_case->concurrent ? 2 : 1;
                if (err != ERR_OK)
                {
                    if (err == ERR_TIMEOUT)
                        _current_case->timeout_rounds++;
                    else
                        _current_case->error_rounds++;

                    _rounds_latency_us[id - 1] = 0;

                    if (err == ERR_TIMEOUT)
                        next = (lr == 0 ? 1 : 0);
                }
                else
                {
                    _rounds_latency_us[id - 1] = env::now_us() - _rounds_latency_us[id - 1];
                }

                // if completed
                if (++_rounds_resp == _current_case->rounds)
                {
                    finalize_case();
                    return;
                }

                // conincontinue further waves
                for (int i = 0; i < next; i++)
                {
                    _suits[_current_suit_index].send_one(_current_case->payload_bytes);
                }
            }
            
        private:
            void finalize_case()
            {
                int sc = 0;
                double sum = 0.0;
                uint64_t lmin_us = 10000000000ULL;
                uint64_t lmax_us = 0;
                for (auto& t : _rounds_latency_us)
                {
                    if (t != 0)
                    {
                        sc++;
                        sum += static_cast<double>(t);

                        if (t < lmin_us)
                            lmin_us = t;
                        if (t > lmax_us)
                            lmax_us = t;
                    }
                }
                
                auto& suit = _suits[_current_suit_index];
                auto& cs = suit.cases[_current_case_index];
                cs.succ_rounds = cs.rounds - cs.timeout_rounds - cs.error_rounds;
                //dassert(cs.succ_rounds == sc, "cs.succ_rounds vs sc = %d vs %d", cs.succ_rounds, sc);

                cs.succ_latency_avg_us = sum / (double)sc;
                cs.succ_qps = (double)sc / ((double)(env::now_us() - _case_start_ts_us) / 1000.0 / 1000.0);
                cs.min_latency_us = static_cast<int>(lmin_us);
                cs.max_latency_us = static_cast<int>(lmax_us);

                std::stringstream ss;
                ss << "TEST " << _name
                    << ", tmo/err/suc(#): " << cs.timeout_rounds << "/" << cs.error_rounds << "/" << cs.succ_rounds
                    << ", latency(us): " << cs.succ_latency_avg_us << "(avg), "
                    << cs.min_latency_us << "(min), "
                    << cs.max_latency_us << "(max)"
                    << ", qps: " << cs.succ_qps << "#/s"
                    << ", timeout(ms) " << cs.timeout_ms
                    << ", payload(byte) " << cs.payload_bytes
                    ;

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
                    {
                        std::stringstream ss;

                        for (auto& s : _suits)
                        {
                            for (auto& cs : s.cases)
                            {
                                ss << "TEST " << s.name
                                    << ", timeout/err/succ: " << cs.timeout_rounds << "/" << cs.error_rounds << "/" << cs.succ_rounds
                                    << ", latency(us): " << cs.succ_latency_avg_us << "(avg), "
                                    << cs.min_latency_us << "(min), "
                                    << cs.max_latency_us << "(max)"
                                    << ", qps: " << cs.succ_qps << "#/s"
                                    << ", target timeout(ms) " << cs.timeout_ms
                                    << std::endl;
                            }
                        }

                        dwarn(ss.str().c_str());
                        return;
                    }
                }

                std::this_thread::sleep_for(std::chrono::seconds(2));

                // get next case
                auto& suit = _suits[_current_suit_index];
                auto& cs = suit.cases[_current_case_index];

                // setup for the case
                _name = suit.name;
                _timeout_ms = cs.timeout_ms;
                _current_case = &cs;
                cs.timeout_rounds = 0;
                cs.error_rounds = 0;
                _case_start_ts_us = env::now_us();

                _live_rpc_count = 0;
                _rounds_req = 0;
                _rounds_resp = 0;
                _rounds_latency_us.resize(cs.rounds, 0);

                // start
                suit.send_one(_current_case->payload_bytes);
            }

        private:
            perf_client_helper(const perf_client_helper&) = delete;
            
        protected:
            int              _timeout_ms;

        private:
            perf_test_opts   _default_opts;
            std::string      _name;            
            perf_test_case   *_current_case;

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

