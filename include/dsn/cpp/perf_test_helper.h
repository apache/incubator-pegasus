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

# include <dsn/service_api_cpp.h>
# include <dsn/cpp/utils.h>
# include <sstream>
# include <atomic>
# include <vector>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "perf.test.helper"

# define INVALID_DURATION_US 0xdeadbeefdeadbeefULL

namespace dsn {
    namespace service {

        struct perf_test_opts
        {
            int  perf_test_rounds;
            // perf_test_concurrency:
            //   - if perf_test_concurrency == 0, means concurrency grow exponentially.
            //   - if perf_test_concurrency >  0, means concurrency maintained to a fixed number.            
            std::vector<int> perf_test_concurrency;
            std::vector<int> perf_test_payload_bytes;
            std::vector<int> perf_test_timeouts_ms;
        };

        CONFIG_BEGIN(perf_test_opts)
            CONFIG_FLD(int, uint64, perf_test_rounds, 100, "how many rounds of test for $each item in concurrency$ rpc request")            
            CONFIG_FLD_INT_LIST(perf_test_concurrency, "concurrency list: 0 for expotentially growing concurrenty, >0 for fixed")
            CONFIG_FLD_INT_LIST(perf_test_payload_bytes, "size list: byte size of each rpc request test")
            CONFIG_FLD_INT_LIST(perf_test_timeouts_ms, "timeout list: timeout (ms) for each rpc call")
        CONFIG_END
        
        template<typename T>
        class perf_client_helper
        {
        protected:
            perf_client_helper()
            {
                if (!read_config("task..default", _default_opts))
                {
                    dassert(false, "read configuration failed for section [task..default]");
                }

                if (_default_opts.perf_test_concurrency.size() == 0)
                {
                    const int ccs[] = { 1, 100, 1000 };
                    for (size_t i = 0; i < sizeof(ccs) / sizeof(int); i++)
                        _default_opts.perf_test_concurrency.push_back(ccs[i]);
                }

                if (_default_opts.perf_test_timeouts_ms.size() == 0)
                {
                    const int timeouts_ms[] = { 1, 1000, 10000 };
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
                int  concurrency;
                int  timeout_ms;

                // statistics 
                std::atomic<int> timeout_rounds;
                std::atomic<int> error_rounds;
                int    succ_rounds;                
                double succ_latency_avg_ns;
                double succ_qps;
                int    min_latency_ns;
                int    max_latency_ns;

                perf_test_case& operator = (const perf_test_case& r)
                {
                    rounds = r.rounds;
                    payload_bytes = r.payload_bytes;
                    timeout_ms = r.timeout_ms;
                    concurrency = r.concurrency;

                    timeout_rounds.store(r.timeout_rounds.load());
                    error_rounds.store(r.error_rounds.load());
                    succ_rounds = r.succ_rounds;
                    succ_latency_avg_ns = r.succ_latency_avg_ns;
                    succ_qps = r.succ_qps;
                    min_latency_ns = r.min_latency_ns;
                    max_latency_ns = r.max_latency_ns;
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
                if (!read_config(s.config_section, opt, &_default_opts))
                {
                    dassert(false, "read configuration failed for section [%s]", s.config_section);
                }

                s.cases.clear();
                for (auto& bytes : opt.perf_test_payload_bytes)
                {
                    int last_index = static_cast<int>(opt.perf_test_timeouts_ms.size()) - 1;                    
                    for (int i = last_index; i >= 0; i--)
                    {
                        for (auto& cc : opt.perf_test_concurrency)
                        {
                            perf_test_case c;
                            c.rounds = opt.perf_test_rounds * cc;
                            c.payload_bytes = bytes;
                            c.timeout_ms = opt.perf_test_timeouts_ms[i];
                            c.concurrency = cc;
                            s.cases.push_back(c);
                        }
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
                _rounds_latency_rdtsc[id - 1] = ::dsn::utils::get_current_rdtsc();
                return (void*)(size_t)(id);
            }

            void end_send_one(void* context, error_code err)
            {
                int id = (int)(size_t)(context);
                int lr = --_live_rpc_count;
                if (err != ERR_OK)
                {
                    if (err == ERR_TIMEOUT)
                        _current_case->timeout_rounds++;
                    else
                        _current_case->error_rounds++;

                    _rounds_latency_rdtsc[id - 1] = INVALID_DURATION_US;
                }
                else
                {
                    auto dt = ::dsn::utils::get_current_rdtsc() - _rounds_latency_rdtsc[id - 1];
                    _rounds_latency_rdtsc[id - 1] = dt;

                    /*if (dt > 0)
                    {
                        dinfo("duration (us): %llu ", dt);
                    } */
                }

                // if completed
                if (++_rounds_resp == _current_case->rounds)
                {
                    finalize_case();
                    return;
                }

                // continue further waves
                if (_current_case->concurrency == 0)
                {
                    // exponentially increase
                    _suits[_current_suit_index].send_one(_current_case->payload_bytes);
                    _suits[_current_suit_index].send_one(_current_case->payload_bytes);
                }
                else
                {
                    // maintain fixed concurrent number
                    while (_rounds_req < _current_case->rounds
                        && _live_rpc_count < _current_case->concurrency)
                    {
                        _suits[_current_suit_index].send_one(_current_case->payload_bytes);
                    }
                }
            }
            
        private:
            void finalize_case()
            {
                int sc = 0;
                double sum = 0.0;
                uint64_t lmin_rdtsc = UINT64_MAX;
                uint64_t lmax_rdtsc = 0;
                uint64_t nts = dsn_now_ns();
                uint64_t nts_rdtsc = ::dsn::utils::get_current_rdtsc();
                double   rdtsc_per_ns = (double)(nts_rdtsc - _case_start_ts_rdtsc) / (double)(nts - _case_start_ts_ns);

                for (auto& t : _rounds_latency_rdtsc)
                {
                    if (t != INVALID_DURATION_US)
                    {
                        sc++;
                        sum += static_cast<double>(t);

                        if (t < lmin_rdtsc)
                            lmin_rdtsc = t;
                        if (t > lmax_rdtsc)
                            lmax_rdtsc = t;
                    }
                }
                
                auto& suit = _suits[_current_suit_index];
                auto& cs = suit.cases[_current_case_index];
                cs.succ_rounds = cs.rounds - cs.timeout_rounds - cs.error_rounds;
                //dassert(cs.succ_rounds == sc, "cs.succ_rounds vs sc = %d vs %d", cs.succ_rounds, sc);

                cs.succ_latency_avg_ns = sum / (double)sc;
                cs.succ_qps = (double)sc / ((double)(nts - _case_start_ts_ns) / 1000.0 / 1000.0 / 1000.0);
                cs.min_latency_ns = static_cast<int>((double)(lmin_rdtsc) / rdtsc_per_ns);
                cs.max_latency_ns = static_cast<int>((double)(lmax_rdtsc) / rdtsc_per_ns);

                std::stringstream ss;
                ss << "TEST " << _name
                    << ", tmo/err/suc(#): " << cs.timeout_rounds << "/" << cs.error_rounds << "/" << cs.succ_rounds
                    << ", latency(ns): " << cs.succ_latency_avg_ns << "(avg), "
                    << cs.min_latency_ns << "(min), "
                    << cs.max_latency_ns << "(max)"
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
                        ss << ">>>>>>>>>>>>>>>>>>>" << std::endl;

                        for (auto& s : _suits)
                        {
                            for (auto& cs : s.cases)
                            {
                                ss << "TEST " << s.name
                                   << ", rounds " << _current_case->rounds
                                   << ", concurrency " << _current_case->concurrency
                                   << ", timeout(ms) " << _current_case->timeout_ms
                                   << ", payload(byte) " << _current_case->payload_bytes
                                   << ", timeout/err/succ: " << cs.timeout_rounds << "/" << cs.error_rounds << "/" << cs.succ_rounds
                                   << ", latency(ns): " << cs.succ_latency_avg_ns << "(avg), "
                                   << cs.min_latency_ns << "(min), "
                                   << cs.max_latency_ns << "(max)"
                                   << ", qps: " << cs.succ_qps << "#/s"
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
                _case_start_ts_ns = dsn_now_ns();
                _case_start_ts_rdtsc = ::dsn::utils::get_current_rdtsc();

                _live_rpc_count = 0;
                _rounds_req = 0;
                _rounds_resp = 0;
                _rounds_latency_rdtsc.resize(cs.rounds, 0);

                std::stringstream ss;
                ss << "TEST " << _name
                   << ", rounds " << _current_case->rounds
                   << ", concurrency " << _current_case->concurrency
                   << ", timeout(ms) " << _current_case->timeout_ms
                   << ", payload(byte) " << _current_case->payload_bytes;
                dwarn(ss.str().c_str());

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
            std::vector<uint64_t> _rounds_latency_rdtsc;

            uint64_t         _case_start_ts_ns;
            uint64_t         _case_start_ts_rdtsc;

            std::vector<perf_test_suite> _suits;
            int                         _current_suit_index;
            int                         _current_case_index;
        };
    }
}

