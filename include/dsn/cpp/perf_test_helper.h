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
 *     helper class for automated performance test in zion
 *
 * Revision history:
 *     Aug., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/service_api_cpp.h>
#include <dsn/utility/utils.h>
#include <sstream>
#include <atomic>
#include <vector>

#define INVALID_DURATION_US 0xdeadbeefdeadbeefULL

namespace dsn {
namespace service {

struct perf_test_opts
{
    int perf_test_seconds;
    int perf_test_key_space_size;
    // perf_test_concurrency:
    //   - if perf_test_concurrency == 0, means concurrency grow exponentially.
    //   - if perf_test_concurrency >  0, means concurrency maintained to a fixed number.
    std::vector<int> perf_test_concurrency;
    std::vector<int> perf_test_payload_bytes;
    std::vector<int> perf_test_timeouts_ms;
    std::vector<int> perf_test_hybrid_request_ratio; // e.g., 1,1,1
};

CONFIG_BEGIN(perf_test_opts)
CONFIG_FLD(int, uint64, perf_test_seconds, 10, "how long for each test case")
CONFIG_FLD(
    int, uint64, perf_test_key_space_size, 1000, "how large is the key space size for the test")
CONFIG_FLD_INT_LIST(perf_test_concurrency,
                    "concurrency list: 0 for expotentially growing concurrenty, >0 for fixed")
CONFIG_FLD_INT_LIST(perf_test_payload_bytes, "size list: byte size of each rpc request test")
CONFIG_FLD_INT_LIST(perf_test_timeouts_ms, "timeout list: timeout (ms) for each rpc call")
CONFIG_FLD_INT_LIST(perf_test_hybrid_request_ratio,
                    "hybrid request ratio, e.g., 1,2,1 - the "
                    "numbers are ordered by the task code appeared "
                    "in task code registration")
CONFIG_END

class perf_client_helper
{
public:
    void start_test(const char *prefix, int max_request_kind_count_in_hybrid);

protected:
    perf_client_helper();

    void *prepare_send_one();
    void end_send_one(void *context, error_code err);

    virtual void
    send_one(int payload_bytes, int key_space_size, const std::vector<double> &ratios) = 0;

private:
    struct perf_test_case
    {
        int id;
        int seconds;
        int payload_bytes;
        int key_space_size;
        int concurrency;
        int timeout_ms;
        std::vector<double> ratios;

        // statistics
        std::atomic<int> timeout_rounds;
        std::atomic<int> error_rounds;
        std::atomic<int> succ_rounds;
        std::atomic<uint64_t> succ_rounds_sum_ns;
        std::atomic<uint64_t> min_latency_ns;
        std::atomic<uint64_t> max_latency_ns;
        double succ_latency_avg_ns;
        double succ_qps;
        double succ_throughput_MB_s;

        perf_test_case &operator=(const perf_test_case &r)
        {
            id = r.id;
            seconds = r.seconds;
            payload_bytes = r.payload_bytes;
            key_space_size = r.key_space_size;
            timeout_ms = r.timeout_ms;
            concurrency = r.concurrency;
            ratios = r.ratios;

            timeout_rounds.store(r.timeout_rounds.load());
            error_rounds.store(r.error_rounds.load());
            succ_rounds.store(r.succ_rounds.load());
            succ_rounds_sum_ns.store(r.succ_rounds_sum_ns.load());
            min_latency_ns.store(r.min_latency_ns.load());
            max_latency_ns.store(r.max_latency_ns.load());

            succ_latency_avg_ns = r.succ_latency_avg_ns;
            succ_qps = r.succ_qps;
            succ_throughput_MB_s = r.succ_throughput_MB_s;

            return *this;
        }

        perf_test_case(const perf_test_case &r) { *this = r; }

        perf_test_case()
            : id(0),
              seconds(0),
              payload_bytes(0),
              key_space_size(1000),
              concurrency(0),
              timeout_ms(0),
              timeout_rounds(0),
              error_rounds(0),
              succ_rounds(0),
              succ_latency_avg_ns(0),
              succ_qps(0),
              succ_throughput_MB_s(0)
        {
        }
    };

    struct perf_test_suite
    {
        const char *name;
        const char *config_section;
        std::vector<perf_test_case> cases;
    };

    void load_suite_config(perf_test_suite &s, int max_request_kind_count_for_hybrid_test);

    void start(const std::vector<perf_test_suite> &suits);

private:
    void finalize_case();

    void start_next_case();

private:
    perf_client_helper(const perf_client_helper &) = delete;

protected:
    std::chrono::milliseconds _timeout;

private:
    perf_test_opts _default_opts;
    std::string _name;
    perf_test_case *_current_case;

    volatile bool _quiting_current_case;
    std::atomic<int> _live_rpc_count;
    uint64_t _case_start_ts_ns;
    uint64_t _case_end_ts_ns;

    std::vector<perf_test_suite> _suits;
    int _current_suit_index;
    int _current_case_index;
    int _case_count;
};
}
}
