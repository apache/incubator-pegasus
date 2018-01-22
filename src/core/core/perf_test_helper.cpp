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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */
#include <dsn/cpp/perf_test_helper.h>
#include <fstream>

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "perf.test.helper"

#define INVALID_DURATION_US 0xdeadbeefdeadbeefULL

namespace dsn {
namespace service {

perf_client_helper::perf_client_helper()
{
    _case_count = 0;
    _live_rpc_count = 0;

    if (!read_config("task..default", _default_opts)) {
        dassert(false, "read configuration failed for section [task..default]");
    }

    if (_default_opts.perf_test_concurrency.size() == 0) {
        const int ccs[] = {1, 10, 100, 1000};
        for (size_t i = 0; i < sizeof(ccs) / sizeof(int); i++)
            _default_opts.perf_test_concurrency.push_back(ccs[i]);
    }

    if (_default_opts.perf_test_timeouts_ms.size() == 0) {
        const int timeouts_ms[] = {10000};
        for (size_t i = 0; i < sizeof(timeouts_ms) / sizeof(int); i++)
            _default_opts.perf_test_timeouts_ms.push_back(timeouts_ms[i]);
    }

    if (_default_opts.perf_test_payload_bytes.size() == 0) {
        const int payload_bytes[] = {1024, 64 * 1024, 512 * 1024, 1024 * 1024};
        for (size_t i = 0; i < sizeof(payload_bytes) / sizeof(int); i++)
            _default_opts.perf_test_payload_bytes.push_back(payload_bytes[i]);
    }
}

void perf_client_helper::start_test(const char *prefix, int max_request_kind_count_in_hybrid)
{
    perf_test_suite s;
    std::vector<perf_test_suite> suits;

    const char *sections[10240];
    int scount, used_count = sizeof(sections) / sizeof(const char *);
    scount = dsn_config_get_all_sections(sections, &used_count);
    dassert(scount == used_count, "too many sections (>10240) defined in config files");

    for (int i = 0; i < used_count; i++) {
        if (strstr(sections[i], prefix) == sections[i]) {
            s.name = sections[i];
            s.config_section = sections[i];
            s.cases.clear();
            load_suite_config(s, max_request_kind_count_in_hybrid);
            suits.push_back(s);
        }
    }

    start(suits);
}

void perf_client_helper::load_suite_config(perf_test_suite &s,
                                           int max_request_kind_count_for_hybrid_test)
{
    perf_test_opts opt;
    if (!read_config(s.config_section, opt, &_default_opts)) {
        dassert(false, "read configuration failed for section [%s]", s.config_section);
    }

    double ratio_sum = 0.0;
    if (opt.perf_test_hybrid_request_ratio.size() == 0)
        opt.perf_test_hybrid_request_ratio.push_back(1);

    for (auto r : opt.perf_test_hybrid_request_ratio) {
        ratio_sum += (double)r;
    }

    s.cases.clear();
    for (auto &bytes : opt.perf_test_payload_bytes) {
        int last_index = static_cast<int>(opt.perf_test_timeouts_ms.size()) - 1;
        for (int i = last_index; i >= 0; i--) {
            for (auto &cc : opt.perf_test_concurrency) {
                perf_test_case c;
                c.id = ++_case_count;
                c.seconds = opt.perf_test_seconds;
                c.payload_bytes = bytes;
                c.key_space_size = opt.perf_test_key_space_size;
                c.timeout_ms = opt.perf_test_timeouts_ms[i];
                c.concurrency = cc;
                c.ratios.resize(max_request_kind_count_for_hybrid_test, 0.0);

                double ratio = 0.0;
                for (size_t i = 0;
                     i < std::min(opt.perf_test_hybrid_request_ratio.size(), c.ratios.size());
                     i++) {
                    ratio += (double)(opt.perf_test_hybrid_request_ratio[i]) / ratio_sum;
                    c.ratios[i] = ratio;
                }

                s.cases.push_back(c);
            }
        }
    }
}

void perf_client_helper::start(const std::vector<perf_test_suite> &suits)
{
    _suits = suits;
    _current_suit_index = -1;
    _current_case_index = 0xffffff;

    start_next_case();
}

void *perf_client_helper::prepare_send_one()
{
    uint64_t nts_ns = ::dsn_now_ns();
    ++_live_rpc_count;
    return (void *)(size_t)(nts_ns);
}

void perf_client_helper::end_send_one(void *context, error_code err)
{
    uint64_t start_ts = (uint64_t)(context);
    uint64_t nts_ns = ::dsn_now_ns();

    if (err != ERR_OK) {
        if (err == ERR_TIMEOUT)
            _current_case->timeout_rounds++;
        else
            _current_case->error_rounds++;
    } else {
        _current_case->succ_rounds++;

        auto d = nts_ns - start_ts;
        _current_case->succ_rounds_sum_ns += d;
        if (d < _current_case->min_latency_ns)
            _current_case->min_latency_ns = d;
        if (d > _current_case->max_latency_ns)
            _current_case->max_latency_ns = d;
    }

    // if completed
    if (_quiting_current_case) {
        if (--_live_rpc_count == 0)
            finalize_case();
        return;
    } else if (nts_ns >= _case_end_ts_ns) {
        _quiting_current_case = true;
        if (--_live_rpc_count == 0)
            finalize_case();
        return;
    } else {
        --_live_rpc_count;
    }

    // continue further waves
    if (_current_case->concurrency == 0) {
        // exponentially increase
        send_one(
            _current_case->payload_bytes, _current_case->key_space_size, _current_case->ratios);
        send_one(
            _current_case->payload_bytes, _current_case->key_space_size, _current_case->ratios);
    } else {
        // maintain fixed concurrent number
        while (!_quiting_current_case && _live_rpc_count <= _current_case->concurrency) {
            send_one(
                _current_case->payload_bytes, _current_case->key_space_size, _current_case->ratios);
        }
    }
}

void perf_client_helper::finalize_case()
{
    dassert(_live_rpc_count == 0, "all live requests must be completed");

    uint64_t nts = dsn_now_ns();
    auto &suit = _suits[_current_suit_index];
    auto &cs = suit.cases[_current_case_index];

    cs.succ_qps =
        (double)cs.succ_rounds / ((double)(nts - _case_start_ts_ns) / 1000.0 / 1000.0 / 1000.0);
    cs.succ_throughput_MB_s = (double)cs.succ_rounds * (double)cs.payload_bytes / 1024.0 / 1024.0 /
                              ((double)(nts - _case_start_ts_ns) / 1000.0 / 1000.0 / 1000.0);
    cs.succ_latency_avg_ns = (double)cs.succ_rounds_sum_ns / (double)cs.succ_rounds;

    std::stringstream ss;
    ss << "TEST " << _name << "(" << cs.id << "/" << _case_count << ")::"
       << "  concurency: " << cs.concurrency << ", timeout(ms): " << cs.timeout_ms
       << ", payload(byte): " << cs.payload_bytes << ", tmo/err/suc(#): " << cs.timeout_rounds
       << "/" << cs.error_rounds << "/" << cs.succ_rounds
       << ", latency(ns): " << cs.succ_latency_avg_ns << "(avg), " << cs.min_latency_ns << "(min), "
       << cs.max_latency_ns << "(max)"
       << ", qps: " << cs.succ_qps << "#/s"
       << ", thp: " << cs.succ_throughput_MB_s << "MB/s";

    dwarn(ss.str().c_str());

    start_next_case();
}

void perf_client_helper::start_next_case()
{
    ++_current_case_index;

    // switch to next suit
    if (_current_suit_index == -1 ||
        _current_case_index >= (int)_suits[_current_suit_index].cases.size()) {
        _current_suit_index++;
        _current_case_index = 0;

        if (_current_suit_index >= (int)_suits.size()) {
            uint64_t ts = dsn_now_ns();
            char str[24];
            ::dsn::utils::time_ms_to_string(ts / 1000000, str);
            std::stringstream ss;

            ss << "TEST end at " << str << std::endl;
            for (auto &s : _suits) {
                for (auto &cs : s.cases) {
                    ss << "TEST " << s.name << "(" << cs.id << "/" << _case_count << ")::"
                       << "  concurency: " << cs.concurrency << ", timeout(ms): " << cs.timeout_ms
                       << ", payload(byte): " << cs.payload_bytes
                       << ", tmo/err/suc(#): " << cs.timeout_rounds << "/" << cs.error_rounds << "/"
                       << cs.succ_rounds << ", latency(ns): " << cs.succ_latency_avg_ns << "(avg), "
                       << cs.min_latency_ns << "(min), " << cs.max_latency_ns << "(max)"
                       << ", qps: " << cs.succ_qps << "#/s"
                       << ", thp: " << cs.succ_throughput_MB_s << "MB/s" << std::endl;
                    ;
                }
            }

            dwarn(ss.str().c_str());

            // dump to perf result file
            if (dsn_config_get_value_bool(
                    "apps.client.perf.test",
                    "exit_after_test",
                    false,
                    "dump the result and exit the process after the test is finished")) {
                std::string data_dir(service_app::current_service_app_info().data_dir);
                std::stringstream fns;
                fns << "perf-result-" << ts << ".txt";
                std::string report = ::dsn::utils::filesystem::path_combine(data_dir, fns.str());
                std::ofstream result_f(report.c_str(), std::ios::out);
                result_f << ss.str() << std::endl;
                result_f.close();

                report += ".config.ini";
                dsn_config_dump(report.c_str());

                dsn_exit(0);
            }

            return;
        }
    }

    std::this_thread::sleep_for(std::chrono::seconds(2));

    // get next case
    auto &suit = _suits[_current_suit_index];
    auto &cs = suit.cases[_current_case_index];
    cs.timeout_rounds = 0;
    cs.error_rounds = 0;
    cs.max_latency_ns = 0;
    cs.min_latency_ns = std::numeric_limits<uint64_t>::max();

    // setup for the case
    _current_case = &cs;
    _name = suit.name;
    _timeout = std::chrono::milliseconds(cs.timeout_ms);
    _case_start_ts_ns = dsn_now_ns();
    _case_end_ts_ns = _case_start_ts_ns + (uint64_t)cs.seconds * 1000 * 1000 * 1000;
    _quiting_current_case = false;
    dassert(_live_rpc_count == 0, "all live requests must be completed");

    std::stringstream ss;
    ss << "TEST " << _name << "(" << cs.id << "/" << _case_count << ")::"
       << "  concurrency " << _current_case->concurrency << ", timeout(ms) "
       << _current_case->timeout_ms << ", payload(byte) " << _current_case->payload_bytes;
    dwarn(ss.str().c_str());

    // start
    send_one(_current_case->payload_bytes, _current_case->key_space_size, _current_case->ratios);
}
}
}
