// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <unordered_map>
#include <cinttypes>

#include "stats.h"
#include "config.h"

namespace pegasus {
namespace test {
std::unordered_map<operation_type, std::string, std::hash<unsigned char>> operation_type_string = {
    {kUnknown, "unKnown"}, {kRead, "read"}, {kWrite, "write"}, {kDelete, "delete"}};

static void append_with_space(std::string *str, const std::string &msg)
{
    if (msg.empty())
        return;

    assert(NULL != str);
    if (!str->empty()) {
        str->push_back(' ');
    }
    str->append(msg);
}

stats::stats(std::shared_ptr<rocksdb::Statistics> hist_stats)
{
    _tid = -1;
    _next_report = 100;
    _done = 0;
    _bytes = 0;
    _start = config::get_instance().env->NowMicros();
    _last_op_finish = _start;
    _hist_stats = hist_stats;
}

void stats::start(int id)
{
    _tid = id;
    _next_report = 100;
    _done = 0;
    _bytes = 0;
    _start = config::get_instance().env->NowMicros();
    _last_op_finish = _start;
    _message.clear();
}

void stats::merge(const stats &other)
{
    _done += other._done;
    _bytes += other._bytes;
    _start = std::min(other._start, _start);
    _finish = std::max(other._finish, _finish);
    append_with_space(&_message, other._message);
}

void stats::stop() { _finish = config::get_instance().env->NowMicros(); }

void stats::finished_ops(int64_t num_ops, enum operation_type op_type)
{
    uint64_t now = config::get_instance().env->NowMicros();
    uint64_t micros = now - _last_op_finish;
    _last_op_finish = now;

    // if there is a long operation, print warning message
    if (micros > 20000) {
        fprintf(stderr, "long op: %" PRIu64 " micros%30s\r", micros, "");
        fflush(stderr);
    }

    // print the benchmark running status
    _done += num_ops;
    if (_done >= _next_report) {
        _next_report += report_step(_next_report);
        fprintf(stderr, "... finished %" PRIu64 " ops%30s\r", _done, "");
    }

    // add excution time of this operation to _hist_stats
    if (_hist_stats) {
        _hist_stats->measureTime(op_type, micros);
    }

    // print thread status(only pthread 0 work)
    if (_tid == 0 && config::get_instance().thread_status_per_interval) {
        print_thread_status();
    }
    fflush(stderr);
}

void stats::report(operation_type op_type)
{
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call finished_ops().
    if (_done < 1)
        _done = 1;

    // elasped time(s)
    double elapsed = (_finish - _start) * 1e-6;

    // append rate(MBytes) message to extra
    std::string extra;
    if (_bytes > 0) {
        // Rate is computed on actual elapsed time, not the sum of per-thread
        // elapsed times.
        char rate[100];
        snprintf(rate, sizeof(rate), "%6.1f MB/s", (_bytes >> 20) / elapsed);
        extra = rate;
    }

    // append _message to extra
    append_with_space(&extra, _message);

    // print report
    fprintf(stdout,
            "%-6s : %11.3f micros/op %ld ops/sec;%s%s\n",
            operation_type_string[op_type].c_str(),
            elapsed * 1e6 / _done,
            static_cast<long>(_done / elapsed),
            (extra.empty() ? "" : " "),
            extra.c_str());
    fflush(stdout);

    // print histogram if _hist_stats is not NULL
    if (_hist_stats) {
        fprintf(stdout,
                "Microseconds per %s:\n%s\n",
                operation_type_string[op_type].c_str(),
                _hist_stats->getHistogramString(op_type).c_str());
    }
}

void stats::add_message(const std::string &msg) { append_with_space(&_message, msg); }

void stats::add_bytes(int64_t n) { _bytes += n; }

void stats::set_hist_stats(std::shared_ptr<rocksdb::Statistics> hist_stats)
{
    _hist_stats = hist_stats;
}

void stats::print_thread_status() const
{
    std::vector<rocksdb::ThreadStatus> thread_list;
    config::get_instance().env->GetThreadList(&thread_list);

    fprintf(stderr,
            "\n%18s %10s %12s %20s %13s %45s %12s %s\n",
            "ThreadID",
            "ThreadType",
            "cfName",
            "Operation",
            "ElapsedTime",
            "Stage",
            "State",
            "OperationProperties");

    int64_t current_time = 0;
    config::get_instance().env->GetCurrentTime(&current_time);
    for (auto ts : thread_list) {
        fprintf(stderr,
                "%18" PRIu64 " %10s %12s %20s %13s %45s %12s",
                ts.thread_id,
                rocksdb::ThreadStatus::GetThreadTypeName(ts.thread_type).c_str(),
                ts.cf_name.c_str(),
                rocksdb::ThreadStatus::GetOperationName(ts.operation_type).c_str(),
                rocksdb::ThreadStatus::MicrosToString(ts.op_elapsed_micros).c_str(),
                rocksdb::ThreadStatus::GetOperationStageName(ts.operation_stage).c_str(),
                rocksdb::ThreadStatus::GetStateName(ts.state_type).c_str());

        auto op_properties = rocksdb::ThreadStatus::InterpretOperationProperties(ts.operation_type,
                                                                                 ts.op_properties);
        for (const auto &op_prop : op_properties) {
            fprintf(stderr, " %s %" PRIu64 " |", op_prop.first.c_str(), op_prop.second);
        }
        fprintf(stderr, "\n");
    }
}

uint32_t stats::report_step(uint64_t current_report) const
{
    uint32_t step = 0;
    switch (current_report) {
    case 0 ... 999:
        step = 100;
        break;
    case 1000 ... 4999:
        step = 500;
        break;
    case 5000 ... 9999:
        step = 1000;
        break;
    case 10000 ... 49999:
        step = 5000;
        break;
    case 50000 ... 99999:
        step = 10000;
        break;
    case 100000 ... 499999:
        step = 50000;
        break;
    default:
        step = 100000;
        break;
    }

    return step;
}
} // namespace test
} // namespace pegasus
