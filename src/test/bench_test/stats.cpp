// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <algorithm>
#include <unordered_map>
#include <cinttypes>

#include "stats.h"
#include "config.h"

namespace pegasus {
namespace test {

static void append_with_space(std::string *str, const std::string &msg)
{
    if (msg.empty())
        return;
    if (!str->empty()) {
        str->push_back(' ');
    }
    str->append(msg.data(), msg.size());
}

stats::stats() { start(-1); }

void stats::set_hist_stats(std::shared_ptr<rocksdb::Statistics> hist_stats_)
{
    hist_stats = hist_stats_;
}

void stats::start(int id)
{
    id_ = id;
    next_report_ =
        config::get_instance()->stats_interval ? config::get_instance()->stats_interval : 100;
    last_op_finish_ = start_;
    done_ = 0;
    last_report_done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = config::get_instance()->env->NowMicros();
    finish_ = start_;
    last_report_finish_ = start_;
    message_.clear();
    // When set, stats from this thread won't be merged with others.
    exclude_from_merge_ = false;
}

void stats::merge(const stats &other)
{
    if (other.exclude_from_merge_)
        return;

    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_)
        start_ = other.start_;
    if (other.finish_ > finish_)
        finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty())
        message_ = other.message_;
}

void stats::stop()
{
    finish_ = config::get_instance()->env->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
}

void stats::add_message(const std::string &msg) { append_with_space(&message_, msg); }

void stats::print_thread_status()
{
    std::vector<rocksdb::ThreadStatus> thread_list;
    config::get_instance()->env->GetThreadList(&thread_list);

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
    rocksdb::Env::Default()->GetCurrentTime(&current_time);
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

void stats::finished_ops(int64_t num_ops, enum operation_type op_type)
{
    if (hist_stats) {
        uint64_t now = config::get_instance()->env->NowMicros();
        uint64_t micros = now - last_op_finish_;
        hist_stats->measureTime(op_type, micros);

        if (micros > 20000 && !config::get_instance()->stats_interval) {
            fprintf(stderr, "long op: %" PRIu64 " micros%30s\r", micros, "");
            fflush(stderr);
        }
        last_op_finish_ = now;
    }

    done_ += num_ops;
    if (done_ >= next_report_) {
        if (!config::get_instance()->stats_interval) {
            next_report_ += report_default_step(next_report_);
            fprintf(stderr, "... finished %" PRIu64 " ops%30s\r", done_, "");
        } else {
            uint64_t now = config::get_instance()->env->NowMicros();
            int64_t usecs_since_last = now - last_report_finish_;

            // Determine whether to print status where interval is either
            // each N operations or each N seconds.
            if (config::get_instance()->stats_interval_seconds &&
                usecs_since_last < (config::get_instance()->stats_interval_seconds * 1000000)) {
            } else {
                fprintf(stderr,
                        "%s ... thread %d: (%" PRIu64 ",%" PRIu64 ") ops and "
                        "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                        config::get_instance()->env->TimeToString(now / 1000000).c_str(),
                        id_,
                        done_ - last_report_done_,
                        done_,
                        (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
                        done_ / ((now - start_) / 1000000.0),
                        (now - last_report_finish_) / 1000000.0,
                        (now - start_) / 1000000.0);

                last_report_finish_ = now;
                last_report_done_ = done_;
            }
            next_report_ += config::get_instance()->stats_interval;
        }
        if (id_ == 0 && config::get_instance()->thread_status_per_interval) {
            print_thread_status();
        }
        fflush(stderr);
    }
}

void stats::add_bytes(int64_t n) { bytes_ += n; }

void stats::report(const std::string &name)
{
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call finished_ops().
    if (done_ < 1)
        done_ = 1;

    // append rate(MBytes) message to extra
    std::string extra;
    if (bytes_ > 0) {
        // Rate is computed on actual elapsed time, not the sum of per-thread
        // elapsed times.
        double elapsed = (finish_ - start_) * 1e-6;
        char rate[100];
        snprintf(rate, sizeof(rate), "%6.1f MB/s", (bytes_ << 20) / elapsed);
        extra = rate;
    }

    // append message_ to extra
    append_with_space(&extra, message_);

    // calculate ops throughtput and elapsed seconds
    double elapsed = (finish_ - start_) * 1e-6;
    double throughput = (double)done_ / elapsed;

    fprintf(stdout,
            "%-12s : %11.3f micros/op %ld ops/sec;%s%s\n",
            name.c_str(),
            elapsed * 1e6 / done_,
            (long)throughput,
            (extra.empty() ? "" : " "),
            extra.c_str());
    fflush(stdout);
}

uint32_t stats::report_default_step(uint64_t current_report)
{
    uint32_t step = 0;
    if (current_report < 1000)
        step = 100;
    else if (current_report < 5000)
        step = 500;
    else if (current_report < 10000)
        step = 1000;
    else if (current_report < 50000)
        step = 5000;
    else if (current_report < 100000)
        step = 10000;
    else if (current_report < 500000)
        step = 50000;
    else
        step = 100000;

    return step;
}

void combined_stats::add_stats(const stats &stat)
{
    // calculate and save ops throughtput
    uint64_t total_ops = stat.done_;
    if (total_ops < 1) {
        total_ops = 1;
    }
    double elapsed = (stat.finish_ - stat.start_) * 1e-6;
    throughput_ops_.emplace_back(total_ops / elapsed);

    // calculate and save MBytes throughtput
    uint64_t total_bytes_ = stat.bytes_;
    if (total_bytes_ > 0) {
        double mbs = total_bytes_ << 20;
        throughput_mbs_.emplace_back(mbs / elapsed);
    }
}

void combined_stats::report(const std::string &bench_name)
{
    const char *name = bench_name.c_str();
    int num_runs = static_cast<int>(throughput_ops_.size());

    if (throughput_mbs_.size() == throughput_ops_.size()) {
        fprintf(stdout,
                "%s [AVG    %d runs] : %d ops/sec; %6.1f MB/sec\n"
                "%s [MEDIAN %d runs] : %d ops/sec; %6.1f MB/sec\n",
                name,
                num_runs,
                static_cast<int>(calc_avg(throughput_ops_)),
                calc_avg(throughput_mbs_),
                name,
                num_runs,
                static_cast<int>(calc_median(throughput_ops_)),
                calc_median(throughput_mbs_));
    } else {
        fprintf(stdout,
                "%s [AVG    %d runs] : %d ops/sec\n"
                "%s [MEDIAN %d runs] : %d ops/sec\n",
                name,
                num_runs,
                static_cast<int>(calc_avg(throughput_ops_)),
                name,
                num_runs,
                static_cast<int>(calc_median(throughput_ops_)));
    }
}

double combined_stats::calc_avg(std::vector<double> &data)
{
    double total = 0;
    for (double x : data) {
        total += x;
    }

    return total / data.size();
}

double combined_stats::calc_median(std::vector<double> &data)
{
    assert(data.size() > 0);
    std::sort(data.begin(), data.end());

    size_t mid = data.size() / 2;
    if (data.size() % 2 == 1) {
        // Odd number of entries
        return data[mid];
    } else {
        // Even number of entries
        return (data[mid] + data[mid - 1]) / 2;
    }
}
} // namespace test
} // namespace pegasus
