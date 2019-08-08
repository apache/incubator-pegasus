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

rocksdb::Env *flags_env;

static void append_with_space(std::string *str, const std::string &msg)
{
    if (msg.empty())
        return;
    if (!str->empty()) {
        str->push_back(' ');
    }
    str->append(msg.data(), msg.size());
}


stats::stats()
{
    start(-1);
}

void stats::set_reporter_agent(reporter_agent *reporter_agent)
{
    reporter_agent_ = reporter_agent;
}

void stats::set_hist_stats(std::shared_ptr<rocksdb::Statistics> hist_stats_)
{
    hist_stats = hist_stats_;
}

void stats::start(int id)
{
    id_ = id;
    next_report_ = config::get_instance()->_stats_interval ? config::get_instance()->_stats_interval : 100;
    last_op_finish_ = start_;
    done_ = 0;
    last_report_done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = flags_env->NowMicros();
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
    finish_ = flags_env->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
}

void stats::add_message(const std::string &msg)
{
    append_with_space(&message_, msg);
}

void stats::set_id(int id)
{
    id_ = id;
}

void stats::set_exclude_from_merge()
{
    exclude_from_merge_ = true;
}

void stats::print_thread_status()
{
    std::vector<rocksdb::ThreadStatus> thread_list;
    flags_env->GetThreadList(&thread_list);

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

        auto op_properties =
                rocksdb::ThreadStatus::InterpretOperationProperties(ts.operation_type, ts.op_properties);
        for (const auto &op_prop : op_properties) {
            fprintf(stderr, " %s %" PRIu64 " |", op_prop.first.c_str(), op_prop.second);
        }
        fprintf(stderr, "\n");
    }
}

void stats::reset_last_op_time()
{
    // Set to now to avoid latency from calls to SleepForMicroseconds
    last_op_finish_ = flags_env->NowMicros();
}

void stats::finished_ops(void *db_with_cfh, void *db, int64_t num_ops, enum operation_type op_type)
{
    if (reporter_agent_) {
        reporter_agent_->report_finished_ops(num_ops);
    }
    if (config::get_instance()->_histogram && hist_stats) {
        uint64_t now = flags_env->NowMicros();
        uint64_t micros = now - last_op_finish_;
        hist_stats->measureTime(op_type, micros);

        if (micros > 20000 && !config::get_instance()->_stats_interval) {
            fprintf(stderr, "long op: %" PRIu64 " micros%30s\r", micros, "");
            fflush(stderr);
        }
        last_op_finish_ = now;
    }

    done_ += num_ops;
    if (done_ >= next_report_) {
        if (!config::get_instance()->_stats_interval) {
            if (next_report_ < 1000)
                next_report_ += 100;
            else if (next_report_ < 5000)
                next_report_ += 500;
            else if (next_report_ < 10000)
                next_report_ += 1000;
            else if (next_report_ < 50000)
                next_report_ += 5000;
            else if (next_report_ < 100000)
                next_report_ += 10000;
            else if (next_report_ < 500000)
                next_report_ += 50000;
            else
                next_report_ += 100000;
            fprintf(stderr, "... finished %" PRIu64 " ops%30s\r", done_, "");
        } else {
            uint64_t now = flags_env->NowMicros();
            int64_t usecs_since_last = now - last_report_finish_;

            // Determine whether to print status where interval is either
            // each N operations or each N seconds.

            if (config::get_instance()->_stats_interval_seconds &&
                usecs_since_last < (config::get_instance()->_stats_interval_seconds * 1000000)) {
                // Don't check again for this many operations
                next_report_ += config::get_instance()->_stats_interval;
            } else {
                fprintf(stderr,
                        "%s ... thread %d: (%" PRIu64 ",%" PRIu64 ") ops and "
                                                                  "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                        flags_env->TimeToString(now / 1000000).c_str(),
                        id_,
                        done_ - last_report_done_,
                        done_,
                        (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
                        done_ / ((now - start_) / 1000000.0),
                        (now - last_report_finish_) / 1000000.0,
                        (now - start_) / 1000000.0);

                next_report_ += config::get_instance()->_stats_interval;
                last_report_finish_ = now;
                last_report_done_ = done_;
            }
        }
        if (id_ == 0 && config::get_instance()->_thread_status_per_interval) {
            print_thread_status();
        }
        fflush(stderr);
    }
}

void stats::add_bytes(int64_t n)
{
    bytes_ += n;
}

void stats::report(const std::string &name)
{
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call finished_ops().
    if (done_ < 1)
        done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
        // Rate is computed on actual elapsed time, not the sum of per-thread
        // elapsed times.
        double elapsed = (finish_ - start_) * 1e-6;
        char rate[100];
        snprintf(rate, sizeof(rate), "%6.1f MB/s", (bytes_ / 1048576.0) / elapsed);
        extra = rate;
    }
    append_with_space(&extra, message_);
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

void combined_stats::add_stats(const stats &stat)
{
    uint64_t total_ops = stat.done_;
    uint64_t total_bytes_ = stat.bytes_;
    double elapsed;

    if (total_ops < 1) {
        total_ops = 1;
    }

    elapsed = (stat.finish_ - stat.start_) * 1e-6;
    throughput_ops_.emplace_back(total_ops / elapsed);

    if (total_bytes_ > 0) {
        double mbs = (total_bytes_ / 1048576.0);
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

double combined_stats::calc_avg(std::vector<double> data)
{
    double avg = 0;
    for (double x : data) {
        avg += x;
    }
    avg = avg / data.size();
    return avg;
}

double combined_stats::calc_median(std::vector<double> data)
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
}
}
