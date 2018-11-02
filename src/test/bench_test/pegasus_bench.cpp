// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#ifdef NUMA
#include <numa.h>
#include <numaif.h>
#endif

#include <fcntl.h>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <sys/types.h>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <algorithm>
#include <zconf.h>
#include <gflags/gflags.h>

#include <rocksdb/env.h>
#include <util/random.h>
#include <port/port_posix.h>
#include <util/string_util.h>
#include <monitoring/histogram.h>
#include <rocksdb/rate_limiter.h>
#include <util/mutexlock.h>

#include "pegasus/client.h"

namespace google {
}
namespace gflags {
}
using namespace google;
using namespace gflags;
using namespace pegasus;
using namespace rocksdb;

DEFINE_string(pegasus_config, "config.ini", "pegasus config file");
DEFINE_string(pegasus_cluster_name, "onebox", "pegasus cluster name");
DEFINE_string(pegasus_app_name, "temp", "pegasus app name");
DEFINE_int32(pegasus_timeout_ms, 10000, "pegasus read/write timeout in milliseconds");

DEFINE_string(benchmarks,
              "fillseq_pegasus,fillrandom_pegasus,readrandom_pegasus,filluniquerandom_pegasus,"
              "deleteseq_pegasus,deleterandom_pegasus,multi_set_pegasus,scan_pegasus",

              "Comma-separated list of operations to run in the specified"
              " order. Available benchmarks:\n"
              "\tfillseq_pegasus          -- pegasus write N values in sequential key order\n"
              "\tfillrandom_pegasus       -- pegasus write N values in random key order\n"
              "\tfilluniquerandom_pegasus -- pegasus write N values in unique random key order\n"
              "\treadrandom_pegasus       -- pegasus read N times in random order\n"
              "\tdeleteseq_pegasus        -- pegasus delete N keys in sequential order\n"
              "\tdeleterandom_pegasus     -- pegasus delete N keys in random order\n"
              "\tmulti_set_pegasus        -- pegasus multi write N keys in random order\n"
              "\tscan_pegasus             -- pegasus scan N keys in random order\n");

DEFINE_int64(num, 10000, "Number of key/values to place in database");
DEFINE_int64(sortkey_count_per_hashkey, 100, "Number of sort key per hash key");

DEFINE_int64(seed,
             0,
             "Seed base for random number generators. "
             "When 0 it is deterministic.");

DEFINE_int32(threads, 1, "Number of concurrent threads to run.");

DEFINE_int32(duration,
             0,
             "Time in seconds for the random-ops tests to run."
             " When 0 then num & reads determine the test duration");

DEFINE_int32(value_size, 100, "Size of each value");

DEFINE_int64(batch_size, 1, "Batch size");

static bool ValidateKeySize(const char *flagname, int32_t value) { return true; }

DEFINE_int32(key_size, 16, "size of each key");

DEFINE_double(compression_ratio,
              0.5,
              "Arrange to generate values that shrink"
              " to this fraction of their original size after compression");

DEFINE_bool(histogram, true, "Print histogram of operation timings");

DEFINE_bool(enable_numa,
            false,
            "Make operations aware of NUMA architecture and bind memory "
            "and cpus corresponding to nodes together. In NUMA, memory "
            "in same node as CPUs are closer when compared to memory in "
            "other nodes. Reads can be faster when the process is bound to "
            "CPU and memory of same node. Use \"$numactl --hardware\" command "
            "to see NUMA memory architecture.");

// The default reduces the overhead of reading time with flash. With HDD, which
// offers much less throughput, however, this number better to be set to 1.
DEFINE_int32(ops_between_duration_checks, 1000, "Check duration limit every x ops");

DEFINE_int64(stats_interval,
             0,
             "Stats are reported every N operations when "
             "this is greater than zero. When 0 the interval grows over time.");

DEFINE_int64(stats_interval_seconds,
             0,
             "Report stats every N seconds. This "
             "overrides stats_interval when both are > 0.");

DEFINE_int64(report_interval_seconds,
             0,
             "If greater than zero, it will write simple stats in CVS format "
             "to --report_file every N seconds");

DEFINE_string(report_file,
              "report.csv",
              "Filename where some simple stats are reported to (if "
              "--report_interval_seconds is bigger than 0)");

DEFINE_int32(thread_status_per_interval,
             0,
             "Takes and report a snapshot of the current status of each thread"
             " when this is greater than 0.");

DEFINE_uint64(benchmark_write_rate_limit,
              0,
              "If non-zero, db_bench will rate-limit the writes going into RocksDB. This "
              "is the global rate in bytes/second.");

DEFINE_uint64(benchmark_read_rate_limit,
              0,
              "If non-zero, db_bench will rate-limit the reads from RocksDB. This "
              "is the global rate in ops/second.");

static bool ValidatePrefixSize(const char *flagname, int32_t value)
{
    if (value < 0 || value >= 2000000000) {
        fprintf(
            stderr, "Invalid value for --%s: %d. 0<= PrefixSize <=2000000000\n", flagname, value);
        return false;
    }
    return true;
}
DEFINE_int32(prefix_size,
             0,
             "control the prefix size for HashSkipList and "
             "plain table");
DEFINE_int64(keys_per_prefix,
             0,
             "control average number of keys generated "
             "per prefix, 0 means no special handling of the prefix, "
             "i.e. use the prefix comes with the generated random number.");

static const bool FLAGS_prefix_size_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_prefix_size, &ValidatePrefixSize);

static const bool FLAGS_key_size_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_key_size, &ValidateKeySize);

static rocksdb::Env *FLAGS_env = rocksdb::Env::Default();

namespace rocksdb {

// Helper for quickly generating random data.
class RandomGenerator
{
private:
    std::string data_;
    unsigned int pos_;

public:
    RandomGenerator()
    {
        // We use a limited amount of data over and over again and ensure
        // that it is larger than the compression window (32KB), and also
        // large enough to serve all typical value sizes we want to write.
        Random rnd(301);
        std::string piece;
        while (data_.size() < (unsigned)std::max(1048576, FLAGS_value_size)) {
            // Add a short fragment that is as compressible as specified
            // by FLAGS_compression_ratio.
            /*test::*/ CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
            data_.append(piece);
        }
        pos_ = 0;
    }

    Slice RandomString(Random *rnd, int len, std::string *dst)
    {
        dst->resize(len);
        for (int i = 0; i < len; i++) {
            (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95)); // ' ' .. '~'
        }
        return Slice(*dst);
    }

    Slice CompressibleString(Random *rnd, double compressed_fraction, int len, std::string *dst)
    {
        int raw = static_cast<int>(len * compressed_fraction);
        if (raw < 1)
            raw = 1;
        std::string raw_data;
        RandomString(rnd, raw, &raw_data);

        // Duplicate the random data until we have filled "len" bytes
        dst->clear();
        while (dst->size() < (unsigned int)len) {
            dst->append(raw_data);
        }
        dst->resize(len);
        return Slice(*dst);
    }

    Slice Generate(unsigned int len)
    {
        assert(len <= data_.size());
        if (pos_ + len > data_.size()) {
            pos_ = 0;
        }
        pos_ += len;
        return Slice(data_.data() + pos_ - len, len);
    }
};

static void AppendWithSpace(std::string *str, Slice msg)
{
    if (msg.empty())
        return;
    if (!str->empty()) {
        str->push_back(' ');
    }
    str->append(msg.data(), msg.size());
}

// a class that reports stats to CSV file
class ReporterAgent
{
public:
    ReporterAgent(Env *env, const std::string &fname, uint64_t report_interval_secs)
        : env_(env),
          total_ops_done_(0),
          last_report_(0),
          report_interval_secs_(report_interval_secs),
          stop_(false)
    {
        auto s = env_->NewWritableFile(fname, &report_file_, EnvOptions());
        if (s.ok()) {
            s = report_file_->Append(Header() + "\n");
        }
        if (s.ok()) {
            s = report_file_->Flush();
        }
        if (!s.ok()) {
            fprintf(stderr, "Can't open %s: %s\n", fname.c_str(), s.ToString().c_str());
            abort();
        }

        reporting_thread_ = port::Thread([&]() { SleepAndReport(); });
    }

    ~ReporterAgent()
    {
        {
            std::unique_lock<std::mutex> lk(mutex_);
            stop_ = true;
            stop_cv_.notify_all();
        }
        reporting_thread_.join();
    }

    // thread safe
    void ReportFinishedOps(int64_t num_ops) { total_ops_done_.fetch_add(num_ops); }

private:
    std::string Header() const { return "secs_elapsed,interval_qps"; }
    void SleepAndReport()
    {
        uint64_t kMicrosInSecond = 1000 * 1000;
        auto time_started = env_->NowMicros();
        while (true) {
            {
                std::unique_lock<std::mutex> lk(mutex_);
                if (stop_ ||
                    stop_cv_.wait_for(
                        lk, std::chrono::seconds(report_interval_secs_), [&]() { return stop_; })) {
                    // stopping
                    break;
                }
                // else -> timeout, which means time for a report!
            }
            auto total_ops_done_snapshot = total_ops_done_.load();
            // round the seconds elapsed
            auto secs_elapsed =
                (env_->NowMicros() - time_started + kMicrosInSecond / 2) / kMicrosInSecond;
            std::string report = ToString(secs_elapsed) + "," +
                                 ToString(total_ops_done_snapshot - last_report_) + "\n";
            auto s = report_file_->Append(report);
            if (s.ok()) {
                s = report_file_->Flush();
            }
            if (!s.ok()) {
                fprintf(stderr,
                        "Can't write to report file (%s), stopping the reporting\n",
                        s.ToString().c_str());
                break;
            }
            last_report_ = total_ops_done_snapshot;
        }
    }

    Env *env_;
    std::unique_ptr<WritableFile> report_file_;
    std::atomic<int64_t> total_ops_done_;
    int64_t last_report_;
    const uint64_t report_interval_secs_;
    rocksdb::port::Thread reporting_thread_;
    std::mutex mutex_;
    // will notify on stop
    std::condition_variable stop_cv_;
    bool stop_;
};

enum OperationType : unsigned char
{
    kRead = 0,
    kWrite,
    kDelete,
    kScan,
    kOthers
};

static std::unordered_map<OperationType, std::string, std::hash<unsigned char>>
    OperationTypeString = {
        {kRead, "read"}, {kWrite, "write"}, {kDelete, "delete"}, {kScan, "scan"}, {kOthers, "op"}};

class CombinedStats;
class Stats
{
private:
    int id_;
    uint64_t start_;
    uint64_t finish_;
    double seconds_;
    uint64_t done_;
    uint64_t last_report_done_;
    uint64_t next_report_;
    uint64_t bytes_;
    uint64_t last_op_finish_;
    uint64_t last_report_finish_;
    std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>, std::hash<unsigned char>>
        hist_;
    std::string message_;
    bool exclude_from_merge_;
    ReporterAgent *reporter_agent_; // does not own
    friend class CombinedStats;

public:
    Stats() { Start(-1); }

    void SetReporterAgent(ReporterAgent *reporter_agent) { reporter_agent_ = reporter_agent; }

    void Start(int id)
    {
        id_ = id;
        next_report_ = FLAGS_stats_interval ? FLAGS_stats_interval : 100;
        last_op_finish_ = start_;
        hist_.clear();
        done_ = 0;
        last_report_done_ = 0;
        bytes_ = 0;
        seconds_ = 0;
        start_ = FLAGS_env->NowMicros();
        finish_ = start_;
        last_report_finish_ = start_;
        message_.clear();
        // When set, stats from this thread won't be merged with others.
        exclude_from_merge_ = false;
    }

    void Merge(const Stats &other)
    {
        if (other.exclude_from_merge_)
            return;

        for (auto it = other.hist_.begin(); it != other.hist_.end(); ++it) {
            auto this_it = hist_.find(it->first);
            if (this_it != hist_.end()) {
                this_it->second->Merge(*(other.hist_.at(it->first)));
            } else {
                hist_.insert({it->first, it->second});
            }
        }

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

    void Stop()
    {
        finish_ = FLAGS_env->NowMicros();
        seconds_ = (finish_ - start_) * 1e-6;
    }

    void AddMessage(Slice msg) { AppendWithSpace(&message_, msg); }

    void SetId(int id) { id_ = id; }
    void SetExcludeFromMerge() { exclude_from_merge_ = true; }

    void PrintThreadStatus()
    {
        std::vector<ThreadStatus> thread_list;
        FLAGS_env->GetThreadList(&thread_list);

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
        Env::Default()->GetCurrentTime(&current_time);
        for (auto ts : thread_list) {
            fprintf(stderr,
                    "%18" PRIu64 " %10s %12s %20s %13s %45s %12s",
                    ts.thread_id,
                    ThreadStatus::GetThreadTypeName(ts.thread_type).c_str(),
                    ts.cf_name.c_str(),
                    ThreadStatus::GetOperationName(ts.operation_type).c_str(),
                    ThreadStatus::MicrosToString(ts.op_elapsed_micros).c_str(),
                    ThreadStatus::GetOperationStageName(ts.operation_stage).c_str(),
                    ThreadStatus::GetStateName(ts.state_type).c_str());

            auto op_properties =
                ThreadStatus::InterpretOperationProperties(ts.operation_type, ts.op_properties);
            for (const auto &op_prop : op_properties) {
                fprintf(stderr, " %s %" PRIu64 " |", op_prop.first.c_str(), op_prop.second);
            }
            fprintf(stderr, "\n");
        }
    }

    void ResetLastOpTime()
    {
        // Set to now to avoid latency from calls to SleepForMicroseconds
        last_op_finish_ = FLAGS_env->NowMicros();
    }

    void FinishedOps(void *db_with_cfh, void *db, int64_t num_ops, enum OperationType op_type)
    {
        if (reporter_agent_) {
            reporter_agent_->ReportFinishedOps(num_ops);
        }
        if (FLAGS_histogram) {
            uint64_t now = FLAGS_env->NowMicros();
            uint64_t micros = now - last_op_finish_;

            if (hist_.find(op_type) == hist_.end()) {
                auto hist_temp = std::make_shared<HistogramImpl>();
                hist_.insert({op_type, std::move(hist_temp)});
            }
            hist_[op_type]->Add(micros);

            if (micros > 20000 && !FLAGS_stats_interval) {
                fprintf(stderr, "long op: %" PRIu64 " micros%30s\r", micros, "");
                fflush(stderr);
            }
            last_op_finish_ = now;
        }

        done_ += num_ops;
        if (done_ >= next_report_) {
            if (!FLAGS_stats_interval) {
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
                uint64_t now = FLAGS_env->NowMicros();
                int64_t usecs_since_last = now - last_report_finish_;

                // Determine whether to print status where interval is either
                // each N operations or each N seconds.

                if (FLAGS_stats_interval_seconds &&
                    usecs_since_last < (FLAGS_stats_interval_seconds * 1000000)) {
                    // Don't check again for this many operations
                    next_report_ += FLAGS_stats_interval;

                } else {

                    fprintf(stderr,
                            "%s ... thread %d: (%" PRIu64 ",%" PRIu64 ") ops and "
                            "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                            FLAGS_env->TimeToString(now / 1000000).c_str(),
                            id_,
                            done_ - last_report_done_,
                            done_,
                            (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
                            done_ / ((now - start_) / 1000000.0),
                            (now - last_report_finish_) / 1000000.0,
                            (now - start_) / 1000000.0);

                    next_report_ += FLAGS_stats_interval;
                    last_report_finish_ = now;
                    last_report_done_ = done_;
                }
            }
            if (id_ == 0 && FLAGS_thread_status_per_interval) {
                PrintThreadStatus();
            }
            fflush(stderr);
        }
    }

    void AddBytes(int64_t n) { bytes_ += n; }

    void Report(const Slice &name)
    {
        // Pretend at least one op was done in case we are running a benchmark
        // that does not call FinishedOps().
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
        AppendWithSpace(&extra, message_);
        double elapsed = (finish_ - start_) * 1e-6;
        double throughput = (double)done_ / elapsed;

        fprintf(stdout,
                "%-12s : %11.3f micros/op %ld ops/sec;%s%s\n",
                name.ToString().c_str(),
                elapsed * 1e6 / done_,
                (long)throughput,
                (extra.empty() ? "" : " "),
                extra.c_str());
        if (FLAGS_histogram) {
            for (auto it = hist_.begin(); it != hist_.end(); ++it) {
                fprintf(stdout,
                        "Microseconds per %s:\n%s\n",
                        OperationTypeString[it->first].c_str(),
                        it->second->ToString().c_str());
            }
        }
        fflush(stdout);
    }
};

class CombinedStats
{
public:
    void AddStats(const Stats &stat)
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

    void Report(const std::string &bench_name)
    {
        const char *name = bench_name.c_str();
        int num_runs = static_cast<int>(throughput_ops_.size());

        if (throughput_mbs_.size() == throughput_ops_.size()) {
            fprintf(stdout,
                    "%s [AVG    %d runs] : %d ops/sec; %6.1f MB/sec\n"
                    "%s [MEDIAN %d runs] : %d ops/sec; %6.1f MB/sec\n",
                    name,
                    num_runs,
                    static_cast<int>(CalcAvg(throughput_ops_)),
                    CalcAvg(throughput_mbs_),
                    name,
                    num_runs,
                    static_cast<int>(CalcMedian(throughput_ops_)),
                    CalcMedian(throughput_mbs_));
        } else {
            fprintf(stdout,
                    "%s [AVG    %d runs] : %d ops/sec\n"
                    "%s [MEDIAN %d runs] : %d ops/sec\n",
                    name,
                    num_runs,
                    static_cast<int>(CalcAvg(throughput_ops_)),
                    name,
                    num_runs,
                    static_cast<int>(CalcMedian(throughput_ops_)));
        }
    }

private:
    double CalcAvg(std::vector<double> data)
    {
        double avg = 0;
        for (double x : data) {
            avg += x;
        }
        avg = avg / data.size();
        return avg;
    }

    double CalcMedian(std::vector<double> data)
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

    std::vector<double> throughput_ops_;
    std::vector<double> throughput_mbs_;
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState
{
    port::Mutex mu;
    port::CondVar cv;
    int total;
    std::shared_ptr<RateLimiter> write_rate_limiter;
    std::shared_ptr<RateLimiter> read_rate_limiter;

    // Each thread goes through the following states:
    //    (1) initializing
    //    (2) waiting for others to be initialized
    //    (3) running
    //    (4) done

    long num_initialized;
    long num_done;
    bool start;

    SharedState() : cv(&mu) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState
{
    int tid;       // 0..n-1 when running in n threads
    Random64 rand; // Has different seeds for different threads
    Stats stats;
    SharedState *shared;

    /* implicit */ ThreadState(int index)
        : tid(index), rand((FLAGS_seed ? FLAGS_seed : 1000) + index)
    {
    }
};

class Duration
{
public:
    Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0)
    {
        max_seconds_ = max_seconds;
        max_ops_ = max_ops;
        ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
        ops_ = 0;
        start_at_ = FLAGS_env->NowMicros();
    }

    int64_t GetStage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

    bool Done(int64_t increment)
    {
        if (increment <= 0)
            increment = 1; // avoid Done(0) and infinite loops
        ops_ += increment;

        if (max_seconds_) {
            // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
            auto granularity = FLAGS_ops_between_duration_checks;
            if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
                uint64_t now = FLAGS_env->NowMicros();
                return ((now - start_at_) / 1000000) >= max_seconds_;
            } else {
                return false;
            }
        } else {
            return ops_ > max_ops_;
        }
    }

private:
    uint64_t max_seconds_;
    int64_t max_ops_;
    int64_t ops_per_stage_;
    int64_t ops_;
    uint64_t start_at_;
};

class Benchmark
{
private:
    int64_t num_;
    int value_size_;
    int key_size_;
    int prefix_size_;
    int64_t keys_per_prefix_;
    int64_t entries_per_batch_; // TODO for multi set
    double read_random_exp_range_;

    void PrintHeader()
    {
        PrintEnvironment();
        fprintf(stdout, "Keys:       %d bytes each\n", FLAGS_key_size);
        fprintf(stdout,
                "Values:     %d bytes each (%d bytes after compression)\n",
                FLAGS_value_size,
                static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
        fprintf(stdout, "Entries:    %" PRIu64 "\n", num_);
        fprintf(stdout, "Prefix:    %d bytes\n", FLAGS_prefix_size);
        fprintf(stdout, "Keys per prefix:    %" PRIu64 "\n", keys_per_prefix_);
        fprintf(stdout,
                "RawSize:    %.1f MB (estimated)\n",
                ((static_cast<int64_t>(FLAGS_key_size + FLAGS_value_size) * num_) / 1048576.0));
        fprintf(
            stdout,
            "FileSize:   %.1f MB (estimated)\n",
            (((FLAGS_key_size + FLAGS_value_size * FLAGS_compression_ratio) * num_) / 1048576.0));
        fprintf(stdout, "Write rate: %" PRIu64 " bytes/second\n", FLAGS_benchmark_write_rate_limit);
        fprintf(stdout, "Read rate: %" PRIu64 " ops/second\n", FLAGS_benchmark_read_rate_limit);
        if (FLAGS_enable_numa) {
            fprintf(stderr, "Running in NUMA enabled mode.\n");
#ifndef NUMA
            fprintf(stderr, "NUMA is not defined in the system.\n");
            exit(1);
#else
            if (numa_available() == -1) {
                fprintf(stderr, "NUMA is not supported by the system.\n");
                exit(1);
            }
#endif
        }

        PrintWarnings("");
        fprintf(stdout, "------------------------------------------------\n");
    }

    void PrintWarnings(const char *compression)
    {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
        fprintf(stdout, "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
        fprintf(stdout, "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
    }

// Current the following isn't equivalent to OS_LINUX.
#if defined(__linux)
    static Slice TrimSpace(Slice s)
    {
        unsigned int start = 0;
        while (start < s.size() && isspace(s[start])) {
            start++;
        }
        unsigned int limit = static_cast<unsigned int>(s.size());
        while (limit > start && isspace(s[limit - 1])) {
            limit--;
        }
        return Slice(s.data() + start, limit - start);
    }
#endif

    void PrintEnvironment()
    {
#if defined(__linux)
        time_t now = time(nullptr);
        char buf[52];
        // Lint complains about ctime() usage, so replace it with ctime_r(). The
        // requirement is to provide a buffer which is at least 26 bytes.
        fprintf(stderr, "Date:       %s", ctime_r(&now, buf)); // ctime_r() adds newline

        FILE *cpuinfo = fopen("/proc/cpuinfo", "r");
        if (cpuinfo != nullptr) {
            char line[1000];
            int num_cpus = 0;
            std::string cpu_type;
            std::string cache_size;
            while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
                const char *sep = strchr(line, ':');
                if (sep == nullptr) {
                    continue;
                }
                Slice key = TrimSpace(Slice(line, sep - 1 - line));
                Slice val = TrimSpace(Slice(sep + 1));
                if (key == "model name") {
                    ++num_cpus;
                    cpu_type = val.ToString();
                } else if (key == "cache size") {
                    cache_size = val.ToString();
                }
            }
            fclose(cpuinfo);
            fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
            fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
        }
#endif
    }

public:
    Benchmark()
        : num_(FLAGS_num),
          value_size_(FLAGS_value_size),
          key_size_(FLAGS_key_size),
          prefix_size_(FLAGS_prefix_size),
          keys_per_prefix_(FLAGS_keys_per_prefix),
          entries_per_batch_(1),
          read_random_exp_range_(0.0)
    {

        if (FLAGS_prefix_size > FLAGS_key_size) {
            fprintf(stderr, "prefix size is larger than key size");
            exit(1);
        }
    }

    Slice AllocateKey(std::unique_ptr<const char[]> *key_guard)
    {
        char *data = new char[key_size_];
        const char *const_data = data;
        key_guard->reset(const_data);
        return Slice(key_guard->get(), key_size_);
    }

    // Generate key according to the given specification and random number.
    // The resulting key will have the following format (if keys_per_prefix_
    // is positive), extra trailing bytes are either cut off or padded with '0'.
    // The prefix value is derived from key value.
    //   ----------------------------
    //   | prefix 00000 | key 00000 |
    //   ----------------------------
    // If keys_per_prefix_ is 0, the key is simply a binary representation of
    // random number followed by trailing '0's
    //   ----------------------------
    //   |        key 00000         |
    //   ----------------------------
    void GenerateKeyFromInt(uint64_t v, int64_t num_keys, Slice *key)
    {
        char *start = const_cast<char *>(key->data());
        char *pos = start;
        if (keys_per_prefix_ > 0) {
            int64_t num_prefix = num_keys / keys_per_prefix_;
            int64_t prefix = v % num_prefix;
            int bytes_to_fill = std::min(prefix_size_, 8);
            if (port::kLittleEndian) {
                for (int i = 0; i < bytes_to_fill; ++i) {
                    pos[i] = (prefix >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
                }
            } else {
                memcpy(pos, static_cast<void *>(&prefix), bytes_to_fill);
            }
            if (prefix_size_ > 8) {
                // fill the rest with 0s
                memset(pos + 8, '0', prefix_size_ - 8);
            }
            pos += prefix_size_;
        }

        int bytes_to_fill = std::min(key_size_ - static_cast<int>(pos - start), 8);
        if (port::kLittleEndian) {
            for (int i = 0; i < bytes_to_fill; ++i) {
                pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
            }
        } else {
            memcpy(pos, static_cast<void *>(&v), bytes_to_fill);
        }
        pos += bytes_to_fill;
        if (key_size_ > pos - start) {
            memset(pos, '0', key_size_ - (pos - start));
        }
    }

    std::string GetPathForMultiple(std::string base_name, size_t id)
    {
        if (!base_name.empty()) {
#ifndef OS_WIN
            if (base_name.back() != '/') {
                base_name += '/';
            }
#else
            if (base_name.back() != '\\') {
                base_name += '\\';
            }
#endif
        }
        return base_name + ToString(id);
    }

    void Run()
    {
        PrintHeader();
        std::stringstream benchmark_stream(FLAGS_benchmarks);
        std::string name;
        while (std::getline(benchmark_stream, name, ',')) {
            // Sanitize parameters
            num_ = FLAGS_num;
            value_size_ = FLAGS_value_size;
            key_size_ = FLAGS_key_size;
            entries_per_batch_ = FLAGS_batch_size;

            void (Benchmark::*method)(ThreadState *) = nullptr;
            void (Benchmark::*post_process_method)() = nullptr;

            bool fresh_db = false;
            int num_threads = FLAGS_threads;

            int num_repeat = 1;
            int num_warmup = 0;
            if (!name.empty() && *name.rbegin() == ']') {
                auto it = name.find('[');
                if (it == std::string::npos) {
                    fprintf(stderr, "unknown benchmark arguments '%s'\n", name.c_str());
                    exit(1);
                }
                std::string args = name.substr(it + 1);
                args.resize(args.size() - 1);
                name.resize(it);

                std::string bench_arg;
                std::stringstream args_stream(args);
                while (std::getline(args_stream, bench_arg, '-')) {
                    if (bench_arg.empty()) {
                        continue;
                    }
                    if (bench_arg[0] == 'X') {
                        // Repeat the benchmark n times
                        std::string num_str = bench_arg.substr(1);
                        num_repeat = std::stoi(num_str);
                    } else if (bench_arg[0] == 'W') {
                        // Warm up the benchmark for n times
                        std::string num_str = bench_arg.substr(1);
                        num_warmup = std::stoi(num_str);
                    }
                }
            }

            // Both fillseqdeterministic and filluniquerandomdeterministic
            // fill the levels except the max level with UNIQUE_RANDOM
            // and fill the max level with fillseq and filluniquerandom, respectively
            if (name == "fillseq_pegasus") {
                method = &Benchmark::WriteSeqRRDB;
            } else if (name == "fillrandom_pegasus") {
                method = &Benchmark::WriteRandomRRDB;
            } else if (name == "multi_set_pegasus") {
                method = &Benchmark::WriteMultiRRDB;
            } else if (name == "scan_pegasus") {
                method = &Benchmark::ScanRRDB;
            } else if (name == "filluniquerandom_pegasus") {
                if (num_threads > 1) {
                    fprintf(stderr,
                            "filluniquerandom_pegasus multithreaded not supported"
                            ", use 1 thread");
                    num_threads = 1;
                }
                method = &Benchmark::WriteUniqueRandomRRDB;
            } else if (name == "readrandom_pegasus") {
                method = &Benchmark::ReadRandomRRDB;
            } else if (name == "deleteseq_pegasus") {
                method = &Benchmark::DeleteSeqRRDB;
            } else if (name == "deleterandom_pegasus") {
                method = &Benchmark::DeleteRandomRRDB;
            } else if (!name.empty()) { // No error message for empty name
                fprintf(stderr, "unknown benchmark '%s'\n", name.c_str());
                exit(1);
            }

            if (method != nullptr) {
                if (num_warmup > 0) {
                    printf("Warming up benchmark by running %d times\n", num_warmup);
                }

                for (int i = 0; i < num_warmup; i++) {
                    RunBenchmark(num_threads, name, method);
                }

                if (num_repeat > 1) {
                    printf("Running benchmark for %d times\n", num_repeat);
                }

                CombinedStats combined_stats;
                for (int i = 0; i < num_repeat; i++) {
                    Stats stats = RunBenchmark(num_threads, name, method);
                    combined_stats.AddStats(stats);
                }
                if (num_repeat > 1) {
                    combined_stats.Report(name);
                }
            }
            if (post_process_method != nullptr) {
                (this->*post_process_method)();
            }
        }
    }

private:
    struct ThreadArg
    {
        Benchmark *bm;
        SharedState *shared;
        ThreadState *thread;
        void (Benchmark::*method)(ThreadState *);
    };

    static void ThreadBody(void *v)
    {
        ThreadArg *arg = reinterpret_cast<ThreadArg *>(v);
        SharedState *shared = arg->shared;
        ThreadState *thread = arg->thread;
        {
            MutexLock l(&shared->mu);
            shared->num_initialized++;
            if (shared->num_initialized >= shared->total) {
                shared->cv.SignalAll();
            }
            while (!shared->start) {
                shared->cv.Wait();
            }
        }

        thread->stats.Start(thread->tid);
        (arg->bm->*(arg->method))(thread);
        thread->stats.Stop();

        {
            MutexLock l(&shared->mu);
            shared->num_done++;
            if (shared->num_done >= shared->total) {
                shared->cv.SignalAll();
            }
        }
    }

    Stats RunBenchmark(int n, Slice name, void (Benchmark::*method)(ThreadState *))
    {
        SharedState shared;
        shared.total = n;
        shared.num_initialized = 0;
        shared.num_done = 0;
        shared.start = false;
        if (FLAGS_benchmark_write_rate_limit > 0) {
            shared.write_rate_limiter.reset(
                NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
        }
        if (FLAGS_benchmark_read_rate_limit > 0) {
            shared.read_rate_limiter.reset(NewGenericRateLimiter(FLAGS_benchmark_read_rate_limit,
                                                                 100000 /* refill_period_us */,
                                                                 10 /* fairness */,
                                                                 RateLimiter::Mode::kReadsOnly));
        }

        std::unique_ptr<ReporterAgent> reporter_agent;
        if (FLAGS_report_interval_seconds > 0) {
            reporter_agent.reset(
                new ReporterAgent(FLAGS_env, FLAGS_report_file, FLAGS_report_interval_seconds));
        }

        ThreadArg *arg = new ThreadArg[n];

        for (int i = 0; i < n; i++) {
#ifdef NUMA
            if (FLAGS_enable_numa) {
                // Performs a local allocation of memory to threads in numa node.
                int n_nodes = numa_num_task_nodes(); // Number of nodes in NUMA.
                numa_exit_on_error = 1;
                int numa_node = i % n_nodes;
                bitmask *nodes = numa_allocate_nodemask();
                numa_bitmask_clearall(nodes);
                numa_bitmask_setbit(nodes, numa_node);
                // numa_bind() call binds the process to the node and these
                // properties are passed on to the thread that is created in
                // StartThread method called later in the loop.
                numa_bind(nodes);
                numa_set_strict(1);
                numa_free_nodemask(nodes);
            }
#endif
            arg[i].bm = this;
            arg[i].method = method;
            arg[i].shared = &shared;
            arg[i].thread = new ThreadState(i);
            arg[i].thread->stats.SetReporterAgent(reporter_agent.get());
            arg[i].thread->shared = &shared;
            FLAGS_env->StartThread(ThreadBody, &arg[i]);
        }

        shared.mu.Lock();
        while (shared.num_initialized < n) {
            shared.cv.Wait();
        }

        shared.start = true;
        shared.cv.SignalAll();
        while (shared.num_done < n) {
            shared.cv.Wait();
        }
        shared.mu.Unlock();

        // Stats for some threads can be excluded.
        Stats merge_stats;
        for (int i = 0; i < n; i++) {
            merge_stats.Merge(arg[i].thread->stats);
        }
        merge_stats.Report(name);

        for (int i = 0; i < n; i++) {
            delete arg[i].thread;
        }
        delete[] arg;

        return merge_stats;
    }

    enum WriteMode
    {
        RANDOM,
        SEQUENTIAL,
        UNIQUE_RANDOM
    };

    void WriteSeqRRDB(ThreadState *thread) { DoWriteRRDB(thread, SEQUENTIAL); }

    void WriteRandomRRDB(ThreadState *thread) { DoWriteRRDB(thread, RANDOM); }

    void WriteMultiRRDB(ThreadState *thread) { DoWriteMultiRRDB(thread); }

    void ScanRRDB(ThreadState *thread) { DoScanRRDB(thread); }

    void WriteUniqueRandomRRDB(ThreadState *thread) { DoWriteRRDB(thread, UNIQUE_RANDOM); }

    class KeyGenerator
    {
    public:
        KeyGenerator(Random64 *rand, WriteMode mode, uint64_t num, uint64_t num_per_set = 64 * 1024)
            : rand_(rand), mode_(mode), num_(num), next_(0)
        {
            if (mode_ == UNIQUE_RANDOM) {
                // NOTE: if memory consumption of this approach becomes a concern,
                // we can either break it into pieces and only random shuffle a section
                // each time. Alternatively, use a bit map implementation
                // (https://reviews.facebook.net/differential/diff/54627/)
                values_.resize(num_);
                for (uint64_t i = 0; i < num_; ++i) {
                    values_[i] = i;
                }
                std::shuffle(values_.begin(),
                             values_.end(),
                             std::default_random_engine(static_cast<unsigned int>(FLAGS_seed)));
            }
        }

        uint64_t Next()
        {
            switch (mode_) {
            case SEQUENTIAL:
                return next_++;
            case RANDOM:
                return rand_->Next() % num_;
            case UNIQUE_RANDOM:
                assert(next_ + 1 <= num_); // TODO < -> <=
                return values_[next_++];
            }
            assert(false);
            return std::numeric_limits<uint64_t>::max();
        }

    private:
        Random64 *rand_;
        WriteMode mode_;
        const uint64_t num_;
        uint64_t next_;
        std::vector<uint64_t> values_;
    };

    void DoWriteRRDB(ThreadState *thread, WriteMode write_mode)
    {
        const int test_duration = write_mode == RANDOM ? FLAGS_duration : 0;
        const int64_t num_ops = num_;

        std::unique_ptr<KeyGenerator> key_gen;
        int64_t max_ops = num_ops;
        int64_t ops_per_stage = max_ops;

        Duration duration(test_duration, max_ops, ops_per_stage);
        key_gen.reset(new KeyGenerator(&(thread->rand), write_mode, num_, ops_per_stage));

        if (num_ != FLAGS_num) {
            char msg[100];
            snprintf(msg, sizeof(msg), "(%" PRIu64 " ops)", num_);
            thread->stats.AddMessage(msg);
        }

        RandomGenerator gen;
        int64_t bytes = 0;
        pegasus_client *client = pegasus_client_factory::get_client(
            FLAGS_pegasus_cluster_name.c_str(), FLAGS_pegasus_app_name.c_str());
        if (client == nullptr) {
            fprintf(stderr, "create client error\n");
            exit(1);
        }

        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);
        while (!duration.Done(1)) {
            if (thread->shared->write_rate_limiter.get() != nullptr) {
                thread->shared->write_rate_limiter->Request(value_size_ + key_size_, Env::IO_HIGH);
            }
            int64_t rand_num = key_gen->Next();
            GenerateKeyFromInt(rand_num, FLAGS_num, &key);
            int try_count = 0;
            while (true) {
                try_count++;
                int ret = client->set(key.ToString(),
                                      "",
                                      gen.Generate(value_size_).ToString(),
                                      FLAGS_pegasus_timeout_ms);
                if (ret == ::pegasus::PERR_OK) {
                    bytes += value_size_ + key_size_;
                    break;
                } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                    fprintf(stderr, "Set returned an error: %s\n", client->get_error_string(ret));
                    exit(1);
                } else {
                    fprintf(stderr, "Set timeout, retry(%d)\n", try_count);
                }
            }
            thread->stats.FinishedOps(nullptr, nullptr, 1, kWrite);
        }
        thread->stats.AddBytes(bytes);
    }

    void DoWriteMultiRRDB(ThreadState *thread)
    {
        const int test_duration = 0;
        const int64_t num_ops = num_;

        std::unique_ptr<KeyGenerator> key_gen;
        int64_t max_ops = num_ops;
        int64_t ops_per_stage = max_ops;

        Duration duration(test_duration, max_ops, ops_per_stage);
        key_gen.reset(new KeyGenerator(&(thread->rand), SEQUENTIAL, num_, ops_per_stage));

        if (num_ != FLAGS_num) {
            char msg[100];
            snprintf(msg, sizeof(msg), "(%" PRIu64 " ops)", num_);
            thread->stats.AddMessage(msg);
        }

        RandomGenerator gen;
        int64_t bytes = 0;
        pegasus_client *client = pegasus_client_factory::get_client(
            FLAGS_pegasus_cluster_name.c_str(), FLAGS_pegasus_app_name.c_str());
        if (client == nullptr) {
            fprintf(stderr, "create client error\n");
            exit(1);
        }

        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);
        while (!duration.Done(1)) {
            if (thread->shared->write_rate_limiter.get() != nullptr) {
                thread->shared->write_rate_limiter->Request(value_size_ + key_size_, Env::IO_HIGH);
            }
            int64_t rand_num = key_gen->Next();
            GenerateKeyFromInt(rand_num, FLAGS_num, &key);
            std::map<std::string, std::string> kvs;
            for (int i = 0; i < FLAGS_sortkey_count_per_hashkey; ++i) {
                std::string i_str = std::to_string(i);
                kvs.emplace(i_str, i_str);
            }

            int try_count = 0;
            while (true) {
                try_count++;
                int ret = client->multi_set(key.ToString(), kvs, FLAGS_pegasus_timeout_ms);
                if (ret == ::pegasus::PERR_OK) {
                    bytes += value_size_ + key_size_;
                    break;
                } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                    fprintf(
                        stderr, "MultiSet returned an error: %s\n", client->get_error_string(ret));
                    exit(1);
                } else {
                    fprintf(stderr, "MultiSet timeout, retry(%d)\n", try_count);
                }
            }
            thread->stats.FinishedOps(nullptr, nullptr, 1, kWrite);
        }
        thread->stats.AddBytes(bytes);
    }

    void DoScanRRDB(ThreadState *thread)
    {
        const int test_duration = 0;
        const int64_t num_ops = num_;

        std::unique_ptr<KeyGenerator> key_gen;
        int64_t max_ops = num_ops;
        int64_t ops_per_stage = max_ops;

        Duration duration(test_duration, max_ops, ops_per_stage);
        key_gen.reset(new KeyGenerator(&(thread->rand), RANDOM, num_, ops_per_stage));

        if (num_ != FLAGS_num) {
            char msg[100];
            snprintf(msg, sizeof(msg), "(%" PRIu64 " ops)", num_);
            thread->stats.AddMessage(msg);
        }

        RandomGenerator gen;
        int64_t bytes = 0;
        pegasus_client *client = pegasus_client_factory::get_client(
            FLAGS_pegasus_cluster_name.c_str(), FLAGS_pegasus_app_name.c_str());
        if (client == nullptr) {
            fprintf(stderr, "create client error\n");
            exit(1);
        }

        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);
        while (!duration.Done(1)) {
            if (thread->shared->write_rate_limiter.get() != nullptr) {
                thread->shared->write_rate_limiter->Request(value_size_ + key_size_, Env::IO_HIGH);
            }
            int64_t rand_num = key_gen->Next();
            GenerateKeyFromInt(rand_num, FLAGS_num, &key);

            int try_count = 0;
            pegasus::pegasus_client::pegasus_scanner *scanner = nullptr;
            while (true) {
                try_count++;
                int ret = client->get_scanner(
                    key.ToString(), "", "", pegasus::pegasus_client::scan_options(), scanner);
                if (ret == ::pegasus::PERR_OK) {
                    break;
                } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                    fprintf(stderr, "Scan returned an error: %s\n", client->get_error_string(ret));
                    exit(1);
                } else {
                    fprintf(stderr, "Scan timeout, retry(%d)\n", try_count);
                }
            }

            assert(scanner != nullptr);
            try_count = 0;
            std::string hashkey;
            std::string sortkey;
            std::string value;
            while (true) {
                try_count++;
                int ret = scanner->next(hashkey, sortkey, value);
                if (ret == ::pegasus::PERR_OK || ret == pegasus::PERR_SCAN_COMPLETE) {
                    bytes += sortkey.length() + value.length();
                    if (ret == pegasus::PERR_SCAN_COMPLETE) {
                        break;
                    }
                } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                    fprintf(stderr, "Scan returned an error: %s\n", client->get_error_string(ret));
                    exit(1);
                } else {
                    fprintf(stderr, "Scan timeout, retry(%d)\n", try_count);
                }
            }
            thread->stats.FinishedOps(nullptr, nullptr, 1, kScan);
        }
        thread->stats.AddBytes(bytes);
    }

    int64_t GetRandomKey(Random64 *rand)
    {
        uint64_t rand_int = rand->Next();
        int64_t key_rand;
        if (read_random_exp_range_ == 0) {
            key_rand = rand_int % FLAGS_num;
        } else {
            const uint64_t kBigInt = static_cast<uint64_t>(1U) << 62;
            long double order = -static_cast<long double>(rand_int % kBigInt) /
                                static_cast<long double>(kBigInt) * read_random_exp_range_;
            long double exp_ran = std::exp(order);
            uint64_t rand_num = static_cast<int64_t>(exp_ran * static_cast<long double>(FLAGS_num));
            // Map to a different number to avoid locality.
            const uint64_t kBigPrime = 0x5bd1e995;
            // Overflow is like %(2^64). Will have little impact of results.
            key_rand = static_cast<int64_t>((rand_num * kBigPrime) % FLAGS_num);
        }
        return key_rand;
    }

    void ReadRandomRRDB(ThreadState *thread)
    {
        int64_t read = 0;
        int64_t found = 0;
        int64_t bytes = 0;
        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);
        pegasus_client *client = pegasus_client_factory::get_client(
            FLAGS_pegasus_cluster_name.c_str(), FLAGS_pegasus_app_name.c_str());
        if (client == nullptr) {
            fprintf(stderr, "Create client error\n");
            exit(1);
        }

        Duration duration(FLAGS_duration, FLAGS_num);
        while (!duration.Done(1)) {
            // We use same key_rand as seed for key and column family so that we can
            // deterministically find the cfh corresponding to a particular key, as it
            // is done in DoWrite method.
            int64_t key_rand = GetRandomKey(&thread->rand);
            GenerateKeyFromInt(key_rand, FLAGS_num, &key);
            read++;
            int try_count = 0;
            while (true) {
                try_count++;
                std::string value;
                int ret = client->get(key.ToString(), "", value, FLAGS_pegasus_timeout_ms);
                if (ret == ::pegasus::PERR_OK) {
                    found++;
                    bytes += key.size() + value.size();
                    break;
                } else if (ret == ::pegasus::PERR_NOT_FOUND) {
                    break;
                } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                    fprintf(stderr, "Get returned an error: %s\n", client->get_error_string(ret));
                    exit(1);
                } else {
                    fprintf(stderr, "Get timeout, retry(%d)\n", try_count);
                }
            }
            thread->stats.FinishedOps(nullptr, nullptr, 1, kRead);
        }

        char msg[100];
        snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)", found, read);

        thread->stats.AddBytes(bytes);
        thread->stats.AddMessage(msg);
    }

    void DoDeleteRRDB(ThreadState *thread, bool seq)
    {
        Duration duration(seq ? 0 : FLAGS_duration, num_);
        int64_t i = 0;
        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);

        pegasus_client *client = pegasus_client_factory::get_client(
            FLAGS_pegasus_cluster_name.c_str(), FLAGS_pegasus_app_name.c_str());
        if (client == nullptr) {
            fprintf(stderr, "create client error\n");
            exit(1);
        }

        while (!duration.Done(1)) {
            const int64_t k = seq ? i : (thread->rand.Next() % FLAGS_num);
            GenerateKeyFromInt(k, FLAGS_num, &key);
            int try_count = 0;
            while (true) {
                try_count++;
                int ret = client->del(key.ToString(), "", FLAGS_pegasus_timeout_ms);
                if (ret == ::pegasus::PERR_OK) {
                    break;
                } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                    fprintf(stderr, "Del returned an error: %s\n", client->get_error_string(ret));
                    exit(1);
                } else {
                    fprintf(stderr, "Get timeout, retry(%d)\n", try_count);
                }
            }
            thread->stats.FinishedOps(nullptr, nullptr, 1, kDelete);
            i++;
        }
    }

    void DeleteSeqRRDB(ThreadState *thread) { DoDeleteRRDB(thread, true); }

    void DeleteRandomRRDB(ThreadState *thread) { DoDeleteRRDB(thread, false); }
};

} // namespace rocksdb

int db_bench_tool(int argc, char **argv)
{
    static bool initialized = false;
    if (!initialized) {
        SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) + " [OPTIONS]...");
        initialized = true;
    }
    ParseCommandLineFlags(&argc, &argv, true);

    bool init = ::pegasus::pegasus_client_factory::initialize(FLAGS_pegasus_config.c_str());
    if (!init) {
        fprintf(stderr, "Init pegasus error\n");
        return -1;
    }
    sleep(1);
    fprintf(stdout, "Init pegasus succeed\n");

    rocksdb::Benchmark benchmark;
    benchmark.Run();

    return 0;
}

int main(int argc, char **argv) { return db_bench_tool(argc, argv); }
