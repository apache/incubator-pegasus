/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <unordered_map>
#include "utils/fmt_logging.h"

#include "statistics.h"
#include "config.h"

namespace pegasus {
namespace test {
std::unordered_map<operation_type, std::string, std::hash<unsigned char>> operation_type_string = {
    {kUnknown, "unKnown"},
    {kRead, "read"},
    {kWrite, "write"},
    {kDelete, "delete"},
    {kMultiSet, "multiSet"},
    {kMultiGet, "multiGet"}};

statistics::statistics(std::shared_ptr<rocksdb::Statistics> hist_stats)
{
    _next_report = 100;
    _done = 0;
    _bytes = 0;
    _start = config::instance().env->NowMicros();
    _last_op_finish = _start;
    _finish = _start;
    _hist_stats = hist_stats;
    _message.clear();
}

void statistics::start()
{
    _start = config::instance().env->NowMicros();
    _last_op_finish = _start;
    _finish = _start;
}

void statistics::merge(const statistics &other)
{
    _done += other._done;
    _bytes += other._bytes;
    _start = std::min(other._start, _start);
    _finish = std::max(other._finish, _finish);
    this->add_message(other._message);
}

void statistics::stop() { _finish = config::instance().env->NowMicros(); }

void statistics::finished_ops(int64_t num_ops, enum operation_type op_type)
{
    uint64_t now = config::instance().env->NowMicros();
    uint64_t micros = now - _last_op_finish;
    _last_op_finish = now;

    // if there is a long operation, print warning message
    if (micros > 20000) {
        fmt::print(stderr, "long op: {} micros\r", micros);
    }

    // print the benchmark running status
    _done += num_ops;
    if (_done >= _next_report) {
        _next_report += report_step(_next_report);
        fmt::print(stderr, "... finished {} ops\r", _done);
    }

    // add excution time of this operation to _hist_stats
    if (_hist_stats) {
        _hist_stats->measureTime(op_type, micros);
    }
}

void statistics::report(operation_type op_type)
{
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call finished_ops().
    if (_done < 1)
        _done = 1;

    // elasped time(s)
    double elapsed = (_finish - _start) * 1e-6;

    // append rate(MBytes) and _message to extra
    std::string extra;
    if (_bytes > 0) {
        // Rate is computed on actual elapsed time, not the sum of per-thread
        // elapsed times.
        extra = fmt::format("{} MB/s ", (_bytes >> 20) / elapsed);
    }
    extra.append(_message);

    // print report
    fmt::print(stdout,
               "Statistics for {}:  \n{} micros/op; {} ops/sec; {}\n",
               operation_type_string[op_type],
               elapsed * 1e6 / _done,
               static_cast<long>(_done / elapsed),
               extra);

    // print histogram if _hist_stats is not NULL
    if (_hist_stats) {
        fmt::print(stdout, "{}\n", _hist_stats->getHistogramString(op_type));
    }
}

void statistics::add_message(const std::string &msg)
{
    if (msg.empty())
        return;

    if (!_message.empty()) {
        _message.push_back(' ');
    }
    _message.append(msg);
}

void statistics::add_bytes(int64_t n) { _bytes += n; }

uint32_t statistics::report_step(uint64_t current_report) const
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
