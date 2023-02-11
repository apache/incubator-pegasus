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

#pragma once

#include <stdint.h>
#include <memory>
#include <string>

#include "utils.h"

namespace rocksdb {
class Statistics;
} // namespace rocksdb

namespace pegasus {
namespace test {
class statistics
{
public:
    statistics(std::shared_ptr<rocksdb::Statistics> hist_stats);
    void start();
    void finished_ops(int64_t num_ops, enum operation_type op_type);
    void stop();
    void merge(const statistics &other);
    void report(operation_type op_type);
    void add_bytes(int64_t n);
    void add_message(const std::string &msg);

private:
    uint32_t report_step(uint64_t current_report) const;

    // the start time of benchmark
    uint64_t _start;
    // the stop time of benchmark
    uint64_t _finish;
    // how many operations are done
    uint64_t _done;
    // the point(operation count) at which the next report
    uint64_t _next_report;
    // how many bytes the benchmark read/write
    uint64_t _bytes;
    // the last operation's finish time
    uint64_t _last_op_finish;
    // the information of benchmark operation
    std::string _message;
    // histogram performance analyzer
    std::shared_ptr<rocksdb::Statistics> _hist_stats;
};
} // namespace test
} // namespace pegasus
