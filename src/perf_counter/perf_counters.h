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

#pragma once

#include <stdint.h>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "perf_counter.h"
#include "utils/singleton.h"
#include "utils/synchronize.h"

namespace dsn {

class command_deregister;

/// Registry of all perf counters, users can get/create a specific perf counter
/// via `get_app_counter` and `get_global_counter`.
/// To push metrics to some monitoring systems (e.g Prometheus), users can
/// collect all the perf counters via `take_snapshot`.
class perf_counters : public utils::singleton<perf_counters>
{
public:
    ///
    /// get counter with (current_app_name, section, name), try to create a new one
    /// if create_if_not_exist==true
    ///
    perf_counter_ptr get_app_counter(const char *section,
                                     const char *name,
                                     dsn_perf_counter_type_t flags,
                                     const char *dsptr,
                                     bool create_if_not_exist);

    ///
    /// get counter with (app, section, name), try to create a new one
    /// if create_if_not_exist==true
    ///
    perf_counter_ptr get_global_counter(const char *app,
                                        const char *section,
                                        const char *name,
                                        dsn_perf_counter_type_t flags,
                                        const char *dsptr,
                                        bool create_if_not_exist);

    ///
    /// please call remove_counter if a previous get_app_counter/get_global_counter is called
    ///
    bool remove_counter(const std::string &full_name);

    perf_counter_ptr get_counter(const std::string &full_name);

    struct counter_snapshot
    {
        double value{0.0};
        std::string name;
        dsn_perf_counter_type_t type;

    private:
        friend class perf_counters;
        bool updated_recently{false};
    };

    ///
    /// Some types of perf counters(rate, volatile_number) may change it's value after you visit
    /// it, so we'd better take a snapshot of all counters before the visiting in case that
    /// we may get value of counters repeatedly.
    ///
    /// here we provider several functions to support these semantics:
    ///     take_snapshot
    ///     iterate_snapshot
    ///     query_snapshot
    ///
    /// we you call take_snapshot, a snapshot of current counters and their values will be
    /// stored in internal variables of perf_counters module,
    /// then you can iterate all counters or query some specific counters.
    /// if another take_snapshot is called, the old one will be overwrite.
    ///
    /// the snapshot will be protected by a read-write lock internally.
    ///
    /// when you read the snapshot, you should provide a callback called "snapshot_visitor".
    /// this callback will be called once for each requested counter.
    ///
    /// TODO: totally eliminate this stupid snapshot feature with a better metrics library
    /// (a metric library which doesn't have SIDE EFFECT when you visit metric!!!)
    ///
    typedef std::function<void(const counter_snapshot &)> snapshot_iterator;
    void take_snapshot();
    void iterate_snapshot(const snapshot_iterator &v) const;

    // if found is not nullptr, then whether a counter was found will be stored in it
    // that is to say:
    //    if (found != nullptr && (*found)[i]==true) {
    //        counters[i] is in the snapshot
    //    }
    void query_snapshot(const std::vector<std::string> &counters,
                        const snapshot_iterator &v,
                        std::vector<bool> *found) const;

    // this function collects all counters to perf_counter_info which match
    // any of the regular expressions in args and returns the json representation
    // of perf_counter_info
    std::string list_snapshot_by_regexp(const std::vector<std::string> &args) const;

    // this function collects all counters to perf_counter_info which satisfy
    // any of the filters generated by args and returns the json representation
    // of perf_counter_info
    std::string list_snapshot_by_literal(
        const std::vector<std::string> &args,
        std::function<bool(const std::string &arg, const counter_snapshot &cs)> filter) const;

private:
    friend class utils::singleton<perf_counters>;

    perf_counters();
    ~perf_counters();

    // full_name = perf_counter::build_full_name(...);
    perf_counter *new_counter(const char *app,
                              const char *section,
                              const char *name,
                              dsn_perf_counter_type_t type,
                              const char *dsptr);

    void get_all_counters(std::vector<perf_counter_ptr> *all) const;

    mutable utils::rw_lock_nr _lock;
    // keep counter as a refptr to make the counter can be safely accessed
    // by get_all_counters and remove_counter concurrently
    //
    // keep an user reference for each counter coz the counter may be shared by different modules
    // called get_xxx_counter
    struct counter_object
    {
        perf_counter_ptr counter;
        int user_reference;
    };
    std::unordered_map<std::string, counter_object> _counters;

    mutable utils::rw_lock_nr _snapshot_lock;
    std::unordered_map<std::string, counter_snapshot> _snapshots;

    // timestamp in seconds when take snapshot of current counters
    int64_t _timestamp;

    std::vector<std::unique_ptr<command_deregister>> _cmds;
};

} // namespace dsn
