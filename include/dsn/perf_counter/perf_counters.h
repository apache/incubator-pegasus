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

#include <dsn/utility/singleton.h>
#include <dsn/utility/synchronize.h>
#include <dsn/perf_counter/perf_counter.h>
#include <map>
#include <sstream>
#include <queue>
#include <functional>

namespace dsn {

///
/// manager of all perf counters of the process, perf counter users can use get_xxx_counter
/// functions to get a specific perf counter and change the value.
/// monitor system can user get_all_counters to get all the perf counters values and push it to
/// some monitor dashboard
///
class perf_counters : public dsn::utils::singleton<perf_counters>
{
public:
    perf_counters();
    ~perf_counters();

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
    bool remove_counter(const char *full_name);

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
    typedef std::function<void(const dsn::perf_counter_ptr &counter, double value)>
        snapshot_iterator;
    void take_snapshot();
    void iterate_snapshot(const snapshot_iterator &v);

    // if found is not nullptr, then whether a counter was found will be stored in it
    // that is to say:
    //    if (found != nullptr && (*found)[i]==true) {
    //        counters[i] is in the snapshot
    //    }
    void query_snapshot(const std::vector<std::string> &counters,
                        const snapshot_iterator &v,
                        std::vector<bool> *found);

    // this function collects all counters to perf_counter_info which matches
    // any of regular expression in args and returns the json representation
    // of perf_counter_info
    std::string list_snapshot_by_regexp(const std::vector<std::string> &args);

private:
    // full_name = perf_counter::build_full_name(...);
    perf_counter *new_counter(const char *app,
                              const char *section,
                              const char *name,
                              dsn_perf_counter_type_t type,
                              const char *dsptr);
    void get_all_counters(std::vector<dsn::perf_counter_ptr> *all);

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
    struct counter_snapshot
    {
        perf_counter_ptr counter{nullptr};
        double value{0.0};
        bool updated_recently{false};
    };
    std::unordered_map<std::string, counter_snapshot> _snapshots;
};

} // end namespace dsn::utils
