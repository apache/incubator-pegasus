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

#include <dsn/tool-api/perf_counter.h>
#include <dsn/utility/singleton.h>
#include <dsn/utility/synchronize.h>
#include <map>
#include <sstream>
#include <queue>

namespace dsn {

// manager of all perf counters of the process, perf counter users can use get_xxx_counter
// functions to get a specific perf counter and change the value.
// monitor system can user get_all_counters to get all the perf counters values and push it to
// some monitor dashboard
class perf_counters : public dsn::utils::singleton<perf_counters>
{
public:
    perf_counters(void);
    ~perf_counters(void);

    // get counter with (current_app_name, section, name), try to create a new one
    // if create_if_not_exist==true
    perf_counter_ptr get_app_counter(const char *section,
                                     const char *name,
                                     dsn_perf_counter_type_t flags,
                                     const char *dsptr,
                                     bool create_if_not_exist);

    // get counter with (app, section, name), try to create a new one
    // if create_if_not_exist==true
    perf_counter_ptr get_global_counter(const char *app,
                                        const char *section,
                                        const char *name,
                                        dsn_perf_counter_type_t flags,
                                        const char *dsptr,
                                        bool create_if_not_exist);

    // please call remove_counter if a previous get_app_counter/get_global_counter is called
    bool remove_counter(const char *full_name);

    // get all the existed perf counters, useful for monitor system
    void get_all_counters(/*out*/ std::vector<dsn::perf_counter_ptr> *counter_vec);

    void register_factory(perf_counter::factory factory);

    static std::string list_counter(const std::vector<std::string> &args);
    static std::string get_counter_value(const std::vector<std::string> &args);
    static std::string get_counter_sample(const std::vector<std::string> &args);

private:
    // full_name = perf_counter::build_full_name(...);
    perf_counter_ptr get_counter(const char *full_name);
    std::string list_counter_internal(const std::vector<std::string> &args);

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
    std::map<std::string, counter_object> _counters;
    perf_counter::factory _factory;
};

} // end namespace dsn::utils
