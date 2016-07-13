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

# pragma once

# include <dsn/tool-api/perf_counter.h>
# include <dsn/utility/singleton.h>
# include <dsn/utility/synchronize.h>
# include <map>
# include <sstream>
# include <queue>

namespace dsn {

class perf_counters : public dsn::utils::singleton<perf_counters>
{
public:
    perf_counters(void);
    ~perf_counters(void);

    perf_counter_ptr get_counter(
                    const char* app,
                    const char *section, 
                    const char *name, 
                    dsn_perf_counter_type_t flags, 
                    const char *dsptr,
                    bool create_if_not_exist = false
                    );


    // full_name = perf_counter::build_full_name(...);
    perf_counter_ptr get_counter(const char* full_name);
    perf_counter_ptr get_counter(uint64_t index);
    bool remove_counter(const char* full_name);
    
    void register_factory(perf_counter::factory factory);
    static std::string list_counter(const std::vector<std::string>& args);
    static std::string get_counter_value(const std::vector<std::string>& args);
    static std::string get_counter_sample(const std::vector<std::string>& args);
    static std::string get_counter_value_i(const std::vector<std::string>& args);
    static std::string get_counter_sample_i(const std::vector<std::string>& args);
    static std::string get_counter_index(const std::vector<std::string>& args);

    typedef std::map<std::string, perf_counter_ptr > all_counters;

private:
    std::string list_counter_internal(const std::vector<std::string>& args);
    mutable utils::rw_lock_nr  _lock;
    all_counters               _counters;
    perf_counter::factory      _factory;

    uint64_t                   _max_counter_count;
    perf_counter               **_quick_counters;
    std::queue<uint64_t>       _quick_counters_empty_slots;
};

} // end namespace dsn::utils
