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
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "utils/autoref_ptr.h"
#include "utils/fmt_utils.h"

typedef enum dsn_perf_counter_type_t
{
    COUNTER_TYPE_NUMBER,
    COUNTER_TYPE_VOLATILE_NUMBER, // special kind of NUMBER which will be reset on get
    COUNTER_TYPE_RATE,
    COUNTER_TYPE_NUMBER_PERCENTILES,
    COUNTER_TYPE_COUNT,
    COUNTER_TYPE_INVALID
} dsn_perf_counter_type_t;
USER_DEFINED_ENUM_FORMATTER(dsn_perf_counter_type_t)

typedef enum dsn_perf_counter_percentile_type_t
{
    COUNTER_PERCENTILE_50,
    COUNTER_PERCENTILE_90,
    COUNTER_PERCENTILE_95,
    COUNTER_PERCENTILE_99,
    COUNTER_PERCENTILE_999,

    COUNTER_PERCENTILE_COUNT,
    COUNTER_PERCENTILE_INVALID
} dsn_perf_counter_percentile_type_t;

const char *dsn_counter_type_to_string(dsn_perf_counter_type_t t);
dsn_perf_counter_type_t dsn_counter_type_from_string(const char *str);

const char *dsn_percentile_type_to_string(dsn_perf_counter_percentile_type_t t);
dsn_perf_counter_percentile_type_t dsn_percentile_type_from_string(const char *str);

namespace dsn {

class perf_counter : public ref_counter
{
public:
    perf_counter(const char *app,
                 const char *section,
                 const char *name,
                 dsn_perf_counter_type_t type,
                 const char *dsptr)
        : _app(app), _section(section), _name(name), _dsptr(dsptr), _type(type)
    {
        build_full_name(app, section, name, _full_name);
    }

    virtual ~perf_counter() {}

    virtual void increment() = 0;
    virtual void decrement() = 0;
    virtual void add(int64_t val) = 0;
    virtual void set(int64_t val) = 0;
    virtual double get_value() = 0;
    virtual int64_t get_integer_value() = 0;
    virtual double get_percentile(dsn_perf_counter_percentile_type_t type) = 0;

    typedef std::vector<std::pair<int64_t *, int>> samples_t;

    // return actual sample count, must <= required_sample_count
    virtual int get_latest_samples(int required_sample_count, /*out*/ samples_t &samples) const
    {
        return 0;
    }

    // return the latest sample value
    virtual int64_t get_latest_sample() const { return 0; }

    const char *full_name() const { return _full_name.c_str(); }
    const char *app() const { return _app.c_str(); }
    const char *section() const { return _section.c_str(); }
    const char *name() const { return _name.c_str(); }
    const char *dsptr() const { return _dsptr.c_str(); }
    dsn_perf_counter_type_t type() const { return _type; }

    static void build_full_name(const char *app,
                                const char *section,
                                const char *name,
                                /*out*/ std::string &counter_name)
    {
        std::stringstream ss;
        ss << app << "*" << section << "*" << name;
        counter_name = ss.str();
    }

private:
    std::string _app;
    std::string _section;
    std::string _name;
    std::string _dsptr;
    dsn_perf_counter_type_t _type;

    std::string _full_name;
    friend class perf_counters;
};
typedef ref_ptr<perf_counter> perf_counter_ptr;

} // namespace dsn
