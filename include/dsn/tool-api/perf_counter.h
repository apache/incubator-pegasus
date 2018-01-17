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

#pragma once

#include <dsn/utility/enum_helper.h>
#include <dsn/utility/autoref_ptr.h>
#include <dsn/utility/dlib.h>
#include <dsn/service_api_c.h>
#include <memory>
#include <sstream>
#include <vector>

typedef enum dsn_perf_counter_type_t {
    COUNTER_TYPE_NUMBER,
    COUNTER_TYPE_VOLATILE_NUMBER, // special kind of NUMBER which will be reset on get
    COUNTER_TYPE_RATE,
    COUNTER_TYPE_NUMBER_PERCENTILES,
    COUNTER_TYPE_INVALID,
    COUNTER_TYPE_COUNT
} dsn_perf_counter_type_t;

typedef enum dsn_perf_counter_percentile_type_t {
    COUNTER_PERCENTILE_50,
    COUNTER_PERCENTILE_90,
    COUNTER_PERCENTILE_95,
    COUNTER_PERCENTILE_99,
    COUNTER_PERCENTILE_999,

    COUNTER_PERCENTILE_COUNT,
    COUNTER_PERCENTILE_INVALID
} dsn_perf_counter_percentile_type_t;

namespace dsn {

/*!
@addtogroup tool-api-providers
@{
*/
ENUM_BEGIN(dsn_perf_counter_type_t, COUNTER_TYPE_INVALID)
ENUM_REG(COUNTER_TYPE_NUMBER)
ENUM_REG(COUNTER_TYPE_VOLATILE_NUMBER)
ENUM_REG(COUNTER_TYPE_RATE)
ENUM_REG(COUNTER_TYPE_NUMBER_PERCENTILES)
ENUM_END(dsn_perf_counter_type_t)

ENUM_BEGIN(dsn_perf_counter_percentile_type_t, COUNTER_PERCENTILE_INVALID)
ENUM_REG(COUNTER_PERCENTILE_50)
ENUM_REG(COUNTER_PERCENTILE_90)
ENUM_REG(COUNTER_PERCENTILE_95)
ENUM_REG(COUNTER_PERCENTILE_99)
ENUM_REG(COUNTER_PERCENTILE_999)
ENUM_END(dsn_perf_counter_percentile_type_t)

class perf_counter;
typedef ref_ptr<perf_counter> perf_counter_ptr;

class perf_counter : public ref_counter
{
public:
    template <typename T>
    static perf_counter *create(const char *app,
                                const char *section,
                                const char *name,
                                dsn_perf_counter_type_t type,
                                const char *dsptr)
    {
        return new T(app, section, name, type, dsptr);
    }

    typedef perf_counter *(*factory)(
        const char *, const char *, const char *, dsn_perf_counter_type_t, const char *);

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

    virtual ~perf_counter(void) {}

    virtual void increment() = 0;
    virtual void decrement() = 0;
    virtual void add(uint64_t val) = 0;
    virtual void set(uint64_t val) = 0;
    virtual double get_value() = 0;
    virtual uint64_t get_integer_value() = 0;
    virtual double get_percentile(dsn_perf_counter_percentile_type_t type) = 0;

    typedef std::vector<std::pair<uint64_t *, int>> samples_t;

    // return actual sample count, must <= required_sample_count
    virtual int get_latest_samples(int required_sample_count, /*out*/ samples_t &samples) const
    {
        return 0;
    }

    // return the latest sample value
    virtual uint64_t get_latest_sample() const { return 0; }

    const char *full_name() const { return _full_name.c_str(); }
    const char *app() const { return _app.c_str(); }
    const char *section() const { return _section.c_str(); }
    const char *name() const { return _name.c_str(); }
    const char *dsptr() const { return _dsptr.c_str(); }
    dsn_perf_counter_type_t type() const { return _type; }

public:
    static void build_full_name(const char *app,
                                const char *section,
                                const char *name,
                                /*out*/ std::string &counter_name)
    {
        std::stringstream ss;
        ss << app << "*" << section << "*" << name;
        counter_name = std::move(ss.str());
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
/*@}*/
} // end namespace
