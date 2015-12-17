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

# include <memory>
# include <dsn/internal/enum_helper.h>
# include <dsn/service_api_c.h>
# include <dsn/cpp/autoref_ptr.h>

namespace dsn {
ENUM_BEGIN(dsn_perf_counter_type_t, COUNTER_TYPE_INVALID)
    ENUM_REG(COUNTER_TYPE_NUMBER)
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
    template <typename T> static perf_counter* create(const char *section, const char *name, dsn_perf_counter_type_t type, const char *dsptr)
    {
        return new T(section, name, type, dsptr);
    }

    typedef perf_counter* (*factory)(const char *, const char *, dsn_perf_counter_type_t, const char *);

public:
    perf_counter(const char *section, const char *name, dsn_perf_counter_type_t type, const char *dsptr) 
        : _name(name), _section(section), _dsptr(dsptr), _type(type)
    {
    }

    virtual ~perf_counter(void) {}

    virtual void   increment() = 0;
    virtual void   decrement() = 0;
    virtual void   add(uint64_t val) = 0;
    virtual void   set(uint64_t val) = 0;
    virtual double get_value() = 0;
    virtual double get_percentile(dsn_perf_counter_percentile_type_t type) = 0;
    virtual uint64_t* get_samples(/*out*/ int& sample_count) const { return nullptr; }
    virtual uint64_t get_current_sample() const { return 0; }

    const char* name() const { return _name.c_str(); }
    const char* section() const { return _section.c_str(); }
    const char* dsptr() const { return _dsptr.c_str(); }
    dsn_perf_counter_type_t type() const { return _type; }

private:
    std::string _name;
    std::string _section;
    std::string _dsptr;
    dsn_perf_counter_type_t _type;
};

} // end namespace
