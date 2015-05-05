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
# pragma once

# include <memory>
# include <dsn/internal/enum_helper.h>

namespace dsn {

enum perf_counter_type
{
    COUNTER_TYPE_NUMBER,
    COUNTER_TYPE_RATE,
    COUNTER_TYPE_NUMBER_PERCENTILES,
    COUNTER_TYPE_INVALID,
    COUNTER_TYPE_COUNT
};

ENUM_BEGIN(perf_counter_type, COUNTER_TYPE_INVALID)
    ENUM_REG(COUNTER_TYPE_NUMBER)
    ENUM_REG(COUNTER_TYPE_RATE)
    ENUM_REG(COUNTER_TYPE_NUMBER_PERCENTILES)
ENUM_END(perf_counter_type)

enum counter_percentile_type
{
    COUNTER_PERCENTILE_999,
    COUNTER_PERCENTILE_99,
    COUNTER_PERCENTILE_95,
    COUNTER_PERCENTILE_90,
    COUNTER_PERCENTILE_50,

    COUNTER_PERCENTILE_COUNT,
    COUNTER_PERCENTILE_INVALID
};

ENUM_BEGIN(counter_percentile_type, COUNTER_PERCENTILE_INVALID)
    ENUM_REG(COUNTER_PERCENTILE_999)
    ENUM_REG(COUNTER_PERCENTILE_99)
    ENUM_REG(COUNTER_PERCENTILE_95)
    ENUM_REG(COUNTER_PERCENTILE_90)
    ENUM_REG(COUNTER_PERCENTILE_50)
ENUM_END(counter_percentile_type)

class perf_counter;
typedef perf_counter* (*perf_counter_factory)(const char *section, const char *name, perf_counter_type type);

class perf_counter
{
public:
    template <typename T> static perf_counter* create(const char *section, const char *name, perf_counter_type type)
    {
        return new T(section, name, type);
    }

public:
    perf_counter(const char *section, const char *name, perf_counter_type type) {}
    virtual ~perf_counter(void) {}

    virtual void   increment() = 0;
    virtual void   decrement() = 0;
    virtual void   add(uint64_t val) = 0;
    virtual void   set(uint64_t val) = 0;
    virtual double get_value() = 0;
    virtual double get_percentile(counter_percentile_type type) = 0;
};

typedef std::shared_ptr<perf_counter> perf_counter_ptr;

} // end namespace
