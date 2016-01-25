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

# include <dsn/service_api_c.h>

namespace dsn 
{
    class perf_counter_
    {
    public:
        perf_counter_()
        {
            _h = nullptr;
        }

        void init(const char* section, const char *name, dsn_perf_counter_type_t type, const char *dsptr)
        {
            _h = dsn_perf_counter_create(section, name, type, dsptr);
        }

        ~perf_counter_(void)
        {
            if (nullptr != _h)
                dsn_perf_counter_remove(_h);
        }

        // make sure they are called after init above
        void   increment() { dsn_perf_counter_increment(_h); }
        void   decrement()  { dsn_perf_counter_decrement(_h); }
        void   add(uint64_t val)  { dsn_perf_counter_add(_h, val); }
        void   set(uint64_t val)  { dsn_perf_counter_set(_h, val); }
        double get_value()  { return dsn_perf_counter_get_value(_h); }
        uint64_t get_integer_value() { return dsn_perf_counter_get_integer_value(_h); }
        double get_percentile(dsn_perf_counter_percentile_type_t type)  { return dsn_perf_counter_get_percentile(_h, type); }

    private:
        dsn_handle_t _h;
    };
} // end namespace
