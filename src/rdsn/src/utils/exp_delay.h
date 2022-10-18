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
 *     delay for admission control
 *
 * Revision history:
 *     Nov., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include "utils/singleton.h"
#include <vector>
#include <cassert>

namespace dsn {
#define DELAY_COUNT 6
const double s_default_delay_points[DELAY_COUNT] = {1.0, 1.2, 1.4, 1.6, 1.8, 2.0};
const int s_default_delay[DELAY_COUNT] = {0, 0, 1, 2, 5, 10}; // millieseconds

class exp_delay
{
public:
    exp_delay()
    {
        memcpy((void *)_delay, (const void *)s_default_delay, sizeof(_delay));
        _threshold = 0x0fffffff;
    }

    void initialize(const std::vector<int> &delays, int threshold)
    {
        assert((int)delays.size() == DELAY_COUNT);

        int i = 0;
        for (auto &d : delays) {
            _delay[i++] = d;
        }
        _threshold = threshold;
    }

    void initialize(int threshold) { _threshold = threshold; }

    inline int delay(int value)
    {
        if (value >= _threshold) {
            double f = (double)value / (double)_threshold;
            int delay_milliseconds;

            if (f < s_default_delay_points[DELAY_COUNT - 1]) {
                int idx = static_cast<int>((f - 1.0) / 0.2);
                delay_milliseconds = _delay[idx];
            } else {
                delay_milliseconds = _delay[DELAY_COUNT - 1];
            }

            return delay_milliseconds;
        } else {
            return 0;
        }
    }

private:
    int _delay[DELAY_COUNT];
    int _threshold;
};

class shared_exp_delay
{
public:
    shared_exp_delay() { memcpy((void *)_delay, (const void *)s_default_delay, sizeof(_delay)); }

    void initialize(const std::vector<int> &delays)
    {
        assert((int)delays.size() == DELAY_COUNT);

        int i = 0;
        for (auto &d : delays) {
            _delay[i++] = d;
        }
    }

    inline int delay(int value, int threshold)
    {
        if (value >= threshold) {
            double f = (double)value / (double)threshold;
            int delay_milliseconds;

            if (f < s_default_delay_points[DELAY_COUNT - 1]) {
                int idx = static_cast<int>((f - 1.0) / 0.2);
                delay_milliseconds = _delay[idx];
            } else {
                delay_milliseconds = _delay[DELAY_COUNT - 1];
            }

            return delay_milliseconds;
        } else {
            return 0;
        }
    }

private:
    int _delay[DELAY_COUNT];
};
}
