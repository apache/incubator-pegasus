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

#include <dsn/cpp/zlocks.h>

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "zlock"

namespace dsn {
namespace service {

//------------------------------- event ----------------------------------

zevent::zevent(bool manualReset, bool initState /* = false*/)
{
    _manualReset = manualReset;
    _signaled = initState;
    if (_signaled) {
        _sema.signal();
    }
}

zevent::~zevent() {}

void zevent::set()
{
    bool nonsignaled = false;
    if (std::atomic_compare_exchange_strong(&_signaled, &nonsignaled, true)) {
        _sema.signal();
    }
}

void zevent::reset()
{
    if (_manualReset) {
        bool signaled = true;
        if (std::atomic_compare_exchange_strong(&_signaled, &signaled, false)) {
        }
    }
}

bool zevent::wait(int timeout_milliseconds)
{
    if (_manualReset) {
        if (std::atomic_load(&_signaled))
            return true;

        _sema.wait(timeout_milliseconds);
        return std::atomic_load(&_signaled);
    }

    else {
        bool signaled = true;
        if (std::atomic_compare_exchange_strong(&_signaled, &signaled, false))
            return true;

        _sema.wait(timeout_milliseconds);
        return std::atomic_compare_exchange_strong(&_signaled, &signaled, false);
    }
}
}
} // end namespace dsn::service
