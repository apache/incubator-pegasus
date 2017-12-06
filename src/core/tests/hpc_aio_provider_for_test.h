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
 *     Unit-test for hpc aio provider.
 *
 * Revision history:
 *     Nov., 2015, @xiaotz (Xiaotong Zhang), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/tool-api/aio_provider.h>
#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/service_api_c.h>
#include <dsn/tool-api/task.h>
#include "../core/disk_engine.h"
#include "test_utils.h"
#include "../tools/hpc/hpc_aio_provider.h"

namespace dsn {
namespace test {
class hpc_aio_provider_for_test : public dsn::tools::hpc_aio_provider
{
public:
    hpc_aio_provider_for_test(disk_engine *disk, aio_provider *inner_provider)
        : dsn::tools::hpc_aio_provider(disk, inner_provider), f(false)
    {
    }

    virtual void aio(aio_task *aio_tsk)
    {
        uint32_t b;
        error_code err;
        if (f) {
            dsn::tools::hpc_aio_provider::aio(aio_tsk);
        } else {
            auto err = aio_internal(aio_tsk, f, &b);
            complete_io(aio_tsk, err, b);
        }
        f = !f;
    }

private:
    bool f;
};
}
}
