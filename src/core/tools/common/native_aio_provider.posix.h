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

#ifndef _WIN32

#include <dsn/tool_api.h>
#include <dsn/utility/synchronize.h>

#include <aio.h>
#include <fcntl.h>

namespace dsn {
namespace tools {
class native_posix_aio_provider : public aio_provider
{
public:
    native_posix_aio_provider(disk_engine *disk, aio_provider *inner_provider);
    ~native_posix_aio_provider();

    virtual dsn_handle_t open(const char *file_name, int flag, int pmode) override;
    virtual error_code close(dsn_handle_t fh) override;
    virtual error_code flush(dsn_handle_t fh) override;
    virtual void aio(aio_task *aio) override;
    virtual disk_aio *prepare_aio_context(aio_task *tsk) override;

    virtual void start() override {}

protected:
    error_code aio_internal(aio_task *aio, bool async, /*out*/ uint32_t *pbytes = nullptr);

private:
    friend void aio_completed(sigval sigval);
};
}
}

#endif
