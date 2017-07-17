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

#include "empty_aio_provider.h"

namespace dsn {
namespace tools {

empty_aio_provider::empty_aio_provider(disk_engine *disk, aio_provider *inner_provider)
    : aio_provider(disk, inner_provider)
{
}

empty_aio_provider::~empty_aio_provider() {}

dsn_handle_t empty_aio_provider::open(const char *file_name, int flag, int pmode)
{
    return (dsn_handle_t)(size_t)(1);
}

error_code empty_aio_provider::close(dsn_handle_t fh) { return ERR_OK; }

error_code empty_aio_provider::flush(dsn_handle_t fh) { return ERR_OK; }

void empty_aio_provider::aio(aio_task *aio)
{
    complete_io(aio, ERR_OK, aio->aio()->buffer_size, 0);
}

disk_aio *empty_aio_provider::prepare_aio_context(aio_task *tsk) { return new disk_aio(); }
}
}
