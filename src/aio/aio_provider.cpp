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

#include "aio_provider.h"

// IWYU pragma: no_include <string>

#include "disk_engine.h"

namespace dsn {
class aio_task;

aio_provider::aio_provider(disk_engine *disk) : _engine(disk) {}

void aio_provider::complete_io(aio_task *aio, error_code err, uint64_t bytes)
{
    _engine->complete_io(aio, err, bytes);
}

namespace tools {
namespace internal_use_only {
bool register_component_provider(const char *name, aio_provider::factory f, dsn::provider_type type)
{
    return dsn::utils::factory_store<aio_provider>::register_factory(name, f, type);
}
} // namespace internal_use_only
} // namespace tools
} // namespace dsn
