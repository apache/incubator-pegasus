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

#include "utils/threadpool_code.h"
#include "utils/customizable_id.h"

namespace dsn {

/*static*/
int threadpool_code::max()
{
    return dsn::utils::customized_id_mgr<dsn::threadpool_code>::instance().max_value();
}
/*static*/
bool threadpool_code::is_exist(const char *name)
{
    return dsn::utils::customized_id_mgr<dsn::threadpool_code>::instance().get_id(name) != -1;
}

threadpool_code::threadpool_code(const char *name)
{
    _internal_code =
        dsn::utils::customized_id_mgr<dsn::threadpool_code>::instance().register_id(name);
}

const char *threadpool_code::to_string() const
{
    return dsn::utils::customized_id_mgr<dsn::threadpool_code>::instance().get_name(_internal_code);
}
} // namespace dsn
