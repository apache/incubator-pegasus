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

#include "utils/error_code.h"

#include "utils/customizable_id.h"

namespace dsn {
/*static*/
int error_code::max()
{
    return dsn::utils::customized_id_mgr<dsn::error_code>::instance().max_value();
}
/*static*/
bool error_code::is_exist(const char *name)
{
    return dsn::utils::customized_id_mgr<dsn::error_code>::instance().get_id(name) != -1;
}
/*static*/
error_code error_code::try_get(const char *name, error_code default_value)
{
    int ans = dsn::utils::customized_id_mgr<dsn::error_code>::instance().get_id(name);
    if (ans == -1)
        return default_value;
    return error_code(ans);
}
/*static*/
error_code error_code::try_get(const std::string &name, error_code default_value)
{
    int ans = dsn::utils::customized_id_mgr<dsn::error_code>::instance().get_id(name);
    if (ans == -1)
        return default_value;
    return error_code(ans);
}

error_code::error_code(const char *name)
{
    _internal_code = dsn::utils::customized_id_mgr<dsn::error_code>::instance().register_id(name);
}

const char *error_code::to_string() const
{
    return dsn::utils::customized_id_mgr<dsn::error_code>::instance().get_name(_internal_code);
}
}
