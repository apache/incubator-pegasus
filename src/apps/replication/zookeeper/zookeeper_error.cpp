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
 *     transform zookeeper error code to dsn error code, implementation
 *
 * Revision history:
 *     2015-12-04, @shengofsun (sunweijie@xiaomi.com)
 */

#include <zookeeper.h>
#include <dsn/dist/error_code.h>
#include <dsn/cpp/auto_codes.h>

#include "zookeeper_error.h"
namespace dsn { namespace dist {

error_code from_zerror(int zerr)
{
    if (ZOK == zerr)
        return ERR_OK;
    if (ZBADARGUMENTS == zerr || ZNOTEMPTY == zerr)
        return ERR_INVALID_PARAMETERS;
    if (ZCONNECTIONLOSS == zerr || ZOPERATIONTIMEOUT == zerr)
        return ERR_TIMEOUT;
    if (ZNONODE == zerr)
        return ERR_OBJECT_NOT_FOUND;
    if (ZNODEEXISTS == zerr)
        return ERR_NODE_ALREADY_EXIST;
    if (ZRUNTIMEINCONSISTENCY == zerr)
        return ERR_INCONSISTENT_STATE;
    return ERR_ZOOKEEPER_OPERATION;
}

}}
