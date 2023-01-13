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
 *     Replication testing framework.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "injector.h"
#include "checker.h"
#include "case.h"

#include "runtime/fault_injector.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/autoref_ptr.h"

#include <iostream>

namespace dsn {
namespace replication {
namespace test {

static void inject_on_task_enqueue(task *caller, task *callee)
{
    if (!test_checker::s_inited)
        return;

    event_on_task_enqueue event;
    event.init(callee);

    test_case::instance().on_event(&event);
}

static void inject_on_task_begin(task *this_)
{
    if (!test_checker::s_inited)
        return;

    event_on_task_begin event;
    event.init(this_);

    test_case::instance().on_event(&event);
}

static void inject_on_task_end(task *this_)
{
    if (!test_checker::s_inited)
        return;

    event_on_task_end event;
    event.init(this_);

    test_case::instance().on_event(&event);
}

static void inject_on_task_cancelled(task *this_)
{
    if (!test_checker::s_inited)
        return;

    event_on_task_cancelled event;
    event.init(this_);

    test_case::instance().on_event(&event);
}

static void inject_on_task_wait_pre(task *caller, task *callee, uint32_t timeout_ms)
{
    if (!test_checker::s_inited)
        return;
}

static void inject_on_task_wait_post(task *caller, task *callee, bool succ)
{
    if (!test_checker::s_inited)
        return;
}

static void inject_on_task_cancel_post(task *caller, task *callee, bool succ)
{
    if (!test_checker::s_inited)
        return;
}

static bool inject_on_aio_call(task *caller, aio_task *callee)
{
    if (!test_checker::s_inited)
        return true;

    event_on_aio_call event;
    event.init(callee);

    return test_case::instance().on_event(&event);
}

static void inject_on_aio_enqueue(aio_task *this_)
{
    if (!test_checker::s_inited)
        return;

    event_on_aio_enqueue event;
    event.init(this_);

    test_case::instance().on_event(&event);
}

static bool inject_on_rpc_call(task *caller, message_ex *req, rpc_response_task *callee)
{
    if (!test_checker::s_inited)
        return true;

    event_on_rpc_call event;
    event.init(req, nullptr);

    return test_case::instance().on_event(&event);
}

static bool inject_on_rpc_request_enqueue(rpc_request_task *callee)
{
    if (!test_checker::s_inited)
        return true;

    event_on_rpc_request_enqueue event;
    event.init(callee);

    return test_case::instance().on_event(&event);
}

static bool inject_on_rpc_reply(task *caller, message_ex *msg)
{
    if (!test_checker::s_inited)
        return true;

    event_on_rpc_reply event;
    event.init(msg, nullptr);

    return test_case::instance().on_event(&event);
}

static bool inject_on_rpc_response_enqueue(rpc_response_task *resp)
{
    if (!test_checker::s_inited)
        return true;

    event_on_rpc_response_enqueue event;
    event.init(resp);

    return test_case::instance().on_event(&event);
}

void test_injector::install(service_spec &svc_spec)
{
    for (int i = 0; i <= dsn::task_code::max(); i++) {
        if (i == TASK_CODE_INVALID)
            continue;

        task_spec *spec = task_spec::get(i);

        spec->on_task_enqueue.put_back(inject_on_task_enqueue, "test_injector");
        spec->on_task_begin.put_back(inject_on_task_begin, "test_injector");
        spec->on_task_end.put_back(inject_on_task_end, "test_injector");
        spec->on_task_cancelled.put_back(inject_on_task_cancelled, "test_injector");
        spec->on_task_wait_pre.put_back(inject_on_task_wait_pre, "test_injector");
        spec->on_task_wait_post.put_back(inject_on_task_wait_post, "test_injector");
        spec->on_task_cancel_post.put_back(inject_on_task_cancel_post, "test_injector");
        spec->on_aio_call.put_native(inject_on_aio_call);
        spec->on_aio_enqueue.put_back(inject_on_aio_enqueue, "test_injector");
        spec->on_rpc_call.put_native(inject_on_rpc_call);
        spec->on_rpc_request_enqueue.put_native(inject_on_rpc_request_enqueue);
        spec->on_rpc_reply.put_native(inject_on_rpc_reply);
        spec->on_rpc_response_enqueue.put_native(inject_on_rpc_response_enqueue);
    }
}

test_injector::test_injector(const char *name) : toollet(name) {}
}
}
}
