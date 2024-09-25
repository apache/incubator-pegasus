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

#pragma once

#include "runtime/tool_api.h"

/*!
@defgroup fault-injector Fault Injector
@ingroup tools-test

Fault Injector toollet

This toollet injects faults to mimic various failures in production environments,
as configed below.

<PRE>

[core]

toollets = fault_injector

[task..default]
; whether enable fault injection
fault_injection_enabled = true

; maximum disk operation delay (ms)
disk_io_delay_ms_max = 12

; miminum disk operation delay (ms)
disk_io_delay_ms_min = 1

; failure ratio for disk read operations
disk_read_fail_ratio = 0.000001

; failure ratio for disk write operations
disk_write_fail_ratio = 0.000001

; extra execution time delay (us) for this task
execution_extra_delay_us_max = 0

; maximum message delay (ms) for rpc messages
rpc_message_delay_ms_max = 1000

; miminum message delay (ms) for rpc messages
rpc_message_delay_ms_min = 0


; drop ratio for rpc request messages
rpc_request_drop_ratio = 0.000100


; drop ratio for rpc response messages
rpc_response_drop_ratio = 0.001000

[task.RPC_PING]
fault_injection_enabled = false

</PRE>
*/
namespace dsn {
struct service_spec;

namespace tools {

class fault_injector : public toollet
{
public:
    explicit fault_injector(const char *name);
    void install(service_spec &spec) override;
};
} // namespace tools
} // namespace dsn
