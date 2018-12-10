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

#include "throttling_controller.h"

#include <dsn/c/api_layer1.h>
#include <dsn/utility/string_conv.h>
#include <dsn/utility/strings.h>
#include <dsn/tool-api/rpc_message.h>

namespace dsn {
namespace replication {

throttling_controller::throttling_controller()
    : _enabled(false),
      _partition_count(0),
      _delay_qps(0),
      _delay_ms(0),
      _reject_qps(0),
      _reject_delay_ms(0),
      _last_request_time(0),
      _cur_request_count(0)
{
}

bool throttling_controller::parse_from_env(const std::string &env_value,
                                           int partition_count,
                                           std::string &parse_error,
                                           bool &changed,
                                           std::string &old_env_value)
{
    changed = false;
    if (_enabled && env_value == _env_value && partition_count == _partition_count)
        return true;
    std::vector<std::string> sargs;
    utils::split_args(env_value.c_str(), sargs, ',', true);
    if (sargs.empty()) {
        parse_error = "empty env value";
        return false;
    }
    bool delay_parsed = false;
    int32_t delay_qps = 0;
    int64_t delay_ms = 0;
    bool reject_parsed = false;
    int32_t reject_qps = 0;
    int64_t reject_delay_ms = 0;
    for (std::string &s : sargs) {
        std::vector<std::string> sargs1;
        utils::split_args(s.c_str(), sargs1, '*', true);
        if (sargs1.size() != 3) {
            parse_error = "invalid field count, should be 3";
            return false;
        }
        int32_t qps = 0;
        if (!buf2int32(sargs1[0], qps) || qps < 0) {
            parse_error = "invalid qps, should be non-negative int";
            return false;
        }
        int64_t ms = 0;
        if (!buf2int64(sargs1[2], ms) || ms < 0) {
            parse_error = "invalid delay ms, should be non-negative int";
            return false;
        }
        if (sargs1[1] == "delay") {
            if (delay_parsed) {
                parse_error = "duplicate delay config";
                return false;
            }
            delay_parsed = true;
            delay_qps = qps / partition_count + 1;
            delay_ms = ms;
        } else if (sargs1[1] == "reject") {
            if (reject_parsed) {
                parse_error = "duplicate reject config";
                return false;
            }
            reject_parsed = true;
            reject_qps = qps / partition_count + 1;
            reject_delay_ms = ms;
        } else {
            parse_error = "invalid throttling type";
            return false;
        }
    }
    changed = true;
    old_env_value = _env_value;
    _enabled = true;
    _env_value = env_value;
    _partition_count = partition_count;
    _delay_qps = delay_qps;
    _delay_ms = delay_ms;
    _reject_qps = reject_qps;
    _reject_delay_ms = reject_delay_ms;
    return true;
}

void throttling_controller::reset(bool &changed, std::string &old_env_value)
{
    if (_enabled) {
        changed = true;
        old_env_value = _env_value;
        _enabled = false;
        _env_value.clear();
        _partition_count = 0;
        _delay_qps = 0;
        _delay_ms = 0;
        _reject_qps = 0;
        _reject_delay_ms = 0;
        _last_request_time = 0;
        _cur_request_count = 0;
    } else {
        changed = false;
    }
}

throttling_controller::throttling_type throttling_controller::control(const message_ex *request,
                                                                      int64_t &delay_ms)
{
    int64_t now_s = dsn_now_s();
    if (now_s != _last_request_time) {
        _cur_request_count = 0;
        _last_request_time = now_s;
    }
    _cur_request_count++;
    if (_reject_qps > 0 && _cur_request_count > _reject_qps) {
        _cur_request_count--;
        int64_t client_timeout = request->header->client.timeout_ms;
        if (client_timeout > 0) {
            delay_ms = std::min(_reject_delay_ms, client_timeout / 2);
        } else {
            delay_ms = _reject_delay_ms;
        }
        return REJECT;
    }
    if (_delay_qps > 0 && _cur_request_count > _delay_qps) {
        int64_t client_timeout = request->header->client.timeout_ms;
        if (client_timeout > 0) {
            delay_ms = std::min(_delay_ms, client_timeout / 2);
        } else {
            delay_ms = _delay_ms;
        }
        return DELAY;
    }
    return PASS;
}

} // namespace replication
} // namespace dsn
