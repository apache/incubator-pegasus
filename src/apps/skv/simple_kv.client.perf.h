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
#include "simple_kv.client.2.h"

namespace dsn {
namespace replication {
namespace application {

class simple_kv_perf_test_client : public simple_kv_client2,
                                   public ::dsn::service::perf_client_helper
{
public:
    using simple_kv_client2::simple_kv_client2;

    virtual void
    send_one(int payload_bytes, int key_space_size, const std::vector<double> &ratios) override
    {
        auto prob = (double)dsn_random32(0, 1000) / 1000.0;
        if (0) {
        } else if (prob <= ratios[0]) {
            send_one_read(payload_bytes, key_space_size);
        } else if (prob <= ratios[1]) {
            send_one_write(payload_bytes, key_space_size);
        } else if (prob <= ratios[2]) {
            send_one_append(payload_bytes, key_space_size);
        } else { /* nothing to do */
        }
    }

    void send_one_read(int payload_bytes, int key_space_size)
    {
        auto rs = dsn_random64(0, 10000000) % key_space_size;
        std::stringstream ss;
        ss << "key." << rs << "." << std::string(payload_bytes, 'x');

        read(ss.str(),
             [ this, context = prepare_send_one() ](error_code err, std::string && resp) {
                 end_send_one(context, err);
             },
             _timeout,
             0,
             rs);
    }

    void send_one_write(int payload_bytes, int key_space_size)
    {
        auto rs = dsn_random64(0, 10000000) % key_space_size;
        std::stringstream ss;
        ss << "key." << rs;

        kv_pair req = {ss.str(), std::string(payload_bytes, 'x')};
        write(req,
              [ this, context = prepare_send_one() ](error_code err, int32_t && resp) {
                  end_send_one(context, err);
              },
              _timeout,
              0,
              rs);
    }

    void send_one_append(int payload_bytes, int key_space_size)
    {
        auto rs = dsn_random64(0, 10000000) % key_space_size;
        std::stringstream ss;
        ss << "key." << rs;
        kv_pair req = {ss.str(), std::string(payload_bytes, 'x')};
        ;
        append(req,
               [ this, context = prepare_send_one() ](error_code err, int32_t && resp) {
                   end_send_one(context, err);
               },
               _timeout,
               0,
               rs);
    }
};
}
}
}
