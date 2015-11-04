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
 *     meta server service for EON (rDSN layer 2)
 *
 * Revision history:
 *     2015-03-09, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include "replication_common.h"

using namespace dsn;
using namespace dsn::service;
using namespace dsn::replication;

class server_state;
class load_balancer;
class meta_server_failure_detector;

namespace dsn {
    namespace replication{
        class replication_checker;
    }
}

class meta_service : public serverlet<meta_service>
{
public:
    meta_service(server_state* state);
    ~meta_service(void);

    void start(const char* data_dir, bool clean_state);
    bool stop();

private:
    void on_request(dsn_message_t request);
    void replay_log(const char* log);

    // partition server & client => meta server
    // query partition configuration
    void on_query_configuration_by_node(dsn_message_t req);
    void on_query_configuration_by_index(dsn_message_t req);

    // update configuration
    void on_update_configuration(dsn_message_t req);

    void update_configuration(std::shared_ptr<configuration_update_request>& update);
    void on_log_completed(error_code err, size_t size, blob buffer, std::shared_ptr<configuration_update_request> req, dsn_message_t resp);
    void update_configuration(const configuration_update_request& request, /*out*/ configuration_update_response& response);
      
    // load balance actions
    void on_load_balance_start();
    void on_load_balance_timer();
    void on_config_changed(global_partition_id gpid);

private:
    friend class meta_server_failure_detector;
    friend class ::dsn::replication::replication_checker;

    meta_server_failure_detector *_failure_detector;
    server_state                 *_state;
    load_balancer                *_balancer;
    dsn::task_ptr                _balancer_timer;
    replication_options          _opts;
    std::string                  _data_dir;
    bool                         _started;

    zlock                        _log_lock;
    dsn_handle_t                 _log;
    uint64_t                     _offset;
}; 

