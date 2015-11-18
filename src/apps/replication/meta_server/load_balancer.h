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

#include "server_state.h"

using namespace dsn;
using namespace dsn::service;
using namespace dsn::replication;

class load_balancer : public serverlet<load_balancer>
{
public:
    load_balancer(server_state* state);
    ~load_balancer();

    void run();
    void run(global_partition_id gpid);

private:
    // meta server => partition server
    void send_proposal(::dsn::rpc_address node, const configuration_update_request& proposal);
    void query_decree(std::shared_ptr<query_replica_decree_request> query);
    void on_query_decree_ack(error_code err, std::shared_ptr<query_replica_decree_request>& query, std::shared_ptr<query_replica_decree_response>& resp);
    
    void run_lb(partition_configuration& pc);
    ::dsn::rpc_address find_minimal_load_machine(bool primaryOnly);

private:
    server_state *_state;
};

