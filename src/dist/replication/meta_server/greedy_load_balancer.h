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
 *     A greedy load balancer
 *
 * Revision history:
 *     2016-02-03, Weijie Sun, first version
 */

# pragma once

# include <functional>
# include "server_load_balancer.h"

namespace dsn { namespace replication {

class greedy_load_balancer: public simple_load_balancer
{
public:
    greedy_load_balancer(meta_service* svc): simple_load_balancer(svc) {}
    bool balance(const meta_view &view, migration_list &list) override;

private:
    enum class balance_type
    {
        move_primary,
        copy_primary,
        copy_secondary
    };
    const meta_view* _view;
    migration_list* _migration;
    int total_partitions;
    int alive_nodes;

private:
    void greedy_balancer();
    void greedy_move_primary(const std::vector<dsn::rpc_address>& node_list, const std::vector<int>& prev, int flows);
    void greedy_copy_secondary();
    void greedy_copy_primary();
    std::shared_ptr<configuration_balancer_request> generate_balancer_request(
        const partition_configuration& pc,
        const balance_type& type,
        const rpc_address& from,
        const rpc_address& to);
};

}}
