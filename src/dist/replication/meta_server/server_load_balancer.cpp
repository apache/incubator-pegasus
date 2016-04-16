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
 *     base implementation of the server load balancer which defines the scheduling
 *     policy of how to place the partition replica to the nodes
 *
 * Revision history:
 *     2015-12-29, @imzhenyu (Zhenyu Guo), first draft
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include "server_load_balancer.h"

namespace dsn
{
    namespace dist
    {

        bool server_load_balancer::s_lb_for_test = false;
        bool server_load_balancer::s_disable_lb = false;

        // meta server => partition server
        void server_load_balancer::send_proposal(::dsn::rpc_address node, const configuration_update_request& proposal)
        {
            dinfo("send proposal %s of %s, current ballot = %" PRId64,
                enum_to_string(proposal.type),
                proposal.node.to_string(),
                proposal.config.ballot
                );

            rpc::call_one_way_typed(node, RPC_CONFIG_PROPOSAL, proposal, gpid_to_hash(proposal.config.gpid));
        }

        void server_load_balancer::explictly_send_proposal(global_partition_id gpid, rpc_address receiver, config_type::type type, rpc_address node)
        {
            if (gpid.app_id <= 0 || gpid.pidx < 0 || type == config_type::CT_INVALID)
            {
                derror("invalid params");
                return;
            }

            configuration_update_request req;
            {
                zauto_read_lock l(_state->_lock);
                if (gpid.app_id > _state->_apps.size())
                {
                    derror("invalid params");
                    return;
                }
                app_state& app = _state->_apps[gpid.app_id-1];
                if (gpid.pidx>=app.partition_count)
                {
                    derror("invalid params");
                    return;
                }
                req.config = app.partitions[gpid.pidx];
            }

            req.type = type;
            req.node = node;
            send_proposal(receiver, req);
        }
    }
}
