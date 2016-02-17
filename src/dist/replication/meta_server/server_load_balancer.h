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
 *     base interface of the server load balancer which defines the scheduling
 *     policy of how to place the partition replica to the nodes
  *
 * Revision history:
 *     2015-12-29, @imzhenyu (Zhenyu Guo), first draft
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/service_api_cpp.h>
# include <dsn/dist/error_code.h>
# include <string>
# include <functional>
# include <memory>

//
// TODO: make server state also general enough for both stateless
// and stateful service
//
# include "server_state.h"

namespace dsn
{
    namespace dist
    {
        class server_load_balancer
        {
        public:
            template <typename T> static server_load_balancer* create(server_state* state)
            {
                return new T(state);
            }

            typedef server_load_balancer* (*factory)(server_state* state);
            
        public:
            server_load_balancer(server_state* state): _state(state) {}
            virtual ~server_load_balancer() {}

            // load balancing for all
            virtual void run() = 0;

            // load balancing for single partition
            virtual void run(global_partition_id gpid) = 0;

            // actions when config is changed
            virtual void on_config_changed(std::shared_ptr<configuration_update_request>) {}

            // do migration according to external command
            virtual void on_balancer_proposal(/*in*/const balancer_proposal_request& request, /*out*/balancer_proposal_response& response) {}

            // control replica migration
            virtual void on_control_migration(/*in*/const control_balancer_migration_request& request,
                                              /*out*/control_balancer_migration_response& response) {}

            void explictly_send_proposal(global_partition_id gpid, rpc_address receiver, config_type type, rpc_address node);
        protected:
            void send_proposal(::dsn::rpc_address node, const configuration_update_request& proposal);

        protected:
            server_state* _state;
        public:
            // switchs for replication test
            static bool s_disable_lb;
            static bool s_lb_for_test;
        };
    }
}
