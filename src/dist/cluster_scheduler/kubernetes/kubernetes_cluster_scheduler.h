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

# pragma once

# include <dsn/dist/cluster_scheduler.h>

using namespace ::dsn::service;

namespace dsn
{
    namespace dist
    {
        class kubernetes_cluster_scheduler 
            : public cluster_scheduler, public clientlet
        {
        public:
            virtual error_code initialize() override;

            /*
            * option 1: combined deploy and failure notification service
            *  failure_notification is specific for this deployment unit
            */
            virtual void schedule(
                std::shared_ptr<deployment_unit>& unit,
                std::function<void(error_code, rpc_address)> deployment_callback,
                std::function<void(error_code, std::string)> failure_notification
                ) override {}

            /*
            * option 2: seperated deploy and failure notification service
            */
            virtual void deploy(
                std::shared_ptr<deployment_unit>& unit,
                std::function<void(error_code, rpc_address)> deployment_callback
                ) override {}

            // *  failure_notification is general for all deployment units
            virtual void register_failure_callback(
                std::function<void(error_code, std::string)> failure_notification
                ) override {}
        };
    }
}
