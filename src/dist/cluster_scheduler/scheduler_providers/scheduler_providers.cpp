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
 *     implementations of providers install
 *
 * Revision history:
 *     2016-1-15, Guoxi Li(goksyli1990@gmail.com), first version
 *   
 */
#include "scheduler_providers.h"
#include <dsn/internal/factory_store.h>

namespace dsn {
    namespace dist {

        static bool register_component_provider(
                const char * name,
                ::dsn::dist::cluster_scheduler::factory f)
        {
            return dsn::utils::factory_store< ::dsn::dist::cluster_scheduler>::register_factory(
                    name,
                    f,
                    PROVIDER_TYPE_MAIN);
        }
        void register_cluster_scheduler_providers()
        {

            // register all cluster provider
            register_component_provider(
                    "dsn::dist::kubernetes_cluster_scheduler",
                    ::dsn::dist::cluster_scheduler::create< ::dsn::dist::kubernetes_cluster_scheduler>
                    );
            register_component_provider(
                    "dsn::dist::docker_scheduler",
                    ::dsn::dist::cluster_scheduler::create< ::dsn::dist::docker_scheduler>
                    );
            register_component_provider(
                    "dsn::dist::windows_cluster_scheduler",
                    ::dsn::dist::cluster_scheduler::create< ::dsn::dist::windows_cluster_scheduler>
            );
        }

    }
}
