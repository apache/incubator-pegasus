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


# include <dsn/tool/providers.hpc.h>
# include "hpc_task_queue.h"
# include "hpc_tail_logger.h"
# include "hpc_logger.h"
# include "hpc_aio_provider.h"
# include "hpc_network_provider.h"
# include "hpc_env_provider.h"
# include "mix_all_io_looper.h"

namespace dsn {
    namespace tools {
        void register_hpc_providers()
        {
            register_component_provider<hpc_tail_logger>("dsn::tools::hpc_tail_logger");
            register_component_provider<hpc_logger>("dsn::tools::hpc_logger");
            register_component_provider<hpc_task_queue>("dsn::tools::hpc_task_queue");
            register_component_provider<hpc_task_priority_queue>("dsn::tools::hpc_task_priority_queue");            
            register_component_provider<hpc_env_provider>("dsn::tools::hpc_env_provider");
            
            register_component_provider<hpc_aio_provider>("dsn::tools::hpc_aio_provider");
            register_component_provider<hpc_network_provider>("dsn::tools::hpc_network_provider");
            register_component_provider<io_looper_task_queue>("dsn::tools::io_looper_task_queue");
            register_component_provider<io_looper_task_worker>("dsn::tools::io_looper_task_worker");
            register_component_provider<io_looper_timer_service>("dsn::tools::io_looper_timer_service");
        }
    }
}
