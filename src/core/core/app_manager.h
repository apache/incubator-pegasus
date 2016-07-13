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

# include <dsn/utility/ports.h>
# include <dsn/utility/singleton.h>
# include <dsn/tool-api/global_config.h>
# include <dsn/cpp/auto_codes.h>
# include <sstream>
# include <dsn/utility/synchronize.h>
# include "rpc_engine.h"

namespace dsn 
{
    class rpc_server_dispatcher;
    class service_node;
    class service_app;
    
    class app_manager
    {
    public:
        struct app_internal
        {
            void*    app_context; // duplicated with info.app_context for faster access
            dsn_gpid gpid;
            dsn_app  *model;
            dsn_app_info info;
            rpc_server_dispatcher server_dispatcher;
        };

    public:
        app_manager(service_node* node);

        error_code create_app(
            const char* type, 
            dsn_gpid gpid,
            const char* data_dir,
            /*out*/ void** app_context,
            /*out*/ void** app_context_for_callbacks);

        error_code start_app(void* app_context, int argc, char** argv);

        error_code destroy_app(void* app_context, bool cleanup);

        bool  rpc_register_handler(dsn_gpid gpid, rpc_handler_info* handler);

        rpc_handler_info* rpc_unregister_handler(dsn_gpid gpid, dsn_task_code_t rpc_code);

        dsn_app_info* get_app_info(dsn_gpid gpid);
        
    private:
        service_node  *_owner_node;
        utils::rw_lock_nr _apps_lock;
        std::unordered_map<uint64_t, std::unique_ptr<app_internal> > _apps;
    };
}