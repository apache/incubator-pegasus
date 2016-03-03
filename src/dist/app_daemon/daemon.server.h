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

# include <dsn/dist/replication.h>
# include <dsn/service_api_cpp.h>
# include <dsn/dist/failure_detector_multimaster.h>

namespace dsn
{
    namespace dist
    {
        class daemon_s_service
            : public ::dsn::serverlet<daemon_s_service>
        {
        public:
            daemon_s_service();
            ~daemon_s_service();

            void open_service();
            void close_service();

            void on_config_proposal(const ::dsn::replication::configuration_update_request& proposal);
            
        private:
            std::unique_ptr<slave_failure_detector_with_multimaster> _fd;
            
            struct layer1_app_info
            {
                ::dsn::replication::partition_configuration configuration;
                std::unique_ptr<std::thread> wait_thread;
                dsn_handle_t process_handle;
                std::atomic<bool> exited;
                std::atomic<bool> resource_ready;
                std::string working_dir;
                std::string package_dir;
                std::string runner_script;
                uint16_t working_port;

                layer1_app_info(const ::dsn::replication::configuration_update_request & proposal)
                {
                    configuration = proposal.config;
                    process_handle = nullptr;
                    exited = false;
                    working_port = 0;
                    resource_ready = false;
                }
            };

            ::dsn::service::zrwlock_nr _lock;
            std::unordered_map< ::dsn::replication::global_partition_id, std::shared_ptr<layer1_app_info>> _apps;
            std::atomic<bool> _online;

            std::string _working_dir;
            rpc_address _package_server;
            std::string _package_dir_on_package_server;
            uint32_t    _app_port_min; // inclusive
            uint32_t    _app_port_max; // inclusive
            task_ptr _app_check_timer;
            
# ifdef _WIN32
            HANDLE   _job; ///< manage all procs which exits when job dies
# endif

        private:
            void on_master_connected();
            void on_master_disconnected();

            void on_add_app(const ::dsn::replication::configuration_update_request& proposal);
            void on_remove_app(const ::dsn::replication::configuration_update_request& proposal);

            void start_app(std::shared_ptr<layer1_app_info> &&  app);
            void kill_app(std::shared_ptr<layer1_app_info> &&  app);

            void update_configuration_on_meta_server(::dsn::replication::config_type type, std::shared_ptr<layer1_app_info>&& app);
            void on_update_configuration_on_meta_server_reply(
                ::dsn::replication::config_type type, std::shared_ptr<layer1_app_info> &&  app,
                error_code err, dsn_message_t request, dsn_message_t response
                );

            void check_apps();
        };
    }
}