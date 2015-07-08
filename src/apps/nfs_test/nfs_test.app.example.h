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
# pragma once
# include <dsn/dist/replication.h>
# include <dsn/service_api.h>

# include "nfs_node_impl.h"
# include "nfs_server_impl.h"

namespace dsn {
    namespace replication {
        namespace application {

            // server app example
            class nfs_server_app : public ::dsn::service::service_app, public virtual ::dsn::service::servicelet
            {
            public:
                nfs_server_app(::dsn::service_app_spec* s)
                    : ::dsn::service::service_app(s) {}

                virtual ::dsn::error_code start(int argc, char** argv)
                {
                    return ::dsn::ERR_OK;
                }

                virtual void stop(bool cleanup = false)
                {
                    _nfs_node_impl->stop();
                }

            private:
                nfs_node_impl* _nfs_node_impl;
            };

            // client app example
            class nfs_client_app : public ::dsn::service::service_app, public virtual ::dsn::service::servicelet
            {
            public:
                nfs_client_app(::dsn::service_app_spec* s)
                    : ::dsn::service::service_app(s) 
                {
                    _req_index = 0;
                }

                ~nfs_client_app()
                {
                    stop();
                }

                virtual ::dsn::error_code start(int argc, char** argv)
                {
                    if (argc < 2)
                        return ::dsn::ERR_INVALID_PARAMETERS;

                    _server = ::dsn::end_point(argv[1], (uint16_t)atoi(argv[2]));

                    //on_request_timer();
                    _request_timer = ::dsn::service::tasking::enqueue(LPC_NFS_REQUEST_TIMER, this, &nfs_client_app::on_request_timer, 0, 0, 1000);

                    return ::dsn::ERR_OK;
                }

                virtual void stop(bool cleanup = false)
                {
                    _timer->cancel(true);
                    _request_timer->cancel(true);
                }

                void on_request_timer()
                {
                    std::string source_dir = "./src"; // add your path
                    std::string dest_dir = "./dst"; // add your path
                    std::string dest_dir2 = "./dst2"; // add your path
                    std::vector<std::string> files; // empty is for all
                    bool overwrite = true;
                    
                    file::copy_remote_files(_server, source_dir, files, dest_dir, overwrite, LPC_NFS_COPY_FILE, nullptr,
                        std::bind(&nfs_client_app::internal_copy_callback,
                        this,
                        std::placeholders::_1,
                        std::placeholders::_2,
                        ++_req_index
                        ));

                    /*file::copy_remote_files(_server, source_dir, files, dest_dir2, overwrite, LPC_NFS_COPY_FILE, nullptr,
                        std::bind(&nfs_client_app::internal_copy_callback,
                        this,
                        std::placeholders::_1,
                        std::placeholders::_2
                        ));*/
                }

                void internal_copy_callback(error_code err, uint32_t size, int index)
                {
                    if (err == ::dsn::ERR_OK)
                    {
                        dinfo("remote file copy request %d completed", index);
                    }
                    else
                    {
                        derror("remote file copy request %d failed, err = %s", index, err.to_string());
                    }
                }
            private:
                ::dsn::task_ptr _timer;
                ::dsn::task_ptr _request_timer;

                ::dsn::end_point _server;
                std::atomic<int> _req_index;

            };

        }
    }
}