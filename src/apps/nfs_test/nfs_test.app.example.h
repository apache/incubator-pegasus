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
# include <dsn/tool/nfs_node_simple.h>
# include "nfs_code_definition.h"

namespace dsn {
    namespace replication {
        namespace application {

            // server app example
            class nfs_server_app : public ::dsn::service_app, public virtual ::dsn::clientlet
            {
            public:
                nfs_server_app() {}

                virtual ::dsn::error_code start(int argc, char** argv)
                {
                    // use builtin nfs_service by set [core] start_nfs = true
                    return ::dsn::ERR_OK;
                }

                virtual void stop(bool cleanup = false)
                {
                }
            };

            // client app example
            class nfs_client_app : public ::dsn::service_app, public virtual ::dsn::clientlet
            {
            public:
                nfs_client_app()
                {
                    _req_index = 0;
                    _is_copying = false;
                }

                ~nfs_client_app()
                {
                    stop();
                }

                virtual ::dsn::error_code start(int argc, char** argv)
                {
                    if (argc < 2)
                        return ::dsn::ERR_INVALID_PARAMETERS;

                    _server.assign_ipv4(argv[1], (uint16_t)atoi(argv[2]));

                    //on_request_timer();
                    _request_timer = ::dsn::tasking::enqueue_timer(::dsn::service::LPC_NFS_REQUEST_TIMER,
                        this, [this] {on_request_timer();}, std::chrono::milliseconds(1000));

                    return ::dsn::ERR_OK;
                }

                virtual void stop(bool cleanup = false)
                {
                    _request_timer->cancel(true);
                }

                void on_request_timer()
                {
                    if (_is_copying)
                        return;

                    _is_copying = true;

                    std::string source_dir = "./"; // add your path
                    std::string dest_dir = "./dst/"; // add your path
                    std::vector<std::string> files; // empty is for all
                    files.push_back("dsn.nfs.test");
                    bool overwrite = true;
                    file::copy_remote_files(_server, source_dir, files, dest_dir, overwrite,
                        ::dsn::service::LPC_NFS_COPY_FILE, nullptr,
                        [this, index = _req_index.fetch_add(1, std::memory_order_relaxed) + 1](error_code err, int sz)
                        {
                            internal_copy_callback(err, sz, index);
                        }
                        );

                    ddebug("remote file copy request %d started", (int)_req_index);
                }

                void internal_copy_callback(error_code err, size_t size, int index)
                {
                    if (err == ::dsn::ERR_OK)
                    {
                        ddebug("remote file copy request %d completed", index);
                    }
                    else
                    {
                        derror("remote file copy request %d failed, err = %s", index, err.to_string());
                    }

                    _is_copying = false;
                }
            private:
                ::dsn::task_ptr _request_timer;

                ::dsn::rpc_address _server;
                std::atomic<int> _req_index;
                bool _is_copying;
            };

        }
    }
}
