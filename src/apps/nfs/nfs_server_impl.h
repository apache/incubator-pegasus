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
# include "nfs_server.h"
# include "nfs_client_impl.h"

namespace dsn {
    namespace service {
        class nfs_service_impl
            : public ::dsn::service::nfs_service, public ::dsn::serverlet<nfs_service_impl>
        {
        public:
            nfs_service_impl(nfs_opts& opts) :
                ::dsn::serverlet<nfs_service_impl>("nfs"), _opts(opts)
            {
                _file_close_timer = ::dsn::tasking::enqueue_timer(
                    LPC_NFS_FILE_CLOSE_TIMER, 
                    this,
                    [this] {close_file();},
                    std::chrono::milliseconds(opts.file_close_timer_interval_ms_on_server));
            }
            virtual ~nfs_service_impl() {}

        protected:
            // RPC_NFS_V2_NFS_COPY 
            virtual void on_copy(const copy_request& request, ::dsn::rpc_replier<copy_response>& reply);
            // RPC_NFS_V2_NFS_GET_FILE_SIZE 
            virtual void on_get_file_size(const get_file_size_request& request, ::dsn::rpc_replier<get_file_size_response>& reply);

        private:
            struct callback_para
            {
                dsn_handle_t hfile;
                std::string file_path;
                std::string dst_dir;
                blob bb;
                uint64_t offset;
                uint32_t size;
                rpc_replier<copy_response> replier;

                callback_para(const rpc_replier<copy_response>& r) : hfile(nullptr), offset(0), size(0), replier(r){}
            };

            struct file_handle_info_on_server
            {
                dsn_handle_t file_handle;
                int32_t file_access_count; // concurrent r/w count
                uint64_t last_access_time; // last touch time

                file_handle_info_on_server() : file_handle(nullptr), file_access_count(0), last_access_time(0) {}
            };

            void internal_read_callback(error_code err, size_t sz, callback_para cp);

            void close_file();

        private:
            nfs_opts  &_opts;

            zlock _handles_map_lock;
            std::unordered_map <std::string, file_handle_info_on_server*> _handles_map; // cache file handles

            ::dsn::task_ptr _file_close_timer;
        };

    }
}
