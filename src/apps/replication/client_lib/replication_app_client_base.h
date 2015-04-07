/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#pragma once

//
// replication_app_client_base is the base class for clients for 
// all app to be replicated using this library
// 

# include "replication_common.h"

namespace dsn { namespace replication {

    DEFINE_ERR_CODE(ERR_REPLICATION_FAILURE)
    
    class replication_app_client_base : public virtual servicelet
    {
    public:
        replication_app_client_base(        
            const std::vector<end_point>& meta_servers, 
            const char* app_name
            );

        ~replication_app_client_base();

        template<typename T, typename TRequest, typename TResponse>
        rpc_response_task_ptr write(
            int partition_index,
            task_code code,
            std::shared_ptr<TRequest>& req,

            // callback
            T* context,
            void (T::*callback)(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&),

            // other specific parameters   
            int timeout_milliseconds = 0,
            int reply_hash = 0
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_WRITE, timeout_milliseconds);
            marshall(msg->writer(), *req);

            auto task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task1<T, TRequest, TResponse>(
                context,
                req,
                callback,
                msg
                );
            write_internal(partition_index, code, task, reply_hash);
            return task;
        }
        
        template<typename TRequest, typename TResponse>
        rpc_response_task_ptr write(
            int partition_index,
            task_code code,
            std::shared_ptr<TRequest>& req,

            // callback
            servicelet* context,
            std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)> callback,

            // other specific parameters   
            int timeout_milliseconds = 0,
            int reply_hash = 0
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_WRITE, timeout_milliseconds);
            marshall(msg->writer(), *req);

            auto task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task2<TRequest, TResponse>(
                context,
                req,
                callback,
                msg
                );
            write_internal(partition_index, code, task, reply_hash);
            return task;
        }

        template<typename T, typename TRequest, typename TResponse>
        rpc_response_task_ptr write(
            int partition_index,
            task_code code,
            const TRequest& req,

            // callback
            T* context,
            void (T::*callback)(error_code, const TResponse&),

            // other specific parameters   
            int timeout_milliseconds = 0,
            int reply_hash = 0
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_WRITE, timeout_milliseconds);
            marshall(msg->writer(), req);

            auto task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task5<T, TResponse>(
                context,
                callback,
                msg
                );
            write_internal(partition_index, code, task, reply_hash);
            return task;
        }

        template<typename TRequest, typename TResponse>
        rpc_response_task_ptr write(
            int partition_index,
            task_code code,
            const TRequest& req,

            // callback
            servicelet* context,
            std::function<void(error_code, const TResponse&)> callback,

            // other specific parameters   
            int timeout_milliseconds = 0,
            int reply_hash = 0
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_WRITE, timeout_milliseconds);
            marshall(msg->writer(), req);

            auto task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task3<TResponse>(
                context,
                callback,
                msg
                );
            write_internal(partition_index, code, task, reply_hash);
            return task;
        }

        template<typename T, typename TRequest, typename TResponse>
        rpc_response_task_ptr read(
            int partition_index,
            task_code code,
            std::shared_ptr<TRequest>& req,

            // callback
            T* context,
            void (T::*callback)(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&),

            // other specific parameters   
            int timeout_milliseconds = 0,
            read_semantic_t read_semantic = ReadOutdated,
            decree snapshot_decree = invalid_decree, // only used when ReadSnapshot        
            int reply_hash = 0
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_READ, timeout_milliseconds);
            marshall(msg->writer(), *req);

            auto task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task1<T, TRequest, TResponse>(
                context,
                req,
                callback,
                msg
                );
            read_internal(partition_index, code, task, read_semantic, snapshot_decree, reply_hash);
            return task;
        }

        template<typename TRequest, typename TResponse>
        rpc_response_task_ptr read(
            int partition_index,
            task_code code,
            std::shared_ptr<TRequest>& req,

            // callback
            servicelet* context,
            std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)> callback,

            // other specific parameters   
            int timeout_milliseconds = 0,
            read_semantic_t read_semantic = ReadOutdated,
            decree snapshot_decree = invalid_decree, // only used when ReadSnapshot        
            int reply_hash = 0
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_READ, timeout_milliseconds);
            marshall(msg->writer(), *req);

            auto task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task2<TRequest, TResponse>(
                context,
                req,
                callback,
                msg
                );
            read_internal(partition_index, code, task, read_semantic, snapshot_decree, reply_hash);
            return task;
        }

        template<typename T, typename TRequest, typename TResponse>
        rpc_response_task_ptr read(
            int partition_index,
            task_code code,
            const TRequest& req,

            // callback
            T* context,
            void (T::*callback)(error_code, const TResponse&),

            // other specific parameters   
            int timeout_milliseconds = 0,
            read_semantic_t read_semantic = ReadOutdated,
            decree snapshot_decree = invalid_decree, // only used when ReadSnapshot        
            int reply_hash = 0
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_READ, timeout_milliseconds);
            marshall(msg->writer(), req);

            auto task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task5<T, TResponse>(
                context,
                callback,
                msg
                );
            read_internal(partition_index, code, task, read_semantic, snapshot_decree, reply_hash);
            return task;
        }

        template<typename TRequest, typename TResponse>
        rpc_response_task_ptr read(
            int partition_index,
            task_code code,
            const TRequest& req,

            // callback
            servicelet* context,
            std::function<void(error_code, const TResponse&)> callback,

            // other specific parameters   
            int timeout_milliseconds = 0,
            read_semantic_t read_semantic = ReadOutdated,
            decree snapshot_decree = invalid_decree, // only used when ReadSnapshot        
            int reply_hash = 0
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_READ, timeout_milliseconds);
            marshall(msg->writer(), req);

            auto task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task3<TResponse>(
                context,
                callback,
                msg
                );
            read_internal(partition_index, code, task, read_semantic, snapshot_decree, reply_hash);
            return task;
        }

        // get read address policy
        virtual end_point get_read_address(read_semantic_t semantic, const partition_configuration& config);

    private:
        void write_internal(
            int partition_index,
            task_code code,
            rpc_response_task_ptr callback,
            int reply_hash = 0
            );

        void read_internal(
            int partition_index,
            task_code code,
            rpc_response_task_ptr callback,
            read_semantic_t read_semantic = ReadOutdated,
            decree snapshot_decree = invalid_decree, // only used when ReadSnapshot        
            int reply_hash = 0
            );

    private:
        std::string                             _app_name;
        std::vector<end_point>                  _meta_servers;

        mutable zrwlock                         _config_lock;
        std::map<int,  partition_configuration> _config_cache;
        int                                     _app_id;
        end_point                               _last_contact_point;

    private:
        struct request_context
        {
            int                   partition_index;
            rpc_response_task_ptr callback_task;
            read_request_header   read_header;
            write_request_header  write_header;
            bool                  is_read;
            uint16_t              header_pos; // write header after body is written
            uint64_t              timeout_ts_us;
            task_ptr              timeout_timer; // when partition config is unknown at the first place
        };

        struct partition_context
        {
            rpc_response_task_ptr query_config_task;
            std::list<request_context*> requests;
        };

        typedef std::map<int, partition_context*> pending_requests;
        
        mutable zlock     _requests_lock;
        pending_requests  _pending_requests;        

    private:
        void call(request_context* request);
        error_code get_address(int pidx, bool is_write, __out_param end_point& addr, __out_param int& app_id, read_semantic_t semantic = read_semantic_t::ReadLastUpdate);
        void on_user_request_timeout(request_context* rc);
        void query_partition_configuration_reply(error_code err, message_ptr& request, message_ptr& response, int pidx);
        void replica_rw_reply(error_code err, message_ptr& request, message_ptr& response, request_context* rc);
        void end_request(request_context* request, error_code err, message_ptr& resp);
        void clear_all_pending_tasks();
    };


}} // namespace
