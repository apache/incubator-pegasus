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
#pragma once

//
// replication_app_client_base is the base class for clients for 
// all app to be replicated using this library
// 

# include <dsn/serverlet.h>
# include <dsn/dist/replication/replication.types.h>
# include <dsn/dist/replication/replication_other_types.h>
# include <dsn/dist/replication/replication.codes.h>

namespace dsn { namespace replication {

    DEFINE_ERR_CODE(ERR_REPLICATION_FAILURE)
    
#pragma pack(push, 4)
    class replication_app_client_base : public virtual servicelet
    {
    public:
        static void load_meta_servers(configuration_ptr& cf, __out_param std::vector<end_point>& servers);

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
            T* owner,
            void (T::*callback)(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&),

            // other specific parameters   
            int timeout_milliseconds = 0,
            int reply_hash = 0
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_WRITE, timeout_milliseconds);
            
            rpc_response_task_ptr task; 
            if (callback == nullptr) 
                task = new rpc_response_task_empty(msg); 
            else 
                task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task1<T, TRequest, TResponse>(
                owner,
                req,
                callback,
                msg
                );
            auto rc = create_write_context(partition_index, code, task, reply_hash);
            marshall(msg->writer(), *req);
            call(rc);
            return std::move(task);
        }
        
        template<typename TRequest, typename TResponse>
        rpc_response_task_ptr write(
            int partition_index,
            task_code code,
            std::shared_ptr<TRequest>& req,

            // callback
            servicelet* owner,
            std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)> callback,

            // other specific parameters   
            int timeout_milliseconds = 0,
            int reply_hash = 0
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_WRITE, timeout_milliseconds);
            
            rpc_response_task_ptr task;
            if (callback == nullptr)
                task = new rpc_response_task_empty(msg);
            else
                task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task2<TRequest, TResponse>(
                owner,
                req,
                callback,
                msg
                );
            auto rc = create_write_context(partition_index, code, task, reply_hash);
            marshall(msg->writer(), *req);
            call(rc);
            return std::move(task);
        }

        template<typename T, typename TRequest, typename TResponse>
        rpc_response_task_ptr write(
            int partition_index,
            task_code code,
            const TRequest& req,

            // callback
            T* owner,
            void (T::*callback)(error_code, const TResponse&, void*),
            void* context,

            // other specific parameters   
            int timeout_milliseconds = 0,
            int reply_hash = 0
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_WRITE, timeout_milliseconds);
            
            rpc_response_task_ptr task;
            if (callback == nullptr)
                task = new rpc_response_task_empty(msg);
            else
                task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task5<T, TResponse>(
                owner,
                callback,
                context,
                msg
                );
            auto rc = create_write_context(partition_index, code, task, reply_hash);
            marshall(msg->writer(), req);
            call(rc);
            return std::move(task);
        }

        template<typename TRequest, typename TResponse>
        rpc_response_task_ptr write(
            int partition_index,
            task_code code,
            const TRequest& req,

            // callback
            servicelet* owner,
            std::function<void(error_code, const TResponse&, void*)> callback,
            void* context,

            // other specific parameters   
            int timeout_milliseconds = 0,
            int reply_hash = 0
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_WRITE, timeout_milliseconds);
            
            rpc_response_task_ptr task;
            if (callback == nullptr)
                task = new rpc_response_task_empty(msg);
            else
                task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task3<TResponse>(
                owner,
                callback,
                context,
                msg
                );
            auto rc = create_write_context(partition_index, code, task, reply_hash);
            marshall(msg->writer(), req);
            call(rc);
            return std::move(task);
        }

        template<typename T, typename TRequest, typename TResponse>
        rpc_response_task_ptr read(
            int partition_index,
            task_code code,
            std::shared_ptr<TRequest>& req,

            // callback
            T* owner,
            void (T::*callback)(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&),

            // other specific parameters   
            int timeout_milliseconds = 0,            
            int reply_hash = 0,
            read_semantic_t read_semantic = ReadOutdated,
            decree snapshot_decree = invalid_decree // only used when ReadSnapshot
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_READ, timeout_milliseconds);
            
            rpc_response_task_ptr task;
            if (callback == nullptr)
                task = new rpc_response_task_empty(msg);
            else
                task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task1<T, TRequest, TResponse>(
                owner,
                req,
                callback,
                msg
                );
            auto rc = create_read_context(partition_index, code, task, read_semantic, snapshot_decree, reply_hash);
            marshall(msg->writer(), *req);
            call(rc);
            return std::move(task);
        }

        template<typename TRequest, typename TResponse>
        rpc_response_task_ptr read(
            int partition_index,
            task_code code,
            std::shared_ptr<TRequest>& req,

            // callback
            servicelet* owner,
            std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)> callback,

            // other specific parameters   
            int timeout_milliseconds = 0,
            int reply_hash = 0,
            read_semantic_t read_semantic = ReadOutdated,
            decree snapshot_decree = invalid_decree // only used when ReadSnapshot
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_READ, timeout_milliseconds);
            
            rpc_response_task_ptr task;
            if (callback == nullptr)
                task = new rpc_response_task_empty(msg);
            else
                task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task2<TRequest, TResponse>(
                owner,
                req,
                callback,
                msg
                );
            auto rc = create_read_context(partition_index, code, task, read_semantic, snapshot_decree, reply_hash);
            marshall(msg->writer(), *req);
            call(rc);
            return std::move(task);
        }

        template<typename T, typename TRequest, typename TResponse>
        rpc_response_task_ptr read(
            int partition_index,
            task_code code,
            const TRequest& req,

            // callback
            T* owner,
            void (T::*callback)(error_code, const TResponse&, void*),
            void* context,

            // other specific parameters   
            int timeout_milliseconds = 0,
            int reply_hash = 0,
            read_semantic_t read_semantic = ReadOutdated,
            decree snapshot_decree = invalid_decree // only used when ReadSnapshot
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_READ, timeout_milliseconds);
            
            rpc_response_task_ptr task;
            if (callback == nullptr)
                task = new rpc_response_task_empty(msg);
            else
                task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task5<T, TResponse>(
                owner,
                callback,
                context,
                msg
                );
            auto rc = create_read_context(partition_index, code, task, read_semantic, snapshot_decree, reply_hash);
            marshall(msg->writer(), req);
            call(rc);
            return std::move(task);
        }

        template<typename TRequest, typename TResponse>
        rpc_response_task_ptr read(
            int partition_index,
            task_code code,
            const TRequest& req,

            // callback
            servicelet* owner,
            std::function<void(error_code, const TResponse&, void*)> callback,
            void* context,

            // other specific parameters   
            int timeout_milliseconds = 0,
            int reply_hash = 0,
            read_semantic_t read_semantic = ReadOutdated,
            decree snapshot_decree = invalid_decree // only used when ReadSnapshot
            )
        {
            timeout_milliseconds = (timeout_milliseconds != 0 ? timeout_milliseconds : task_spec::get(code)->rpc_timeout_milliseconds);
            message_ptr msg = message::create_request(RPC_REPLICATION_CLIENT_READ, timeout_milliseconds);
            
            rpc_response_task_ptr task;
            if (callback == nullptr)
                task = new rpc_response_task_empty(msg);
            else
                task = new ::dsn::service::rpc::internal_use_only::service_rpc_response_task3<TResponse>(
                owner,
                callback,
                context,
                msg
                );
            auto rc = create_read_context(partition_index, code, task, read_semantic, snapshot_decree, reply_hash);
            marshall(msg->writer(), req);
            call(rc);
            return std::move(task);
        }

        // get read address policy
        virtual end_point get_read_address(read_semantic_t semantic, const partition_configuration& config);
        
    public:
        struct request_context : public ref_object
        {
            int                   partition_index;
            rpc_response_task_ptr callback_task;
            read_request_header   read_header;
            write_request_header  write_header;
            bool                  is_read;
            uint16_t              header_pos; // write header after body is written
            uint64_t              timeout_ts_us; // timeout at this timing point

            zlock                 lock; // [
            task_ptr              timeout_timer; // when partition config is unknown at the first place            
            task_ptr              rw_task;
            bool                  completed;
            // ]
        };

        typedef ::boost::intrusive_ptr<request_context> request_context_ptr;

    private:
        struct partition_context
        {
            rpc_response_task_ptr query_config_task;
            std::list<request_context_ptr> requests;
        };

        typedef std::unordered_map<int, partition_context*> pending_requests;
        
        mutable zlock     _requests_lock;
        pending_requests  _pending_requests;        

    private:
        request_context* create_write_context(
            int partition_index,
            task_code code,
            rpc_response_task_ptr& callback,
            int reply_hash = 0
            );

        request_context* create_read_context(
            int partition_index,
            task_code code,
            rpc_response_task_ptr& callback,
            read_semantic_t read_semantic = ReadOutdated,
            decree snapshot_decree = invalid_decree, // only used when ReadSnapshot        
            int reply_hash = 0
            );

    private:
        std::string                             _app_name;
        std::vector<end_point>                  _meta_servers;
        
        mutable zrwlock_nr                      _config_lock;
        std::unordered_map<int, partition_configuration> _config_cache;
        int                                     _app_id;
        int                                     _app_partition_count;
        end_point                               _last_contact_point;

    private:
        void call(request_context_ptr request, bool no_delay = true);
        error_code get_address(int pidx, bool is_write, __out_param end_point& addr, __out_param int& app_id, read_semantic_t semantic = read_semantic_t::ReadLastUpdate);
        void query_partition_configuration_reply(error_code err, message_ptr& request, message_ptr& response, int pidx);
        void replica_rw_reply(error_code err, message_ptr& request, message_ptr& response, request_context_ptr& rc);
        void end_request(request_context_ptr& request, error_code err, message_ptr& resp);
        void on_user_request_timeout(request_context_ptr& rc);
        void clear_all_pending_tasks();
    };

    DEFINE_REF_OBJECT(replication_app_client_base::request_context);

#pragma pack(pop)

}} // namespace
