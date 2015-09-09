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

# include <dsn/cpp/task_helper.h>

namespace dsn 
{
    //
    // clientlet is the base class for RPC service and client
    // there can be multiple clientlet in the system
    //
    class clientlet
    {
    public:
        // used inside rDSN apps
        clientlet(int task_bucket_count = 13);

        // used outside rDSN app models (e.g., when used in external app's threads)
        clientlet(const char* host_app_name, int host_app_index, int task_bucket_count = 13);
        
        virtual ~clientlet();

        dsn_app_t app() const { return _app; }
        dsn_task_tracker_t tracker() const { return _tracker; }
        void primary_address(rpc_address& addr) { dsn_primary_address2(addr.c_addr_ptr(), _app); }
        rpc_address primary_address() { rpc_address addr; dsn_primary_address2(addr.c_addr_ptr(), _app); return addr; }

        static uint32_t random32(uint32_t min, uint32_t max) { return dsn_random32(min, max); }
        static uint64_t random64(uint64_t min, uint64_t max) { return dsn_random64(min, max); }
        static uint64_t now_ns() { return dsn_now_ns(); }
        static uint64_t now_us() { return dsn_now_us(); }
        static uint64_t now_ms() { return dsn_now_ms(); }
        
    protected:
        void check_hashed_access();

    private:
        int                            _access_thread_id;
        bool                           _access_thread_id_inited;
        dsn_task_tracker_t             _tracker;
        dsn_app_t                      _app;
    };

    // common APIs

    namespace tasking 
    {
        task_ptr enqueue(
            dsn_task_code_t evt,
            clientlet* svc,
            task_handler callback,
            int hash = 0,
            int delay_milliseconds = 0,
            int timer_interval_milliseconds = 0,
            dsn_app_t app = nullptr
            );


        template<typename T> // where T : public virtual clientlet
        inline task_ptr enqueue(
            dsn_task_code_t evt,
            T* owner,
            void (T::*callback)(),
            //TParam param,
            int hash = 0,
            int delay_milliseconds = 0,
            int timer_interval_milliseconds = 0,
            dsn_app_t app = nullptr
            )
        {
            task_handler h = std::bind(callback, owner);
            return enqueue(
                evt,
                owner,
                h,
                hash,
                delay_milliseconds,
                timer_interval_milliseconds,
                app
                );
        }
    }

    namespace rpc
    {
        //  std::function<void(error_code, dsn_message_t, dsn_message_t)>
        task_ptr call(
            const ::dsn::rpc_address& server,
            dsn_message_t request,
            clientlet* svc,
            rpc_reply_handler callback,
            int reply_hash = 0,
            dsn_app_t app = nullptr
            );

        //
        // for TRequest/TResponse, we assume that the following routines are defined:
        //    marshall(binary_writer& writer, const T& val); 
        //    unmarshall(binary_reader& reader, /*out*/ T& val);
        // either in the namespace of ::dsn::utils or T
        // developers may write these helper functions by their own, or use tools
        // such as protocol-buffer, thrift, or bond to generate these functions automatically
        // for their TRequest and TResponse
        //

        // no callback
        template<typename TRequest>
        void call_one_way_typed(
            const ::dsn::rpc_address& server,
            dsn_task_code_t code,
            const TRequest& req,
            int hash = 0,
            dsn_app_t app = nullptr
            );

        template<typename TRequest>
        ::dsn::error_code call_typed_wait(
            /*out*/ ::dsn::message_ptr* response,
            const ::dsn::rpc_address& server,
            dsn_task_code_t code,
            const TRequest& req,
            int hash = 0,
            int timeout_milliseconds = 0,
            dsn_app_t app = nullptr
            );

        //  std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)>
        template<typename TRequest, typename TResponse>
        task_ptr call_typed(
            const ::dsn::rpc_address& server,
            dsn_task_code_t code,
            std::shared_ptr<TRequest>& req,
            clientlet* svc,
            std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)> callback,
            int request_hash = 0,
            int timeout_milliseconds = 0,
            int reply_hash = 0,
            dsn_app_t app = nullptr
            );

        //  std::function<void(error_code, const TResponse&, void*)>
        template<typename TRequest, typename TResponse>
        task_ptr call_typed(
            const ::dsn::rpc_address& server,
            dsn_task_code_t code,
            const TRequest& req,
            clientlet* svc,
            std::function<void(error_code, const TResponse&, void*)> callback,
            void* context,
            int request_hash = 0,
            int timeout_milliseconds = 0,
            int reply_hash = 0,
            dsn_app_t app = nullptr
            );

        // void (T::*callback)(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)
        // where T : public virtual clientlet
        template<typename T, typename TRequest, typename TResponse>
        task_ptr call_typed(
            const ::dsn::rpc_address& server,
            dsn_task_code_t code,
            std::shared_ptr<TRequest>& req,
            T* owner,
            void (T::*callback)(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&),
            int request_hash = 0,
            int timeout_milliseconds = 0,
            int reply_hash = 0,
            dsn_app_t app = nullptr
            );

        // void (T::*)(error_code, const TResponse&, void*);
        // where T : public virtual clientlet
        template<typename T, typename TRequest, typename TResponse>
        task_ptr call_typed(
            const ::dsn::rpc_address& server,
            dsn_task_code_t code,
            const TRequest& req,
            T* owner,
            void(T::*callback)(error_code, const TResponse&, void*),
            void* context,
            int request_hash = 0,
            int timeout_milliseconds = 0,
            int reply_hash = 0,
            dsn_app_t app = nullptr
            );
    }
    
    namespace file
    {
        task_ptr read(
            dsn_handle_t hFile,
            char* buffer,
            int count,
            uint64_t offset,
            dsn_task_code_t callback_code,
            clientlet* svc,
            aio_handler callback,
            int hash = 0,
            dsn_app_t app = nullptr
            );

        task_ptr write(
            dsn_handle_t hFile,
            const char* buffer,
            int count,
            uint64_t offset,
            dsn_task_code_t callback_code,
            clientlet* svc,
            aio_handler callback,
            int hash = 0,
            dsn_app_t app = nullptr
            );


        template<typename T> // where T : public virtual clientlet
        inline task_ptr read(
            dsn_handle_t hFile,
            char* buffer,
            int count,
            uint64_t offset,
            dsn_task_code_t callback_code,
            T* owner,
            void(T::*callback)(error_code, uint32_t),
            int hash = 0,
            dsn_app_t app = nullptr
            )
        {
            aio_handler h = std::bind(callback, owner, std::placeholders::_1, std::placeholders::_2);
            return file::read(hFile, buffer, count, offset, callback_code, owner, h, hash);
        }

        template<typename T> // where T : public virtual clientlet
        inline task_ptr write(
            dsn_handle_t hFile,
            const char* buffer,
            int count,
            uint64_t offset,
            dsn_task_code_t callback_code,
            T* owner,
            void(T::*callback)(error_code, uint32_t),
            int hash = 0,
            dsn_app_t app = nullptr
            )
        {
            aio_handler h = std::bind(callback, owner, std::placeholders::_1, std::placeholders::_2);
            return file::write(hFile, buffer, count, offset, callback_code, owner, h, hash, app);
        }

        task_ptr copy_remote_files(
            const ::dsn::rpc_address& remote,
            const std::string& source_dir,
            std::vector<std::string>& files,  // empty for all
            const std::string& dest_dir,
            bool overwrite,
            dsn_task_code_t callback_code,
            clientlet* svc,
            aio_handler callback,
            int hash = 0,
            dsn_app_t app = nullptr
            );

        inline task_ptr copy_remote_directory(
            const ::dsn::rpc_address& remote,
            const std::string& source_dir,
            const std::string& dest_dir,
            bool overwrite,
            dsn_task_code_t callback_code,
            clientlet* svc,
            aio_handler callback,
            int hash = 0,
            dsn_app_t app = nullptr
            )
        {
            std::vector<std::string> files;
            return copy_remote_files(
                remote, source_dir, files, dest_dir, overwrite,
                callback_code, svc, callback, hash, app
                );
        }
    }

    // ------------- inline implementation ----------------

    namespace rpc
    {
        namespace internal_use_only
        {
            template<typename TRequest, typename TResponse, typename TCallback, typename TService>
            void on_rpc_response1(TService* svc, TCallback cb, std::shared_ptr<TRequest>& req, ::dsn::error_code err, dsn_message_t request, dsn_message_t response)
            {
                if (err == ERR_OK)
                {
                    std::shared_ptr<TResponse> resp(new TResponse);
                    ::unmarshall(response, *resp);
                    (svc->*cb)(err, req, resp);
                }
                else
                {
                    std::shared_ptr<TResponse> resp(nullptr);
                    (svc->*cb)(err, req, resp);
                }
            }

            template<typename TRequest, typename TResponse, typename TCallback>
            void on_rpc_response2(TCallback& cb, std::shared_ptr<TRequest>& req, ::dsn::error_code err, dsn_message_t request, dsn_message_t response)
            {
                if (err == ERR_OK)
                {
                    std::shared_ptr<TResponse> resp(new TResponse);
                    ::unmarshall(response, *resp);
                    cb(err, req, resp);
                }
                else
                {
                    std::shared_ptr<TResponse> resp(nullptr);
                    cb(err, req, resp);
                }
            }

            template<typename TResponse, typename TCallback, typename TService>
            void on_rpc_response3(TService* svc, TCallback cb, void* param, ::dsn::error_code err, dsn_message_t request, dsn_message_t response)
            {
                TResponse resp;
                if (err == ERR_OK)
                {
                    ::unmarshall(response, resp);
                    (svc->*cb)(err, resp, param);
                }
                else
                {
                    (svc->*cb)(err, resp, param);
                }
            }

            template<typename TResponse, typename TCallback>
            void on_rpc_response4(TCallback& cb, void* param, ::dsn::error_code err, dsn_message_t request, dsn_message_t response)
            {
                TResponse resp;
                if (err == ERR_OK)
                {
                    ::unmarshall(response, resp);
                    cb(err, resp, param);
                }
                else
                {
                    cb(err, resp, param);
                }
            }

            inline task_ptr create_empty_rpc_call(
                dsn_message_t msg,
                int reply_hash,
                dsn_app_t app
                )
            {
                auto task = new safe_task_handle();
                auto t = dsn_rpc_create_response_task(
                    msg,
                    nullptr,
                    nullptr,
                    reply_hash,
                    app
                    );
                task->set_task_info(t);
                return task;
            }

            template<typename T, typename TRequest, typename TResponse>
            inline task_ptr create_rpc_call(
                dsn_message_t msg,
                std::shared_ptr<TRequest>& req,
                T* owner,
                void (T::*callback)(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&),
                int reply_hash,
                dsn_app_t app
                )
            {
                if (callback != nullptr)
                {
                    rpc_reply_handler cb = std::bind(
                        &on_rpc_response1<
                        TRequest,
                        TResponse,
                        void (T::*)(::dsn::error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&),
                        T>,
                        owner,
                        callback,
                        req,
                        std::placeholders::_1,
                        std::placeholders::_2,
                        std::placeholders::_3
                        );

                    auto task = new safe_task<rpc_reply_handler>(cb);

                    task->add_ref(); // released in exec_rpc_response
                    auto t = dsn_rpc_create_response_task(
                        msg,
                        safe_task<rpc_reply_handler >::exec_rpc_response,
                        (void*)task,
                        reply_hash,
                        app
                        );
                    task->set_task_info(t);
                    return task;
                }
                else
                {
                    return create_empty_rpc_call(msg, reply_hash, app);
                }
            }

            template<typename TRequest, typename TResponse>
            inline task_ptr create_rpc_call(
                dsn_message_t msg,
                std::shared_ptr<TRequest>& req,
                std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)>& callback,
                int reply_hash,
                dsn_app_t app
                )
            {
                if (callback != nullptr)
                {
                    rpc_reply_handler cb = std::bind(
                        &on_rpc_response2<
                        TRequest,
                        TResponse,
                        std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)>
                        >,
                        callback,
                        req,
                        std::placeholders::_1,
                        std::placeholders::_2,
                        std::placeholders::_3
                        );

                    auto task = new safe_task<rpc_reply_handler>(cb);

                    task->add_ref(); // released in exec_rpc_response
                    auto t = dsn_rpc_create_response_task(
                        msg,
                        safe_task<rpc_reply_handler >::exec_rpc_response,
                        (void*)task,
                        reply_hash,
                        app
                        );
                    task->set_task_info(t);
                    return task;
                }
                else
                {
                    return create_empty_rpc_call(msg, reply_hash, app);
                }
            }

            template<typename T, typename TResponse>
            inline task_ptr create_rpc_call(
                dsn_message_t msg,
                T* owner,
                void(T::*callback)(error_code, const TResponse&, void*),
                void* context,
                int reply_hash,
                dsn_app_t app
                )
            {
                if (callback != nullptr)
                {
                    rpc_reply_handler cb = std::bind(
                        &on_rpc_response3<
                        TResponse,
                        void(T::*)(::dsn::error_code, const TResponse&, void*),
                        T>,
                        owner,
                        callback,
                        context,
                        std::placeholders::_1,
                        std::placeholders::_2,
                        std::placeholders::_3
                        );

                    auto task = new safe_task<rpc_reply_handler>(cb);

                    task->add_ref(); // released in exec_rpc_response
                    auto t = dsn_rpc_create_response_task(
                        msg,
                        safe_task<rpc_reply_handler >::exec_rpc_response,
                        (void*)task,
                        reply_hash,
                        app
                        );
                    task->set_task_info(t);
                    return task;
                }
                else
                {
                    return create_empty_rpc_call(msg, reply_hash, app);
                }
            }

            template<typename TResponse>
            inline task_ptr create_rpc_call(
                dsn_message_t msg,
                std::function<void(error_code, const TResponse&, void*)>& callback,
                void* context,
                int reply_hash,
                dsn_app_t app
                )
            {
                if (callback != nullptr)
                {
                    rpc_reply_handler cb = std::bind(
                        &on_rpc_response4<
                        TResponse,
                        std::function<void(::dsn::error_code, const TResponse&, void*)>
                        >,
                        callback,
                        context,
                        std::placeholders::_1,
                        std::placeholders::_2,
                        std::placeholders::_3
                        );

                    auto task = new safe_task<rpc_reply_handler>(cb);

                    task->add_ref(); // released in exec_rpc_response
                    auto t = dsn_rpc_create_response_task(
                        msg,
                        safe_task<rpc_reply_handler >::exec_rpc_response,
                        (void*)task,
                        reply_hash,
                        app
                        );
                    task->set_task_info(t);
                    return task;
                }
                else
                {
                    return create_empty_rpc_call(msg, reply_hash, app);
                }
            }
        }

        template<typename TRequest>
        inline void call_one_way_typed(
            const ::dsn::rpc_address& server,
            dsn_task_code_t code,
            const TRequest& req,
            int hash,
            dsn_app_t app
            )
        {
            dsn_message_t msg = dsn_msg_create_request(code, 0, hash);
            ::marshall(msg, req);
            dsn_rpc_call_one_way(&server.c_addr(), msg, app);
        }

        template<typename TRequest>
        inline ::dsn::error_code call_typed_wait(
            /*out*/ ::dsn::message_ptr* response,
            const ::dsn::rpc_address& server,
            dsn_task_code_t code,
            const TRequest& req,
            int hash,
            int timeout_milliseconds /*= 0*/,
            dsn_app_t app
            )
        {
            dsn_message_t msg = dsn_msg_create_request(code, timeout_milliseconds, hash);
            ::marshall(msg, req);

            auto resp = dsn_rpc_call_wait(&server.c_addr(), msg, app);
            if (resp != nullptr)
            {
                if (response)
                    (*response).reset((char*)resp);
                else
                    dsn_msg_release_ref(resp);
                return ::dsn::ERR_OK;
            }
            else
                return ::dsn::ERR_TIMEOUT;
        }

        template<typename T, typename TRequest, typename TResponse>
        inline task_ptr call_typed(
            const ::dsn::rpc_address& server,
            dsn_task_code_t code,
            std::shared_ptr<TRequest>& req,
            T* owner,
            void (T::*callback)(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&),
            int request_hash /*= 0*/,
            int timeout_milliseconds /*= 0*/,
            int reply_hash /*= 0*/,
            dsn_app_t app
            )
        {
            dsn_message_t msg = dsn_msg_create_request(code, timeout_milliseconds, request_hash);
            ::marshall(msg, *req);

            auto t = internal_use_only::create_rpc_call<T, TRequest, TResponse>(
                msg, req, owner, callback, reply_hash, app);

            dsn_rpc_call(&server.c_addr(), t->native_handle(), owner ? owner->tracker() : nullptr, app);
            return t;
        }

        // callback type 5
        //   void (T::*)(error_code, const TResponse&, void*);
        template<typename T, typename TRequest, typename TResponse>
        inline task_ptr call_typed(
            const ::dsn::rpc_address& server,
            dsn_task_code_t code,
            const TRequest& req,
            T* owner,
            void(T::*callback)(error_code, const TResponse&, void*),
            void* context,
            int request_hash /*= 0*/,
            int timeout_milliseconds /*= 0*/,
            int reply_hash /*= 0*/,
            dsn_app_t app
            )
        {
            dsn_message_t msg = dsn_msg_create_request(code, timeout_milliseconds, request_hash);
            ::marshall(msg, req);

            auto t = internal_use_only::create_rpc_call<T, TResponse>(
                msg, owner, callback, context, reply_hash, app);

            dsn_rpc_call(&server.c_addr(), t->native_handle(), owner ? owner->tracker() : nullptr, app);
            return t;
        }

        template<typename TRequest, typename TResponse>
        inline task_ptr call_typed(
            const ::dsn::rpc_address& server,
            dsn_task_code_t code,
            std::shared_ptr<TRequest>& req,
            clientlet* owner,
            std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)> callback,
            int request_hash/* = 0*/,
            int timeout_milliseconds /*= 0*/,
            int reply_hash /*= 0*/,
            dsn_app_t app
            )
        {
            dsn_message_t msg = dsn_msg_create_request(code, timeout_milliseconds, request_hash);
            marshall(msg, *req);

            auto t = internal_use_only::create_rpc_call<TRequest, TResponse>(
                msg, req, owner, callback, reply_hash, app);

            dsn_rpc_call(&server.c_addr(), t->native_handle(), owner ? owner->tracker() : nullptr, app);
            return t;
        }

        template<typename TRequest, typename TResponse>
        inline task_ptr call_typed(
            const ::dsn::rpc_address& server,
            dsn_task_code_t code,
            const TRequest& req,
            clientlet* owner,
            std::function<void(error_code, const TResponse&, void*)> callback,
            void* context,
            int request_hash/* = 0*/,
            int timeout_milliseconds /*= 0*/,
            int reply_hash /*= 0*/,
            dsn_app_t app
            )
        {
            dsn_message_t msg = dsn_msg_create_request(code, timeout_milliseconds, request_hash);
            marshall(msg, req);

            auto t = internal_use_only::create_rpc_call<TResponse>(
                msg, owner, callback, context, reply_hash, app);

            dsn_rpc_call(&server.c_addr(), t->native_handle(), owner ? owner->tracker() : nullptr, app);
            return t;
        }
    }
    
} // end namespace



