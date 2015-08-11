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

namespace dsn 
{
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
                servicelet* owner,
                int reply_hash
                )
            {
                auto task = new safe_task_handle();
                auto t = dsn_rpc_create_response_task(
                    msg,
                    nullptr,
                    nullptr,
                    reply_hash
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
                int reply_hash /*= 0*/
                )
            {
                if (callback != nullptr)
                {
                    rpc_reply_handler cb = std::bind(
                        &internal_use_only::on_rpc_response1<
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
                        reply_hash
                        );
                    task->set_task_info(t);
                    return task;
                }
                else
                {
                    return create_empty_rpc_call(msg, (servicelet*)owner, reply_hash);
                }
            }

            template<typename TRequest, typename TResponse>
            inline task_ptr create_rpc_call(
                dsn_message_t msg,
                std::shared_ptr<TRequest>& req,
                servicelet* owner,
                std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)>& callback,
                int reply_hash /*= 0*/
                )
            {
                if (callback != nullptr)
                {
                    rpc_reply_handler cb = std::bind(
                        &internal_use_only::on_rpc_response2<
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
                        reply_hash
                        );
                    task->set_task_info(t);
                    return task;
                }
                else
                {
                    return create_empty_rpc_call(msg, (servicelet*)owner, reply_hash);
                }
            }

            template<typename T, typename TResponse>
            inline task_ptr create_rpc_call(
                dsn_message_t msg,
                T* owner,
                void(T::*callback)(error_code, const TResponse&, void*),
                void* context,
                int reply_hash /*= 0*/
                )
            {
                if (callback != nullptr)
                {
                    rpc_reply_handler cb = std::bind(
                        &internal_use_only::on_rpc_response3<
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
                        reply_hash
                        );
                    task->set_task_info(t);
                    return task;
                }
                else
                {
                    return create_empty_rpc_call(msg, (servicelet*)owner, reply_hash);
                }
            }

            template<typename TResponse>
            inline task_ptr create_rpc_call(
                dsn_message_t msg,
                servicelet* owner,
                std::function<void(error_code, const TResponse&, void*)>& callback,
                void* context,
                int reply_hash /*= 0*/
                )
            {
                if (callback != nullptr)
                {
                    rpc_reply_handler cb = std::bind(
                        &internal_use_only::on_rpc_response4<
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
                        reply_hash
                        );
                    task->set_task_info(t);
                    return task;
                }
                else
                {
                    return create_empty_rpc_call(msg, (servicelet*)owner, reply_hash);
                }
            }
        }

        // ------------- inline implementation ----------------
        template<typename TRequest>
        inline void call_one_way_typed(
            const dsn_address_t& server,
            dsn_task_code_t code,
            const TRequest& req,
            int hash
            )
        {
            dsn_message_t msg = dsn_msg_create_request(code, 0, hash);
            ::marshall(msg, req);
            dsn_rpc_call_one_way(server, msg);
        }

        template<typename TRequest>
        ::dsn::error_code call_typed_wait(
            /*out*/ ::dsn::message_ptr* response,
            const dsn_address_t& server,
            dsn_task_code_t code,
            const TRequest& req,
            int hash,
            int timeout_milliseconds /*= 0*/
            )
        {
                
            dsn_message_t msg = dsn_msg_create_request(code, timeout_milliseconds, hash);                
            ::marshall(msg, req);

            auto resp = dsn_rpc_call_wait(server, msg);
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
            const dsn_address_t& server,
            dsn_task_code_t code,
            std::shared_ptr<TRequest>& req,
            T* owner,
            void (T::*callback)(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&),
            int request_hash/* = 0*/,
            int timeout_milliseconds /*= 0*/,
            int reply_hash /*= 0*/
            )
        {
            dsn_message_t msg = dsn_msg_create_request(code, timeout_milliseconds, request_hash);
            ::marshall(msg, *req);

            auto t = internal_use_only::create_rpc_call<T, TRequest, TResponse>(
                msg, req, owner, callback, reply_hash);
               
            dsn_rpc_call(server, t->native_handle(), owner ? ((servicelet*)owner)->tracker() : nullptr);
            return t;
        }

        template<typename TRequest, typename TResponse>
        inline task_ptr call_typed(
            const dsn_address_t& server,
            dsn_task_code_t code,
            std::shared_ptr<TRequest>& req,
            servicelet* owner,
            std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)> callback,
            int request_hash/* = 0*/,
            int timeout_milliseconds /*= 0*/,
            int reply_hash /*= 0*/
            )
        {
            dsn_message_t msg = dsn_msg_create_request(code, timeout_milliseconds, request_hash);
            marshall(msg, *req);

            auto t = internal_use_only::create_rpc_call<TRequest, TResponse>(
                msg, req, owner, callback, reply_hash);

            dsn_rpc_call(server, t->native_handle(), owner ? ((servicelet*)owner)->tracker() : nullptr);
            return t;
        }

        template<typename T, typename TRequest, typename TResponse>
        inline task_ptr call_typed(
            const dsn_address_t& server,
            dsn_task_code_t code,
            const TRequest& req,
            T* owner,
            void(T::*callback)(error_code, const TResponse&, void*),
            void* context,
            int request_hash/* = 0*/,
            int timeout_milliseconds /*= 0*/,
            int reply_hash /*= 0*/
            )
        {
            dsn_message_t msg = dsn_msg_create_request(code, timeout_milliseconds, request_hash);
            ::marshall(msg, req);

            auto t = internal_use_only::create_rpc_call<T, TResponse>(
                msg, owner, callback, context, reply_hash);

            dsn_rpc_call(server, t->native_handle(), owner ? ((servicelet*)owner)->tracker() : nullptr);
            return t;
        }

        template<typename TRequest, typename TResponse>
        inline task_ptr call_typed(
            const dsn_address_t& server,
            dsn_task_code_t code,
            const TRequest& req,
            servicelet* owner,
            std::function<void(error_code, const TResponse&, void*)> callback,
            void* context,
            int request_hash/* = 0*/,
            int timeout_milliseconds /*= 0*/,
            int reply_hash /*= 0*/
            )
        {
            dsn_message_t msg = dsn_msg_create_request(code, timeout_milliseconds, request_hash);
            marshall(msg, req);

            auto t = internal_use_only::create_rpc_call<TResponse>(
                msg, owner, callback, context, reply_hash);

            dsn_rpc_call(server, t->native_handle(), owner ? ((servicelet*)owner)->tracker() : nullptr);
            return t;
        }

    } // end namespace rpc
} // end namespace



