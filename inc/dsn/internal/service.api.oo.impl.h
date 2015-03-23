/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

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
# pragma once

namespace dsn {
    namespace service 
    {
        namespace rpc
        {
            namespace internal_use_only
            {
                template<typename T, typename TRequest, typename TResponse>
                class service_rpc_response_task1 : public rpc_response_task, public service_context_manager
                {
                public:
                    service_rpc_response_task1(
                        T* svc,
                        std::shared_ptr<TRequest>& req,
                        void (T::*callback)(error_code, std::shared_ptr<TRequest>, std::shared_ptr<TResponse>),
                        message_ptr& request,
                        int hash = 0
                        )
                        : rpc_response_task(request, hash), service_context_manager(svc, this)
                    {
                        _svc = svc;
                        _req = req;
                        _callback = callback;
                    }

                    virtual void on_response(error_code err, message_ptr& request, message_ptr& response)
                    {
                        if (err == ERR_SUCCESS)
                        {
                            std::shared_ptr<TResponse> resp(new TResponse);
                            unmarshall(response->reader(), *resp);
                            (_svc->*_callback)(err, _req, resp);
                        }
                        else
                        {
                            (_svc->*_callback)(err, _req, nullptr);
                        }
                    }

                private:
                    T* _svc;
                    std::shared_ptr<TRequest> _req;
                    void (T::*_callback)(error_code, std::shared_ptr<TRequest>, std::shared_ptr<TResponse>);
                };

                template<typename TRequest, typename TResponse>
                class service_rpc_response_task2 : public rpc_response_task, public service_context_manager
                {
                public:
                    service_rpc_response_task2(
                        servicelet* svc,
                        std::shared_ptr<TRequest>& req,
                        std::function<void(error_code, std::shared_ptr<TRequest>, std::shared_ptr<TResponse>)>& callback,
                        message_ptr& request,
                        int hash = 0
                        )
                        : rpc_response_task(request, hash), service_context_manager(svc, this)
                    {
                        _req = req;
                        _callback = callback;
                    }

                    virtual void on_response(error_code err, message_ptr& request, message_ptr& response)
                    {
                        if (err == ERR_SUCCESS)
                        {
                            std::shared_ptr<TResponse> resp(new TResponse);
                            unmarshall(response->reader(), *resp);
                            _callback(err, _req, resp);
                        }
                        else
                        {
                            _callback(err, _req, nullptr);
                        }
                        _callback = nullptr;
                    }

                private:
                    std::shared_ptr<TRequest> _req;
                    std::function<void(error_code, std::shared_ptr<TRequest>, std::shared_ptr<TResponse>)> _callback;
                };

                template<typename TRequest, typename TResponse>
                class service_rpc_response_task3 : public rpc_response_task, public service_context_manager
                {
                public:
                    service_rpc_response_task3(
                        servicelet* svc,
                        std::function<void(error_code, const TResponse&)>& callback,
                        message_ptr& request,
                        int hash = 0
                        )
                        : rpc_response_task(request, hash), service_context_manager(svc, this)
                    {
                        _callback = callback;
                    }

                    virtual void on_response(error_code err, message_ptr& request, message_ptr& response)
                    {
                        TResponse resp;
                        if (err == ERR_SUCCESS)
                        {
                            unmarshall(response->reader(), resp);
                            _callback(err, resp);
                        }
                        else
                        {
                            _callback(err, resp);
                        }
                        _callback = nullptr;
                    }

                private:
                    std::function<void(error_code, const TResponse&)> _callback;
                };

                class service_rpc_response_task4 : public rpc_response_task, public service_context_manager
                {
                public:
                    service_rpc_response_task4(
                        servicelet* svc,
                        std::function<void(error_code, message_ptr&, message_ptr&)>& callback,
                        message_ptr& request,
                        int hash = 0
                        )
                        : rpc_response_task(request, hash), service_context_manager(svc, this)
                    {
                        _callback = callback;
                    }

                    virtual void on_response(error_code err, message_ptr& request, message_ptr& response)
                    {
                        _callback(err, request, response);
                        _callback = nullptr;
                    }

                private:
                    std::function<void(error_code, message_ptr&, message_ptr&)> _callback;
                };
            }

            // ------------- inline implementation ----------------
            template<typename TRequest>
            inline void call_one_way_typed(
                const end_point& server,
                task_code code,
                const TRequest& req,
                int hash
                )
            {
                message_ptr msg = message::create_request(code, 0, hash);
                marshall(msg->writer(), req);
                rpc::call_one_way(server, msg);
            }

            template<typename T, typename TRequest, typename TResponse>
            inline rpc_response_task_ptr call_typed(
                const end_point& server,
                task_code code,
                std::shared_ptr<TRequest>& req,
                T* context,
                void (T::*callback)(error_code, std::shared_ptr<TRequest>, std::shared_ptr<TResponse>),
                int request_hash/* = 0*/,
                int timeout_milliseconds /*= 0*/,
                int reply_hash /*= 0*/
                )
            {
                message_ptr msg = message::create_request(code, timeout_milliseconds, request_hash);
                marshall(msg->writer(), *req);

                rpc_response_task_ptr resp_task(new internal_use_only::service_rpc_response_task1<T, TRequest, TResponse>(
                    context,
                    req,
                    callback,
                    msg,
                    reply_hash
                    ));

                return rpc::call(server, msg, resp_task);
            }

            template<typename TRequest, typename TResponse>
            inline rpc_response_task_ptr call_typed(
                const end_point& server,
                task_code code,
                std::shared_ptr<TRequest>& req,
                servicelet* context,
                std::function<void(error_code, std::shared_ptr<TRequest>, std::shared_ptr<TResponse>)> callback,
                int request_hash/* = 0*/,
                int timeout_milliseconds /*= 0*/,
                int reply_hash /*= 0*/
                )
            {
                message_ptr msg = message::create_request(code, timeout_milliseconds, request_hash);
                marshall(msg->writer(), *req);

                rpc_response_task_ptr resp_task(new internal_use_only::service_rpc_response_task2<TRequest, TResponse>(
                    context,
                    req,
                    callback,
                    msg,
                    reply_hash
                    ));

                return rpc::call(server, msg, resp_task);
            }


            template<typename TRequest, typename TResponse>
            inline rpc_response_task_ptr call_typed(
                const end_point& server,
                task_code code,
                const TRequest& req,
                servicelet* context,
                std::function<void(error_code, const TResponse&)> callback,
                int request_hash/* = 0*/,
                int timeout_milliseconds /*= 0*/,
                int reply_hash /*= 0*/
                )
            {
                message_ptr msg = message::create_request(code, timeout_milliseconds, request_hash);
                marshall(msg->writer(), req);

                rpc_response_task_ptr resp_task(new internal_use_only::service_rpc_response_task3<TRequest, TResponse>(
                    context,
                    callback,
                    msg,
                    reply_hash
                    ));

                return rpc::call(server, msg, resp_task);
            }

        } // end namespace rpc

        namespace file
        {
            namespace internal_use_only 
            {
                class service_aio_task : public aio_task, public service_context_manager
                {
                public:
                    service_aio_task(task_code code, servicelet* svc, aio_handler& handler, int hash = 0)
                        : aio_task(code, hash), service_context_manager(svc, this)
                    {
                        _handler = handler;
                    }

                    virtual void on_completed(error_code err, uint32_t transferred_size)
                    {
                        if (_handler != nullptr)
                        {
                            _handler(err, transferred_size);
                            _handler = nullptr;
                        }
                    }

                private:
                    aio_handler _handler;
                };

            }
        }

    } // end namespace service
} // end namespace



