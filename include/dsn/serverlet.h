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

# include <dsn/internal/service.api.oo.h>

namespace dsn {
    namespace service {

        //
        // for TRequest/TResponse, we assume that the following routines are defined:
        //    marshall(binary_writer& writer, const T& val); 
        //    unmarshall(binary_reader& reader, __out_param T& val);
        // either in the namespace of ::dsn or T
        // developers may write these helper functions by their own, or use tools
        // such as protocol-buffer, thrift, or bond to generate these functions automatically
        // for their TRequest and TResponse
        //

        template <typename TResponse>
        class rpc_replier
        {
        public:
            rpc_replier(message_ptr& request)
            {
                _request = request;
                _response = request->create_response();
            }

            rpc_replier(message_ptr& request, message_ptr& response)
            {
                _request = request;
                _response = response;
            }

            rpc_replier(const rpc_replier& r)
            {
                _request = r._request;
                _response = r._response;
            }

            void operator () (const TResponse& resp)
            {
                if (_response != nullptr)
                {
                    marshall(_response->writer(), resp);
                    rpc::reply(_response);
                }
            }

            template<typename T2, typename T2Request, typename T2Response>
            void continue_next(
                const TResponse& local_response,
                T2* next_service,
                void (T2::*handler)(const T2Request&, rpc_replier<T2Response>&)
                );

            template<typename T2, typename T2Request, typename T2Response>
            void continue_next_async(
                const TResponse& local_response,
                task_code code,
                T2* next_service,
                void (T2::*handler)(const T2Request&, rpc_replier<T2Response>&),
                int hash = 0,
                int delay_milliseconds = 0
                );

        private:
            message_ptr _request;
            message_ptr _response;
        };

        template <typename T> // where T : serverlet<T>
        class serverlet : public virtual servicelet
        {
        public:
            serverlet(const char* nm);
            virtual ~serverlet();

        protected:
            template<typename TRequest>
            bool register_rpc_handler(task_code rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&));

            template<typename TRequest, typename TResponse>
            bool register_rpc_handler(task_code rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&, TResponse&));

            template<typename TRequest, typename TResponse>
            bool register_async_rpc_handler(task_code rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&, rpc_replier<TResponse>&));

            bool register_rpc_handler(task_code rpc_code, const char* rpc_name_, void (T::*handler)(message_ptr&));

            bool unregister_rpc_handler(task_code rpc_code);

            template<typename TResponse>
            void reply(message_ptr request, const TResponse& resp);

        public:
            const std::string& name() const { return _name; }

        private:
            std::string _name;

        private:
            // type 1 --------------------------
            template<typename TRequest>
            class service_rpc_request_task1 : public rpc_request_task, public service_context_manager
            {
            public:
                service_rpc_request_task1(message_ptr& request, service_node* node, T* svc, void (T::*handler)(const TRequest&))
                    : rpc_request_task(request, node), service_context_manager(svc, this)
                {
                    _handler = handler;
                    _svc = svc;
                }

                void exec()
                {
                    TRequest req;
                    unmarshall(_request->reader(), req);
                    (_svc->*_handler)(req);
                }

            private:
                void (T::*_handler)(const TRequest&);
                T* _svc;
            };

            template<typename TRequest>
            class service_rpc_server_handler1 : public rpc_server_handler
            {
            public:
                service_rpc_server_handler1(T* svc, void (T::*handler)(const TRequest&))
                {
                    _handler = handler;
                    _svc = svc;
                }

                virtual rpc_request_task_ptr new_request_task(message_ptr& request, service_node* node)
                {
                    return new service_rpc_request_task1<TRequest>(request, node, _svc, _handler);
                }

            private:
                void (T::*_handler)(const TRequest&);
                T* _svc;
            };

            // type 2 ---------------------------
            template<typename TRequest, typename TResponse>
            class service_rpc_request_task2 : public rpc_request_task, public service_context_manager
            {
            public:
                service_rpc_request_task2(message_ptr& request, service_node* node, T* svc, void (T::*handler)(const TRequest&, TResponse&))
                    : rpc_request_task(request, node), service_context_manager(svc, this)
                {
                    _handler = handler;
                    _svc = svc;
                }

                void exec()
                {
                    TRequest req;
                    unmarshall(_request->reader(), req);

                    TResponse resp;
                    (_svc->*_handler)(req, resp);

                    rpc_replier<TResponse> replier(_request);
                    replier(resp);
                }

            private:
                void (T::*_handler)(const TRequest&, TResponse&);
                T* _svc;
            };

            template<typename TRequest, typename TResponse>
            class service_rpc_server_handler2 : public rpc_server_handler
            {
            public:
                service_rpc_server_handler2(T* svc, void (T::*handler)(const TRequest&, TResponse&))
                {
                    _handler = handler;
                    _svc = svc;
                }

                virtual rpc_request_task_ptr new_request_task(message_ptr& request, service_node* node)
                {
                    return new service_rpc_request_task2<TRequest, TResponse>(request, node, _svc, _handler);
                }

            private:
                void (T::*_handler)(const TRequest&, TResponse&);
                T* _svc;
            };

            // type 3 -----------------------------------
            template<typename TRequest, typename TResponse>
            class service_rpc_request_task3 : public rpc_request_task, public service_context_manager
            {
            public:
                service_rpc_request_task3(message_ptr& request, service_node* node, T* svc, void (T::*handler)(const TRequest&, rpc_replier<TResponse>&))
                    : rpc_request_task(request, node), service_context_manager(svc, this)
                {
                    _handler = handler;
                    _svc = svc;
                }

                void exec()
                {
                    TRequest req;
                    unmarshall(_request->reader(), req);

                    rpc_replier<TResponse> replier(_request);
                    (_svc->*_handler)(req, replier);
                }

            private:
                void (T::*_handler)(const TRequest&, rpc_replier<TResponse>&);
                T* _svc;
            };

            template<typename TRequest, typename TResponse>
            class service_rpc_server_handler3 : public rpc_server_handler
            {
            public:
                service_rpc_server_handler3(T* svc, void (T::*handler)(const TRequest&, rpc_replier<TResponse>&))
                {
                    _handler = handler;
                    _svc = svc;
                }

                virtual rpc_request_task_ptr new_request_task(message_ptr& request, service_node* node)
                {
                    return new service_rpc_request_task3<TRequest, TResponse>(request, node, _svc, _handler);
                }

            private:
                void (T::*_handler)(const TRequest&, rpc_replier<TResponse>&);
                T* _svc;
            };

            // type 4 ------------------------------------------
            class service_rpc_request_task4 : public rpc_request_task, public service_context_manager
            {
            public:
                service_rpc_request_task4(message_ptr& request, service_node* node, T* svc, void (T::*handler)(message_ptr&))
                    : rpc_request_task(request, node), service_context_manager(svc, this)
                {
                    _handler = handler;
                    _svc = svc;
                }

                void exec()
                {
                    (_svc->*_handler)(_request);
                }

            private:
                void (T::*_handler)(message_ptr&);
                T* _svc;
            };

            class service_rpc_server_handler4 : public rpc_server_handler
            {
            public:
                service_rpc_server_handler4(T* svc, void (T::*handler)(message_ptr&))
                {
                    _handler = handler;
                    _svc = svc;
                }

                virtual rpc_request_task_ptr new_request_task(message_ptr& request, service_node* node)
                {
                    return new service_rpc_request_task4(request, node, _svc, _handler);
                }

            private:
                void (T::*_handler)(message_ptr&);
                T* _svc;
            };
        };

        // ------------- inline implementation ----------------
        template<typename TResponse> template<typename T2, typename T2Request, typename T2Response>
        inline void rpc_replier<TResponse>::continue_next(
            const TResponse& local_response,
            T2* next_service,
            void (T2::*handler)(const T2Request&, rpc_replier<T2Response>&)
            )
        {
            marshall(_response->writer(), local_response);

            T2Request req;
            unmarshall(_request->reader(), req);

            rpc_replier<T2Response> reply(_request, _response);
            (next_service->*handler)(req, reply);
        }

        template<typename T, typename TRequest, typename TResponse>
        class service_rpc_request_continue_task : public task, public service_context_manager
        {
        public:
            service_rpc_request_continue_task(
                message_ptr& request, 
                message_ptr& response,
                task_code code,
                T* svc, 
                void (T::*handler)(const TRequest&, rpc_replier<TResponse>&),
                int hash = 0
                )
                : task(code, hash), service_context_manager(svc, this)
            {
                _handler = handler;
                _svc = svc;
                _request = request;
                _response = response;
            }

            void exec()
            {
                TRequest req;
                unmarshall(_request->reader(), req);

                rpc_replier<TResponse> replier(_request, _response);
                (_svc->*_handler)(req, replier);
            }

        private:
            void (T::*_handler)(const TRequest&, rpc_replier<TResponse>&);
            T* _svc;
            message_ptr _request;
            message_ptr _response;
        };

        template<typename TResponse> template<typename T2, typename T2Request, typename T2Response>
        inline void rpc_replier<TResponse>::continue_next_async(
            const TResponse& local_response,
            task_code code,
            T2* next_service,
            void (T2::*handler)(const T2Request&, rpc_replier<T2Response>&),
            int hash,
            int delay_milliseconds
            )
        {
            marshall(_response->writer(), local_response);

            task_ptr tsk(new service_rpc_request_continue_task<T2, T2Request, T2Response>(
                _request,
                _response,
                code,
                next_service,
                handler,
                hash                
                ));

            service::tasking::enqueue(tsk, delay_milliseconds);
        }

        template<typename T>
        serverlet<T>::serverlet(const char* nm)
            : _name(nm)
        {
        }

        template<typename T>
        serverlet<T>::~serverlet()
        {
        }

        template<typename T> template<typename TRequest>
        inline bool serverlet<T>::register_rpc_handler(task_code rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&))
        {
            return rpc::register_rpc_handler(rpc_code, rpc_name_,
                new service_rpc_server_handler1<TRequest>(static_cast<T*>(this), handler));
        }

        template<typename T> template<typename TRequest, typename TResponse>
        inline bool serverlet<T>::register_rpc_handler(task_code rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&, TResponse&))
        {
            return rpc::register_rpc_handler(rpc_code, rpc_name_,
                new service_rpc_server_handler2<TRequest, TResponse>(static_cast<T*>(this), handler));
        }

        template<typename T> template<typename TRequest, typename TResponse>
        inline bool serverlet<T>::register_async_rpc_handler(task_code rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&, rpc_replier<TResponse>&))
        {
            return rpc::register_rpc_handler(rpc_code, rpc_name_,
                new service_rpc_server_handler3<TRequest, TResponse>(static_cast<T*>(this), handler));
        }

        template<typename T>
        inline bool serverlet<T>::register_rpc_handler(task_code rpc_code, const char* rpc_name_, void (T::*handler)(message_ptr&))
        {
            return rpc::register_rpc_handler(rpc_code, rpc_name_,
                new service_rpc_server_handler4(static_cast<T*>(this), handler));
        }

        template<typename T>
        inline bool serverlet<T>::unregister_rpc_handler(task_code rpc_code)
        {
            return rpc::unregister_rpc_handler(rpc_code);
        }

        template<typename T>template<typename TResponse>
        inline void serverlet<T>::reply(message_ptr request, const TResponse& resp)
        {
            auto msg = request->create_response();
            marshall(msg->writer(), resp);
            rpc::reply(msg);
        }
    } // end namespace service
} // end namespace



