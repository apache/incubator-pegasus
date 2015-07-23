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
# include <dsn/internal/service_app.h>
# include <dsn/internal/rpc_message.h>

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

        private:
            message_ptr _request;
            message_ptr _response;
        };

        template <typename T> // where T : serverlet<T>
        class serverlet : public virtual servicelet
        {
        public:
            serverlet(const char* nm, int task_bucket_count = 8);
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
            static void on_rpc_request1(T* svc, void (T::*cb)(const TRequest&), message_ptr& request)
            {
                TRequest req;
                unmarshall(request->reader(), req);
                (svc->*cb)(req);
            }
            
            // type 2 ---------------------------
            template<typename TRequest, typename TResponse>
            static void on_rpc_request2(T* svc, void (T::*cb)(const TRequest&, TResponse&), message_ptr& request)
            {
                TRequest req;
                unmarshall(request->reader(), req);

                TResponse resp;
                (svc->*cb)(req, resp);

                rpc_replier<TResponse> replier(request);
                replier(resp);
            }
            
            // type 3 -----------------------------------
            template<typename TRequest, typename TResponse>
            static void on_rpc_request3(T* svc, void (T::*cb)(const TRequest&, rpc_replier<TResponse>&), message_ptr& request)
            {
                TRequest req;
                unmarshall(request->reader(), req);

                rpc_replier<TResponse> replier(request);
                (svc->*cb)(req, replier);
            }
            
            // type 4 ------------------------------------------
            static void on_rpc_request4(T* svc, void (T::*cb)(message_ptr&), message_ptr& request)
            {
                (svc->*cb)(request);
            }

            // rpc request handler factory
            static void cpp_rpc_request_handler_rountine(dsn_message_t* req, void* param)
            {
                rpc_request_handler* pcb = (rpc_request_handler*)param;
                ::dsn::message_ptr request = ::dsn::message::from_c_msg(req);
                (*pcb)(request);
            }
        };

        // ------------- inline implementation ----------------
        template<typename T>
        inline serverlet<T>::serverlet(const char* nm, int task_bucket_count)
            : _name(nm), servicelet(task_bucket_count)
        {
        }

        template<typename T>
        inline serverlet<T>::~serverlet()
        {
        }

        template<typename T> template<typename TRequest>
        inline bool serverlet<T>::register_rpc_handler(task_code rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&))
        {
            auto cb = new rpc_request_handler(std::bind(&serverlet<T>::on_rpc_request1<TRequest>, static_cast<T*>(this), handler, std::placeholders::_1));
            return dsn_rpc_register_handler(rpc_code, rpc_name_, cpp_rpc_request_handler_rountine, cb);
        }

        template<typename T> template<typename TRequest, typename TResponse>
        inline bool serverlet<T>::register_rpc_handler(task_code rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&, TResponse&))
        {
            auto cb = new rpc_request_handler(std::bind(&serverlet<T>::on_rpc_request2<TRequest, TResponse>, static_cast<T*>(this), handler, std::placeholders::_1));
            return dsn_rpc_register_handler(rpc_code, rpc_name_, cpp_rpc_request_handler_rountine, cb);
        }

        template<typename T> template<typename TRequest, typename TResponse>
        inline bool serverlet<T>::register_async_rpc_handler(task_code rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&, rpc_replier<TResponse>&))
        {
            auto cb = new rpc_request_handler(std::bind(&serverlet<T>::on_rpc_request3<TRequest, TResponse>, static_cast<T*>(this), handler, std::placeholders::_1));
            return dsn_rpc_register_handler(rpc_code, rpc_name_, cpp_rpc_request_handler_rountine, cb);
        }

        template<typename T>
        inline bool serverlet<T>::register_rpc_handler(task_code rpc_code, const char* rpc_name_, void (T::*handler)(message_ptr&))
        {
            auto cb = new rpc_request_handler(std::bind(&serverlet<T>::on_rpc_request4, static_cast<T*>(this), handler, std::placeholders::_1));
            return dsn_rpc_register_handler(rpc_code, rpc_name_, cpp_rpc_request_handler_rountine, cb);
        }

        template<typename T>
        inline bool serverlet<T>::unregister_rpc_handler(task_code rpc_code)
        {
            auto cb = (rpc_request_handler*)dsn_rpc_unregiser_handler(rpc_code);
            if (cb != nullptr) delete cb;
            return cb != nullptr;
        }

        template<typename T>template<typename TResponse>
        inline void serverlet<T>::reply(message_ptr request, const TResponse& resp)
        {
            auto msg = request->create_response();
            marshall(msg->writer(), resp);
            dsn_rpc_reply(msg->c_msg());
        }
    } // end namespace service
} // end namespace



