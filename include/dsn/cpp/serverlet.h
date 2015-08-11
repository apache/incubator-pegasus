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

# include <dsn/cpp/service.api.oo.h>
# include <dsn/cpp/service_app.h>

namespace dsn 
{
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
        rpc_replier(dsn_message_t response)
        {
            _response = response;
        }

        rpc_replier(const rpc_replier& r)
        {
            _response = r._response;
        }

        void operator () (const TResponse& resp)
        {
            if (_response != nullptr)
            {
                ::marshall(_response, resp);
                dsn_rpc_reply(_response);
            }
        }

    private:
        dsn_message_t _response;
    };

    template <typename T> // where T : serverlet<T>
    class serverlet : public virtual servicelet
    {
    public:
        serverlet(const char* nm, int task_bucket_count = 8);
        virtual ~serverlet();

    protected:
        template<typename TRequest>
        bool register_rpc_handler(dsn_task_code_t rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&));

        template<typename TRequest, typename TResponse>
        bool register_rpc_handler(dsn_task_code_t rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&, TResponse&));

        template<typename TRequest, typename TResponse>
        bool register_async_rpc_handler(dsn_task_code_t rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&, rpc_replier<TResponse>&));

        bool register_rpc_handler(dsn_task_code_t rpc_code, const char* rpc_name_, void (T::*handler)(dsn_message_t));

        bool unregister_rpc_handler(dsn_task_code_t rpc_code);

        template<typename TResponse>
        void reply(dsn_message_t request, const TResponse& resp);

    public:
        const std::string& name() const { return _name; }

    private:
        template<typename TCallback>
        struct handler_context
        {
            T         *this_;
            TCallback cb;
        };

        std::string _name;
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
    inline bool serverlet<T>::register_rpc_handler(dsn_task_code_t rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&))
    {
        typedef handler_context<void (T::*)(const TRequest&)> hc_type1;
        auto hc = (hc_type1*)malloc(sizeof(hc_type1));
        hc->this_ = (T*)this;
        hc->cb = handler;

        dsn_rpc_request_handler_t cb = [](dsn_message_t request, void* param)
        {
            auto hc2 = (hc_type1*)param;

            TRequest req;
            ::unmarshall(request, req);
            ((hc2->this_)->*(hc2->cb))(req);
        };

        return dsn_rpc_register_handler(rpc_code, rpc_name_, cb, hc);
    }

    template<typename T> template<typename TRequest, typename TResponse>
    inline bool serverlet<T>::register_rpc_handler(dsn_task_code_t rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&, TResponse&))
    {
        typedef handler_context<void (T::*)(const TRequest&, TResponse&)> hc_type2;
        auto hc = (hc_type2*)malloc(sizeof(hc_type2));
        hc->this_ = (T*)this;
        hc->cb = handler;

        dsn_rpc_request_handler_t cb = [](dsn_message_t request, void* param)
        {
            auto hc2 = (hc_type2*)param;

            TRequest req;
            ::unmarshall(request, req);

            TResponse resp;
            ((hc2->this_)->*(hc2->cb))(req, resp);

            rpc_replier<TResponse> replier(dsn_msg_create_response(request));
            replier(resp);
        };

        return dsn_rpc_register_handler(rpc_code, rpc_name_, cb, hc);
    }

    template<typename T> template<typename TRequest, typename TResponse>
    inline bool serverlet<T>::register_async_rpc_handler(dsn_task_code_t rpc_code, const char* rpc_name_, void (T::*handler)(const TRequest&, rpc_replier<TResponse>&))
    {
        typedef handler_context<void (T::*)(const TRequest&, rpc_replier<TResponse>&)> hc_type3;
        auto hc = (hc_type3*)malloc(sizeof(hc_type3));
        hc->this_ = (T*)this;
        hc->cb = handler;

        dsn_rpc_request_handler_t cb = [](dsn_message_t request, void* param)
        {
            auto hc2 = (hc_type3*)param;

            TRequest req;
            ::unmarshall(request, req);

            rpc_replier<TResponse> replier(dsn_msg_create_response(request));
            ((hc2->this_)->*(hc2->cb))(req, replier);
        };

        return dsn_rpc_register_handler(rpc_code, rpc_name_, cb, hc);
    }

    template<typename T>
    inline bool serverlet<T>::register_rpc_handler(dsn_task_code_t rpc_code, const char* rpc_name_, void (T::*handler)(dsn_message_t))
    {
        typedef handler_context<void (T::*)(dsn_message_t)> hc_type4;
        auto hc = (hc_type4*)malloc(sizeof(hc_type4));
        hc->this_ = (T*)this;
        hc->cb = handler;

        dsn_rpc_request_handler_t cb = [](dsn_message_t request, void* param)
        {
            auto hc2 = (hc_type4*)param;
            ((hc2->this_)->*(hc2->cb))(request);
        };

        return dsn_rpc_register_handler(rpc_code, rpc_name_, cb, hc);
    }

    template<typename T>
    inline bool serverlet<T>::unregister_rpc_handler(dsn_task_code_t rpc_code)
    {
        auto cb = (void*)dsn_rpc_unregiser_handler(rpc_code);
        if (cb != nullptr) free(cb);
        return cb != nullptr;
    }

    template<typename T>template<typename TResponse>
    inline void serverlet<T>::reply(dsn_message_t request, const TResponse& resp)
    {
        auto msg = dsn_msg_create_response(request);
        ::marshall(msg, resp);
        dsn_rpc_reply(msg);
    }
} // end namespace



