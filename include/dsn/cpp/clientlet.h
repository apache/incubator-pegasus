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
 *     c++ client side service API
 *
 * Revision history:
 *     Sep., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/cpp/task_helper.h>
# include <dsn/cpp/function_traits.h>

namespace dsn
{
    //
    // clientlet is the base class for RPC service and client
    // there can be multiple clientlet in the system
    //
    class clientlet
    {
    public:
        clientlet(int task_bucket_count = 13);
        virtual ~clientlet();

        dsn_task_tracker_t tracker() const { return _tracker; }
        rpc_address primary_address() { return dsn_primary_address(); }

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
    };

    // common APIs
    struct empty_callback {};

    namespace tasking
    {
        template<typename TCallback>
        task_ptr create_task(
            dsn_task_code_t evt,
            clientlet* svc,
            TCallback&& callback,
            int hash = 0)
        {
            using callback_storage_t = typename std::remove_reference<TCallback>::type;
            auto tsk = new safe_task<callback_storage_t>(std::forward<TCallback>(callback));
            tsk->add_ref(); // released in exec callback
            auto native_tsk = dsn_task_create_ex(
                evt,
                safe_task<callback_storage_t>::exec,
                safe_task<callback_storage_t>::on_cancel,
                tsk,
                hash,
                svc ? svc->tracker() : nullptr);
            tsk->set_task_info(native_tsk);
            return tsk;
        }

        template<typename TCallback>
        task_ptr create_timer_task(
            dsn_task_code_t evt,
            clientlet* svc,
            TCallback&& callback,
            std::chrono::milliseconds timer_interval,
            int hash = 0)
        {
            using callback_storage_t = typename std::remove_reference<TCallback>::type;
            auto tsk = new safe_task<callback_storage_t>(std::forward<TCallback>(callback));
            tsk->add_ref(); // released in exec callback
            auto native_tsk = dsn_task_create_timer_ex(
                evt,
                safe_task<callback_storage_t>::exec_timer,
                safe_task<callback_storage_t>::on_cancel,
                tsk,
                hash,
                static_cast<int>(timer_interval.count()),
                svc ? svc->tracker() : nullptr);
            tsk->set_task_info(native_tsk);
            return tsk;
        }

        template<typename TCallback>
        task_ptr enqueue(
            dsn_task_code_t evt,
            clientlet* svc,
            TCallback&& callback,
            int hash = 0,
            std::chrono::milliseconds delay = std::chrono::milliseconds(0))
        {
            auto tsk = create_task(evt, svc, std::forward<TCallback>(callback), hash);
            tsk->enqueue(delay);
            return tsk;
        }

        template<typename TCallback>
        task_ptr enqueue_timer(
            dsn_task_code_t evt,
            clientlet* svc,
            TCallback&& callback,
            std::chrono::milliseconds timer_interval,
            int hash = 0,
            std::chrono::milliseconds delay = std::chrono::milliseconds(0))
        {
            auto tsk = create_timer_task(evt, svc, std::forward<TCallback>(callback), timer_interval, hash);
            tsk->enqueue(delay);
            return tsk;
        }

        template<typename THandler>
        inline safe_late_task<THandler>* create_late_task(
            dsn_task_code_t evt,
            THandler callback,
            int hash = 0,
            clientlet* svc = nullptr
            )
        {
            auto tsk = new safe_late_task<THandler>(callback);
            tsk->add_ref(); // released in exec callback
            auto t = dsn_task_create_ex(evt,
                safe_late_task<THandler>::exec,
                safe_late_task<THandler>::on_cancel,
                tsk, hash, svc ? svc->tracker() : nullptr);

            tsk->set_task_info(t);
            return tsk;
        }
    }

    namespace rpc
    {
        template<typename TCallback>
        //where TCallback = void(error_code, dsn_message, dsn_message_t)
        task_ptr create_rpc_response_task(
            dsn_message_t request,
            clientlet* svc,
            TCallback&& callback,
            int reply_hash = 0)
        {
            using callback_storage_t = typename std::remove_reference<TCallback>::type;
            auto tsk = new safe_task<callback_storage_t>(std::forward<TCallback>(callback));
            tsk->add_ref(); // released in exec_rpc_response

            auto t = dsn_rpc_create_response_task_ex(
                request,
                safe_task<callback_storage_t >::exec_rpc_response,
                safe_task<callback_storage_t >::on_cancel,
                tsk,
                reply_hash,
                svc ? svc->tracker() : nullptr
                );
            tsk->set_task_info(t);
            return tsk;
        }

        task_ptr create_rpc_response_task(
            dsn_message_t request,
            clientlet* svc,
            empty_callback,
            int reply_hash = 0);

        template<typename TCallback>
        //where TCallback = void(error_code, TResponse&&)
        //  where TResponse = DefaultConstructible && DSNSerializable
        task_ptr create_rpc_response_task_typed(
            dsn_message_t request,
            clientlet* svc,
            TCallback&& callback,
            int reply_hash = 0)
        {
            using callback_inspect_t = dsn::function_traits<TCallback>;
            static_assert(callback_inspect_t::arity == 2, "invalid callback function");
            using errcode_t = typename std::decay<typename callback_inspect_t::template arg_t<0>>::type;
            using response_t = typename std::decay<typename callback_inspect_t::template arg_t<1>>::type;

            static_assert(std::is_same<error_code, errcode_t>::value, "the first argument of callback should be error_code");
            static_assert(std::is_default_constructible<response_t>::value, "cannot wrap a non-trival-constructible type");

            return create_rpc_response_task(
                request,
                svc,
                [cb_fwd = std::forward<TCallback>(callback)](error_code err, dsn_message_t req, dsn_message_t resp)
                {
                    response_t response;
                    if (err == ERR_OK)
                    {
                        ::unmarshall(resp, response);
                    }
                    cb_fwd(err, std::move(response));
                },
                reply_hash);
        }

        template<typename TCallback>
        //where TCallback = void(error_code, dsn_message_t, dsn_message_t)
        task_ptr call(
            ::dsn::rpc_address server,
            dsn_message_t request,
            clientlet* svc,
            TCallback&& callback,
            int reply_hash = 0
            )
        {
            task_ptr t = create_rpc_response_task(request, svc, std::forward<TCallback>(callback), reply_hash);
            dsn_rpc_call(server.c_addr(), t->native_handle());
            return t;
        }

        template<typename TRequest>
        task_ptr call(
            ::dsn::rpc_address server,
            dsn_task_code_t code,
            TRequest&& req,
            clientlet* owner,
            empty_callback,
            int request_hash = 0,
            std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
            int reply_hash = 0)
        {
            dsn_message_t msg = dsn_msg_create_request(code, static_cast<int>(timeout.count()), request_hash);
            ::marshall(msg, std::forward<TRequest>(req));
            auto task = create_rpc_response_task(msg, owner, empty_callback{}, reply_hash);
            dsn_rpc_call(server.c_addr(), task->native_handle());
            return task;
        }

        template<typename TRequest, typename TCallback>
        //where TCallback = void(error_code, TResponse&&)
        task_ptr call_typed(
            ::dsn::rpc_address server,
            dsn_task_code_t code,
            TRequest&& req,
            clientlet* owner,
            TCallback&& callback,
            int request_hash = 0,
            std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
            int reply_hash = 0)
        {
            using callback_inspect_t = dsn::function_traits<TCallback>;
            static_assert(callback_inspect_t::arity == 2, "invalid callback function");
            using errcode_t = typename std::decay<typename callback_inspect_t::template arg_t<0>>::type;
            using response_t = typename std::decay<typename callback_inspect_t::template arg_t<1>>::type;

            static_assert(std::is_same<error_code, errcode_t>::value, "the first argument of callback should be error_code");
            static_assert(std::is_default_constructible<response_t>::value, "cannot wrap a non-trival-constructible type");

            dsn_message_t msg = dsn_msg_create_request(code, static_cast<int>(timeout.count()), request_hash);
            ::marshall(msg, std::forward<TRequest>(req));
            auto task = create_rpc_response_task_typed(msg, owner, std::forward<TCallback>(callback), reply_hash);
            dsn_rpc_call(server.c_addr(), task->native_handle());
            return task;
        }


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
            ::dsn::rpc_address server,
            dsn_task_code_t code,
            const TRequest& req,
            int hash = 0
            )
        {
            dsn_message_t msg = dsn_msg_create_request(code, 0, hash);
            ::marshall(msg, req);
            dsn_rpc_call_one_way(server.c_addr(), msg);
        }

        template<typename TRequest>
        ::dsn::error_code call_typed_wait(
            /*out*/ ::dsn::rpc_read_stream* response,
            ::dsn::rpc_address server,
            dsn_task_code_t code,
            const TRequest& req,
            int hash = 0,
            std::chrono::milliseconds timeout = std::chrono::milliseconds(0)
            )
        {
            dsn_message_t msg = dsn_msg_create_request(code, static_cast<int>(timeout.count()), hash);
            ::marshall(msg, req);

            auto resp = dsn_rpc_call_wait(server.c_addr(), msg);
            if (resp != nullptr)
            {
                if (response)
                {
                    response->set_read_msg(resp);
                }
                else
                    dsn_msg_release_ref(resp);
                return ::dsn::ERR_OK;
            }
            else
                return ::dsn::ERR_TIMEOUT;
        }
    }

    namespace file
    {
        template<typename TCallback>
        task_ptr create_aio_task(
            dsn_task_code_t callback_code,
            clientlet* svc,
            TCallback&& callback,
            int hash)
        {
            using callback_storage_t = typename std::remove_reference<TCallback>::type;
            auto tsk = new safe_task<callback_storage_t>(std::forward<TCallback>(callback));
            tsk->add_ref(); // released in exec_aio

            dsn_task_t t = dsn_file_create_aio_task_ex(callback_code,
                safe_task<callback_storage_t>::exec_aio,
                safe_task<callback_storage_t>::on_cancel,
                tsk, hash, svc ? svc->tracker() : nullptr
                );

            tsk->set_task_info(t);
            return tsk;
        }

        task_ptr create_aio_task(
            dsn_task_code_t callback_code,
            clientlet* svc,
            empty_callback,
            int hash);

        template<typename TCallback>
        task_ptr read(
            dsn_handle_t fh,
            char* buffer,
            int count,
            uint64_t offset,
            dsn_task_code_t callback_code,
            clientlet* svc,
            TCallback&& callback,
            int hash = 0
            )
        {
            auto tsk = create_aio_task(callback_code, svc, std::forward<TCallback>(callback), hash);
            dsn_file_read(fh, buffer, count, offset, tsk->native_handle());
            return tsk;
        }

        template<typename TCallback>
        task_ptr write(
            dsn_handle_t fh,
            const char* buffer,
            int count,
            uint64_t offset,
            dsn_task_code_t callback_code,
            clientlet* svc,
            TCallback&& callback,
            int hash = 0
            )
        {
            auto tsk = create_aio_task(callback_code, svc, std::forward<TCallback>(callback), hash);
            dsn_file_write(fh, buffer, count, offset, tsk->native_handle());
            return tsk;
        }

        template<typename TCallback>
        task_ptr write_vector(
            dsn_handle_t fh,
            const dsn_file_buffer_t* buffers,
            int buffer_count,
            uint64_t offset,
            dsn_task_code_t callback_code,
            clientlet* svc,
            TCallback&& callback,
            int hash = 0
            )
        {
            auto tsk = create_aio_task(callback_code, svc, std::forward<TCallback>(callback), hash);
            dsn_file_write_vector(fh, buffers, buffer_count, offset, tsk->native_handle());
            return tsk;
        }

        void copy_remote_files_impl(
            ::dsn::rpc_address remote,
            const std::string& source_dir,
            std::vector<std::string>& files,  // empty for all
            const std::string& dest_dir,
            bool overwrite,
            dsn_task_t native_task
            );

        template<typename TCallback>
        task_ptr copy_remote_files(
            ::dsn::rpc_address remote,
            const std::string& source_dir,
            std::vector<std::string>& files,  // empty for all
            const std::string& dest_dir,
            bool overwrite,
            dsn_task_code_t callback_code,
            clientlet* svc,
            TCallback&& callback,
            int hash = 0
            )
        {
            auto tsk = create_aio_task(callback_code, svc, std::forward<TCallback>(callback), hash);
            copy_remote_files_impl(remote, source_dir, files, dest_dir, overwrite, tsk->native_handle());
            return tsk;
        }

        template<typename TCallback>
        task_ptr copy_remote_directory(
            ::dsn::rpc_address remote,
            const std::string& source_dir,
            const std::string& dest_dir,
            bool overwrite,
            dsn_task_code_t callback_code,
            clientlet* svc,
            TCallback&& callback,
            int hash = 0
            )
        {
            std::vector<std::string> files;
            return copy_remote_files(
                remote, source_dir, files, dest_dir, overwrite,
                callback_code, svc, std::forward<TCallback>(callback), hash
                );
        }
    }

    // ------------- inline implementation ----------------

} // end namespace
