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

# include <dsn/cpp/servicelet.h>

namespace dsn 
{
    namespace tasking
    {
        task_ptr enqueue(
            dsn_task_code_t evt,
            servicelet *context,
            task_handler callback,
            int hash = 0,
            int delay_milliseconds = 0,
            int timer_interval_milliseconds = 0
            );

        template<typename T> // where T : public virtual servicelet
        inline task_ptr enqueue(
            dsn_task_code_t evt,
            T* owner,
            void (T::*callback)(),
            //TParam param,
            int hash = 0,
            int delay_milliseconds = 0,
            int timer_interval_milliseconds = 0
            )
        {
            /*typedef safe_task_context< T, void(T::*)(TParam), TParam> local_context;

            auto tc = new local_context(owner, callback);
            dsn_task_handler_t cb = [](void* param)
            {
                auto tc2 = (local_context*)param;
                ((tc2->_this)->*(tc2->_handler)(tc2->_param);
            };*/

            task_handler h = std::bind(callback, owner);
            return enqueue(
                evt,
                owner,
                h,
                hash,
                delay_milliseconds,
                timer_interval_milliseconds
                );
        }
    }

    namespace rpc
    {
        //
        // for TRequest/TResponse, we assume that the following routines are defined:
        //    marshall(binary_writer& writer, const T& val); 
        //    unmarshall(binary_reader& reader, __out_param T& val);
        // either in the namespace of ::dsn::utils or T
        // developers may write these helper functions by their own, or use tools
        // such as protocol-buffer, thrift, or bond to generate these functions automatically
        // for their TRequest and TResponse
        //

        // no callback
        template<typename TRequest>
        void call_one_way_typed(
            const dsn_address_t& server,
            dsn_task_code_t code,
            const TRequest& req,
            int hash = 0
            );

        template<typename TRequest>
        ::dsn::error_code call_typed_wait(
            /*out*/ ::dsn::message_ptr* response,
            const dsn_address_t& server,
            dsn_task_code_t code,
            const TRequest& req,
            int hash = 0,
            int timeout_milliseconds = 0
            );

        // callback type 1:
        //  void (T::*callback)(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)
        template<typename T, typename TRequest, typename TResponse>
        task_ptr call_typed(
            const dsn_address_t& server,
            dsn_task_code_t code,
            std::shared_ptr<TRequest>& req,
            T* owner,
            void (T::*callback)(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&),
            int request_hash = 0,
            int timeout_milliseconds = 0,
            int reply_hash = 0
            );

        // callback type 2:
        //  std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)>
        template<typename TRequest, typename TResponse>
        task_ptr call_typed(
            const dsn_address_t& server,
            dsn_task_code_t code,
            std::shared_ptr<TRequest>& req,
            servicelet* owner,
            std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)> callback,
            int request_hash = 0,
            int timeout_milliseconds = 0,
            int reply_hash = 0
            );

        // callback type 5
        //   void (T::*)(error_code, const TResponse&, void*);
        template<typename T, typename TRequest, typename TResponse>
        task_ptr call_typed(
            const dsn_address_t& server,
            dsn_task_code_t code,
            const TRequest& req,
            T* owner,
            void(T::*callback)(error_code, const TResponse&, void*),
            void* context,
            int request_hash = 0,
            int timeout_milliseconds = 0,
            int reply_hash = 0
            );

        // callback type 3:
        //  std::function<void(error_code, const TResponse&, void*)>
        template<typename TRequest, typename TResponse>
        task_ptr call_typed(
            const dsn_address_t& server,
            dsn_task_code_t code,
            const TRequest& req,
            servicelet* owner,
            std::function<void(error_code, const TResponse&, void*)> callback,
            void* context,
            int request_hash = 0,
            int timeout_milliseconds = 0,
            int reply_hash = 0
            );

        // callback type 4:
        //  std::function<void(error_code, dsn_message_t, dsn_message_t)>
        task_ptr call(
            const dsn_address_t& server,
            dsn_message_t request,
            servicelet* owner,
            rpc_reply_handler callback,
            int reply_hash = 0
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
            servicelet* owner,
            aio_handler callback,
            int hash = 0
            );

        task_ptr write(
            dsn_handle_t hFile,
            const char* buffer,
            int count,
            uint64_t offset,
            dsn_task_code_t callback_code,
            servicelet* owner,
            aio_handler callback,
            int hash = 0
            );

        template<typename T>
        inline task_ptr read(
            dsn_handle_t hFile,
            char* buffer,
            int count,
            uint64_t offset,
            dsn_task_code_t callback_code,
            T* owner,
            void(T::*callback)(error_code, uint32_t),
            int hash = 0
            )
        {
            aio_handler h = std::bind(callback, owner, std::placeholders::_1, std::placeholders::_2);
            return read(hFile, buffer, count, offset, callback_code, owner, h, hash);
        }

        template<typename T>
        inline task_ptr write(
            dsn_handle_t hFile,
            const char* buffer,
            int count,
            uint64_t offset,
            dsn_task_code_t callback_code,
            T* owner,
            void(T::*callback)(error_code, uint32_t),
            int hash = 0
            )
        {
            aio_handler h = std::bind(callback, owner, std::placeholders::_1, std::placeholders::_2);
            return write(hFile, buffer, count, offset, callback_code, owner, h, hash);
        }

        task_ptr copy_remote_files(
            const dsn_address_t& remote,
            const std::string& source_dir,
            std::vector<std::string>& files,  // empty for all
            const std::string& dest_dir,
            bool overwrite,
            dsn_task_code_t callback_code,
            servicelet* owner,
            aio_handler callback,
            int hash = 0
            );

        inline task_ptr copy_remote_directory(
            const dsn_address_t& remote,
            const std::string& source_dir,
            const std::string& dest_dir,
            bool overwrite,
            dsn_task_code_t callback_code,
            servicelet* owner,
            aio_handler callback,
            int hash = 0
            )
        {
            std::vector<std::string> files;
            return copy_remote_files(
                remote, source_dir, files, dest_dir, overwrite,
                callback_code, owner, callback, hash
                );
        }
    }
} // end namespace

# include <dsn/cpp/service.api.oo.impl.h>



