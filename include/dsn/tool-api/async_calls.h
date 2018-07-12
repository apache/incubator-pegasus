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

#include <dsn/service_api_c.h>
#include <dsn/utility/function_traits.h>
#include <dsn/tool-api/task.h>
#include <dsn/tool-api/task_tracker.h>
#include <dsn/cpp/serialization.h>

namespace dsn {

inline void empty_rpc_handler(error_code, dsn_message_t, dsn_message_t) {}

// callback(error_code, TResponse&& response)
template <typename TFunction, class Enable = void>
struct is_typed_rpc_callback
{
    constexpr static bool const value = false;
};
template <typename TFunction>
struct is_typed_rpc_callback<TFunction,
                             typename std::enable_if<function_traits<TFunction>::arity == 2>::type>
{
    // todo: check if response_t is marshallable
    using inspect_t = function_traits<TFunction>;
    constexpr static bool const value =
        std::is_same<typename inspect_t::template arg_t<0>, error_code>::value &&
        std::is_default_constructible<
            typename std::decay<typename inspect_t::template arg_t<1>>::type>::value;
    using response_t = typename std::decay<typename inspect_t::template arg_t<1>>::type;
};

/*!
@addtogroup tasking
@{
*/

namespace tasking {
inline task_ptr
create_task(task_code code, task_tracker *tracker, task_handler &&callback, int hash = 0)
{
    task_ptr t(new raw_task(code, std::move(callback), hash, nullptr));
    t->set_tracker(tracker);
    t->spec().on_task_create.execute(task::get_current_task(), t);
    return t;
}

inline task_ptr create_timer_task(task_code code,
                                  task_tracker *tracker,
                                  task_handler &&callback,
                                  std::chrono::milliseconds interval,
                                  int hash = 0)
{
    task_ptr t(new timer_task(code, std::move(callback), interval.count(), hash, nullptr));
    t->set_tracker(tracker);
    t->spec().on_task_create.execute(task::get_current_task(), t);
    return t;
}

inline task_ptr enqueue(task_code code,
                        task_tracker *tracker,
                        task_handler &&callback,
                        int hash = 0,
                        std::chrono::milliseconds delay = std::chrono::milliseconds(0))
{
    auto tsk = create_task(code, tracker, std::move(callback), hash);
    tsk->set_delay(static_cast<int>(delay.count()));
    tsk->enqueue();
    return tsk;
}

inline task_ptr enqueue_timer(task_code evt,
                              task_tracker *tracker,
                              task_handler &&callback,
                              std::chrono::milliseconds timer_interval,
                              int hash = 0,
                              std::chrono::milliseconds delay = std::chrono::milliseconds(0))
{
    auto tsk = create_timer_task(evt, tracker, std::move(callback), timer_interval, hash);
    tsk->set_delay(static_cast<int>(delay.count()));
    tsk->enqueue();
    return tsk;
}
}
/*@}*/

/*!
@addtogroup rpc-client
@{
*/
namespace rpc {

inline rpc_response_task_ptr create_rpc_response_task(dsn_message_t req,
                                                      task_tracker *tracker,
                                                      rpc_response_handler &&callback,
                                                      int reply_thread_hash = 0)
{
    rpc_response_task_ptr t(
        new rpc_response_task((message_ex *)req, std::move(callback), reply_thread_hash, nullptr));
    t->set_tracker(tracker);
    t->spec().on_task_create.execute(task::get_current_task(), t);
    return t;
}

template <typename TCallback>
typename std::enable_if<is_typed_rpc_callback<TCallback>::value, rpc_response_task_ptr>::type
create_rpc_response_task(dsn_message_t req,
                         task_tracker *tracker,
                         TCallback &&callback,
                         int reply_thread_hash = 0)
{
    return create_rpc_response_task(
        req,
        tracker,
        [cb_fwd =
             std::move(callback)](error_code err, dsn_message_t req, dsn_message_t resp) mutable {
            typename is_typed_rpc_callback<TCallback>::response_t response = {};
            if (err == ERR_OK) {
                unmarshall(resp, response);
            }
            cb_fwd(err, std::move(response));
        },
        reply_thread_hash);
}

template <typename TCallback>
rpc_response_task_ptr call(rpc_address server,
                           dsn_message_t request,
                           task_tracker *tracker,
                           TCallback &&callback,
                           int reply_thread_hash = 0)
{
    rpc_response_task_ptr t = create_rpc_response_task(
        request, tracker, std::forward<TCallback>(callback), reply_thread_hash);
    dsn_rpc_call(server, t.get());
    return t;
}

//
// for TRequest/TResponse, we assume that the following routines are defined:
//    marshall(binary_writer& writer, const T& val);
//    unmarshall(binary_reader& reader, /*out*/ T& val);
// either in the namespace of utils or T
// developers may write these helper functions by their own, or use tools
// such as protocol-buffer, thrift, or bond to generate these functions automatically
// for their TRequest and TResponse
//
template <typename TRequest, typename TCallback>
rpc_response_task_ptr
call(rpc_address server,
     task_code code,
     TRequest &&req,
     task_tracker *tracker,
     TCallback &&callback,
     std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
     int thread_hash = 0, ///< if thread_hash == 0 && partition_hash != 0, thread_hash is
                          /// computed from partition_hash
     uint64_t partition_hash = 0,
     int reply_thread_hash = 0)
{
    dsn_message_t msg = dsn_msg_create_request(
        code, static_cast<int>(timeout.count()), thread_hash, partition_hash);
    marshall(msg, std::forward<TRequest>(req));
    return call(server, msg, tracker, std::forward<TCallback>(callback), reply_thread_hash);
}

// no callback
template <typename TRequest>
void call_one_way_typed(rpc_address server,
                        task_code code,
                        const TRequest &req,
                        int thread_hash = 0, ///< if thread_hash == 0 && partition_hash != 0,
                                             /// thread_hash is computed from partition_hash
                        uint64_t partition_hash = 0)
{
    dsn_message_t msg = dsn_msg_create_request(code, 0, thread_hash, partition_hash);
    marshall(msg, req);
    dsn_rpc_call_one_way(server, msg);
}

template <typename TResponse>
std::pair<error_code, TResponse> wait_and_unwrap(const rpc_response_task_ptr &tsk)
{
    tsk->wait();
    std::pair<error_code, TResponse> result;
    result.first = tsk->error();
    if (tsk->error() == ERR_OK) {
        unmarshall(tsk->get_response(), result.second);
    }
    return result;
}

template <typename TResponse, typename TRequest>
std::pair<error_code, TResponse>
call_wait(rpc_address server,
          task_code code,
          TRequest &&req,
          std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
          int thread_hash = 0,
          uint64_t partition_hash = 0)
{
    return wait_and_unwrap<TResponse>(call(server,
                                           code,
                                           std::forward<TRequest>(req),
                                           nullptr,
                                           empty_rpc_handler,
                                           timeout,
                                           thread_hash,
                                           partition_hash));
}
}
/*@}*/

/*!
@addtogroup file
@{
*/
namespace file {

inline aio_task_ptr
create_aio_task(task_code code, task_tracker *tracker, aio_handler &&callback, int hash = 0)
{
    aio_task_ptr t(new aio_task(code, std::move(callback), hash));
    t->set_tracker((task_tracker *)tracker);
    t->spec().on_task_create.execute(task::get_current_task(), t);
    return t;
}

inline aio_task_ptr read(dsn_handle_t fh,
                         char *buffer,
                         int count,
                         uint64_t offset,
                         task_code callback_code,
                         task_tracker *tracker,
                         aio_handler &&callback,
                         int hash = 0)
{
    auto tsk = create_aio_task(callback_code, tracker, std::move(callback), hash);
    dsn_file_read(fh, buffer, count, offset, tsk);
    return tsk;
}

inline aio_task_ptr write(dsn_handle_t fh,
                          const char *buffer,
                          int count,
                          uint64_t offset,
                          task_code callback_code,
                          task_tracker *tracker,
                          aio_handler &&callback,
                          int hash = 0)
{
    auto tsk = create_aio_task(callback_code, tracker, std::move(callback), hash);
    dsn_file_write(fh, buffer, count, offset, tsk);
    return tsk;
}

inline aio_task_ptr write_vector(dsn_handle_t fh,
                                 const dsn_file_buffer_t *buffers,
                                 int buffer_count,
                                 uint64_t offset,
                                 task_code callback_code,
                                 task_tracker *tracker,
                                 aio_handler &&callback,
                                 int hash = 0)
{
    auto tsk = create_aio_task(callback_code, tracker, std::move(callback), hash);
    dsn_file_write_vector(fh, buffers, buffer_count, offset, tsk.get());
    return tsk;
}

void copy_remote_files_impl(rpc_address remote,
                            const std::string &source_dir,
                            const std::vector<std::string> &files, // empty for all
                            const std::string &dest_dir,
                            bool overwrite,
                            bool high_priority,
                            aio_task *tsk);

inline aio_task_ptr copy_remote_files(rpc_address remote,
                                      const std::string &source_dir,
                                      const std::vector<std::string> &files, // empty for all
                                      const std::string &dest_dir,
                                      bool overwrite,
                                      bool high_priority,
                                      task_code callback_code,
                                      task_tracker *tracker,
                                      aio_handler &&callback,
                                      int hash = 0)
{
    auto tsk = create_aio_task(callback_code, tracker, std::move(callback), hash);
    copy_remote_files_impl(
        remote, source_dir, files, dest_dir, overwrite, high_priority, tsk.get());
    return tsk;
}

inline aio_task_ptr copy_remote_directory(rpc_address remote,
                                          const std::string &source_dir,
                                          const std::string &dest_dir,
                                          bool overwrite,
                                          bool high_priority,
                                          task_code callback_code,
                                          task_tracker *tracker,
                                          aio_handler &&callback,
                                          int hash = 0)
{
    return copy_remote_files(remote,
                             source_dir,
                             {},
                             dest_dir,
                             overwrite,
                             high_priority,
                             callback_code,
                             tracker,
                             std::move(callback),
                             hash);
}
}
/*@}*/
// ------------- inline implementation ----------------

} // end namespace
