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

#pragma once

#include <dsn/cpp/task_helper.h>
#include <dsn/utility/function_traits.h>

namespace dsn {
/*
clientlet is the base class for RPC service and client
there can be multiple clientlet in the system
*/
class clientlet
{
public:
    clientlet(int task_bucket_count = 1);
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
    int _access_thread_id;
    bool _access_thread_id_inited;
    dsn_task_tracker_t _tracker;
};

// callback function concepts
struct empty_callback_t
{
    void operator()() const {}
};
constexpr empty_callback_t empty_callback{};

// callback(error_code, dsn_message_t request, dsn_message_t response)
template <typename TFunction, class Enable = void>
struct is_raw_rpc_callback
{
    constexpr static bool const value = false;
};
template <typename TFunction>
struct is_raw_rpc_callback<TFunction,
                           typename std::enable_if<function_traits<TFunction>::arity == 3>::type>
{
    using inspect_t = function_traits<TFunction>;
    constexpr static bool const value =
        std::is_same<typename inspect_t::template arg_t<0>, dsn::error_code>::value &&
        std::is_same<typename inspect_t::template arg_t<1>, dsn_message_t>::value &&
        std::is_same<typename inspect_t::template arg_t<2>, dsn_message_t>::value;
};

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
        std::is_same<typename inspect_t::template arg_t<0>, dsn::error_code>::value &&
        std::is_default_constructible<
            typename std::decay<typename inspect_t::template arg_t<1>>::type>::value;
    using response_t = typename std::decay<typename inspect_t::template arg_t<1>>::type;
};

// callback(error_code, int io_size)
template <typename TFunction, class Enable = void>
struct is_aio_callback
{
    constexpr static bool const value = false;
};
template <typename TFunction>
struct is_aio_callback<TFunction,
                       typename std::enable_if<function_traits<TFunction>::arity == 2>::type>
{
    using inspect_t = function_traits<TFunction>;
    constexpr static bool const value =
        std::is_same<typename inspect_t::template arg_t<0>, dsn::error_code>::value &&
        std::is_convertible<typename inspect_t::template arg_t<1>, uint64_t>::value;
};

/*!
@addtogroup tasking
@{
*/
namespace tasking {
template <typename TCallback>
task_ptr create_task(dsn::task_code evt, clientlet *svc, TCallback &&callback, int hash = 0)
{
    using callback_storage_t = typename std::remove_reference<TCallback>::type;
    auto tsk = new transient_safe_task<callback_storage_t>(std::forward<TCallback>(callback));
    tsk->add_ref(); // released in exec callback
    auto native_tsk = dsn_task_create_ex(evt,
                                         transient_safe_task<callback_storage_t>::exec,
                                         transient_safe_task<callback_storage_t>::on_cancel,
                                         tsk,
                                         hash,
                                         svc ? svc->tracker() : nullptr);
    tsk->set_task_info(native_tsk);
    return tsk;
}

template <typename TCallback>
task_ptr create_timer_task(dsn::task_code evt,
                           clientlet *svc,
                           TCallback &&callback,
                           std::chrono::milliseconds timer_interval,
                           int hash = 0)
{
    using callback_storage_t = typename std::remove_reference<TCallback>::type;
    auto tsk = new timer_safe_task<callback_storage_t>(std::forward<TCallback>(callback));
    tsk->add_ref(); // released in exec callback
    auto native_tsk = dsn_task_create_timer_ex(evt,
                                               timer_safe_task<callback_storage_t>::exec_timer,
                                               timer_safe_task<callback_storage_t>::on_cancel,
                                               tsk,
                                               hash,
                                               static_cast<int>(timer_interval.count()),
                                               svc ? svc->tracker() : nullptr);
    tsk->set_task_info(native_tsk);
    return tsk;
}

template <typename TCallback>
task_ptr enqueue(dsn::task_code evt,
                 clientlet *svc,
                 TCallback &&callback,
                 int hash = 0,
                 std::chrono::milliseconds delay = std::chrono::milliseconds(0))
{
    auto tsk = create_task(evt, svc, std::forward<TCallback>(callback), hash);
    tsk->enqueue(delay);
    return tsk;
}

template <typename TCallback>
task_ptr enqueue_timer(dsn::task_code evt,
                       clientlet *svc,
                       TCallback &&callback,
                       std::chrono::milliseconds timer_interval,
                       int hash = 0,
                       std::chrono::milliseconds delay = std::chrono::milliseconds(0))
{
    auto tsk = create_timer_task(evt, svc, std::forward<TCallback>(callback), timer_interval, hash);
    tsk->enqueue(delay);
    return tsk;
}

template <typename THandler>
inline safe_late_task<THandler> *
create_late_task(dsn::task_code evt, THandler callback, int hash = 0, clientlet *svc = nullptr)
{
    auto tsk = new safe_late_task<THandler>(callback);
    tsk->add_ref(); // released in exec callback
    auto t = dsn_task_create_ex(evt,
                                safe_late_task<THandler>::exec,
                                safe_late_task<THandler>::on_cancel,
                                tsk,
                                hash,
                                svc ? svc->tracker() : nullptr);

    tsk->set_task_info(t);
    return tsk;
}
}
/*@}*/

/*!
@addtogroup rpc-client
@{
*/
namespace rpc {
task_ptr create_rpc_response_task(dsn_message_t request,
                                  clientlet *svc,
                                  empty_callback_t,
                                  int reply_thread_hash = 0);

template <typename TCallback>
typename std::enable_if<is_raw_rpc_callback<TCallback>::value, task_ptr>::type
create_rpc_response_task(dsn_message_t request,
                         clientlet *svc,
                         TCallback &&callback,
                         int reply_thread_hash = 0)
{
    using callback_storage_t = typename std::remove_reference<TCallback>::type;
    auto tsk = new transient_safe_task<callback_storage_t>(std::forward<TCallback>(callback));
    tsk->add_ref(); // released in exec_rpc_response

    auto t =
        dsn_rpc_create_response_task_ex(request,
                                        transient_safe_task<callback_storage_t>::exec_rpc_response,
                                        transient_safe_task<callback_storage_t>::on_cancel,
                                        tsk,
                                        reply_thread_hash,
                                        svc ? svc->tracker() : nullptr);
    tsk->set_task_info(t);
    return tsk;
}

template <typename TCallback>
typename std::enable_if<is_typed_rpc_callback<TCallback>::value, task_ptr>::type
create_rpc_response_task(dsn_message_t request,
                         clientlet *svc,
                         TCallback &&callback,
                         int reply_thread_hash = 0)
{
    return create_rpc_response_task(
        request,
        svc,
        [cb_fwd = std::forward<TCallback>(callback)](
            error_code err, dsn_message_t req, dsn_message_t resp) mutable {
            typename is_typed_rpc_callback<TCallback>::response_t response = {};
            if (err == ERR_OK) {
                ::dsn::unmarshall(resp, response);
            }
            cb_fwd(err, std::move(response));
        },
        reply_thread_hash);
}

template <typename TCallback>
task_ptr call(::dsn::rpc_address server,
              dsn_message_t request,
              clientlet *svc,
              TCallback &&callback,
              int reply_thread_hash = 0)
{
    task_ptr t = create_rpc_response_task(
        request, svc, std::forward<TCallback>(callback), reply_thread_hash);
    dsn_rpc_call(server.c_addr(), t->native_handle());
    return t;
}

template <typename TRequest, typename TCallback>
task_ptr call(::dsn::rpc_address server,
              dsn::task_code code,
              TRequest &&req,
              clientlet *owner,
              TCallback &&callback,
              std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
              int thread_hash = 0, ///< if thread_hash == 0 && partition_hash != 0, thread_hash is
                                   /// computed from partition_hash
              uint64_t partition_hash = 0,
              int reply_thread_hash = 0)
{
    dsn_message_t msg = dsn_msg_create_request(
        code, static_cast<int>(timeout.count()), thread_hash, partition_hash);
    ::dsn::marshall(msg, std::forward<TRequest>(req));
    return call(server, msg, owner, std::forward<TCallback>(callback), reply_thread_hash);
}

struct rpc_message_helper
{
public:
    explicit rpc_message_helper(dsn_message_t request) : request(request) {}
    template <typename TCallback>
    task_ptr call(::dsn::rpc_address server,
                  clientlet *owner,
                  TCallback &&callback,
                  int reply_thread_hash = 0)
    {
        return ::dsn::rpc::call(
            server, request, owner, std::forward<TCallback>(callback), reply_thread_hash);
    }

private:
    dsn_message_t request;
};

template <typename TRequest>
rpc_message_helper create_message(dsn::task_code code,
                                  TRequest &&req,
                                  std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                                  int thread_hash = 0, ///< if thread_hash == 0 && partition_hash !=
                                                       /// 0, thread_hash is computed from
                                  /// partition_hash
                                  uint64_t partition_hash = 0)
{
    dsn_message_t msg = dsn_msg_create_request(
        code, static_cast<int>(timeout.count()), thread_hash, partition_hash);
    ::dsn::marshall(msg, std::forward<TRequest>(req));
    return rpc_message_helper(msg);
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
template <typename TRequest>
void call_one_way_typed(::dsn::rpc_address server,
                        dsn::task_code code,
                        const TRequest &req,
                        int thread_hash = 0, ///< if thread_hash == 0 && partition_hash != 0,
                                             /// thread_hash is computed from partition_hash
                        uint64_t partition_hash = 0)
{
    dsn_message_t msg = dsn_msg_create_request(code, 0, thread_hash, partition_hash);
    ::dsn::marshall(msg, req);
    dsn_rpc_call_one_way(server.c_addr(), msg);
}

template <typename TResponse>
std::pair<::dsn::error_code, TResponse> wait_and_unwrap(safe_task_handle *task)
{
    task->wait();
    std::pair<::dsn::error_code, TResponse> result;
    result.first = task->error();
    if (task->error() == ::dsn::ERR_OK) {
        ::dsn::unmarshall(task->response(), result.second);
    }
    return result;
}

template <typename TResponse, typename TRequest>
std::pair<::dsn::error_code, TResponse>
call_wait(::dsn::rpc_address server,
          dsn::task_code code,
          TRequest &&req,
          std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
          int thread_hash = 0,
          uint64_t partition_hash = 0)
{
    return wait_and_unwrap<TResponse>(call(server,
                                           code,
                                           std::forward<TRequest>(req),
                                           nullptr,
                                           empty_callback,
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
task_ptr create_aio_task(dsn::task_code callback_code, clientlet *svc, empty_callback_t, int hash);

template <typename TCallback>
task_ptr
create_aio_task(dsn::task_code callback_code, clientlet *svc, TCallback &&callback, int hash)
{
    static_assert(is_aio_callback<TCallback>::value, "invalid aio callback");
    using callback_storage_t = typename std::remove_reference<TCallback>::type;
    auto tsk = new transient_safe_task<callback_storage_t>(std::forward<TCallback>(callback));
    tsk->add_ref(); // released in exec_aio

    dsn_task_t t = dsn_file_create_aio_task_ex(callback_code,
                                               transient_safe_task<callback_storage_t>::exec_aio,
                                               transient_safe_task<callback_storage_t>::on_cancel,
                                               tsk,
                                               hash,
                                               svc ? svc->tracker() : nullptr);

    tsk->set_task_info(t);
    return tsk;
}

template <typename TCallback>
task_ptr read(dsn_handle_t fh,
              char *buffer,
              int count,
              uint64_t offset,
              dsn::task_code callback_code,
              clientlet *svc,
              TCallback &&callback,
              int hash = 0)
{
    auto tsk = create_aio_task(callback_code, svc, std::forward<TCallback>(callback), hash);
    dsn_file_read(fh, buffer, count, offset, tsk->native_handle());
    return tsk;
}

template <typename TCallback>
task_ptr write(dsn_handle_t fh,
               const char *buffer,
               int count,
               uint64_t offset,
               dsn::task_code callback_code,
               clientlet *svc,
               TCallback &&callback,
               int hash = 0)
{
    auto tsk = create_aio_task(callback_code, svc, std::forward<TCallback>(callback), hash);
    dsn_file_write(fh, buffer, count, offset, tsk->native_handle());
    return tsk;
}

template <typename TCallback>
task_ptr write_vector(dsn_handle_t fh,
                      const dsn_file_buffer_t *buffers,
                      int buffer_count,
                      uint64_t offset,
                      dsn::task_code callback_code,
                      clientlet *svc,
                      TCallback &&callback,
                      int hash = 0)
{
    auto tsk = create_aio_task(callback_code, svc, std::forward<TCallback>(callback), hash);
    dsn_file_write_vector(fh, buffers, buffer_count, offset, tsk->native_handle());
    return tsk;
}

void copy_remote_files_impl(::dsn::rpc_address remote,
                            const std::string &source_dir,
                            const std::vector<std::string> &files, // empty for all
                            const std::string &dest_dir,
                            bool overwrite,
                            bool high_priority,
                            dsn_task_t native_task);

template <typename TCallback>
task_ptr copy_remote_files(::dsn::rpc_address remote,
                           const std::string &source_dir,
                           const std::vector<std::string> &files, // empty for all
                           const std::string &dest_dir,
                           bool overwrite,
                           bool high_priority,
                           dsn::task_code callback_code,
                           clientlet *svc,
                           TCallback &&callback,
                           int hash = 0)
{
    auto tsk = create_aio_task(callback_code, svc, std::forward<TCallback>(callback), hash);
    copy_remote_files_impl(
        remote, source_dir, files, dest_dir, overwrite, high_priority, tsk->native_handle());
    return tsk;
}

template <typename TCallback>
task_ptr copy_remote_directory(::dsn::rpc_address remote,
                               const std::string &source_dir,
                               const std::string &dest_dir,
                               bool overwrite,
                               bool high_priority,
                               dsn::task_code callback_code,
                               clientlet *svc,
                               TCallback &&callback,
                               int hash = 0)
{
    return copy_remote_files(remote,
                             source_dir,
                             {},
                             dest_dir,
                             overwrite,
                             high_priority,
                             callback_code,
                             svc,
                             std::forward<TCallback>(callback),
                             hash);
}
}
/*@}*/
// ------------- inline implementation ----------------

} // end namespace
