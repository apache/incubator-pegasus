
# include <dsn/service_api_c.h>
# include <dsn/internal/enum_helper.h>
# include <dsn/internal/error_code.h>
# include <dsn/internal/task_code.h>
# include <dsn/internal/zlock_provider.h>
# include <dsn/internal/nfs.h>
# include <dsn/internal/env_provider.h>
# include <dsn/internal/factory_store.h>
# include <dsn/internal/task.h>

# include "service_engine.h"
# include "rpc_engine.h"
# include "disk_engine.h"


//------------------------------------------------------------------------------
//
// common types
//
//------------------------------------------------------------------------------

ENUM_BEGIN(dsn_task_type_t, TASK_TYPE_INVALID)
    ENUM_REG(TASK_TYPE_RPC_REQUEST)
    ENUM_REG(TASK_TYPE_RPC_RESPONSE)
    ENUM_REG(TASK_TYPE_COMPUTE)
    ENUM_REG(TASK_TYPE_AIO)
    ENUM_REG(TASK_TYPE_CONTINUATION)
ENUM_END(dsn_task_type_t)


DSN_API dsn_error_t dsn_error_register(const char* name)
{
    return static_cast<dsn_error_t>(::dsn::utils::customized_id_mgr<dsn_error_t>::instance().register_id(name));
}

DSN_API const char* dsn_error_to_string(dsn_error_t err)
{
    return ::dsn::utils::customized_id_mgr<dsn_error_t>::instance().get_name(static_cast<int>(err));
}

DSN_API dsn_threadpool_code_t dsn_threadpool_register(const char* name)
{
    return static_cast<dsn_threadpool_code_t>(::dsn::utils::customized_id_mgr<::dsn::threadpool_code>::instance().register_id(name));
}

DSN_API const char* dsn_threadpool_to_string(dsn_threadpool_code_t pool_code)
{
    return ::dsn::utils::customized_id_mgr<::dsn::threadpool_code>::instance().get_name(static_cast<int>(pool_code));
}

DSN_API dsn_task_code_t dsn_task_code_register(const char* name, dsn_task_type_t type, dsn_threadpool_code_t pool)
{
    return static_cast<dsn_task_code_t>(::dsn::utils::customized_id_mgr<::dsn::task_code>::instance().register_id(name));
}

DSN_API const char* dsn_task_code_to_string(dsn_task_code_t code)
{
    return ::dsn::utils::customized_id_mgr<::dsn::task_code>::instance().get_name(static_cast<int>(code));
}

//------------------------------------------------------------------------------
//
// tasking - asynchronous tasks and timers tasks executed in target thread pools
// (configured in config files)
// [task.RPC_PREPARE
// // TODO: what can be configured for a task
//
// [threadpool.THREAD_POOL_REPLICATION]
// // TODO: what can be configured for a thread pool
//
//------------------------------------------------------------------------------
DSN_API dsn_task_t dsn_task_create(dsn_task_code_t code, dsn_task_callback_t cb, void* param, int hash)
{
    auto t = new ::dsn::task_c(code, cb, param, hash);
    t->add_ref();
    return t;
}

DSN_API dsn_task_t dsn_task_create_timer(dsn_task_code_t code, dsn_task_callback_t cb, void* param, int hash, int interval_milliseconds)
{
    auto t = new ::dsn::timer_task_c(code, cb, param, interval_milliseconds, hash);
    t->add_ref();
    return t;
}

DSN_API void dsn_task_call(dsn_task_t task, int delay_milliseconds)
{
    auto t = ((::dsn::task*)(task));
    dassert(t->spec().type == TASK_TYPE_COMPUTE, "must be common or timer task");

    t->set_delay(delay_milliseconds);
    t->enqueue();
}

DSN_API void dsn_task_close(dsn_task_t task)
{
    ((::dsn::task*)(task))->release_ref();
}

DSN_API bool dsn_task_cancel(dsn_task_t task, bool wait_until_finished)
{
    return ((::dsn::task*)(task))->cancel(wait_until_finished);
}

DSN_API bool dsn_task_cancel2(dsn_task_t task, bool wait_until_finished, bool* finished)
{
    return ((::dsn::task*)(task))->cancel(wait_until_finished, finished);
}

DSN_API bool dsn_task_wait(dsn_task_t task)
{
    return ((::dsn::task*)(task))->wait();
}

DSN_API bool dsn_task_wait_timeout(dsn_task_t task, int timeout_milliseconds)
{
    return ((::dsn::task*)(task))->wait(timeout_milliseconds);
}

DSN_API dsn_error_t dsn_task_error(dsn_task_t task)
{
    return ((::dsn::task*)(task))->error().get();
}

//------------------------------------------------------------------------------
//
// synchronization - concurrent access and coordination among threads
//
//------------------------------------------------------------------------------
DSN_API dsn_handle_t dsn_exlock_create()
{
    ::dsn::lock_provider* last = ::dsn::utils::factory_store<::dsn::lock_provider>::create(
            ::dsn::service_engine::instance().spec().lock_factory_name.c_str(), PROVIDER_TYPE_MAIN, nullptr);

    // TODO: perf opt by saving the func ptrs somewhere
    for (auto& s : ::dsn::service_engine::instance().spec().lock_aspects)
    {
        last = ::dsn::utils::factory_store<::dsn::lock_provider>::create(s.c_str(), PROVIDER_TYPE_ASPECT, last);
    }
    return (dsn_handle_t)(last);
}

DSN_API void dsn_exlock_destroy(dsn_handle_t l)
{
    delete (::dsn::lock_provider*)(l);
}

DSN_API void dsn_exlock_lock(dsn_handle_t l)
{
    ((::dsn::lock_provider*)(l))->lock();
}

DSN_API bool dsn_exlock_try_lock(dsn_handle_t l)
{
    return ((::dsn::lock_provider*)(l))->try_lock();
}

DSN_API void dsn_exlock_unlock(dsn_handle_t l)
{
    ((::dsn::lock_provider*)(l))->unlock();
}

// non-recursive rwlock
DSN_API dsn_handle_t dsn_rwlock_nr_create()
{
    ::dsn::rwlock_nr_provider* last = ::dsn::utils::factory_store<::dsn::rwlock_nr_provider>::create(
        ::dsn::service_engine::instance().spec().rwlock_nr_factory_name.c_str(), PROVIDER_TYPE_MAIN, nullptr);

    // TODO: perf opt by saving the func ptrs somewhere
    for (auto& s : ::dsn::service_engine::instance().spec().rwlock_nr_aspects)
    {
        last = ::dsn::utils::factory_store<::dsn::rwlock_nr_provider>::create(s.c_str(), PROVIDER_TYPE_ASPECT, last);
    }
    return (dsn_handle_t)(last);
}

DSN_API void dsn_rwlock_nr_destroy(dsn_handle_t l)
{
    delete (::dsn::rwlock_nr_provider*)(l);
}

DSN_API void dsn_rwlock_nr_lock_read(dsn_handle_t l)
{
    ((::dsn::rwlock_nr_provider*)(l))->lock_read();
}

DSN_API void dsn_rwlock_nr_unlock_read(dsn_handle_t l)
{
    ((::dsn::rwlock_nr_provider*)(l))->unlock_read();
}

DSN_API void dsn_rwlock_nr_lock_write(dsn_handle_t l)
{
    ((::dsn::rwlock_nr_provider*)(l))->lock_write();
}

DSN_API void dsn_rwlock_nr_unlock_write(dsn_handle_t l)
{
    ((::dsn::rwlock_nr_provider*)(l))->unlock_write();
}


DSN_API dsn_handle_t dsn_semaphore_create(int initial_count)
{
    ::dsn::semaphore_provider* last = ::dsn::utils::factory_store<::dsn::semaphore_provider>::create(
        ::dsn::service_engine::instance().spec().semaphore_factory_name.c_str(), PROVIDER_TYPE_MAIN, initial_count, nullptr);

    // TODO: perf opt by saving the func ptrs somewhere
    for (auto& s : ::dsn::service_engine::instance().spec().semaphore_aspects)
    {
        last = ::dsn::utils::factory_store<::dsn::semaphore_provider>::create(s.c_str(), PROVIDER_TYPE_ASPECT, initial_count, last);
    }
    return (dsn_handle_t)(last);
}

DSN_API void dsn_semaphore_destroy(dsn_handle_t s)
{
    delete (::dsn::semaphore_provider*)(s);
}

DSN_API void dsn_semaphore_signal(dsn_handle_t s, int count)
{
    ((::dsn::semaphore_provider*)(s))->signal(count);
}

DSN_API void dsn_semaphore_wait(dsn_handle_t s)
{
    ((::dsn::semaphore_provider*)(s))->wait();
}

DSN_API bool dsn_semaphore_wait_timeout(dsn_handle_t s, int timeout_milliseconds)
{
    return ((::dsn::semaphore_provider*)(s))->wait(timeout_milliseconds);
}

//------------------------------------------------------------------------------
//
// rpc
//
//------------------------------------------------------------------------------

// rpc message management (by rDSN)
DSN_API dsn_message_t* dsn_rpc_create_request(dsn_task_code_t rpc_code, int timeout_milliseconds, int hash)
{
    auto msg = ::dsn::message::create_request(rpc_code, timeout_milliseconds, hash);
    msg->add_ref();
    return msg->c_msg();
}

DSN_API dsn_message_t* dsn_rpc_create_response(dsn_message_t* request)
{
    auto msg = ::dsn::message::from_c_msg(request)->create_response();
    msg->add_ref();
    return msg->c_msg();
}

DSN_API void dsn_rpc_release_message(dsn_message_t* msg)
{
    ::dsn::message::from_c_msg(msg)->release_ref();
}

//
// rpc buffer management (by upper apps themselves)
//
// - send buffer
//   apps prepare the buffer, call rpc API, and use dsn_msg_callback_t to get a chance to release the buffer
//
// - recv buffer
//   apps provide buffer allocate to rDSN to prepare the buffer to receive the RPC message,
//   for messages successfully handled by the apps, apps take charge of the buffer release.
//   otherwise (e.g., throttling), rDSN use app provided buffer deallocator to release the buffers.
//
DSN_API void dsn_rpc_set_buffer_management(dsn_buffer_allocator allocator, dsn_buffer_deallocator deallocator)
{
    dassert(false, "not implemented");
}

// rpc calls
DSN_API dsn_address_t dsn_rpc_primary_address()
{
    auto tsk = ::dsn::task::get_current_task();
    dassert(tsk != nullptr, "this function can only be invoked inside tasks");

    return tsk->node()->rpc()->primary_address();
}

DSN_API bool dsn_rpc_register_handler(dsn_task_code_t code, const char* name, dsn_rpc_request_handler_t cb, void* param)
{
    auto tsk = ::dsn::task::get_current_task();
    dassert(tsk != nullptr, "this function can only be invoked inside tasks");

    ::dsn::rpc_handler_ptr h(new ::dsn::rpc_handler_info(code));
    h->name = std::string(name);
    h->handler = nullptr;
    h->c_handler = cb;
    h->parameter = param;

    return tsk->node()->rpc()->register_rpc_handler(h);
}

DSN_API void* dsn_rpc_unregiser_handler(dsn_task_code_t code)
{
    auto tsk = ::dsn::task::get_current_task();
    dassert(tsk != nullptr, "this function can only be invoked inside tasks");

    // TODO: not use shared_ptr, return rpc_handler* here, delete as paried above
    // return parameter for upper app deletion
    tsk->node()->rpc()->unregister_rpc_handler(code);
    return nullptr;
}

DSN_API dsn_task_t dsn_rpc_create(dsn_message_t* request, dsn_rpc_response_handler_t cb, void* param, int reply_hash)
{
    ::dsn::message_ptr msg = ::dsn::message::from_c_msg(request);
    ::dsn::task* rtask;

    if (cb != nullptr) 
        rtask = new ::dsn::rpc_response_task_c(msg, cb, param, reply_hash);
    else 
        rtask = new ::dsn::rpc_response_task_empty(msg, reply_hash);

    rtask->add_ref();
    return rtask;
}

DSN_API void dsn_rpc_call(dsn_address_t server, dsn_task_t rpc_call)
{
    auto tsk = ::dsn::task::get_current_task();
    dassert(tsk != nullptr, "this function can only be invoked inside tasks");

    ::dsn::rpc_response_task_ptr task = (::dsn::rpc_response_task_c*)rpc_call;
    dassert(task->spec().type == TASK_TYPE_RPC_RESPONSE, "");

    auto rpc = tsk->node()->rpc();

    // TODO: remove this parameter in future
    ::dsn::message_ptr msg = task->get_request();
    msg->c_msg()->hdr.to_address = server;
    rpc->call(msg, task);
}

DSN_API dsn_message_t* dsn_rpc_call_wait(dsn_address_t server, dsn_message_t* request)
{
    auto tsk = ::dsn::task::get_current_task();
    dassert(tsk != nullptr, "this function can only be invoked inside tasks");

    auto rpc = tsk->node()->rpc();
    request->hdr.to_address = server;

    ::dsn::message_ptr msg = ::dsn::message::from_c_msg(request);
    ::dsn::rpc_response_task_ptr rtask = new ::dsn::rpc_response_task_empty(msg);
    rpc->call(msg, rtask);
    rtask->wait();
    if (rtask->error() == ::dsn::ERR_OK)
    {
        auto msg = rtask->get_response();
        msg->add_ref();
        return msg->c_msg();
    }
    else
    {
        return nullptr;
    }
}

DSN_API void dsn_rpc_call_one_way(dsn_address_t server, dsn_message_t* request)
{
    auto tsk = ::dsn::task::get_current_task();
    dassert(tsk != nullptr, "this function can only be invoked inside tasks");

    auto rpc = tsk->node()->rpc();
    request->hdr.to_address = server;

    ::dsn::message_ptr msg = ::dsn::message::from_c_msg(request);
    ::dsn::rpc_response_task_ptr rtask(nullptr);

    rpc->call(msg, rtask);
}

DSN_API void dsn_rpc_reply(dsn_message_t* response)
{
    ::dsn::message_ptr msg = ::dsn::message::from_c_msg(response);
    msg->server_session()->send(msg);
}

DSN_API dsn_message_t* dsn_rpc_get_response(dsn_task_t rpc_call)
{
    ::dsn::rpc_response_task* task = (::dsn::rpc_response_task*)rpc_call;
    dassert(task->spec().type == TASK_TYPE_RPC_RESPONSE, "");
    auto msg = task->get_response();
    if (nullptr != msg)
    {
        msg->add_ref();
        return msg->c_msg();
    }
    else
        return nullptr;
}

DSN_API void dsn_rpc_enqueue_response(dsn_task_t rpc_call, dsn_error_t err, dsn_message_t* response)
{
    ::dsn::rpc_response_task* task = (::dsn::rpc_response_task*)rpc_call;
    dassert(task->spec().type == TASK_TYPE_RPC_RESPONSE, "");

    ::dsn::message_ptr resp = ::dsn::message::from_c_msg(response);
    ::dsn::error_code err2;
    err2.set(err);
    task->enqueue(err2, resp);
}

//------------------------------------------------------------------------------
//
// file operations
//
//------------------------------------------------------------------------------
DSN_API dsn_handle_t dsn_file_open(const char* file_name, int flag, int pmode)
{
    auto tsk = ::dsn::task::get_current_task();
    dassert(tsk != nullptr, "this function can only be invoked inside tasks");

    return tsk->node()->disk()->open(file_name, flag, pmode);
}

DSN_API void dsn_file_close(dsn_handle_t file)
{
    auto tsk = ::dsn::task::get_current_task();
    dassert(tsk != nullptr, "this function can only be invoked inside tasks");

    tsk->node()->disk()->close(file);
}

DSN_API dsn_task_t dsn_file_create_callback_task(dsn_task_code_t code, dsn_file_callback_t cb, void* param, int hash)
{
    auto callback = new ::dsn::aio_task_c(code, cb, param, hash);
    callback->add_ref();
    return callback;
}

DSN_API void dsn_file_read(dsn_handle_t file, char* buffer, int count, uint64_t offset, dsn_task_t cb)
{
    auto tsk = ::dsn::task::get_current_task();
    dassert(tsk != nullptr, "this function can only be invoked inside tasks");

    ::dsn::aio_task_ptr callback((::dsn::aio_task*)cb);
    callback->aio()->buffer = buffer;
    callback->aio()->buffer_size = count;
    callback->aio()->engine = nullptr;
    callback->aio()->file = file;
    callback->aio()->file_offset = offset;
    callback->aio()->type = ::dsn::AIO_Read;

    tsk->node()->disk()->read(callback);
}

DSN_API void dsn_file_write(dsn_handle_t file, const char* buffer, int count, uint64_t offset, dsn_task_t cb)
{
    auto tsk = ::dsn::task::get_current_task();
    dassert(tsk != nullptr, "this function can only be invoked inside tasks");

    ::dsn::aio_task_ptr callback((::dsn::aio_task*)cb);
    callback->aio()->buffer = (char*)buffer;
    callback->aio()->buffer_size = count;
    callback->aio()->engine = nullptr;
    callback->aio()->file = file;
    callback->aio()->file_offset = offset;
    callback->aio()->type = ::dsn::AIO_Write;

    tsk->node()->disk()->write(callback);
}

DSN_API void dsn_file_copy_remote_directory(dsn_address_t remote, const char* source_dir, const char* dest_dir, bool overwrite, dsn_task_t cb)
{
    std::shared_ptr<::dsn::remote_copy_request> rci(new ::dsn::remote_copy_request());
    rci->source = remote;
    rci->source_dir = source_dir;
    rci->files.clear();
    rci->dest_dir = dest_dir;
    rci->overwrite = overwrite;

    auto tsk = ::dsn::task::get_current_task();
    dassert(tsk != nullptr, "this function can only be invoked inside tasks");

    ::dsn::aio_task_ptr callback((::dsn::aio_task*)cb);
    return tsk->node()->nfs()->call(rci, callback);
}

DSN_API void dsn_file_copy_remote_files(dsn_address_t remote, const char* source_dir, const char** source_files, const char* dest_dir, bool overwrite, dsn_task_t cb)
{
    std::shared_ptr<::dsn::remote_copy_request> rci(new ::dsn::remote_copy_request());
    rci->source = remote;
    rci->source_dir = source_dir;

    rci->files.clear();
    const char** p = source_files;
    while (*p != nullptr &&& **p != '\0')
    {
        rci->files.push_back(std::string(*p));
        p++;
    }

    rci->dest_dir = dest_dir;
    rci->overwrite = overwrite;

    auto tsk = ::dsn::task::get_current_task();
    dassert(tsk != nullptr, "this function can only be invoked inside tasks");

    ::dsn::aio_task_ptr callback((::dsn::aio_task*)cb);
    return tsk->node()->nfs()->call(rci, callback);
}

DSN_API size_t dsn_file_get_io_size(dsn_task_t cb_task)
{
    ::dsn::task* task = (::dsn::task*)cb_task;
    dassert(task->spec().type == TASK_TYPE_AIO, "");
    return ((::dsn::aio_task*)task)->get_transferred_size();
}

DSN_API void dsn_file_task_enqueue(dsn_task_t cb_task, dsn_error_t err, size_t size)
{
    ::dsn::task* task = (::dsn::task*)cb_task;
    dassert(task->spec().type == TASK_TYPE_AIO, "");

    ::dsn::error_code err2;
    err2.set(err);
    ((::dsn::aio_task*)task)->enqueue(err2, size, nullptr);
}

//------------------------------------------------------------------------------
//
// env
//
//------------------------------------------------------------------------------
DSN_API uint64_t dsn_env_now_ns()
{
    return ::dsn::service_engine::instance().env()->now_ns();
}

DSN_API uint64_t dsn_env_random64(uint64_t min, uint64_t max) // [min, max]
{
    return ::dsn::service_engine::instance().env()->random64(min, max);
}

//------------------------------------------------------------------------------
//
// system
//
//------------------------------------------------------------------------------
DSN_API bool dsn_register_app(const char* name, dsn_app_start start, dsn_app_stop stop)
{
    return false;
}

DSN_API bool dsn_run_with_config(const char* config, bool sleep_after_init)
{
    return false;
}

//
// run the system with arguments
//   config [-cargs k1=v1;k2=v2] [-app app_name] [-app_index index]
// e.g., config.ini -app replica -app_index 1 to start the first replica as a new process
//       config.ini -app replica to start ALL replicas (count specified in config) as a new process
//       config.ini -app replica -cargs replica-port=34556 to start ALL replicas with given port variable specified in config.ini
//       config.ini to start ALL apps as a new process
//
DSN_API void dsn_run(int argc, char** argv, bool sleep_after_init)
{

}
