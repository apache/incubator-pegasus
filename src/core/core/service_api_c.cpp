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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/service_api_c.h>
#include <dsn/tool_api.h>
#include <dsn/utility/enum_helper.h>
#include <dsn/cpp/auto_codes.h>
#include <dsn/cpp/serialization.h>
#include <dsn/tool-api/task_spec.h>
#include <dsn/tool-api/zlock_provider.h>
#include <dsn/tool-api/nfs.h>
#include <dsn/tool-api/env_provider.h>
#include <dsn/utility/factory_store.h>
#include <dsn/tool-api/task.h>
#include <dsn/utility/singleton_store.h>
#include <dsn/utility/utils.h>

#include <dsn/utility/configuration.h>
#include <dsn/utility/filesystem.h>
#include <dsn/tool-api/command_manager.h>
#include "service_engine.h"
#include "rpc_engine.h"
#include "disk_engine.h"
#include "task_engine.h"
#include "coredump.h"
#include "transient_memory.h"
#include <fstream>

#ifndef _WIN32
#include <signal.h>
#include <unistd.h>
#else
#include <TlHelp32.h>
#endif

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "service_api_c"

//
// global state
//
static struct _all_info_
{
    unsigned int magic;
    bool engine_ready;
    bool config_completed;
    ::dsn::tools::tool_app *tool;
    ::dsn::configuration_ptr config;
    ::dsn::service_engine *engine;
    std::vector<::dsn::task_spec *> task_specs;
    ::dsn::memory_provider *memory;

    bool is_config_completed() const { return magic == 0xdeadbeef && config_completed; }

    bool is_engine_ready() const { return magic == 0xdeadbeef && engine_ready; }

} dsn_all;

//------------------------------------------------------------------------------
//
// common types
//
//------------------------------------------------------------------------------
struct dsn_error_placeholder
{
};
class error_code_mgr : public ::dsn::utils::customized_id_mgr<dsn_error_placeholder>
{
public:
    error_code_mgr()
    {
        auto err = register_id("ERR_OK"); // make sure ERR_OK is always registered first
        dassert(0 == err, "register_id failed, err = %d", err);
    }
};

DSN_API dsn_error_t dsn_error_register(const char *name)
{
    dassert(strlen(name) < DSN_MAX_ERROR_CODE_NAME_LENGTH,
            "error code '%s' is too long - length must be smaller than %d",
            name,
            DSN_MAX_ERROR_CODE_NAME_LENGTH);
    return static_cast<dsn_error_t>(error_code_mgr::instance().register_id(name));
}

DSN_API const char *dsn_error_to_string(dsn_error_t err)
{
    return error_code_mgr::instance().get_name(static_cast<int>(err));
}

DSN_API dsn_error_t dsn_error_from_string(const char *s, dsn_error_t default_err)
{
    auto r = error_code_mgr::instance().get_id(s);
    return r == -1 ? default_err : r;
}

DSN_API volatile int *dsn_task_queue_virtual_length_ptr(dsn_task_code_t code, int hash)
{
    return dsn::task::get_current_node()->computation()->get_task_queue_virtual_length_ptr(code,
                                                                                           hash);
}

// use ::dsn::threadpool_code2; for parsing purpose
DSN_API dsn_threadpool_code_t dsn_threadpool_code_register(const char *name)
{
    return static_cast<dsn_threadpool_code_t>(
        ::dsn::utils::customized_id_mgr<::dsn::threadpool_code2_>::instance().register_id(name));
}

DSN_API const char *dsn_threadpool_code_to_string(dsn_threadpool_code_t pool_code)
{
    return ::dsn::utils::customized_id_mgr<::dsn::threadpool_code2_>::instance().get_name(
        static_cast<int>(pool_code));
}

DSN_API dsn_threadpool_code_t dsn_threadpool_code_from_string(const char *s,
                                                              dsn_threadpool_code_t default_code)
{
    auto r = ::dsn::utils::customized_id_mgr<::dsn::threadpool_code2_>::instance().get_id(s);
    return r == -1 ? default_code : r;
}

DSN_API int dsn_threadpool_code_max()
{
    return ::dsn::utils::customized_id_mgr<::dsn::threadpool_code2_>::instance().max_value();
}

DSN_API int dsn_threadpool_get_current_tid() { return ::dsn::utils::get_current_tid(); }

struct task_code_placeholder
{
};
DSN_API dsn_task_code_t dsn_task_code_register(const char *name,
                                               dsn_task_type_t type,
                                               dsn_task_priority_t pri,
                                               dsn_threadpool_code_t pool)
{
    dassert(strlen(name) < DSN_MAX_TASK_CODE_NAME_LENGTH,
            "task code '%s' is too long - length must be smaller than %d",
            name,
            DSN_MAX_TASK_CODE_NAME_LENGTH);
    auto r = static_cast<dsn_task_code_t>(
        ::dsn::utils::customized_id_mgr<task_code_placeholder>::instance().register_id(name));
    ::dsn::task_spec::register_task_code(r, type, pri, pool);
    return r;
}

DSN_API void dsn_task_code_query(dsn_task_code_t code,
                                 dsn_task_type_t *ptype,
                                 dsn_task_priority_t *ppri,
                                 dsn_threadpool_code_t *ppool)
{
    auto sp = ::dsn::task_spec::get(code);
    dassert(sp != nullptr, "task code = %d", code);
    if (ptype)
        *ptype = sp->type;
    if (ppri)
        *ppri = sp->priority;
    if (ppool)
        *ppool = sp->pool_code;
}

DSN_API void dsn_task_code_set_threadpool(dsn_task_code_t code, dsn_threadpool_code_t pool)
{
    auto sp = ::dsn::task_spec::get(code);
    dassert(sp != nullptr, "task code = %d", code);
    sp->pool_code = pool;
}

DSN_API void dsn_task_code_set_priority(dsn_task_code_t code, dsn_task_priority_t pri)
{
    auto sp = ::dsn::task_spec::get(code);
    dassert(sp != nullptr, "task code = %d", code);
    sp->priority = pri;
}

DSN_API const char *dsn_task_code_to_string(dsn_task_code_t code)
{
    return ::dsn::utils::customized_id_mgr<task_code_placeholder>::instance().get_name(
        static_cast<int>(code));
}

DSN_API dsn_task_code_t dsn_task_code_from_string(const char *s, dsn_task_code_t default_code)
{
    auto r = ::dsn::utils::customized_id_mgr<task_code_placeholder>::instance().get_id(s);
    return r == -1 ? default_code : r;
}

DSN_API int dsn_task_code_max()
{
    return ::dsn::utils::customized_id_mgr<task_code_placeholder>::instance().max_value();
}

DSN_API const char *dsn_task_type_to_string(dsn_task_type_t tt) { return enum_to_string(tt); }

DSN_API const char *dsn_task_priority_to_string(dsn_task_priority_t tt)
{
    return enum_to_string(tt);
}

DSN_API bool dsn_task_is_running_inside(dsn_task_t t)
{
    return ::dsn::task::get_current_task() == (::dsn::task *)(t);
}

DSN_API const char *dsn_config_get_value_string(const char *section,
                                                const char *key,
                                                const char *default_value,
                                                const char *dsptr)
{
    return dsn_all.config->get_string_value(section, key, default_value, dsptr);
}

DSN_API bool dsn_config_get_value_bool(const char *section,
                                       const char *key,
                                       bool default_value,
                                       const char *dsptr)
{
    return dsn_all.config->get_value<bool>(section, key, default_value, dsptr);
}

DSN_API uint64_t dsn_config_get_value_uint64(const char *section,
                                             const char *key,
                                             uint64_t default_value,
                                             const char *dsptr)
{
    return dsn_all.config->get_value<uint64_t>(section, key, default_value, dsptr);
}

DSN_API double dsn_config_get_value_double(const char *section,
                                           const char *key,
                                           double default_value,
                                           const char *dsptr)
{
    return dsn_all.config->get_value<double>(section, key, default_value, dsptr);
}

DSN_API int dsn_config_get_all_sections(const char **buffers, /*inout*/ int *buffer_count)
{
    std::vector<const char *> sections;
    dsn_all.config->get_all_section_ptrs(sections);
    int scount = (int)sections.size();

    if (*buffer_count > scount)
        *buffer_count = scount;

    for (int i = 0; i < *buffer_count; i++) {
        buffers[i] = sections[i];
    }

    return scount;
}

DSN_API int dsn_config_get_all_keys(
    const char *section,
    const char **buffers,
    /*inout*/ int *buffer_count) // return all key count (may greater than buffer_count)
{
    std::vector<const char *> keys;
    dsn_all.config->get_all_keys(section, keys);
    int kcount = (int)keys.size();

    if (*buffer_count > kcount)
        *buffer_count = kcount;

    for (int i = 0; i < *buffer_count; i++) {
        buffers[i] = keys[i];
    }

    return kcount;
}

DSN_API void dsn_config_dump(const char *file)
{
    std::ofstream os(file, std::ios::out);
    dsn_all.config->dump(os);
    os.close();
}

DSN_API void dsn_coredump()
{
    ::dsn::utils::coredump::write();
    ::abort();
}

DSN_API dsn_task_t dsn_task_create(dsn_task_code_t code,
                                   dsn_task_handler_t cb,
                                   void *context,
                                   int hash,
                                   dsn_task_tracker_t tracker)
{
    auto t = new ::dsn::task_c(code, cb, context, nullptr, hash);
    t->set_tracker((dsn::task_tracker *)tracker);
    t->spec().on_task_create.execute(::dsn::task::get_current_task(), t);
    return t;
}

DSN_API dsn_task_t dsn_task_create_timer(dsn_task_code_t code,
                                         dsn_task_handler_t cb,
                                         void *context,
                                         int hash,
                                         int interval_milliseconds,
                                         dsn_task_tracker_t tracker)
{
    auto t = new ::dsn::timer_task(code, cb, context, nullptr, interval_milliseconds, hash);
    t->set_tracker((dsn::task_tracker *)tracker);
    t->spec().on_task_create.execute(::dsn::task::get_current_task(), t);
    return t;
}

DSN_API dsn_task_t dsn_task_create_ex(dsn_task_code_t code,
                                      dsn_task_handler_t cb,
                                      dsn_task_cancelled_handler_t on_cancel,
                                      void *context,
                                      int hash,
                                      dsn_task_tracker_t tracker)
{
    auto t = new ::dsn::task_c(code, cb, context, on_cancel, hash);
    t->set_tracker((dsn::task_tracker *)tracker);
    t->spec().on_task_create.execute(::dsn::task::get_current_task(), t);
    return t;
}

DSN_API dsn_task_t dsn_task_create_timer_ex(dsn_task_code_t code,
                                            dsn_task_handler_t cb,
                                            dsn_task_cancelled_handler_t on_cancel,
                                            void *context,
                                            int hash,
                                            int interval_milliseconds,
                                            dsn_task_tracker_t tracker)
{
    auto t = new ::dsn::timer_task(code, cb, context, on_cancel, interval_milliseconds, hash);
    t->set_tracker((dsn::task_tracker *)tracker);
    t->spec().on_task_create.execute(::dsn::task::get_current_task(), t);
    return t;
}

DSN_API dsn_task_tracker_t dsn_task_tracker_create(int task_bucket_count)
{
    return (dsn_task_tracker_t)(new ::dsn::task_tracker(task_bucket_count));
}

DSN_API void dsn_task_tracker_destroy(dsn_task_tracker_t tracker)
{
    delete ((::dsn::task_tracker *)tracker);
}

DSN_API void dsn_task_tracker_cancel_all(dsn_task_tracker_t tracker)
{
    ((::dsn::task_tracker *)tracker)->cancel_outstanding_tasks();
}

DSN_API void dsn_task_tracker_wait_all(dsn_task_tracker_t tracker)
{
    ((::dsn::task_tracker *)tracker)->wait_outstanding_tasks();
}

DSN_API void dsn_task_call(dsn_task_t task, int delay_milliseconds)
{
    auto t = ((::dsn::task *)(task));
    dassert(t->spec().type == TASK_TYPE_COMPUTE, "must be common or timer task");

    t->set_delay(delay_milliseconds);
    t->enqueue();
}

DSN_API void dsn_task_add_ref(dsn_task_t task) { ((::dsn::task *)(task))->add_ref(); }

DSN_API void dsn_task_release_ref(dsn_task_t task) { ((::dsn::task *)(task))->release_ref(); }

DSN_API int dsn_task_get_ref(dsn_task_t task) { return ((::dsn::task *)(task))->get_count(); }

DSN_API bool dsn_task_cancel(dsn_task_t task, bool wait_until_finished)
{
    return ((::dsn::task *)(task))->cancel(wait_until_finished);
}

DSN_API void dsn_task_set_delay(dsn_task_t task, int delay_ms)
{
    ((::dsn::task *)(task))->set_delay(delay_ms);
}

DSN_API bool dsn_task_cancel2(dsn_task_t task, bool wait_until_finished, bool *finished)
{
    return ((::dsn::task *)(task))->cancel(wait_until_finished, finished);
}

DSN_API void dsn_task_cancel_current_timer()
{
    ::dsn::task *t = ::dsn::task::get_current_task();
    if (nullptr != t) {
        t->cancel(false);
    }
}

DSN_API void dsn_task_wait(dsn_task_t task)
{
    auto r = ((::dsn::task *)(task))->wait();
    dassert(r,
            "task wait without timeout must succeeds (task_id = %016" PRIx64 ")",
            ((::dsn::task *)(task))->id());
}

DSN_API bool dsn_task_wait_timeout(dsn_task_t task, int timeout_milliseconds)
{
    return ((::dsn::task *)(task))->wait(timeout_milliseconds);
}

DSN_API dsn_error_t dsn_task_error(dsn_task_t task)
{
    return ((::dsn::task *)(task))->error().get();
}

//------------------------------------------------------------------------------
//
// synchronization - concurrent access and coordination among threads
//
//------------------------------------------------------------------------------
DSN_API dsn_handle_t dsn_exlock_create(bool recursive)
{
    if (recursive) {
        ::dsn::lock_provider *last = ::dsn::utils::factory_store<::dsn::lock_provider>::create(
            ::dsn::service_engine::fast_instance().spec().lock_factory_name.c_str(),
            ::dsn::PROVIDER_TYPE_MAIN,
            nullptr);

        // TODO: perf opt by saving the func ptrs somewhere
        for (auto &s : ::dsn::service_engine::fast_instance().spec().lock_aspects) {
            last = ::dsn::utils::factory_store<::dsn::lock_provider>::create(
                s.c_str(), ::dsn::PROVIDER_TYPE_ASPECT, last);
        }

        return (dsn_handle_t) dynamic_cast<::dsn::ilock *>(last);
    } else {
        ::dsn::lock_nr_provider *last =
            ::dsn::utils::factory_store<::dsn::lock_nr_provider>::create(
                ::dsn::service_engine::fast_instance().spec().lock_nr_factory_name.c_str(),
                ::dsn::PROVIDER_TYPE_MAIN,
                nullptr);

        // TODO: perf opt by saving the func ptrs somewhere
        for (auto &s : ::dsn::service_engine::fast_instance().spec().lock_nr_aspects) {
            last = ::dsn::utils::factory_store<::dsn::lock_nr_provider>::create(
                s.c_str(), ::dsn::PROVIDER_TYPE_ASPECT, last);
        }

        return (dsn_handle_t) dynamic_cast<::dsn::ilock *>(last);
    }
}

DSN_API void dsn_exlock_destroy(dsn_handle_t l) { delete (::dsn::ilock *)(l); }

DSN_API void dsn_exlock_lock(dsn_handle_t l)
{
    ((::dsn::ilock *)(l))->lock();
    ::dsn::lock_checker::zlock_exclusive_count++;
}

DSN_API bool dsn_exlock_try_lock(dsn_handle_t l)
{
    auto r = ((::dsn::ilock *)(l))->try_lock();
    if (r) {
        ::dsn::lock_checker::zlock_exclusive_count++;
    }
    return r;
}

DSN_API void dsn_exlock_unlock(dsn_handle_t l)
{
    ::dsn::lock_checker::zlock_exclusive_count--;
    ((::dsn::ilock *)(l))->unlock();
}

// non-recursive rwlock
DSN_API dsn_handle_t dsn_rwlock_nr_create()
{
    ::dsn::rwlock_nr_provider *last =
        ::dsn::utils::factory_store<::dsn::rwlock_nr_provider>::create(
            ::dsn::service_engine::fast_instance().spec().rwlock_nr_factory_name.c_str(),
            ::dsn::PROVIDER_TYPE_MAIN,
            nullptr);

    // TODO: perf opt by saving the func ptrs somewhere
    for (auto &s : ::dsn::service_engine::fast_instance().spec().rwlock_nr_aspects) {
        last = ::dsn::utils::factory_store<::dsn::rwlock_nr_provider>::create(
            s.c_str(), ::dsn::PROVIDER_TYPE_ASPECT, last);
    }
    return (dsn_handle_t)(last);
}

DSN_API void dsn_rwlock_nr_destroy(dsn_handle_t l) { delete (::dsn::rwlock_nr_provider *)(l); }

DSN_API void dsn_rwlock_nr_lock_read(dsn_handle_t l)
{
    ((::dsn::rwlock_nr_provider *)(l))->lock_read();
    ::dsn::lock_checker::zlock_shared_count++;
}

DSN_API void dsn_rwlock_nr_unlock_read(dsn_handle_t l)
{
    ::dsn::lock_checker::zlock_shared_count--;
    ((::dsn::rwlock_nr_provider *)(l))->unlock_read();
}

DSN_API bool dsn_rwlock_nr_try_lock_read(dsn_handle_t l)
{
    auto r = ((::dsn::rwlock_nr_provider *)(l))->try_lock_read();
    if (r)
        ::dsn::lock_checker::zlock_shared_count++;
    return r;
}

DSN_API void dsn_rwlock_nr_lock_write(dsn_handle_t l)
{
    ((::dsn::rwlock_nr_provider *)(l))->lock_write();
    ::dsn::lock_checker::zlock_exclusive_count++;
}

DSN_API void dsn_rwlock_nr_unlock_write(dsn_handle_t l)
{
    ::dsn::lock_checker::zlock_exclusive_count--;
    ((::dsn::rwlock_nr_provider *)(l))->unlock_write();
}

DSN_API bool dsn_rwlock_nr_try_lock_write(dsn_handle_t l)
{
    auto r = ((::dsn::rwlock_nr_provider *)(l))->try_lock_write();
    if (r)
        ::dsn::lock_checker::zlock_exclusive_count++;
    return r;
}

DSN_API dsn_handle_t dsn_semaphore_create(int initial_count)
{
    ::dsn::semaphore_provider *last =
        ::dsn::utils::factory_store<::dsn::semaphore_provider>::create(
            ::dsn::service_engine::fast_instance().spec().semaphore_factory_name.c_str(),
            ::dsn::PROVIDER_TYPE_MAIN,
            initial_count,
            nullptr);

    // TODO: perf opt by saving the func ptrs somewhere
    for (auto &s : ::dsn::service_engine::fast_instance().spec().semaphore_aspects) {
        last = ::dsn::utils::factory_store<::dsn::semaphore_provider>::create(
            s.c_str(), ::dsn::PROVIDER_TYPE_ASPECT, initial_count, last);
    }
    return (dsn_handle_t)(last);
}

DSN_API void dsn_semaphore_destroy(dsn_handle_t s) { delete (::dsn::semaphore_provider *)(s); }

DSN_API void dsn_semaphore_signal(dsn_handle_t s, int count)
{
    ((::dsn::semaphore_provider *)(s))->signal(count);
}

DSN_API void dsn_semaphore_wait(dsn_handle_t s)
{
    ::dsn::lock_checker::check_wait_safety();
    ((::dsn::semaphore_provider *)(s))->wait();
}

DSN_API bool dsn_semaphore_wait_timeout(dsn_handle_t s, int timeout_milliseconds)
{
    return ((::dsn::semaphore_provider *)(s))->wait(timeout_milliseconds);
}

//------------------------------------------------------------------------------
//
// rpc
//
//------------------------------------------------------------------------------

// rpc calls
DSN_API dsn_address_t dsn_primary_address()
{
    return ::dsn::task::get_current_rpc()->primary_address().c_addr();
}

DSN_API bool dsn_rpc_register_handler(dsn_task_code_t code,
                                      const char *name,
                                      dsn_rpc_request_handler_t cb,
                                      void *param,
                                      dsn_gpid gpid)
{
    ::dsn::rpc_handler_info *h(new ::dsn::rpc_handler_info(code));
    h->name = std::string(name);
    h->c_handler = cb;
    h->parameter = param;

    h->add_ref();

    bool r = ::dsn::task::get_current_node()->rpc_register_handler(h, gpid);
    if (!r) {
        delete h;
    }
    return r;
}

DSN_API void *dsn_rpc_unregiser_handler(dsn_task_code_t code, dsn_gpid gpid)
{
    auto h = ::dsn::task::get_current_node()->rpc_unregister_handler(code, gpid);
    void *param = nullptr;

    if (nullptr != h) {
        param = h->parameter;
        if (1 == h->release_ref())
            delete h;
    }

    return param;
}

DSN_API dsn_task_t dsn_rpc_create_response_task(dsn_message_t request,
                                                dsn_rpc_response_handler_t cb,
                                                void *context,
                                                int reply_thread_hash,
                                                dsn_task_tracker_t tracker)
{
    auto msg = ((::dsn::message_ex *)request);
    auto t = new ::dsn::rpc_response_task(msg, cb, context, nullptr, reply_thread_hash);
    t->set_tracker((dsn::task_tracker *)tracker);
    t->spec().on_task_create.execute(::dsn::task::get_current_task(), t);
    return t;
}

DSN_API dsn_task_t dsn_rpc_create_response_task_ex(dsn_message_t request,
                                                   dsn_rpc_response_handler_t cb,
                                                   dsn_task_cancelled_handler_t on_cancel,
                                                   void *context,
                                                   int reply_thread_hash,
                                                   dsn_task_tracker_t tracker)
{
    auto msg = ((::dsn::message_ex *)request);
    auto t = new ::dsn::rpc_response_task(msg, cb, context, on_cancel, reply_thread_hash);
    t->set_tracker((dsn::task_tracker *)tracker);
    t->spec().on_task_create.execute(::dsn::task::get_current_task(), t);
    return t;
}

DSN_API void dsn_rpc_call(dsn_address_t server, dsn_task_t rpc_call)
{
    ::dsn::rpc_response_task *task = (::dsn::rpc_response_task *)rpc_call;
    dassert(task->spec().type == TASK_TYPE_RPC_RESPONSE,
            "invalid task_type, type = %s",
            enum_to_string(task->spec().type));

    auto msg = task->get_request();
    msg->server_address = server;
    ::dsn::task::get_current_rpc()->call(msg, task);
}

DSN_API dsn_message_t dsn_rpc_call_wait(dsn_address_t server, dsn_message_t request)
{
    auto msg = ((::dsn::message_ex *)request);
    msg->server_address = server;

    ::dsn::rpc_response_task *rtask = new ::dsn::rpc_response_task(msg, nullptr, nullptr, 0);
    rtask->add_ref();
    ::dsn::task::get_current_rpc()->call(msg, rtask);
    rtask->wait();
    if (rtask->error() == ::dsn::ERR_OK) {
        auto msg = rtask->get_response();
        msg->add_ref();       // released by callers
        rtask->release_ref(); // added above
        return msg;
    } else {
        rtask->release_ref(); // added above
        return nullptr;
    }
}

DSN_API void dsn_rpc_call_one_way(dsn_address_t server, dsn_message_t request)
{
    auto msg = ((::dsn::message_ex *)request);
    msg->server_address = server;

    ::dsn::task::get_current_rpc()->call(msg, nullptr);
}

DSN_API void dsn_rpc_reply(dsn_message_t response, dsn_error_t err)
{
    auto msg = ((::dsn::message_ex *)response);
    ::dsn::task::get_current_rpc()->reply(msg, err);
}

DSN_API void dsn_rpc_forward(dsn_message_t request, dsn_address_t addr)
{
    ::dsn::task::get_current_rpc()->forward((::dsn::message_ex *)(request),
                                            ::dsn::rpc_address(addr));
}

DSN_API dsn_message_t dsn_rpc_get_response(dsn_task_t rpc_call)
{
    ::dsn::rpc_response_task *task = (::dsn::rpc_response_task *)rpc_call;
    dassert(task->spec().type == TASK_TYPE_RPC_RESPONSE,
            "invalid task_type, type = %s",
            enum_to_string(task->spec().type));
    auto msg = task->get_response();
    if (nullptr != msg) {
        msg->add_ref(); // released by callers
        return msg;
    } else
        return nullptr;
}

DSN_API void dsn_rpc_enqueue_response(dsn_task_t rpc_call, dsn_error_t err, dsn_message_t response)
{
    ::dsn::rpc_response_task *task = (::dsn::rpc_response_task *)rpc_call;
    dassert(task->spec().type == TASK_TYPE_RPC_RESPONSE,
            "invalid task_type, type = %s",
            enum_to_string(task->spec().type));
    auto resp = ((::dsn::message_ex *)response);
    task->enqueue(err, resp);
}

//------------------------------------------------------------------------------
//
// file operations
//
//------------------------------------------------------------------------------

DSN_API dsn_handle_t dsn_file_open(const char *file_name, int flag, int pmode)
{
    return ::dsn::task::get_current_disk()->open(file_name, flag, pmode);
}

DSN_API dsn_error_t dsn_file_close(dsn_handle_t file)
{
    return ::dsn::task::get_current_disk()->close(file);
}

DSN_API dsn_error_t dsn_file_flush(dsn_handle_t file)
{
    return ::dsn::task::get_current_disk()->flush(file);
}

// native HANDLE: HANDLE for windows, int for non-windows
DSN_API void *dsn_file_native_handle(dsn_handle_t file)
{
    auto dfile = (::dsn::disk_file *)file;
    return dfile->native_handle();
}

DSN_API dsn_task_t dsn_file_create_aio_task(
    dsn_task_code_t code, dsn_aio_handler_t cb, void *context, int hash, dsn_task_tracker_t tracker)
{
    auto t = new ::dsn::aio_task(code, cb, context, nullptr, hash);
    t->set_tracker((dsn::task_tracker *)tracker);
    t->spec().on_task_create.execute(::dsn::task::get_current_task(), t);
    return t;
}

DSN_API dsn_task_t dsn_file_create_aio_task_ex(dsn_task_code_t code,
                                               dsn_aio_handler_t cb,
                                               dsn_task_cancelled_handler_t on_cancel,
                                               void *context,
                                               int hash,
                                               dsn_task_tracker_t tracker)
{
    auto t = new ::dsn::aio_task(code, cb, context, on_cancel, hash);
    t->set_tracker((dsn::task_tracker *)tracker);
    t->spec().on_task_create.execute(::dsn::task::get_current_task(), t);
    return t;
}

DSN_API void
dsn_file_read(dsn_handle_t file, char *buffer, int count, uint64_t offset, dsn_task_t cb)
{
    ::dsn::aio_task *callback((::dsn::aio_task *)cb);
    callback->aio()->buffer = buffer;
    callback->aio()->buffer_size = count;
    callback->aio()->engine = nullptr;
    callback->aio()->file = file;
    callback->aio()->file_offset = offset;
    callback->aio()->type = ::dsn::AIO_Read;

    ::dsn::task::get_current_disk()->read(callback);
}

DSN_API void
dsn_file_write(dsn_handle_t file, const char *buffer, int count, uint64_t offset, dsn_task_t cb)
{
    ::dsn::aio_task *callback((::dsn::aio_task *)cb);
    callback->aio()->buffer = (char *)buffer;
    callback->aio()->buffer_size = count;
    callback->aio()->engine = nullptr;
    callback->aio()->file = file;
    callback->aio()->file_offset = offset;
    callback->aio()->type = ::dsn::AIO_Write;

    ::dsn::task::get_current_disk()->write(callback);
}

DSN_API void dsn_file_write_vector(dsn_handle_t file,
                                   const dsn_file_buffer_t *buffers,
                                   int buffer_count,
                                   uint64_t offset,
                                   dsn_task_t cb)
{
    ::dsn::aio_task *callback((::dsn::aio_task *)cb);
    callback->aio()->buffer = nullptr;
    callback->aio()->buffer_size = 0;
    callback->aio()->engine = nullptr;
    callback->aio()->file = file;
    callback->aio()->file_offset = offset;
    callback->aio()->type = ::dsn::AIO_Write;
    for (int i = 0; i < buffer_count; i++) {
        callback->_unmerged_write_buffers.push_back(buffers[i]);
        callback->aio()->buffer_size += buffers[i].size;
    }

    ::dsn::task::get_current_disk()->write(callback);
}

DSN_API void dsn_file_copy_remote_directory(dsn_address_t remote,
                                            const char *source_dir,
                                            const char *dest_dir,
                                            bool overwrite,
                                            bool high_priority,
                                            dsn_task_t cb)
{
    std::shared_ptr<::dsn::remote_copy_request> rci(new ::dsn::remote_copy_request());
    rci->source = remote;
    rci->source_dir = source_dir;
    rci->files.clear();
    rci->dest_dir = dest_dir;
    rci->overwrite = overwrite;
    rci->high_priority = high_priority;

    ::dsn::aio_task *callback((::dsn::aio_task *)cb);

    ::dsn::task::get_current_nfs()->call(rci, callback);
}

DSN_API void dsn_file_copy_remote_files(dsn_address_t remote,
                                        const char *source_dir,
                                        const char **source_files,
                                        const char *dest_dir,
                                        bool overwrite,
                                        bool high_priority,
                                        dsn_task_t cb)
{
    std::shared_ptr<::dsn::remote_copy_request> rci(new ::dsn::remote_copy_request());
    rci->source = remote;
    rci->source_dir = source_dir;

    rci->files.clear();
    const char **p = source_files;
    while (*p != nullptr && **p != '\0') {
        rci->files.push_back(std::string(*p));
        p++;

        dinfo("copy remote file %s from %s", *(p - 1), rci->source.to_string());
    }

    rci->dest_dir = dest_dir;
    rci->overwrite = overwrite;
    rci->high_priority = high_priority;

    ::dsn::aio_task *callback((::dsn::aio_task *)cb);

    ::dsn::task::get_current_nfs()->call(rci, callback);
}

DSN_API size_t dsn_file_get_io_size(dsn_task_t cb_task)
{
    ::dsn::task *task = (::dsn::task *)cb_task;
    dassert(task->spec().type == TASK_TYPE_AIO,
            "invalid task_type, type = %s",
            enum_to_string(task->spec().type));
    return ((::dsn::aio_task *)task)->get_transferred_size();
}

DSN_API void dsn_file_task_enqueue(dsn_task_t cb_task, dsn_error_t err, size_t size)
{
    ::dsn::task *task = (::dsn::task *)cb_task;
    dassert(task->spec().type == TASK_TYPE_AIO,
            "invalid task_type, type = %s",
            enum_to_string(task->spec().type));

    ((::dsn::aio_task *)task)->enqueue(err, size);
}

//------------------------------------------------------------------------------
//
// env
//
//------------------------------------------------------------------------------
DSN_API uint64_t dsn_now_ns()
{
    // return ::dsn::task::get_current_env()->now_ns();
    return ::dsn::service_engine::instance().env()->now_ns();
}

DSN_API uint64_t dsn_random64(uint64_t min, uint64_t max) // [min, max]
{
    return ::dsn::service_engine::instance().env()->random64(min, max);
}

static uint64_t s_runtime_init_time_ms;
DSN_API uint64_t dsn_runtime_init_time_ms() { return s_runtime_init_time_ms; }

//------------------------------------------------------------------------------
//
// system
//
//------------------------------------------------------------------------------

static bool run(const char *config_file,
                const char *config_arguments,
                bool sleep_after_init,
                std::string &app_list);

DSN_API bool dsn_run_config(const char *config, bool sleep_after_init)
{
    std::string name;
    return run(config, nullptr, sleep_after_init, name);
}

#ifdef _WIN32
static BOOL SuspendAllThreads()
{
    std::map<uint32_t, HANDLE> threads;
    uint32_t dwCurrentThreadId = ::GetCurrentThreadId();
    uint32_t dwCurrentProcessId = ::GetCurrentProcessId();
    HANDLE hSnapshot;
    bool bChange = TRUE;

    while (bChange) {
        hSnapshot = ::CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
        if (hSnapshot == INVALID_HANDLE_VALUE) {
            derror("CreateToolhelp32Snapshot failed, err = %d", ::GetLastError());
            return FALSE;
        }

        THREADENTRY32 ti;
        ZeroMemory(&ti, sizeof(ti));
        ti.dwSize = sizeof(ti);
        bChange = FALSE;

        if (FALSE == ::Thread32First(hSnapshot, &ti)) {
            derror("Thread32First failed, err = %d", ::GetLastError());
            goto err;
        }

        do {
            if (ti.th32OwnerProcessID == dwCurrentProcessId &&
                ti.th32ThreadID != dwCurrentThreadId &&
                threads.find(ti.th32ThreadID) == threads.end()) {
                HANDLE hThread = ::OpenThread(THREAD_ALL_ACCESS, FALSE, ti.th32ThreadID);
                if (hThread == NULL) {
                    derror("OpenThread failed, err = %d", ::GetLastError());
                    goto err;
                }
                ::SuspendThread(hThread);
                ddebug("thread %d find and suspended ...", ti.th32ThreadID);
                threads.insert(std::make_pair(ti.th32ThreadID, hThread));
                bChange = TRUE;
            }
        } while (::Thread32Next(hSnapshot, &ti));

        ::CloseHandle(hSnapshot);
    }

    return TRUE;

err:
    ::CloseHandle(hSnapshot);
    return FALSE;
}
#endif

NORETURN DSN_API void dsn_exit(int code)
{
    printf("dsn exit with code %d\n", code);
    fflush(stdout);
    ::dsn::tools::sys_exit.execute(::dsn::SYS_EXIT_NORMAL);

#if defined(_WIN32)
    // TODO: do not use std::map above, coz when suspend the other threads, they may stop
    // inside certain locks which causes deadlock
    // SuspendAllThreads();
    ::TerminateProcess(::GetCurrentProcess(), code);
#else
    _exit(code);
// kill(getpid(), SIGKILL);
#endif
}

DSN_API bool dsn_mimic_app(const char *app_role, int index)
{
    auto worker = ::dsn::task::get_current_worker2();
    dassert(worker == nullptr, "cannot call dsn_mimic_app in rDSN threads");

    auto cnode = ::dsn::task::get_current_node2();
    if (cnode != nullptr) {
        const std::string &name = cnode->spec().full_name;
        if (cnode->spec().role_name == std::string(app_role) && cnode->spec().index == index) {
            return true;
        } else {
            derror("current thread is already attached to another rDSN app %s", name.c_str());
            return false;
        }
    }

    auto nodes = ::dsn::service_engine::instance().get_all_nodes();
    for (auto &n : nodes) {
        if (n.second->spec().role_name == std::string(app_role) &&
            n.second->spec().index == index) {
            ::dsn::task::set_tls_dsn_context(n.second, nullptr, nullptr);
            return true;
        }
    }

    derror("cannot find host app %s with index %d", app_role, index);
    return false;
}

//
// run the system with arguments
//   config [-cargs k1=v1;k2=v2] [-app_list app_name1@index1;app_name2@index]
// e.g., config.ini -app_list replica@1 to start the first replica as a new process
//       config.ini -app_list replica to start ALL replicas (count specified in config) as a new
//       process
//       config.ini -app_list replica -cargs replica-port=34556 to start ALL replicas with given
//       port variable specified in config.ini
//       config.ini to start ALL apps as a new process
//
DSN_API void dsn_run(int argc, char **argv, bool sleep_after_init)
{
    if (argc < 2) {
        printf(
            "invalid options for dsn_run\n"
            "// run the system with arguments\n"
            "//   config [-cargs k1=v1;k2=v2] [-app_list app_name1@index1;app_name2@index]\n"
            "// e.g., config.ini -app_list replica@1 to start the first replica as a new process\n"
            "//       config.ini -app_list replica to start ALL replicas (count specified in "
            "config) as a new process\n"
            "//       config.ini -app_list replica -cargs replica-port=34556 to start with "
            "%%replica-port%% var in config.ini\n"
            "//       config.ini to start ALL apps as a new process\n");
        dsn_exit(1);
        return;
    }

    char *config = argv[1];
    std::string config_args = "";
    std::string app_list = "";

    for (int i = 2; i < argc;) {
        if (0 == strcmp(argv[i], "-cargs")) {
            if (++i < argc) {
                config_args = std::string(argv[i++]);
            }
        }

        else if (0 == strcmp(argv[i], "-app_list")) {
            if (++i < argc) {
                app_list = std::string(argv[i++]);
            }
        } else {
            printf("unknown arguments %s\n", argv[i]);
            dsn_exit(1);
            return;
        }
    }

    if (!run(config,
             config_args.size() > 0 ? config_args.c_str() : nullptr,
             sleep_after_init,
             app_list)) {
        printf("run the system failed\n");
        dsn_exit(-1);
        return;
    }
}

namespace dsn {
namespace tools {
bool is_engine_ready() { return dsn_all.is_engine_ready(); }

tool_app *get_current_tool() { return dsn_all.tool; }
}
}

extern void dsn_log_init();
extern void dsn_core_init();

bool run(const char *config_file,
         const char *config_arguments,
         bool sleep_after_init,
         std::string &app_list)
{
    s_runtime_init_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::high_resolution_clock::now().time_since_epoch())
                                 .count();

    char init_time_buf[32];
    dsn::utils::time_ms_to_string(dsn_runtime_init_time_ms(), init_time_buf);
    ddebug(
        "dsn_runtime_init_time_ms = %" PRIu64 " (%s)", dsn_runtime_init_time_ms(), init_time_buf);

    dsn_core_init();
    ::dsn::task::set_tls_dsn_context(nullptr, nullptr, nullptr);

    dsn_all.engine_ready = false;
    dsn_all.config_completed = false;
    dsn_all.tool = nullptr;
    dsn_all.engine = &::dsn::service_engine::instance();
    dsn_all.config.reset(new ::dsn::configuration());
    dsn_all.memory = nullptr;
    dsn_all.magic = 0xdeadbeef;

    if (!dsn_all.config->load(config_file, config_arguments)) {
        printf("Fail to load config file %s\n", config_file);
        return false;
    }

    // pause when necessary
    if (dsn_all.config->get_value<bool>("core",
                                        "pause_on_start",
                                        false,
                                        "whether to pause at startup time for easier debugging")) {
#if defined(_WIN32)
        printf("\nPause for debugging (pid = %d)...\n", static_cast<int>(::GetCurrentProcessId()));
#else
        printf("\nPause for debugging (pid = %d)...\n", static_cast<int>(getpid()));
#endif
        getchar();
    }

    for (int i = 0; i <= dsn_task_code_max(); i++) {
        dsn_all.task_specs.push_back(::dsn::task_spec::get(i));
    }

    // initialize global specification from config file
    ::dsn::service_spec spec;
    if (!spec.init()) {
        printf("error in config file %s, exit ...\n", config_file);
        return false;
    }

    dsn_all.config_completed = true;

    // setup data dir
    auto &data_dir = spec.data_dir;
    dassert(!dsn::utils::filesystem::file_exists(data_dir),
            "%s should not be a file.",
            data_dir.c_str());
    if (!dsn::utils::filesystem::directory_exists(data_dir.c_str())) {
        if (!dsn::utils::filesystem::create_directory(data_dir)) {
            dassert(false, "Fail to create %s.", data_dir.c_str());
        }
    }
    std::string cdir;
    if (!dsn::utils::filesystem::get_absolute_path(data_dir.c_str(), cdir)) {
        dassert(false, "Fail to get absolute path from %s.", data_dir.c_str());
    }
    spec.data_dir = cdir;

    // setup coredump dir
    spec.dir_coredump = ::dsn::utils::filesystem::path_combine(cdir, "coredumps");
    dsn::utils::filesystem::create_directory(spec.dir_coredump);
    ::dsn::utils::coredump::init(spec.dir_coredump.c_str());

    // setup log dir
    spec.dir_log = ::dsn::utils::filesystem::path_combine(cdir, "log");
    dsn::utils::filesystem::create_directory(spec.dir_log);

    // init tools
    dsn_all.tool = ::dsn::utils::factory_store<::dsn::tools::tool_app>::create(
        spec.tool.c_str(), ::dsn::PROVIDER_TYPE_MAIN, spec.tool.c_str());
    dsn_all.tool->install(spec);

    // init app specs
    if (!spec.init_app_specs()) {
        printf("error in config file %s, exit ...\n", config_file);
        return false;
    }

    // init tool memory
    auto tls_trans_memory_KB = (size_t)dsn_all.config->get_value<int>(
        "core",
        "tls_trans_memory_KB",
        1024, // 1 MB
        "thread local transient memory buffer size (KB), default is 1024");
    ::dsn::tls_trans_mem_init(tls_trans_memory_KB * 1024);
    dsn_all.memory = ::dsn::utils::factory_store<::dsn::memory_provider>::create(
        spec.tools_memory_factory_name.c_str(), ::dsn::PROVIDER_TYPE_MAIN);

    // prepare minimum necessary
    ::dsn::service_engine::fast_instance().init_before_toollets(spec);

    // init logging
    dsn_log_init();

#ifndef _WIN32
    ddebug("init rdsn runtime, pid = %d, dsn_runtime_init_time_ms = %" PRIu64 " (%s)",
           (int)getpid(),
           dsn_runtime_init_time_ms(),
           init_time_buf);
#endif

    // init toollets
    for (auto it = spec.toollets.begin(); it != spec.toollets.end(); ++it) {
        auto tlet =
            dsn::tools::internal_use_only::get_toollet(it->c_str(), ::dsn::PROVIDER_TYPE_MAIN);
        dassert(tlet, "toolet not found");
        tlet->install(spec);
    }

    // init provider specific system inits
    dsn::tools::sys_init_before_app_created.execute();

    // TODO: register sys_exit execution

    // init runtime
    ::dsn::service_engine::fast_instance().init_after_toollets();

    dsn_all.engine_ready = true;

    // split app_name and app_index
    std::list<std::string> applistkvs;
    ::dsn::utils::split_args(app_list.c_str(), applistkvs, ';');

    // init apps
    for (auto &sp : spec.app_specs) {
        if (!sp.run)
            continue;

        bool create_it = false;

        if (app_list == "") // create all apps
        {
            create_it = true;
        } else {
            for (auto &kv : applistkvs) {
                std::list<std::string> argskvs;
                ::dsn::utils::split_args(kv.c_str(), argskvs, '@');
                if (std::string("apps.") + argskvs.front() == sp.config_section) {
                    if (argskvs.size() < 2)
                        create_it = true;
                    else
                        create_it = (std::stoi(argskvs.back()) == sp.index);
                    break;
                }
            }
        }

        if (create_it) {
            ::dsn::service_engine::fast_instance().start_node(sp);
        }
    }

    if (::dsn::service_engine::fast_instance().get_all_nodes().size() == 0) {
        printf("no app are created, usually because \n"
               "app_name is not specified correctly, should be 'xxx' in [apps.xxx]\n"
               "or app_index (1-based) is greater than specified count in config file\n");
        exit(1);
    }

    if (dsn_all.config->get_value<bool>(
            "core",
            "cli_remote",
            true,
            "whether to enable remote command line interface (using dsn.cli)")) {
        ::dsn::command_manager::instance().start_remote_cli();
    }

    // register local cli commands
    ::dsn::command_manager::instance().register_command({"config-dump"},
                                                        "config-dump - dump configuration",
                                                        "config-dump [to-this-config-file]",
                                                        [](const std::vector<std::string> &args) {
                                                            std::ostringstream oss;
                                                            std::ofstream off;
                                                            std::ostream *os = &oss;
                                                            if (args.size() > 0) {
                                                                off.open(args[0]);
                                                                os = &off;

                                                                oss << "config dump to file "
                                                                    << args[0] << std::endl;
                                                            }

                                                            dsn_all.config->dump(*os);
                                                            return oss.str();
                                                        });

    // invoke customized init after apps are created
    dsn::tools::sys_init_after_app_created.execute();

    // start the tool
    dsn_all.tool->run();

    //
    if (sleep_after_init) {
        while (true) {
            std::this_thread::sleep_for(std::chrono::hours(1));
        }
    }

    // add this to allow mimic app call from this thread.
    memset((void *)&dsn::tls_dsn, 0, sizeof(dsn::tls_dsn));

    return true;
}

namespace dsn {
configuration_ptr get_main_config() { return dsn_all.config; }
}

namespace dsn {
service_app *service_app::new_service_app(const std::string &type,
                                          const dsn::service_app_info *info)
{
    return dsn::utils::factory_store<service_app>::create(
        type.c_str(), dsn::PROVIDER_TYPE_MAIN, info);
}

service_app::service_app(const dsn::service_app_info *info) : _info(info), _started(false) {}

const service_app_info &service_app::info() const { return *_info; }

const service_app_info &service_app::current_service_app_info()
{
    return tls_dsn.node->get_service_app_info();
}

void service_app::get_all_service_apps(std::vector<service_app *> *apps)
{
    const service_nodes_by_app_id &nodes = dsn_all.engine->get_all_nodes();
    for (const auto &kv : nodes) {
        const service_node *node = kv.second;
        apps->push_back(const_cast<service_app *>(node->get_service_app()));
    }
}
}
