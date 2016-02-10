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
 *     the tracer toollets traces all the asynchonous execution flow
 *     in the system through the join-point mechanism
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/c/api_common.h>

# ifdef __cplusplus
extern "C" {
# endif


//------------------------------------------------------------------------------
//
// thread synchronization
//
//------------------------------------------------------------------------------
extern DSN_API dsn_handle_t dsn_exlock_create(bool recursive);
extern DSN_API void         dsn_exlock_destroy(dsn_handle_t l);
extern DSN_API void         dsn_exlock_lock(dsn_handle_t l);
extern DSN_API bool         dsn_exlock_try_lock(dsn_handle_t l);
extern DSN_API void         dsn_exlock_unlock(dsn_handle_t l);

// non-recursive rwlock
extern DSN_API dsn_handle_t dsn_rwlock_nr_create();
extern DSN_API void         dsn_rwlock_nr_destroy(dsn_handle_t l);
extern DSN_API void         dsn_rwlock_nr_lock_read(dsn_handle_t l);
extern DSN_API void         dsn_rwlock_nr_unlock_read(dsn_handle_t l);
extern DSN_API void         dsn_rwlock_nr_lock_write(dsn_handle_t l);
extern DSN_API void         dsn_rwlock_nr_unlock_write(dsn_handle_t l);

extern DSN_API dsn_handle_t dsn_semaphore_create(int initial_count);
extern DSN_API void         dsn_semaphore_destroy(dsn_handle_t s);
extern DSN_API void         dsn_semaphore_signal(dsn_handle_t s, int count);
extern DSN_API void         dsn_semaphore_wait(dsn_handle_t s);
extern DSN_API bool         dsn_semaphore_wait_timeout(
                                dsn_handle_t s, 
                                int timeout_milliseconds
                                );

//------------------------------------------------------------------------------
//
// rpc
//
//------------------------------------------------------------------------------

// rpc address utilities
extern DSN_API uint32_t      dsn_ipv4_from_host(const char* name);

extern DSN_API uint32_t      dsn_ipv4_local(const char* network_interface);

extern DSN_API dsn_address_t dsn_address_build(
                                const char* host, 
                                uint16_t port
                                );
extern DSN_API dsn_address_t dsn_address_build_ipv4(
                                uint32_t ipv4,
                                uint16_t port
                                );
extern DSN_API dsn_address_t dsn_address_build_group(
                                dsn_group_t g
                                );
extern DSN_API dsn_address_t dsn_address_build_uri(
                                dsn_uri_t uri
                                );

extern DSN_API const char*   dsn_address_to_string(dsn_address_t addr);

extern DSN_API dsn_uri_t     dsn_uri_build(const char* url); // must be paired with destroy later
extern DSN_API void          dsn_uri_destroy(dsn_uri_t uri);

extern DSN_API dsn_group_t   dsn_group_build(const char* name); // must be paired with release later
extern DSN_API bool          dsn_group_add(dsn_group_t g, dsn_address_t ep);
extern DSN_API bool          dsn_group_remove(dsn_group_t g, dsn_address_t ep);
extern DSN_API void          dsn_group_set_leader(dsn_group_t g, dsn_address_t ep);
extern DSN_API dsn_address_t dsn_group_get_leader(dsn_group_t g);
extern DSN_API bool          dsn_group_is_leader(dsn_group_t g, dsn_address_t ep);
extern DSN_API bool          dsn_group_is_update_leader_on_rpc_forward(dsn_group_t g);
extern DSN_API void          dsn_group_set_update_leader_on_rpc_forward(dsn_group_t g, bool v);
extern DSN_API dsn_address_t dsn_group_next(dsn_group_t g, dsn_address_t ep);
extern DSN_API dsn_address_t dsn_group_forward_leader(dsn_group_t g);
extern DSN_API void          dsn_group_destroy(dsn_group_t g);

extern DSN_API dsn_address_t dsn_primary_address();

// rpc message and buffer management
//
// all returned dsn_message_t are NOT add_ref by rDSN, 
// so you do not need to call msg_release_ref to release the msgs.
// the decision is made for easier programming, and you may consider the later
// dsn_rpc_xxx calls do the resource gc work for you.
// however, if you want to hold the message further after call dsn_rpc_xxx,
// you need to call dsn_msg_add_ref first before these operations, 
// and call dsn_msg_release_ref later to ensure there is no memory leak.
// This is very similar to what we have above with task handles.
//
// however, this is not true for returned message from
// dsn_rpc_call_wait and dsn_rpc_get_response. For these two cases,
// developers are responsible for releasing the message handle
// by calling dsn_msg_release_ref.
//
// similarily, for all msgs accessable in callbacks, if you want to hold them in 
// upper apps further beyond the callbacks, 
// you need to call msg_add_ref, and msg_release_ref explicitly.
//
// when timeout_milliseconds == 0,  [task.%rpc_code%] rpc_timeout_milliseconds is used.
// 
extern DSN_API void*         dsn_transient_malloc(uint32_t size);
extern DSN_API void          dsn_transient_free(void* ptr);
extern DSN_API dsn_message_t dsn_msg_create_request(
                                dsn_task_code_t rpc_code, 
                                int timeout_milliseconds DEFAULT(0),
                                int hash DEFAULT(0)
                                );
extern DSN_API dsn_message_t dsn_msg_create_response(dsn_message_t request);
extern DSN_API dsn_message_t dsn_msg_copy(dsn_message_t msg);
extern DSN_API void          dsn_msg_add_ref(dsn_message_t msg);
extern DSN_API void          dsn_msg_release_ref(dsn_message_t msg);
extern DSN_API void          dsn_msg_set_options(
                                dsn_message_t msg,
                                dsn_msg_options_t *opts,
                                uint32_t mask // set opt bits using DSN_MSGM_XXX
                                );

extern DSN_API void         dsn_msg_get_options(
                                dsn_message_t msg,
                                /*out*/ dsn_msg_options_t* opts
                                );

// apps write rpc message as follows:
//   void* ptr;
//   size_t size;
//   dsn_msg_write_next(msg, &ptr, &size, min_size);
//   write msg content to [ptr, ptr + size)
//   dsn_msg_write_commit(msg, real_written_size);
//
// allocate a buffer for message write
// - *ptr returns the writable memory pointer
// - *size returns the writable memory buffer size,
//   which can be larger than min_size
extern DSN_API void          dsn_msg_write_next(
                                dsn_message_t msg, 
                                /*out*/ void** ptr, 
                                /*out*/ size_t* size, 
                                size_t min_size
                                );
// commit the write buffer after the message content is written
extern DSN_API void          dsn_msg_write_commit(dsn_message_t msg, size_t size);

// apps read rpc message as follows:
//   void* ptr;
//   size_t size;
//   dsn_msg_read_next(msg, &ptr, &size);
//   read msg content in [ptr, ptr + size)
//   dsn_msg_read_commit(msg, real read size);
// if not committed, next dsn_msg_read_next returns the same read buffer
extern DSN_API bool          dsn_msg_read_next(
                                dsn_message_t msg, 
                                /*out*/ void** ptr, 
                                /*out*/ size_t* size
                                );
extern DSN_API void          dsn_msg_read_commit(dsn_message_t msg, size_t size);

extern DSN_API size_t        dsn_msg_body_size(dsn_message_t msg);
extern DSN_API void*         dsn_msg_rw_ptr(dsn_message_t msg, size_t offset_begin);
extern DSN_API dsn_address_t dsn_msg_from_address(dsn_message_t msg);
extern DSN_API dsn_address_t dsn_msg_to_address(dsn_message_t msg);
extern DSN_API uint64_t      dsn_msg_rpc_id(dsn_message_t msg);

//
// server-side rpc calls
//
extern DSN_API bool          dsn_rpc_register_handler(
                                dsn_task_code_t code, 
                                const char* name,
                                dsn_rpc_request_handler_t cb, 
                                void* context
                                );

// return void* context on dsn_rpc_register_handler  
extern DSN_API void*         dsn_rpc_unregiser_handler(
                                dsn_task_code_t code
                                );

// reply with a response which is created using dsn_msg_create_response
extern DSN_API void          dsn_rpc_reply(dsn_message_t response);

// forward the request to another server instead
extern DSN_API void          dsn_rpc_forward(dsn_message_t request, dsn_address_t addr);


//
// client-side rpc calls
//
// create a callback task to be used in dsn_rpc_call (@rpc-call)
extern DSN_API dsn_task_t    dsn_rpc_create_response_task(
                                dsn_message_t request, 
                                dsn_rpc_response_handler_t cb, 
                                void* context, 
                                int reply_hash DEFAULT(0),
                                dsn_task_tracker_t tracker DEFAULT(nullptr)
                                );
extern DSN_API dsn_task_t    dsn_rpc_create_response_task_ex(
                                dsn_message_t request, 
                                dsn_rpc_response_handler_t cb, 
                                dsn_task_cancelled_handler_t on_cancel,
                                void* context, 
                                int reply_hash DEFAULT(0),
                                dsn_task_tracker_t tracker DEFAULT(nullptr)
                                );

// tracker can be empty
extern DSN_API void          dsn_rpc_call(
                                dsn_address_t server,
                                dsn_task_t rpc_call
                                );

// WARNING: returned msg must be explicitly msg_release_ref
extern DSN_API dsn_message_t dsn_rpc_call_wait(
                                dsn_address_t server, 
                                dsn_message_t request
                                );
extern DSN_API void          dsn_rpc_call_one_way(
                                dsn_address_t server, 
                                dsn_message_t request
                                );

// WARNING: returned msg must be explicitly msg_release_ref
extern DSN_API dsn_message_t dsn_rpc_get_response(dsn_task_t rpc_call);

// this is to mimic a response is received when no real rpc is called
extern DSN_API void          dsn_rpc_enqueue_response(
                                dsn_task_t rpc_call, 
                                dsn_error_t err, 
                                dsn_message_t response
                                );

//------------------------------------------------------------------------------
//
// file operations
//
//------------------------------------------------------------------------------

typedef struct
{
    void* buffer;
    int size;
}dsn_file_buffer_t;

// return nullptr if open failed
extern DSN_API dsn_handle_t dsn_file_open(
                                const char* file_name, 
                                int flag, 
                                int pmode
                                );
extern DSN_API dsn_error_t  dsn_file_close(
                                dsn_handle_t file
                                );
extern DSN_API dsn_error_t  dsn_file_flush(
                                dsn_handle_t file
                                );
// native handle: HANDLE for windows, int for non-windows
extern DSN_API void*        dsn_file_native_handle(dsn_handle_t file);
extern DSN_API dsn_task_t   dsn_file_create_aio_task(
                                dsn_task_code_t code, 
                                dsn_aio_handler_t cb, 
                                void* context,
                                int hash DEFAULT(0),
                                dsn_task_tracker_t tracker DEFAULT(nullptr)
                                );
extern DSN_API dsn_task_t   dsn_file_create_aio_task_ex(
                                dsn_task_code_t code, 
                                dsn_aio_handler_t cb, 
                                dsn_task_cancelled_handler_t on_cancel,
                                void* context,
                                int hash DEFAULT(0),
                                dsn_task_tracker_t tracker DEFAULT(nullptr)
                                );
extern DSN_API void         dsn_file_read(
                                dsn_handle_t file, 
                                char* buffer, 
                                int count, 
                                uint64_t offset, 
                                dsn_task_t cb
                                );
extern DSN_API void         dsn_file_write(
                                dsn_handle_t file, 
                                const char* buffer, 
                                int count, 
                                uint64_t offset, 
                                dsn_task_t cb
                                );
extern DSN_API void         dsn_file_write_vector(
                                dsn_handle_t file,
                                const dsn_file_buffer_t* buffers,
                                int buffer_count,
                                uint64_t offset,
                                dsn_task_t cb
                                );
extern DSN_API void         dsn_file_copy_remote_directory(
                                dsn_address_t remote, 
                                const char* source_dir, 
                                const char* dest_dir,
                                bool overwrite, 
                                dsn_task_t cb
                                );
extern DSN_API void         dsn_file_copy_remote_files(
                                dsn_address_t remote,
                                const char* source_dir, 
                                const char** source_files, 
                                const char* dest_dir, 
                                bool overwrite, 
                                dsn_task_t cb
                                );
extern DSN_API size_t       dsn_file_get_io_size(dsn_task_t cb_task);
extern DSN_API void         dsn_file_task_enqueue(
                                dsn_task_t cb_task, 
                                dsn_error_t err, 
                                size_t size
                                );

//------------------------------------------------------------------------------
//
// environment inputs
//
//------------------------------------------------------------------------------
extern DSN_API uint64_t dsn_now_ns();
extern DSN_API uint64_t dsn_random64(uint64_t min, uint64_t max); // [min, max]

__inline uint64_t dsn_now_us() { return dsn_now_ns() / 1000; }
__inline uint64_t dsn_now_ms() { return dsn_now_ns() / 1000000; }

__inline uint32_t dsn_random32(uint32_t min, uint32_t max)
{
    return (uint32_t)(dsn_random64(min, max)); 
}

__inline double   dsn_probability()
{
    return (double)(dsn_random64(0, 1000000000)) / 1000000000.0; 
}


# ifdef __cplusplus
}
# endif