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

# include <stdint.h>

# ifdef __cplusplus
extern "C" {
# endif

    # define DSN_API 

    # define DSN_MAX_TASK_CODE_NAME_LENGTH 48
    # define DSN_MAX_ADDRESS_NAME_LENGTH 16

    //------------------------------------------------------------------------------
    //
    // common types
    //
    //------------------------------------------------------------------------------
    enum dsn_task_type_t
    {
        TASK_TYPE_RPC_REQUEST, 
        TASK_TYPE_RPC_RESPONSE,
        TASK_TYPE_COMPUTE,
        TASK_TYPE_AIO,
        TASK_TYPE_CONTINUATION,
        TASK_TYPE_COUNT,
        TASK_TYPE_INVALID,
    };
    typedef int dsn_error_t;
    typedef int dsn_task_code_t;
    typedef int dsn_threadpool_code_t;
    typedef unsigned long long dsn_handle_t;
    
    extern DSN_API dsn_error_t dsn_error_register(const char* name);
    extern DSN_API const char* dsn_error_to_string(dsn_error_t err);

    extern DSN_API dsn_threadpool_code_t dsn_threadpool_register(const char* name);
    extern DSN_API const char*           dsn_threadpool_to_string(dsn_threadpool_code_t pool_code);

    extern DSN_API dsn_task_code_t dsn_task_code_register(const char* name, dsn_task_type_t type, dsn_threadpool_code_t pool);
    extern DSN_API const char*     dsn_task_code_to_string(dsn_task_code_t code);
    
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
    typedef void* dsn_task_t;
    typedef void(*dsn_task_callback_t)(void*);

    extern DSN_API void        dsn_task_close(dsn_task_t task);
    extern DSN_API dsn_task_t  dsn_task_create(dsn_task_code_t code, dsn_task_callback_t cb, void* param, int hash);
    extern DSN_API dsn_task_t  dsn_task_create_timer(dsn_task_code_t code, dsn_task_callback_t cb, void* param, int hash, int interval_milliseconds);
    extern DSN_API void        dsn_task_call(dsn_task_t task, int delay_milliseconds);
    extern DSN_API bool        dsn_task_cancel(dsn_task_t task, bool wait_until_finished);
    extern DSN_API bool        dsn_task_cancel2(dsn_task_t task, bool wait_until_finished, /*out*/ bool* finished);
    extern DSN_API bool        dsn_task_wait(dsn_task_t task); 
    extern DSN_API bool        dsn_task_wait_timeout(dsn_task_t task, int timeout_milliseconds);
    extern DSN_API dsn_error_t dsn_task_error(dsn_task_t task);

    //------------------------------------------------------------------------------
    //
    // synchronization - concurrent access and coordination among threads
    //
    //------------------------------------------------------------------------------
    extern DSN_API dsn_handle_t dsn_exlock_create();
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
    extern DSN_API bool         dsn_semaphore_wait_timeout(dsn_handle_t s, int timeout_milliseconds);

    //------------------------------------------------------------------------------
    //
    // rpc
    //
    //------------------------------------------------------------------------------
    typedef struct dsn_address_t
    {
        uint32_t ip;
        uint16_t port;
        char     name[DSN_MAX_ADDRESS_NAME_LENGTH];
    } dsn_address_t;
    
    typedef struct dsn_buffer_t
    {
        size_t length;
        char   *buffer;
    } dsn_buffer_t;

    typedef struct dsn_message_header
    {
        int32_t       hdr_crc32;
        int32_t       body_crc32;
        int32_t       body_length;
        int32_t       version;
        uint64_t      id;
        uint64_t      rpc_id;
        char          rpc_name[DSN_MAX_TASK_CODE_NAME_LENGTH];

        // info from client => server
        union
        {
            struct
            {
                int32_t  timeout_ms;
                int32_t  hash;
                uint16_t port;
            } client;

            struct
            {
                int32_t  error;
            } server;
        };

        // local fields - no need to be transmitted
        dsn_address_t from_address;
        dsn_address_t to_address;
        uint16_t      local_rpc_code;
    } dsn_message_header;

    # define DSN_MSG_HDR_SERIALIZED_SIZE \
        (static_cast<int>((((size_t)&((dsn_message_header *)(10))->from_address) - 10)))

    typedef struct dsn_message_t dsn_message_t;
    typedef void* (*dsn_buffer_allocator)(size_t size);
    typedef void(*dsn_buffer_deallocator)(void* buffer);
    typedef void(*dsn_rpc_request_handler_t)(dsn_message_t*, void*);
    typedef void(*dsn_rpc_response_handler_t)(dsn_error_t, dsn_message_t*, dsn_message_t*, void*);

    struct dsn_message_t
    {
        dsn_message_header     hdr;
        int                    buffer_count; // <= 64
        dsn_buffer_t           buffers[64];
        dsn_buffer_deallocator deallocator;
    };

    //
    // rpc message and internal buffer management in rDSN
    //-----------------------------------------------------
    // Goals:
    //    * High performance: zero-copy and re-usable memory blocks
    //      between app and rDSN network stack, and across many RPC calls;
    //    * Flexibility: customizable buffer allocation and deallocation.
    //     
    // Buffer management:
    //    * buffers in send messages are allocated and set by upper apps, 
    //      together with *dsn_message_t::dealloator*;
    //    * buffers in recv messages are allocated by rDSN using allocators
    //      specified by *dsn_rpc_set_buffer_management* or using ::malloc;
    //    * buffers in dsn_message_t is deallocated 
    //     (by calling *dsn_message_t::dealloator* or using default deallocator
    //      as specified by *dsn_rpc_set_buffer_management* or using ::free),
    //      when the hosting message is released. Since customizable deallocator
    //      is allowed, it is possible to reuse the buffers across messages.
    //
    // Message management:
    //    * send messages are created by upper apps calling *dsn_rpc_create_xxx";
    //    * recv messages are allocated by rDSN implicitly;
    //    * for messages visible to upper app callbacks (i.e., rpc request handler
    //      *dsn_rpc_request_handler_t* on rpc server and rpc response handler 
    //      *dsn_rpc_response_handler_t*, upper apps in charge of calling
    //      *dsn_rpc_release_message* to release the messages, in case they want to
    //      reuse the message for other purposes, e.g., forwarding it to others;
    //      for other cases, rDSN call it automatically (e.g. for all sent messages
    //      or recved but time-out messages).
    //

    // rpc address utilities
    extern DSN_API dsn_address_t  dsn_address_invalid;
    extern DSN_API void           dsn_address_build(/*out*/ dsn_address_t* ep, const char* host, uint16_t port);
    extern DSN_API dsn_address_t  dsn_rpc_primary_address();
    
    // rpc message and buffer management
    extern DSN_API dsn_message_t* dsn_rpc_create_request(dsn_task_code_t rpc_code, int timeout_milliseconds, int hash);
    extern DSN_API dsn_message_t* dsn_rpc_create_response(dsn_message_t* request);
    extern DSN_API void           dsn_rpc_release_message(dsn_message_t* msg);
    extern DSN_API void           dsn_rpc_set_buffer_management(dsn_buffer_allocator allocator, dsn_buffer_deallocator deallocator);
    
    // rpc calls
    extern DSN_API bool           dsn_rpc_register_handler(dsn_task_code_t code, const char* name, dsn_rpc_request_handler_t cb, void* param);
    extern DSN_API void*          dsn_rpc_unregiser_handler(dsn_task_code_t code);   // return void* param on registration  
    extern DSN_API dsn_task_t     dsn_rpc_create(dsn_message_t* request, dsn_rpc_response_handler_t cb, void* param, int reply_hash);
    extern DSN_API void           dsn_rpc_call(dsn_address_t server, dsn_task_t rpc_call);
    extern DSN_API dsn_message_t* dsn_rpc_call_wait(dsn_address_t server, dsn_message_t* request);
    extern DSN_API void           dsn_rpc_call_one_way(dsn_address_t server, dsn_message_t* request);
    extern DSN_API void           dsn_rpc_reply(dsn_message_t* response);
    extern DSN_API dsn_message_t* dsn_rpc_get_response(dsn_task_t rpc_call);
    extern DSN_API void           dsn_rpc_enqueue_response(dsn_task_t rpc_call, dsn_error_t err, dsn_message_t* response);

    //------------------------------------------------------------------------------
    //
    // file operations
    //
    //------------------------------------------------------------------------------
    typedef void(*dsn_file_callback_t)(dsn_error_t, size_t, void*);

    extern DSN_API dsn_handle_t dsn_file_open(const char* file_name, int flag, int pmode);
    extern DSN_API void         dsn_file_close(dsn_handle_t file);
    extern DSN_API dsn_task_t   dsn_file_create_callback_task(dsn_task_code_t code, dsn_file_callback_t cb, void* param, int hash);
    extern DSN_API void         dsn_file_read(dsn_handle_t file, char* buffer, int count, uint64_t offset, dsn_task_t cb);
    extern DSN_API void         dsn_file_write(dsn_handle_t file, const char* buffer, int count, uint64_t offset, dsn_task_t cb);
    extern DSN_API void         dsn_file_copy_remote_directory(dsn_address_t remote, const char* source_dir, const char* dest_dir, bool overwrite, dsn_task_t cb);
    extern DSN_API void         dsn_file_copy_remote_files(dsn_address_t remote, const char* source_dir, const char** source_files, const char* dest_dir, bool overwrite, dsn_task_t cb);
    extern DSN_API size_t       dsn_file_get_io_size(dsn_task_t cb_task);
    extern DSN_API void         dsn_file_task_enqueue(dsn_task_t cb_task, dsn_error_t err, size_t size);

    //------------------------------------------------------------------------------
    //
    // env
    //
    //------------------------------------------------------------------------------
    extern DSN_API uint64_t dsn_env_now_ns();
    extern DSN_API uint64_t dsn_env_random64(uint64_t min, uint64_t max); // [min, max]

    inline uint64_t dsn_env_now_us() { return dsn_env_now_ns() / 1000; }
    inline uint64_t dsn_env_now_ms() { return dsn_env_now_ns() / 1000000; }
    inline uint32_t dsn_env_random32(uint32_t min, uint32_t max) { return static_cast<uint32_t>(dsn_env_random64(min, max)); }
    inline double   dsn_env_probability() { return static_cast<double>(dsn_env_random64(0, 1000000000)) / 1000000000.0; }

    //------------------------------------------------------------------------------
    //
    // system
    //
    //------------------------------------------------------------------------------
    typedef void (*dsn_app_start)(int, char**); // int argc, char** argv
    typedef void(*dsn_app_stop)(bool); // bool cleanup
    
    extern DSN_API bool dsn_register_app(const char* name, dsn_app_start start, dsn_app_stop stop);
    extern DSN_API bool dsn_run_with_config(const char* config, bool sleep_after_init);

    //
    // run the system with arguments
    //   config [-cargs k1=v1;k2=v2] [-app app_name] [-app_index index]
    // e.g., config.ini -app replica -app_index 1 to start the first replica as a new process
    //       config.ini -app replica to start ALL replicas (count specified in config) as a new process
    //       config.ini -app replica -cargs replica-port=34556 to start ALL replicas with given port variable specified in config.ini
    //       config.ini to start ALL apps as a new process
    //
    extern DSN_API void dsn_run(int argc, char** argv, bool sleep_after_init);
    
# ifdef __cplusplus
}
# endif
