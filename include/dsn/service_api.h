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

# include <dsn/internal/task.h>
# include <dsn/internal/service_app.h>
# include <dsn/internal/zlocks.h>
# include <dsn/internal/callocator.h>

namespace dsn { namespace service {

namespace tasking
{
    //
    // put a task into the thread pools for running
    //
    inline void enqueue(task_ptr& task, int delay_milliseconds = 0)
    { 
        if (delay_milliseconds > 0)
        {
            task->set_delay(delay_milliseconds);
        }           
        task->enqueue(); 
    }

    //
    // cancel a task
    // return - whether this cancel succeed
    //
    inline bool cancel(task_ptr& task, bool wait_until_finished)
    {
        return task->cancel(wait_until_finished);
    }

    //
    // wait a task to be completed
    //
    inline bool wait(task_ptr& task, int timeout_milliseconds = TIME_MS_MAX)
    {
        return task->wait(timeout_milliseconds);
    }
}

namespace rpc
{
    //
    // first listening address for the current node
    // !!! must invoked inside task to know the current node
    //
    extern const end_point& primary_address();

    //
    // register a rpc handler for the given code, usually invoked
    // automatically by the code generators
    // !!! must invoked inside task to know the current node
    //
    extern bool register_rpc_handler(task_code code, const char* name, rpc_server_handler* handler);

    //
    // unregister a rpc handler for the given code, usually invoked
    // automatically by the code generators
    // !!! must invoked inside task to know the current node
    //
    extern bool unregister_rpc_handler(task_code code);

    //
    // send response message (destination known in message)
    //
    extern void reply(message_ptr& response);

    //
    // call a RPC
    //   developers can always call callback::wait for synchronous calls
    //
    extern void                  call(const end_point& server, message_ptr& request, rpc_response_task_ptr& callback);
    extern rpc_response_task_ptr call(const end_point& server, message_ptr& request); // return callback

    //
    // one way RPC call, no need to expect a return response value
    //
    extern void call_one_way(const end_point& server, message_ptr& request);
}

namespace file
{
    //
    // open a file with the given flag and permission mode
    // - example: open(path, O_RDWR | O_CREAT | O_BINARY, 0666);
    //
    extern handle_t open(const char* file_name, int flag, int pmode = 0666);

    //
    // asynchonous read with callback invoked when the read operation is completed
    // an error code and read size is transmitted to the callback
    // to performance synchronous read, simply call:
    //    callback->wait();
    //    auto err = callback->error();
    //    auto size = callback->get_transferred_size();
    //
    extern void read(handle_t hFile, char* buffer, int count, uint64_t offset, aio_task_ptr& callback);

    //
    // asynchronous write, similar to read above
    //
    extern void write(handle_t hFile, const char* buffer, int count, uint64_t offset, aio_task_ptr& callback); 

    //
    // close the file handle
    //
    extern error_code close(handle_t hFile);

    //
    // copy remote files to local
    //  - source_dir: dir on remote machine (e.g., d:\test)
    //  - files: files in the source dir (e.g., test1.txt for d:\test\test1.txt), 
    //           empty for all files under source dir
    //  - dest_dir: destination dir on local machine
    //  - overwrite: whether to overwrite the local file if it already exsits, error otherwise
    //  - callback: invoked when all files are copied to local, or any error happens in the process
    //
    extern void copy_remote_files(
        const end_point& remote,
        const std::string& source_dir,
        std::vector<std::string>& files,  // empty for all
        const std::string& dest_dir,
        bool overwrite,
        aio_task_ptr& callback
        );
}

namespace env
{
    // since Epoch (1970-01-01 00:00:00 +0000 (UTC))
    extern uint64_t now_ns();

    // generate random number [min, max]
    extern uint64_t random64(uint64_t min, uint64_t max);

    inline uint64_t now_us() { return now_ns() / 1000; }
    inline uint64_t now_ms() { return now_ns() / 1000000; }
    inline uint32_t random32(uint32_t min, uint32_t max) { return static_cast<uint32_t>(random64(min, max)); }
    inline double   probability() { return static_cast<double>(random32(0, 1000000000)) / 1000000000.0; }
}

//
// in case the apps need to use a dedicated memory allocator
// e.g., when required by a replay tool to ensure deterministic memory
// allocation/deallocation results
//
namespace memory
{
    extern void* allocate(size_t sz);
    extern void* reallocate(void* ptr, size_t sz);
    extern void  deallocate(void* ptr);

    template <typename T>
    using sallocator = ::dsn::callocator<T, allocate, deallocate>;

    using sallocator_object = callocator_object<allocate, deallocate>;
}

namespace system
{
    //
    // run the system with the given configuration file
    //
    extern bool run(const char* config, bool sleep_after_init);
    
    //
    // run the system with arguments
    //   config [app_name [app_index]]
    // e.g., config.ini replica 1 to start the first replica as a new process
    //       config.ini replica to start ALL replicas (count specified in config) as a new process
    //       config.ini to start ALL apps as a new process
    //
    extern void run(int argc, char** argv, bool sleep_after_init);

    //
    //  whether the service API is ready to be called
    //  usually used by tool developers though
    //
    extern bool is_ready();
    
    namespace internal_use_only
    {
        extern bool register_service(const char* name, service_app_factory factory);
    }

    //
    // regiser a serivce app role
    //
    template<typename T> bool register_service(const char* name)
    {
        return internal_use_only::register_service(name, service_app::create<T>);
    }

    //
    // get the configuration object
    //
    extern configuration_ptr config();

    //
    // get current app
    // - must be called inside a task to get the current node/app
    //
    extern service_app* get_current_app();

    //
    // get all serivce app instances
    // usually used by a tool
    //
    extern const std::map<std::string, service_app*>& get_all_apps();

    // get service spec
    extern const service_spec& spec();
}

}} // end namespace dsn::service
