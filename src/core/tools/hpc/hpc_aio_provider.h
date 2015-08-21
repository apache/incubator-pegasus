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

# include <dsn/tool_api.h>
# include <dsn/internal/synchronize.h>
# include "io_looper.h"

# ifdef __linux__
# include <libaio.h>
# endif

namespace dsn {
    namespace tools {
        class hpc_aio_provider : public aio_provider
        {
        public:
            hpc_aio_provider(disk_engine* disk, aio_provider* inner_provider);
            ~hpc_aio_provider();

            virtual dsn_handle_t open(const char* file_name, int flag, int pmode);
            virtual error_code   close(dsn_handle_t hFile);
            virtual void         aio(aio_task* aio);            
            virtual disk_aio*    prepare_aio_context(aio_task* tsk);

        protected:
            error_code aio_internal(aio_task* aio, bool async, __out_param uint32_t* pbytes = nullptr);
            io_looper* get_looper() { return _looper ? _looper : dynamic_cast<io_looper*>(task::get_current_worker()); }

        private:
            class hpc_aio_io_loop_callback : public io_loop_callback
            {
            public:
                hpc_aio_io_loop_callback(hpc_aio_provider* provider)
                {
                    _provider = provider;
                }

                virtual void handle_event(int native_error, uint32_t io_size, uintptr_t lolp_or_events) override;

            private:
                hpc_aio_provider *_provider;
            };

        private:            
            io_looper *_looper;
            hpc_aio_io_loop_callback _callback;

# ifdef __linux__
            void on_aio_completed(uint32_t events);
            void complete_aio(struct iocb* io, int bytes, int err);

            io_context_t _ctx;
            int          _event_fd;
            bool         _event_fd_registered;
# endif 
        };
    }
}
