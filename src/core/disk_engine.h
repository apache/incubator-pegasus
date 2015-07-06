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

# include "service_engine.h"
# include <dsn/internal/synchronize.h>
# include <dsn/internal/aio_provider.h>

namespace dsn {

class disk_engine
{
public:
    disk_engine(service_node* node);
    ~disk_engine();

    void start(aio_provider* provider);

    // asynchonous file read/write
    handle_t        open(const char* fileName, int flag, int pmode);
    error_code      close(handle_t hFile);
    void            read(aio_task_ptr& aio);
    void            write(aio_task_ptr& aio);  

    disk_aio_ptr    prepare_aio_context(aio_task* tsk) { return _provider->prepare_aio_context(tsk); }
    service_node*   node() const { return _node; }
    
private:
    friend class aio_provider;
    void start_io(aio_task_ptr& aio);
    void complete_io(aio_task_ptr& aio, error_code err, uint32_t bytes, int delay_milliseconds = 0);

private:
    bool           _is_running;
    aio_provider    *_provider;
    service_node    *_node;

    ::dsn::utils::ex_lock_nr _lock;
    int                      _request_count;
};

} // end namespace
