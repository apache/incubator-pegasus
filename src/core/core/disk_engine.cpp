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
# include "disk_engine.h"
# include <dsn/internal/perf_counters.h>
# include <dsn/internal/aio_provider.h>
# include <dsn/cpp/utils.h>
# include "transient_memory.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "disk_engine"

using namespace dsn::utils;

namespace dsn {

DEFINE_TASK_CODE_AIO(LPC_AIO_BATCH_WRITE, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

//----------------- disk_file ------------------------
dlink* disk_write_queue::unlink_next_workload(dlink& hdr, void* plength)
{
    dlink* to = hdr.next();
    dlink* current = to;
    uint64_t next_offset;
    uint32_t& sz = *(uint32_t*)plength;
    sz = 0;

    while (current != &hdr)
    {
        aio_task* tsk = CONTAINING_RECORD(current, aio_task, _task_queue_dl);
        auto io = tsk->aio();
        if (sz == 0)
        {
            sz = io->buffer_size;
            next_offset = io->file_offset + sz;
        }
        else
        {
            // batch condition
            if (next_offset == io->file_offset
                && sz + io->buffer_size <= _max_batch_bytes)
            {
                sz += io->buffer_size;
                next_offset += io->buffer_size;
                to = current;
            }

            // no batch is possible
            else
            {
                break;
            }
        }

        // continue next
        current = current->next();
    }

    return hdr.range_remove(to);
}

disk_file::disk_file(dsn_handle_t handle)
    : _handle(handle)
{

}

void disk_file::ctrl(dsn_ctrl_code_t code, int param)
{

}

dlink* disk_file::read(aio_task* tsk)
{
    tsk->add_ref(); // release on completion
    return _read_queue.add_work(&tsk->_task_queue_dl, nullptr);
}

dlink* disk_file::write(aio_task* tsk, void* ctx)
{
    tsk->add_ref(); // release on completion
    return _write_queue.add_work(&tsk->_task_queue_dl, ctx);
}

dlink* disk_file::on_read_completed(dlink* wk, error_code err, size_t size)
{
    dassert(wk->is_alone(), "");
    auto ret = _read_queue.on_work_completed(wk, nullptr);
    auto aio = CONTAINING_RECORD(wk, aio_task, _task_queue_dl);
    aio->enqueue(err, size);
    aio->release_ref(); // added in above read

    return ret;
}

dlink* disk_file::on_write_completed(dlink* wk, void* ctx, error_code err, size_t size)
{
    auto ret = _write_queue.on_work_completed(wk, ctx);
    auto tail = wk;
    
    while (true)
    {
        auto wk2 = wk->next();
        wk->remove();

        auto aio = CONTAINING_RECORD(wk, aio_task, _task_queue_dl);
        if (err == ERR_OK)
        {
            aio->enqueue(err, (size_t)aio->aio()->buffer_size);
            size -= (size_t)aio->aio()->buffer_size;
        }
        else
        {
            aio->enqueue(err, size);
        }

        aio->release_ref(); // added in above write

        if (wk == wk2)
            break;
        else
            wk = wk2;
    }

    return ret;
}

//----------------- disk_engine ------------------------
disk_engine::disk_engine(service_node* node)
{
    _is_running = false;    
    _provider = nullptr;
    _node = node;        
}

disk_engine::~disk_engine()
{
}

void disk_engine::start(aio_provider* provider, io_modifer& ctx)
{
    if (_is_running)
        return;  

    _provider = provider;
    _provider->start(ctx);
    _is_running = true;
}

void disk_engine::ctrl(dsn_handle_t fh, dsn_ctrl_code_t code, int param)
{
    if (nullptr == fh)
        return;

    auto df = (disk_file*)fh;
    df->ctrl(code, param);
}

dsn_handle_t disk_engine::open(const char* file_name, int flag, int pmode)
{            
    dsn_handle_t nh = _provider->open(file_name, flag, pmode);
    if (nh != nullptr)
    {
        return new disk_file(nh);
    }
    else
    {
        return nullptr;
    }   
}

error_code disk_engine::close(dsn_handle_t fh)
{
    if (nullptr != fh)
    {
        auto df = (disk_file*)fh;
        auto ret = _provider->close(df->native_handle());
        delete df;
        return ret;
    }
    else
    {
        return ERR_INVALID_HANDLE;
    }   
}

void disk_engine::read(aio_task* aio)
{
    if (!_is_running)
    {
        aio->enqueue(ERR_SERVICE_NOT_FOUND, 0);
        return;
    }

    if (!aio->spec().on_aio_call.execute(task::get_current_task(), aio, true))
    {
        aio->enqueue(ERR_FILE_OPERATION_FAILED, 0);
        return;
    }

    auto dio = aio->aio();
    auto df = (disk_file*)dio->file;
    dio->file = df->native_handle();
    dio->file_object = df;
    dio->engine = this;
    dio->type = AIO_Read;

    auto wk = df->read(aio);
    if (wk)
    {
        aio = CONTAINING_RECORD(wk, aio_task, _task_queue_dl);
        return _provider->aio(aio);
    }
}

class batch_write_io_task : public aio_task
{
public:
    batch_write_io_task(dlink* tasks, blob& buffer)
        : aio_task(LPC_AIO_BATCH_WRITE, nullptr, tasks)
    {
        _buffer = buffer;
    }
    
    virtual void exec() override
    {
        dlink* tasks = (dlink*)_param;
        auto faio = CONTAINING_RECORD(tasks, aio_task, _task_queue_dl);
        auto df = (disk_file*)faio->aio()->file_object;
        uint32_t sz;

        auto wk = df->on_write_completed(tasks, (void*)&sz, error(), _transferred_size);
        if (wk)
        {
            faio->aio()->engine->process_write(wk, sz);
        }
    }

public:
    dlink*       _tasks;
    blob         _buffer;
};

void disk_engine::write(aio_task* aio)
{
    if (!_is_running)
    {
        aio->enqueue(ERR_SERVICE_NOT_FOUND, 0);
        return;
    }

    if (!aio->spec().on_aio_call.execute(task::get_current_task(), aio, true))
    {
        aio->enqueue(ERR_FILE_OPERATION_FAILED, 0);
        return;
    }

    auto dio = aio->aio();
    auto df = (disk_file*)dio->file;
    dio->file = df->native_handle();
    dio->file_object = df;
    dio->engine = this;
    dio->type = AIO_Write;    

    uint32_t sz;
    auto wk = df->write(aio, &sz);
    if (wk)
    {
        process_write(wk, sz);
    }
}

void disk_engine::process_write(dlink* wk, uint32_t sz)
{
    auto aio = CONTAINING_RECORD(wk, aio_task, _task_queue_dl);

    // no batching
    if (aio->aio()->buffer_size == sz)
    {
        return _provider->aio(aio);
    }

    // batching
    else
    {
        // merge the buffers
        auto bb = tls_trans_mem_alloc_blob((size_t)sz);
        char* ptr = (char*)bb.data();
        auto current_wk = wk;
        do
        {
            auto dio = CONTAINING_RECORD(current_wk, aio_task, _task_queue_dl)->aio();
            memcpy(
                (void*)ptr,
                (const void*)(dio->buffer),
                (size_t)(dio->buffer_size)
                );

            ptr += dio->buffer_size;
            current_wk = current_wk->next();
        } while (current_wk != wk);

        dassert(ptr == (char*)bb.data() + bb.length(), "");

        // setup io task
        auto new_task = new batch_write_io_task(
            wk,
            bb
            );
        auto dio = new_task->aio();
        dio->buffer = (void*)bb.data();
        dio->buffer_size = sz;
        dio->file_offset = aio->aio()->file_offset;

        dio->file = aio->aio()->file;
        dio->file_object = aio->aio()->file_object;
        dio->engine = aio->aio()->engine;
        dio->type = AIO_Write;

        new_task->add_ref(); // released in complete_io
        return _provider->aio(new_task);
    }
}

void disk_engine::complete_io(aio_task* aio, error_code err, uint32_t bytes, int delay_milliseconds)
{
    if (err != ERR_OK)
    {
        dinfo(
            "disk operation failure with code %s, err = %s, aio task id = %llx",
            aio->spec().name.c_str(),
            err.to_string(),
            aio->id()
            );
    }
    
    // batching
    if (aio->code() == LPC_AIO_BATCH_WRITE)
    {
        aio->enqueue(err, (size_t)bytes);
        aio->release_ref(); // added in process_write
    }

    // no batching
    else
    {
        auto df = (disk_file*)(aio->aio()->file_object);
        if (aio->aio()->type == AIO_Read)
        {
            auto wk = df->on_read_completed(&aio->_task_queue_dl, err, (size_t)bytes);
            if (wk)
            {
                auto faio = CONTAINING_RECORD(wk, aio_task, _task_queue_dl);
                _provider->aio(faio);
            }            
        }

        // write
        else
        {
            uint32_t sz;
            auto wk = df->on_write_completed(&aio->_task_queue_dl, (void*)&sz, err, (size_t)bytes);
            if (wk)
            {
                process_write(wk, sz);
            }
        }
    }
}


} // end namespace
