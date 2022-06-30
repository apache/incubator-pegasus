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

#include <dsn/dist/fmt_logging.h>
#include <dsn/tool-api/aio_task.h>
#include <dsn/utility/flags.h>

#include "disk_engine.h"
#include "runtime/service_engine.h"
#include "native_linux_aio_provider.h"

using namespace dsn::utils;

namespace dsn {
DEFINE_TASK_CODE_AIO(LPC_AIO_BATCH_WRITE, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

const char *native_aio_provider = "dsn::tools::native_aio_provider";
DSN_REGISTER_COMPONENT_PROVIDER(native_linux_aio_provider, native_aio_provider);

struct disk_engine_initializer
{
    disk_engine_initializer() { disk_engine::instance(); }
};

// make disk_engine destructed after service_engine, which is inited in dsn_global_init,
// because service_engine relies on the former to close files.
static disk_engine_initializer disk_engine_init;

//----------------- disk_file ------------------------
aio_task *disk_write_queue::unlink_next_workload(void *plength)
{
    uint64_t next_offset = 0;
    uint64_t &sz = *(uint64_t *)plength;
    sz = 0;

    aio_task *first = _hdr._first, *current = first, *last = first;
    while (nullptr != current) {
        auto io = current->get_aio_context();
        if (sz == 0) {
            sz = io->buffer_size;
            next_offset = io->file_offset + sz;
        } else {
            // batch condition
            if (next_offset == io->file_offset && sz + io->buffer_size <= _max_batch_bytes) {
                sz += io->buffer_size;
                next_offset += io->buffer_size;
            }

            // no batch is possible
            else {
                break;
            }
        }

        // continue next
        last = current;
        current = (aio_task *)current->next;
    }

    // unlink [first, last] -> current
    if (last) {
        _hdr._first = current;
        if (last == _hdr._last)
            _hdr._last = nullptr;
        last->next = nullptr;
    }

    return first;
}

disk_file::disk_file(dsn_handle_t handle) : _handle(handle) {}

aio_task *disk_file::read(aio_task *tsk)
{
    tsk->add_ref(); // release on completion, see `on_read_completed`.
    return _read_queue.add_work(tsk, nullptr);
}

aio_task *disk_file::write(aio_task *tsk, void *ctx)
{
    tsk->add_ref(); // release on completion
    return _write_queue.add_work(tsk, ctx);
}

aio_task *disk_file::on_read_completed(aio_task *wk, error_code err, size_t size)
{
    dassert(wk->next == nullptr, "");
    auto ret = _read_queue.on_work_completed(wk, nullptr);
    wk->enqueue(err, size);
    wk->release_ref(); // added in above read

    return ret;
}

aio_task *disk_file::on_write_completed(aio_task *wk, void *ctx, error_code err, size_t size)
{
    auto ret = _write_queue.on_work_completed(wk, ctx);

    while (wk) {
        aio_task *next = (aio_task *)wk->next;
        wk->next = nullptr;

        if (err == ERR_OK) {
            size_t this_size = (size_t)wk->get_aio_context()->buffer_size;
            dcheck_ge(size, this_size);
            wk->enqueue(err, this_size);
            size -= this_size;
        } else {
            wk->enqueue(err, size);
        }

        wk->release_ref(); // added in above write

        wk = next;
    }

    if (err == ERR_OK) {
        dassert(size == 0, "written buffer size does not equal to input buffer's size");
    }

    return ret;
}

//----------------- disk_engine ------------------------
disk_engine::disk_engine()
{
    aio_provider *provider = utils::factory_store<aio_provider>::create(
        native_aio_provider, dsn::PROVIDER_TYPE_MAIN, this);
    _provider.reset(provider);
}

class batch_write_io_task : public aio_task
{
public:
    explicit batch_write_io_task(aio_task *tasks)
        : aio_task(LPC_AIO_BATCH_WRITE, nullptr), _tasks(tasks)
    {
    }

    virtual void exec() override
    {
        auto df = (disk_file *)_tasks->get_aio_context()->file_object;
        uint64_t sz;

        auto wk = df->on_write_completed(_tasks, (void *)&sz, error(), get_transferred_size());
        if (wk) {
            wk->get_aio_context()->engine->process_write(wk, sz);
        }
    }

public:
    aio_task *_tasks;
};

void disk_engine::write(aio_task *aio)
{
    if (!aio->spec().on_aio_call.execute(task::get_current_task(), aio, true)) {
        aio->enqueue(ERR_FILE_OPERATION_FAILED, 0);
        return;
    }

    auto dio = aio->get_aio_context();
    auto df = (disk_file *)dio->file;
    dio->file = df->native_handle();
    dio->file_object = df;
    dio->engine = this;
    dio->type = AIO_Write;

    uint64_t sz;
    auto wk = df->write(aio, &sz);
    if (wk) {
        process_write(wk, sz);
    }
}

void disk_engine::process_write(aio_task *aio, uint64_t sz)
{
    aio_context *dio = aio->get_aio_context();

    // no batching
    if (dio->buffer_size == sz) {
        aio->collapse();
        _provider->submit_aio_task(aio);
    }

    // batching
    else {
        // setup io task
        auto new_task = new batch_write_io_task(aio);
        auto new_dio = new_task->get_aio_context();
        new_dio->buffer_size = sz;
        new_dio->file_offset = dio->file_offset;
        new_dio->file = dio->file;
        new_dio->file_object = dio->file_object;
        new_dio->engine = dio->engine;
        new_dio->type = AIO_Write;

        auto cur_task = aio;
        do {
            auto cur_dio = cur_task->get_aio_context();
            if (cur_dio->buffer) {
                dsn_file_buffer_t buf;
                buf.buffer = cur_dio->buffer;
                buf.size = cur_dio->buffer_size;
                new_task->_unmerged_write_buffers.push_back(std::move(buf));
            } else {
                new_task->_unmerged_write_buffers.insert(new_task->_unmerged_write_buffers.end(),
                                                         cur_task->_unmerged_write_buffers.begin(),
                                                         cur_task->_unmerged_write_buffers.end());
            }
            cur_task = (aio_task *)cur_task->next;
        } while (cur_task);

        new_task->add_ref(); // released in complete_io
        process_write(new_task, sz);
    }
}

void disk_engine::complete_io(aio_task *aio, error_code err, uint64_t bytes)
{
    if (err != ERR_OK) {
        dinfo("disk operation failure with code %s, err = %s, aio_task_id = %016" PRIx64,
              aio->spec().name.c_str(),
              err.to_string(),
              aio->id());
    }

    // batching
    if (aio->code() == LPC_AIO_BATCH_WRITE) {
        aio->enqueue(err, (size_t)bytes);
        aio->release_ref(); // added in process_write
    }

    // no batching
    else {
        auto df = (disk_file *)(aio->get_aio_context()->file_object);
        if (aio->get_aio_context()->type == AIO_Read) {
            auto wk = df->on_read_completed(aio, err, (size_t)bytes);
            if (wk) {
                _provider->submit_aio_task(wk);
            }
        }

        // write
        else {
            uint64_t sz;
            auto wk = df->on_write_completed(aio, (void *)&sz, err, (size_t)bytes);
            if (wk) {
                process_write(wk, sz);
            }
        }
    }
}
} // namespace dsn
