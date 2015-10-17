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

# include <dsn/internal/synchronize.h>
# include <dsn/internal/link.h>

namespace dsn {

    class work_queue
    {
    public:
        work_queue(int max_concurrent_op = 1)
            : _max_concurrent_op(max_concurrent_op)
        {
            _runnings = new dlink*[_max_concurrent_op];
            memset((void*)_runnings, 0, sizeof(dlink*)*_max_concurrent_op);
        }

        ~work_queue()
        {
            delete[] _runnings;
        }

        // return not-null for what's to be run next
        dlink* add_work(dlink* dl, void* ctx)
        {
            scope_lk l(_lock);
            dl->insert_before(&_hdr);

            // allocate slot and run
            for (int i = 0; i < _max_concurrent_op; i++)
            {
                if (nullptr == _runnings[i])
                {
                    auto wk = unlink_next_workload(_hdr, ctx);
                    _runnings[i] = wk;
                    return wk;
                }
            }

            // no empty slot for concurrent ops
            return nullptr;
        }

        // called when the curren operation is completed,
        // which triggers further round of operations as returned
        dlink* on_work_completed(dlink* running, void* ctx)
        {
            scope_lk l(_lock);
            int i = 0;
            for (; i < _max_concurrent_op; i++)
            {
                if (_runnings[i] == running)
                {
                    _runnings[i] = nullptr;
                    break;
                }
            }

            dassert(i < _max_concurrent_op,
                "cannot find the given elements %p in running records",
                running
                );

            // no further workload
            if (_hdr.is_alone())
            {
                return nullptr;
            }

            // run further workload
            else
            {
                auto wk = unlink_next_workload(_hdr, ctx);
                _runnings[i] = wk;
                return wk;
            }
        }

    protected:
        // lock is already hold
        virtual dlink* unlink_next_workload(dlink& hdr, void* ctx)
        {
            return hdr.next()->remove();
        }

        // make sure no ops is started yet when calling this
        void reset_max_concurrent_ops(int max_c)
        {
            _max_concurrent_op = max_c;
            delete[] _runnings;
            _runnings = new dlink*[max_c];
            memset((void*)_runnings, 0, sizeof(dlink*)*_max_concurrent_op);
        }


    private:
        typedef utils::auto_lock<utils::ex_lock_nr_spin> scope_lk;
        utils::ex_lock_nr_spin _lock;
        dlink _hdr;
        dlink **_runnings;

        int _max_concurrent_op;
    };

}