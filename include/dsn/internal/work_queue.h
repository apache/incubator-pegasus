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
            _current_op_count = 0;
        }

        ~work_queue()
        {
            scope_lk l(_lock);
            dassert(_current_op_count == 0 && _hdr.is_alone(), 
                "work queue cannot be deleted when there are still %d running ops or pending work items in queue",
                _current_op_count
                );
        }

        // return not-null for what's to be run next
        dlink* add_work(dlink* dl, void* ctx)
        {
            scope_lk l(_lock);
            dl->insert_before(&_hdr);

            // allocate slot and run
            if (_current_op_count == _max_concurrent_op)
                return nullptr;
            else
            {
                _current_op_count++;
                return unlink_next_workload(_hdr, ctx);
            }
        }

        // called when the curren operation is completed,
        // which triggers further round of operations as returned
        dlink* on_work_completed(dlink* running, void* ctx)
        {
            scope_lk l(_lock);
            _current_op_count--;
            
            // no further workload
            if (_hdr.is_alone())
            {
                return nullptr;
            }

            // run further workload
            else
            {
                _current_op_count++;
                return unlink_next_workload(_hdr, ctx);
            }
        }

    protected:
        // lock is already hold
        virtual dlink* unlink_next_workload(dlink& hdr, void* ctx)
        {
            return hdr.next()->remove();
        }

        void reset_max_concurrent_ops(int max_c)
        {
            _max_concurrent_op = max_c;
        }

    private:
        typedef utils::auto_lock<utils::ex_lock_nr_spin> scope_lk;
        utils::ex_lock_nr_spin _lock;
        dlink _hdr;
        int _current_op_count;

        int _max_concurrent_op;
    };

}