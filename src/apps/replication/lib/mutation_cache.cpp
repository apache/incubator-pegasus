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
#include "mutation_cache.h"
#include "mutation.h"

namespace dsn { namespace replication {

mutation_cache::mutation_cache(decree init_decree, int max_count)
{
    _max_count = max_count;
    _array.resize(max_count, nullptr);

    reset(init_decree, false);
}

mutation_cache::~mutation_cache()
{
    _array.clear();
}

error_code mutation_cache::put(mutation_ptr& mu)
{
    decree decree = mu->data.header.decree;
    int delta = 0, tag = 0;
    if (_interval == 0)
    {
        delta = 1;
        tag = 0;
    }
    else if (decree > _end_decree)
    {
        delta = static_cast<int>(decree - _end_decree);
        tag = 1;
    }
    else if (decree < _start_decree)
    {
        delta = static_cast<int>(_start_decree - decree);
        tag = -1;
    }

    if (delta + _interval > _max_count)
    {
        return ERR_CAPACITY_EXCEEDED;
    }

    int idx = ((decree - _end_decree) + _end_idx + _max_count) % _max_count;
    mutation_ptr& old = _array[idx];
    if (old != nullptr)
    {
        dassert (old->data.header.ballot <= mu->data.header.ballot, "");
    }

    _array[idx] = mu;
        
    // update tracking data
    _interval += delta;

    if (tag > 0)
    {
        _end_idx = idx;
        _end_decree = decree;
    }
    else if (tag < 0)
    {
        _start_idx = idx;
        _start_decree = decree;
    }
    else if (_interval == 1)
    {
        _start_idx = _end_idx = idx;
        _start_decree = _end_decree = decree;
    }
    return ERR_OK;
}

mutation_ptr mutation_cache::pop_min()
{
    if (_interval > 0)
    {
        mutation_ptr mu = _array[_start_idx];
        _array[_start_idx] = nullptr;

        _interval--;
        _start_idx = (_start_idx + 1) % _max_count;
        
        if (_interval == 0)
        {
            //TODO: FIXE ME LATER
            //dassert (_total_size_bytes == 0, "");

            _end_decree = _start_decree;
            _end_idx = _start_idx;
        }
        else
        {
            _start_decree++;
        }
        return mu;
    }
    else
    {
        return nullptr;
    }
}

void mutation_cache::reset(decree init_decree, bool clear_mutations)
{
    _start_decree = _end_decree = init_decree;
    _start_idx = _end_idx = 0;
    _interval = 0;    

    if (clear_mutations)
    {
        for (int i = 0; i < _max_count; i++)
            _array[i] = nullptr;        
    }
}

mutation_ptr mutation_cache::get_mutation_by_decree(decree decree)
{
    if (decree < _start_decree || decree > _end_decree)
        return nullptr;
    else
        return _array[(_start_idx + (decree - _start_decree) + _max_count) % _max_count];
}

mutation_ptr mutation_cache::remove_mutation_by_decree(decree decree)
{
    if (decree < _start_decree || decree > _end_decree)
        return nullptr;
    else
    {
        int idx = (_start_idx + (decree - _start_decree) + _max_count) % _max_count;
        auto ret = _array[idx];
        _array[idx] = nullptr;
        return ret;
    }
}

}} // namespace end
