/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

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

mutation_cache::mutation_cache(decree initDecree, int maxCount)
{
    _maxCount = maxCount;
    _array.resize(maxCount, nullptr);
    _totalSizeInBytes = 0;

    reset(initDecree, false);
}

mutation_cache::~mutation_cache()
{
    _array.clear();
}

int mutation_cache::put(mutation_ptr& mu)
{
    decree decree = mu->data.header.decree;
    int delta = 0, tag = 0;
    if (_interval == 0)
    {
        delta = 1;
        tag = 0;
    }
    else if (decree > _endDecree)
    {
        delta = (int)(decree - _endDecree);
        tag = 1;
    }
    else if (decree < _startDecree)
    {
        delta = (int)(_startDecree - decree);
        tag = -1;
    }

    if (delta + _interval > _maxCount)
    {
        return ERR_CAPACITY_EXCEEDED;
    }

    int idx = ((decree - _endDecree) + _endIndex + _maxCount) % _maxCount;
    mutation_ptr old = _array[idx];
    if (old != nullptr)
    {
        dassert (old->data.header.ballot <= mu->data.header.ballot, "");
    }

    _array[idx] = mu;
        
    // update tracking data
    _interval += delta;
    _totalSizeInBytes += mu->memory_size();
    if (old != nullptr)
    {
        _totalSizeInBytes -= old->memory_size();
        old = nullptr;
    }

    if (tag > 0)
    {
        _endIndex = idx;
        _endDecree = decree;
    }
    else if (tag < 0)
    {
        _startIndex = idx;
        _startDecree = decree;
    }
    else if (_interval == 1)
    {
        _startIndex = _endIndex = idx;
        _startDecree = _endDecree = decree;
    }
    return ERR_SUCCESS;
}

mutation_ptr mutation_cache::pop_min()
{
    if (_interval > 0)
    {
        mutation_ptr mu = _array[_startIndex];
        _array[_startIndex] = nullptr;

        _interval--;
        _startIndex = (_startIndex + 1) % _maxCount;
        
        if (mu != nullptr)
        {
            _totalSizeInBytes -= mu->memory_size();
        }

        if (_interval == 0)
        {
            //TODO: FIXE ME LATER
            //dassert (_totalSizeInBytes == 0, "");

            _endDecree = _startDecree;
            _endIndex = _startIndex;
        }
        else
        {
            _startDecree++;
        }
        return mu;
    }
    else
    {
        return nullptr;
    }
}

void mutation_cache::reset(decree initDecree, bool clearMutations)
{
    _startDecree = _endDecree = initDecree;
    _startIndex = _endIndex = 0;
    _interval = 0;    
    _totalSizeInBytes = 0;

    if (clearMutations)
    {
        for (int i = 0; i < _maxCount; i++)
            _array[i] = nullptr;        
    }
}

mutation_ptr mutation_cache::get_mutation_by_decree(decree decree)
{
    if (decree < _startDecree || decree > _endDecree)
        return nullptr;
    else
        return _array[(_startIndex + (decree - _startDecree) + _maxCount) % _maxCount];
}


}} // namespace end
