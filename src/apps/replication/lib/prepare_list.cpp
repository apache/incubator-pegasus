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
#include "prepare_list.h"
#include "mutation.h"

#define __TITLE__ "prepare_list"

namespace dsn { namespace replication {

prepare_list::prepare_list(
        decree init_decree, int max_count,
        mutation_committer committer)
        : mutation_cache(init_decree, max_count)
{
    _committer = committer;
    _lastCommittedDecree = 0;
}

void prepare_list::sanity_check()
{
    dassert (
        last_committed_decree() <= min_decree(), ""
        );
}

void prepare_list::reset(decree init_decree)
{
    _lastCommittedDecree = init_decree;
    mutation_cache::reset(init_decree, true);
}

void prepare_list::truncate(decree init_decree)
{
    while (min_decree() <= init_decree && count() > 0)
    {
        pop_min();
    }
    _lastCommittedDecree = init_decree;
}

int prepare_list::prepare(mutation_ptr& mu, partition_status status)
{
    dassert (mu->data.header.decree > last_committed_decree(), "");

    int err;
    switch (status)
    {
    case PS_PRIMARY:
        return mutation_cache::put(mu);

    case PS_SECONDARY: 
        commit(mu->data.header.last_committed_decree, true);
        err = mutation_cache::put(mu);
        dassert (err == ERR_SUCCESS, "");
        return err;

    case PS_POTENTIAL_SECONDARY:
        while (true)
        {
            err = mutation_cache::put(mu);
            if (err == ERR_CAPACITY_EXCEEDED)
            {
                dassert (min_decree() == last_committed_decree() + 1, "");
                dassert (mu->data.header.last_committed_decree > last_committed_decree(), "");
                commit (last_committed_decree() + 1, true);
            }
            else
                break;
        }
        dassert (err == ERR_SUCCESS, "");
        return err;
     
    case PS_INACTIVE: // only possible during init  
        err = ERR_SUCCESS;
        if (mu->data.header.last_committed_decree > max_decree())
        {
            reset(mu->data.header.last_committed_decree);
        }
        else if (mu->data.header.last_committed_decree > _lastCommittedDecree)
        {
            for (decree d = last_committed_decree() + 1; d <= mu->data.header.last_committed_decree; d++)
            {
                _lastCommittedDecree++;   
                if (count() == 0)
                    break;
                
                if (d == min_decree())
                {
                    mutation_ptr mu2 = get_mutation_by_decree(d);
                    pop_min();
                    if (mu2 != nullptr) _committer(mu2);
                }
            }

            dassert (_lastCommittedDecree == mu->data.header.last_committed_decree, "");
            sanity_check();
        }
        
        err = mutation_cache::put(mu);
        dassert (err == ERR_SUCCESS, "");
        return err;

    default:
        dassert (false, "");
        return 0;
    }
}

//
// ordered commit
//
bool prepare_list::commit(decree d, bool force)
{
    if (d <= last_committed_decree())
        return false;

    if (!force)
    {
        if (d != last_committed_decree() + 1)
            return false;

        mutation_ptr mu = get_mutation_by_decree(last_committed_decree() + 1);

        while (mu != nullptr && mu->is_ready_for_commit())
        {
            _lastCommittedDecree++;
            _committer(mu);

            dassert (mutation_cache::min_decree() == _lastCommittedDecree, "");
            pop_min();

            mu = mutation_cache::get_mutation_by_decree(_lastCommittedDecree + 1);
        }
    }
    else
    {
        for (decree d0 = last_committed_decree() + 1; d0 <= d; d0++)
        {
            mutation_ptr mu = get_mutation_by_decree(d0);
            dassert (mu != nullptr && mu->is_prepared(), "");

            _lastCommittedDecree++;
            _committer(mu);

            dassert (mutation_cache::min_decree() == _lastCommittedDecree, "");
            pop_min();
        }
    }

    sanity_check();
    return true;
}

}} // namespace end
