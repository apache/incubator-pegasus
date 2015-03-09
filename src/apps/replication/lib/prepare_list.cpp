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
#include "prepare_list.h"
#include "mutation.h"

#define __TITLE__ "prepare_list"

namespace rdsn { namespace replication {

prepare_list::prepare_list(
        decree initDecree, int maxCount,
        mutation_committer committer)
        : mutation_cache(initDecree, maxCount)
{
    _committer = committer;
    _lastCommittedDecree = 0;
}

void prepare_list::sanity_check()
{
    rassert (
        last_committed_decree() <= min_decree(), ""
        );
}

void prepare_list::reset(decree initDecree)
{
    _lastCommittedDecree = initDecree;
    mutation_cache::reset(initDecree, true);
}

void prepare_list::truncate(decree initDecree)
{
    while (min_decree() <= initDecree && count() > 0)
    {
        pop_min();
    }
    _lastCommittedDecree = initDecree;
}

int prepare_list::prepare(mutation_ptr& mu, partition_status status)
{
    rassert(mu->data.header.decree > last_committed_decree(), "");

    int err;
    switch (status)
    {
    case PS_PRIMARY:
        return mutation_cache::put(mu);

    case PS_SECONDARY: 
        commit(mu->data.header.lastCommittedDecree, true);
        err = mutation_cache::put(mu);
        rassert(err == ERR_SUCCESS, "");
        return err;

    case PS_POTENTIAL_SECONDARY:
        while (true)
        {
            err = mutation_cache::put(mu);
            if (err == ERR_CAPACITY_EXCEEDED)
            {
                rassert (min_decree() == last_committed_decree() + 1, "");
                rassert (mu->data.header.lastCommittedDecree > last_committed_decree(), "");
                commit (last_committed_decree() + 1, true);
            }
            else
                break;
        }
        rassert(err == ERR_SUCCESS, "");
        return err;
     
    case PS_INACTIVE: // only possible during init  
        err = ERR_SUCCESS;
        if (mu->data.header.lastCommittedDecree > max_decree())
        {
            reset(mu->data.header.lastCommittedDecree);
        }
        else if (mu->data.header.lastCommittedDecree > _lastCommittedDecree)
        {
            for (decree d = last_committed_decree() + 1; d <= mu->data.header.lastCommittedDecree; d++)
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

            rassert (_lastCommittedDecree == mu->data.header.lastCommittedDecree, "");
            sanity_check();
        }
        
        err = mutation_cache::put(mu);
        rassert (err == ERR_SUCCESS, "");
        return err;

    default:
        rassert (false, "");
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

            rassert(mutation_cache::min_decree() == _lastCommittedDecree, "");
            pop_min();

            mu = mutation_cache::get_mutation_by_decree(_lastCommittedDecree + 1);
        }
    }
    else
    {
        for (decree d0 = last_committed_decree() + 1; d0 <= d; d0++)
        {
            mutation_ptr mu = get_mutation_by_decree(d0);
            rassert(mu != nullptr && mu->is_prepared(), "");

            _lastCommittedDecree++;
            _committer(mu);

            rassert (mutation_cache::min_decree() == _lastCommittedDecree, "");
            pop_min();
        }
    }

    sanity_check();
    return true;
}

}} // namespace end
