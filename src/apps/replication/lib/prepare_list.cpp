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

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "prepare_list"

namespace dsn { namespace replication {

prepare_list::prepare_list(
        decree init_decree, int max_count,
        mutation_committer committer,
        bool allow_prepare_ack_before_logging)
        : mutation_cache(init_decree, max_count)
{
    _committer = committer;
    _last_committed_decree = 0;
    _allow_prepare_ack_before_logging = allow_prepare_ack_before_logging;
}

void prepare_list::sanity_check()
{

}

void prepare_list::reset(decree init_decree)
{
    _last_committed_decree = init_decree;
    mutation_cache::reset(init_decree, true);
}

void prepare_list::truncate(decree init_decree)
{
    while (min_decree() <= init_decree && count() > 0)
    {
        pop_min();
    }
    _last_committed_decree = init_decree;
}

error_code prepare_list::prepare(mutation_ptr& mu, partition_status status)
{
    decree d = mu->data.header.decree;
    dassert (d > last_committed_decree(), "");

    while (d - min_decree() >= capacity() && last_committed_decree() > min_decree())
    {
        pop_min();
    }   

    error_code err;
    switch (status)
    {
    case PS_PRIMARY:
        return mutation_cache::put(mu);

    case PS_SECONDARY: 
        commit(mu->data.header.last_committed_decree, true);
        err = mutation_cache::put(mu);
        dassert (err == ERR_OK, "");
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
        dassert (err == ERR_OK, "");
        return err;
     
    case PS_INACTIVE: // only possible during init  
        err = ERR_OK;
        if (mu->data.header.last_committed_decree > max_decree())
        {
            reset(mu->data.header.last_committed_decree);
        }
        else if (mu->data.header.last_committed_decree > _last_committed_decree)
        {
            for (decree d = last_committed_decree() + 1; d <= mu->data.header.last_committed_decree; d++)
            {
                _last_committed_decree++;   
                if (count() == 0)
                    break;
                
                if (d == min_decree())
                {
                    mutation_ptr mu2 = get_mutation_by_decree(d);
                    pop_min();
                    if (mu2 != nullptr) _committer(mu2);
                }
            }

            dassert (_last_committed_decree == mu->data.header.last_committed_decree, "");
            sanity_check();
        }
        
        err = mutation_cache::put(mu);
        dassert (err == ERR_OK, "");
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

        while (mu != nullptr && mu->is_ready_for_commit(_allow_prepare_ack_before_logging))
        {
            _last_committed_decree++;
            _committer(mu);

            mu = mutation_cache::get_mutation_by_decree(_last_committed_decree + 1);
        }
    }
    else
    {
        for (decree d0 = last_committed_decree() + 1; d0 <= d; d0++)
        {
            mutation_ptr mu = get_mutation_by_decree(d0);
            dassert(mu != nullptr &&
                (mu->is_logged() || _allow_prepare_ack_before_logging),
                "mutation %lld is missing in prepare list",
                d0
                );

            _last_committed_decree++;
            _committer(mu);
        }
    }

    sanity_check();
    return true;
}

}} // namespace end
