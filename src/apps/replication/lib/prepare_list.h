#pragma once

#include "mutation_cache.h"

namespace rdsn { namespace replication {


class prepare_list : public mutation_cache
{
public:
    typedef std::function<void (mutation_ptr&)> mutation_committer;

public:
    prepare_list(
        decree initDecree, int maxCount,
        mutation_committer committer);

    decree last_committed_decree() const { return _lastCommittedDecree; }
    void   reset(decree initDecree);
    void   truncate(decree initDecree);
    
    //
    // for two-phase commit
    //
    int  prepare(mutation_ptr& mu, partition_status status); // unordered prepare
    bool commit(decree decree, bool force); // ordered commit
    
private:
    void sanity_check();

private:
    decree                   _lastCommittedDecree;    
    mutation_committer        _committer;
};
 
}} // namespace

