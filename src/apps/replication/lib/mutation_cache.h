#pragma once


#include "replication_common.h"
#include <vector>

namespace rdsn { namespace replication {

class mutation_cache
{
public:
    mutation_cache(decree initDecree, int maxCount);
    ~mutation_cache();

    int         put(mutation_ptr& mu);
    mutation_ptr pop_min();
    mutation_ptr get_mutation_by_decree(decree decree);
    void        reset(decree initDecree, bool clearMutations);

    decree  min_decree() const { return _startDecree; } 
    decree  max_decree() const { return _endDecree; }
    long    total_size_in_bytes() const { return _totalSizeInBytes; }
    int     count()     const { return _interval; }

private:
    std::vector<mutation_ptr> _array;
    int          _maxCount;
    
    int          _interval;
    long         _totalSizeInBytes;

    int          _startIndex;
    int          _endIndex;
    decree       _startDecree;
    decree       _endDecree;
};

}} // namespace

