// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <algorithm>
#include <random>
#include <assert.h>
#include <dsn/utility/rand.h>
#include "key_generator.h"
#include "config.h"

namespace pegasus {
namespace test {

key_generator::key_generator(write_mode mode, uint64_t num, uint64_t num_per_set)
: _mode(mode), _num(num), _next(0)
{
    if (_mode == UNIQUE_RANDOM) {
        // NOTE: if memory consumption of this approach becomes a concern,
        // we can either break it into pieces and only random shuffle a section
        // each time. Alternatively, use a bit map implementation
        // (https://reviews.facebook.net/differential/diff/54627/)
        _values.resize(_num);
        for (uint64_t i = 0; i < _num; ++i) {
            _values[i] = i;
        }
        std::shuffle(_values.begin(),
                     _values.end(),
                     std::default_random_engine(static_cast<unsigned int>(config::get_instance()->_seed)));
    }
}

uint64_t key_generator::next()
{
    switch (_mode) {
        case SEQUENTIAL:
            return _next++;
        case RANDOM:
            return dsn::rand::next_u64() % _num;
        case UNIQUE_RANDOM:
            assert(_next + 1 <= _num); // TODO < -> <=
            return _values[_next++];
    }
    assert(false);
    return std::numeric_limits<uint64_t>::max();
}
}
}
