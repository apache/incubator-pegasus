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
# include <dsn/internal/env_provider.h>
# include <dsn/cpp/utils.h>
# include <chrono>

namespace dsn {

//------------ env_provider ---------------

env_provider::env_provider(env_provider* inner_provider)
{
}
    
uint64_t env_provider::random64(uint64_t min, uint64_t max)
{
    uint64_t gap = max - min + 1;
    if (gap == 0)
    {
        /*uint64_t a,b,c,d;*/
        return utils::get_random64();
    }
    else if (gap == (uint64_t)RAND_MAX + 1)
    {
        return (uint64_t)std::rand();
    }
    else
    {
        gap = static_cast<uint64_t>(static_cast<double>(97 * gap) * static_cast<double>(std::rand()) / static_cast<double>(RAND_MAX));
        gap = gap % (max - min + 1);
        return min + gap;
    }
}

} // end namespace
