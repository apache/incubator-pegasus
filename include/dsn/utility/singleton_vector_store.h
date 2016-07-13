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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/utility/singleton.h>
# include <vector>

namespace dsn { namespace utils {

template<typename T, T default_value>
class singleton_vector_store : public dsn::utils::singleton<singleton_vector_store<T, default_value>>
{
public:
    singleton_vector_store(void){}
    ~singleton_vector_store(void){}

    bool contains(int index) const
    {
        if (index >= static_cast<int>(_contains.size()))
            return false;
        else
            return _contains[index];
    }

    T get(int index) const
    {
        if (index >= static_cast<int>(_contains.size()))
            return default_value;
        else
            return _values[index];
    }

    bool put(int index, T value)
    {
        if (index >= static_cast<int>(_contains.size()))
        {
            for (int i = static_cast<int>(_contains.size()); i < index; i++)
            {
                _contains.push_back(false);
                _values.push_back(default_value);
            }

            _contains.push_back(true);
            _values.push_back(value);
            return true;
        }
        else if (_contains[index])
            return false;
        else
        {
            _contains[index] = true;
            _values[index] = value;
            return true;
        }
    }

private:
    std::vector<bool> _contains;
    std::vector<T>    _values;
};

}} // end namespace dsn::utils
