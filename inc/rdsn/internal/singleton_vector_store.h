# pragma once

# include <rdsn/internal/singleton.h>
# include <vector>

namespace rdsn { namespace utils {

template<typename T, T default_value>
class singleton_vector_store : public rdsn::utils::singleton<singleton_vector_store<T, default_value>>
{
public:
    singleton_vector_store(void){}
    ~singleton_vector_store(void){}

    bool Contains(int index) const
    {
        if (index >= (int)_contains.size())
            return false;
        else
            return _contains[index];
    }

    T get(int index) const
    {
        if (index >= (int)_contains.size())
            return default_value;
        else
            return _values[index];
    }

    bool put(int index, T value)
    {
        if (index >= (int)_contains.size())
        {
            for (int i = (int)_contains.size(); i < index; i++)
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

}} // end namespace rdsn::utils
