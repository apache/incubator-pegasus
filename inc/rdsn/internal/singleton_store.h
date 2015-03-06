# pragma once

# include <rdsn/internal/singleton.h>
# include <rdsn/internal/synchronize.h>
# include <map>
# include <vector>

namespace rdsn { namespace utils {

template<typename TKey, typename TValue, typename TCompare = std::less<TKey>>
class singleton_store : public rdsn::utils::singleton<singleton_store<TKey, TValue, TCompare>>
{
public:
    bool put(TKey key, TValue val)
    {
        auto_write_lock l(_lock);
        auto it = _store.find(key);
        if (it != _store.end())
            return false;
        else
        {
            _store.insert(std::make_pair(key, val));
            return true;
        }
    }

    bool get(TKey key, __out TValue& val) const
    {
        auto_read_lock l(_lock);
        auto it = _store.find(key);
        if (it != _store.end())
        {
            val = it->second;
            return true;
        }
        else
            return false;
    }

    bool remove(TKey key)
    {
        auto_write_lock l(_lock);
        return _store.erase(key) > 0;
    }

    void get_all_keys(__out std::vector<TKey>& keys)
    {
        auto_read_lock l(_lock);
        for (auto it = _store.begin(); it != _store.end(); it++)
        {
            keys.push_back(it->first);
        }
    }

private:
    std::map<TKey, TValue, TCompare> _store;
    mutable rw_lock                  _lock;
};

//------------- inline implementation ----------

}} // end namespace rdsn::utils
