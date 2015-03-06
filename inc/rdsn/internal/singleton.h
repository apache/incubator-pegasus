# pragma once

# include <mutex>

namespace rdsn { namespace utils {

template<typename T>
class singleton
{
public:
    singleton() {}

    static T& instance()
    {
        static std::once_flag flag;

        if (nullptr == _instance)
        {
            std::call_once(flag, [&]() { _instance = new T(); });
        }
        return *_instance;
    }

    static bool is_instance_created()
    {
        return nullptr != _instance;
    }
    
protected:
    static T*    _instance;
    
private:
    singleton(const singleton&);
    singleton& operator=(const singleton&);
};

// ----- inline implementations -------------------------------------------------------------------

template<typename T> T*    singleton<T>::_instance = 0;

}} // end namespace rdsn::utils
