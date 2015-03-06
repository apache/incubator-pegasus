# pragma once

# include <rdsn/internal/singleton.h>
# include <string>
# include <map>
# include <vector>
# include <rdsn/internal/rdsn_types.h>

namespace rdsn { namespace utils {

#define DEFINE_CUSTOMIZED_ID(T, name) __selectany const T name(#name);
#define DEFINE_CUSTOMIZED_ID_LONG(T, name, ...) __selectany const T name(#name, __VA_ARGS__);

#define DEFINE_CUSTOMIZED_ID_TYPE(T) struct T##_{}; \
    typedef rdsn::utils::customized_id<T##_> T;

template<typename T>
class customized_id_mgr : public rdsn::utils::singleton<customized_id_mgr<T>>
{
public:
    int get_id(const char* name) const;    
    const char* get_name(int id) const;
    int register_id(const char* name);
    int max_value() const { return (int)_names2.size() - 1; }

private:
    std::map<std::string, int> _names;
    std::vector<std::string>   _names2;
};

template<typename T>
struct customized_id
{
    customized_id(const char* name);
    customized_id(const customized_id& source);
    operator int() const;
    const char* to_string() const;
    void reset(const customized_id& r);

    static int max_value();
    static const char* to_string(int code);
    static bool is_exist(const char* name);
    static customized_id from_string(const char* name, customized_id invalid_value);

protected:
    static int assign(const char* xxx);
    customized_id(int code);

protected:
    int _internal_code;

//private:
//    // no assignment operator
//    customized_id<T>& operator=(const customized_id<T>& source);
};

// -------------------------- inline implementation ----------------------------

template<typename T>
customized_id<T>::customized_id(const char* name) 
    : _internal_code(assign(name)) 
{
}

template<typename T>
customized_id<T>::customized_id(const customized_id& source) 
    : _internal_code(source._internal_code) 
{
}

template<typename T>
customized_id<T>::operator int() const 
{ 
    return _internal_code; 
}

template<typename T>
const char*  customized_id<T>::to_string() const
{
    return customized_id_mgr<T>::instance().get_name(_internal_code);
}

template<typename T>
void customized_id<T>::reset(const customized_id<T>& r)
{
    _internal_code = r._internal_code;
}

template<typename T>
int customized_id<T>::max_value()
{
    return customized_id_mgr<T>::instance().max_value();
}

template<typename T>
const char* customized_id<T>::to_string(int code)
{
    return customized_id_mgr<T>::instance().get_name(code);
}

template<typename T>
bool customized_id<T>::is_exist(const char* name)
{
    return customized_id_mgr<T>::instance().get_id(name) != -1;
}

template<typename T>
customized_id<T> customized_id<T>::from_string(const char* name, customized_id invalid_value)
{
    int id = customized_id_mgr<T>::instance().get_id(name);
    if (id == -1) return invalid_value;
    else return customized_id<T>(id);
}

template<typename T>
int customized_id<T>::assign(const char* name)
{
    return customized_id_mgr<T>::instance().register_id(name);
}

template<typename T>
customized_id<T>::customized_id(int code)
    : _internal_code(code)
{
}

template<typename T>
int customized_id_mgr<T>::get_id(const char* name) const
{
    auto it = _names.find(std::string(name));
    if (it == _names.end())
        return -1;
    else
        return it->second;
}

template<typename T>
const char* customized_id_mgr<T>::get_name(int id) const
{
    if (id < (int)_names2.size())
        return _names2[id].c_str();
    else
        return "unknown";
}

template<typename T>
int customized_id_mgr<T>::register_id(const char* name)
{
    int id = get_id(name);
    if (-1 != id)
    {
        return id;
    }

    int code = (int)_names.size();
    _names[std::string(name)] = code;
    _names2.push_back(std::string(name));
    return code;
}

}} // end namespace rdsn::utils
