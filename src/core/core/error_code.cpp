#include <dsn/utility/error_code.h>

namespace dsn {
/*static*/
int error_code::max()
{
    return dsn::utils::customized_id_mgr<dsn::error_code>::instance().max_value();
}
/*static*/
bool error_code::is_exist(const char *name)
{
    return dsn::utils::customized_id_mgr<dsn::error_code>::instance().get_id(name) != -1;
}
/*static*/
error_code error_code::try_get(const char *name, error_code default_value)
{
    int ans = dsn::utils::customized_id_mgr<dsn::error_code>::instance().get_id(name);
    if (ans == -1)
        return default_value;
    return error_code(ans);
}
/*static*/
error_code error_code::try_get(const std::string &name, error_code default_value)
{
    int ans = dsn::utils::customized_id_mgr<dsn::error_code>::instance().get_id(name);
    if (ans == -1)
        return default_value;
    return error_code(ans);
}

error_code::error_code(const char *name)
{
    _internal_code = dsn::utils::customized_id_mgr<dsn::error_code>::instance().register_id(name);
}

const char *error_code::to_string() const
{
    return dsn::utils::customized_id_mgr<dsn::error_code>::instance().get_name(_internal_code);
}
}
