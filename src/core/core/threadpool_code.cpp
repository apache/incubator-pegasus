#include <dsn/tool-api/threadpool_code.h>
#include <dsn/utility/customizable_id.h>

namespace dsn {

/*static*/
int threadpool_code::max()
{
    return dsn::utils::customized_id_mgr<dsn::threadpool_code>::instance().max_value();
}
/*static*/
bool threadpool_code::is_exist(const char *name)
{
    return dsn::utils::customized_id_mgr<dsn::threadpool_code>::instance().get_id(name) != -1;
}

threadpool_code::threadpool_code(const char *name)
{
    _internal_code =
        dsn::utils::customized_id_mgr<dsn::threadpool_code>::instance().register_id(name);
}

const char *threadpool_code::to_string() const
{
    return dsn::utils::customized_id_mgr<dsn::threadpool_code>::instance().get_name(_internal_code);
}
}
