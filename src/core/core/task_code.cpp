#include <dsn/utility/customizable_id.h>
#include <dsn/tool-api/task_code.h>
#include <dsn/tool-api/task_spec.h>

namespace dsn {

typedef dsn::utils::customized_id_mgr<dsn::task_code> task_code_mgr;

/*static*/
int task_code::max() { return task_code_mgr::instance().max_value(); }

/*static*/
bool task_code::is_exist(const char *name) { return task_code_mgr::instance().get_id(name) != -1; }

/*static*/
task_code task_code::try_get(const char *name, task_code default_value)
{
    int code = task_code_mgr::instance().get_id(name);
    if (code == -1)
        return default_value;
    return task_code(code);
}

/*static*/
task_code task_code::try_get(const std::string &name, task_code default_value)
{
    int code = task_code_mgr::instance().get_id(name);
    if (code == -1)
        return default_value;
    return task_code(code);
}

task_code::task_code(const char *name) : _internal_code(task_code_mgr::instance().register_id(name))
{
}

task_code::task_code(const char *name,
                     dsn_task_type_t tt,
                     dsn_task_priority_t pri,
                     dsn::threadpool_code pool)
    : task_code(name)
{
    task_spec::register_task_code(*this, tt, pri, pool);
}

task_code::task_code(const char *name,
                     dsn_task_type_t tt,
                     dsn_task_priority_t pri,
                     dsn::threadpool_code pool,
                     bool is_storage_write,
                     bool allow_batch,
                     bool is_idempotent)
    : task_code(name)
{
    task_spec::register_storage_task_code(
        *this, tt, pri, pool, is_storage_write, allow_batch, is_idempotent);
}

const char *task_code::to_string() const
{
    return task_code_mgr::instance().get_name(_internal_code);
}
}
