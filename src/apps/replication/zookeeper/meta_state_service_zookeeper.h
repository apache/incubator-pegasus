#include <dsn/dist/meta_state_service.h>
#include <dsn/dist/distributed_lock_service.h>
#include <dsn/internal/synchronize.h>
#include <dsn/cpp/autoref_ptr.h>

namespace dsn{ namespace dist {

class zookeeper_session;
class meta_state_service_zookeeper:
        public meta_state_service,
        public clientlet,
        public ref_counter
{
public:
    explicit meta_state_service_zookeeper();
    virtual ~meta_state_service_zookeeper();

    virtual error_code initialize(const char*) override;

    virtual task_ptr create_node(
        const std::string& node,
        task_code cb_code,
        const err_callback& cb_create,
        const blob& value = blob(),
        clientlet* tracker = nullptr
        ) override;

    virtual task_ptr delete_node(
        const std::string& node,
        bool recursively_delete,
        task_code cb_code,
        const err_callback& cb_delete,
        clientlet* tracker = nullptr) override;

    virtual task_ptr node_exist(
        const std::string& node,
        task_code cb_code,
        const err_callback& cb_exist,
        clientlet* tracker = nullptr) override;

    virtual task_ptr get_data(
        const std::string& node,
        task_code cb_code,
        const err_value_callback& cb_get_data,
        clientlet* tracker = nullptr) override;

    virtual task_ptr set_data(
        const std::string& node,
        const blob& value,
        task_code cb_code,
        const err_callback& cb_set_data,
        clientlet* tracker = nullptr) override;

    virtual task_ptr get_children(
        const std::string& node,
        task_code cb_code,
        const err_stringv_callback& cb_get_children,
        clientlet* tracker = nullptr) override;

    task_ptr delete_empty_node(
            const std::string& node,
            task_code cb_code,
            const err_callback& cb_delete,
            clientlet* tracker);
    int hash() const { return (int)(((uint64_t)this)&0xffffffff); }

private:
    typedef ref_ptr<meta_state_service_zookeeper> ref_this;

    bool _first_call;
    int _zoo_state;
    zookeeper_session* _session;
    utils::notify_event _notifier;

    static void on_zoo_session_evt(ref_this ptr, int zoo_state);
    static void visit_zookeeper_internal(ref_this ptr, task_ptr callback, void* result/*zookeeper_session::zoo_opcontext**/);
};

}}
