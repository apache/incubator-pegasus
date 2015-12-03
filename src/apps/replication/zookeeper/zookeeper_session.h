#include <dsn/cpp/utils.h>
#include <dsn/cpp/clientlet.h>
#include <dsn/internal/singleton.h>
#include <dsn/internal/synchronize.h>
#include <dsn/internal/task.h>

#include <thread>
#include <zookeeper.h>
#include "zookeeper_session_mgr.h"

namespace dsn { namespace dist {

class zookeeper_session
{
public:
    enum ZOO_OPERATION
    {
        ZOO_CREATE,
        ZOO_DELETE,
        ZOO_EXISTS,
        ZOO_GET,
        ZOO_GETCHILDREN,
        ZOO_SET,
        ZOO_ASYNC,
        ZOO_OPCOUNT,
        ZOO_OPINVALID
    };

    struct zoo_input
    {
        std::string _path;

        /* for create and set */
        blob _value;
        /* for create */
        int _flags;
        /* for get/exists/get_children */
        int _is_set_watch;

        /* for watcher callback */
        void* _owner;
        std::function<void (int)> _watcher_callback;
    };

    struct zoo_output
    {
        int error;
        union{
            struct {
                const char* _created_path;
            }create_op;
            struct {
                const struct Stat* _node_stat;
            }exists_op;
            struct {
                const struct Stat* _node_stat;
            }set_op;
            struct {
                const String_vector* strings;
            }getchildren_op;
            struct {
                const char* value;
                int value_length;
            }get_op;
        };
    };

    struct zoo_opcontext {
        ZOO_OPERATION _optype;
        zoo_input _input;
        zoo_output _output;
        std::function<void (zoo_opcontext*)> _callback_function;
        zookeeper_session* _priv_session_ref;
    };

    static zoo_opcontext* create_context()
    {
        zoo_opcontext* result = new zoo_opcontext();
        result->_input._flags = 0;
        result->_input._is_set_watch = false;
        result->_input._owner = nullptr;
        result->_input._watcher_callback = nullptr;

        memset(&(result->_output), 0, sizeof(zoo_output));

        result->_optype = ZOO_OPINVALID;
        result->_callback_function = nullptr;
        result->_priv_session_ref = nullptr;
        return result;
    }
    static void free_context(zoo_opcontext* ctx) { delete ctx; }

public:
    typedef std::function<void (int)> state_callback;
    zookeeper_session(void* srv_node);
    ~zookeeper_session();
    int attach(void* callback_owner, const state_callback& cb);
    void detach(void* callback_owner);

    int session_state() const { return zoo_state(_handle); }
    void visit(zoo_opcontext* op_context);
    void init_non_dsn_thread();

private:
    utils::rw_lock_nr _watcher_lock;
    struct watcher_object {
        std::string watcher_path;
        void* callback_owner;
        state_callback watcher_callback;
    };
    std::list<watcher_object> _watchers;
    service_node* _srv_node;
    zhandle_t* _handle;

    void dispatch_event(int type, int zstate, const char* path);
    static void global_watcher(
            zhandle_t* handle,
            int type,
            int state,
            const char* path,
            void* ctx);
    static void global_string_completion(
            int rc,
            const char* name,
            const void* data);
    static void global_data_completion(
            int rc,
            const char* value,
            int value_length,
            const struct Stat* stat,
            const void* data);
    static void global_state_completion(
            int rc,
            const struct Stat* stat,
            const void* data);
    static void global_strings_completion(
            int rc,
            const struct String_vector* strings,
            const void* data);
    static void global_void_completion(
            int rc,
            const void* data);
};

}}
