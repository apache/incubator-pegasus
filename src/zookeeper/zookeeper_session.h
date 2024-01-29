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

#include <stdint.h>
#include <string.h>
#include <zookeeper/zookeeper.h>
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "runtime/service_app.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/fmt_utils.h"
#include "utils/synchronize.h"

struct String_vector;

namespace dsn {
namespace dist {

// A C++ wrapper of zookeeper c async APIs.
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
        ZOO_TRANSACTION,
        ZOO_OPCOUNT,
        ZOO_OPINVALID
    };

    struct zoo_atomic_packet
    {
    public:
        unsigned int _capacity;
        unsigned int _count;
        zoo_op_t *_ops;
        zoo_op_result_t *_results;

        std::vector<std::string> _paths;
        std::vector<blob> _datas;

    public:
        zoo_atomic_packet(unsigned int size);
        ~zoo_atomic_packet();

        static char *alloc_buffer(int buffer_length);
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
        void *_owner;
        std::function<void(int)> _watcher_callback;

        /* for multi-op transaction */
        std::shared_ptr<zoo_atomic_packet> _pkt;
    };

    struct zoo_output
    {
        int error;
        union
        {
            struct
            {
                const char *_created_path;
            } create_op;
            struct
            {
                const struct Stat *_node_stat;
            } exists_op;
            struct
            {
                const struct Stat *_node_stat;
            } set_op;
            struct
            {
                const String_vector *strings;
            } getchildren_op;
            struct
            {
                const char *value;
                int value_length;
            } get_op;
        };
    };

    struct zoo_opcontext : public dsn::ref_counter
    {
        ZOO_OPERATION _optype;
        zoo_input _input;
        zoo_output _output;
        std::function<void(zoo_opcontext *)> _callback_function;

        // this are for implement usage, user shouldn't modify this directly
        zookeeper_session *_priv_session_ref;
        int32_t _ref_count;
    };

    static zoo_opcontext *create_context()
    {
        zoo_opcontext *result = new zoo_opcontext();
        result->_input._flags = 0;
        result->_input._is_set_watch = false;
        result->_input._owner = nullptr;
        result->_input._watcher_callback = nullptr;

        memset(&(result->_output), 0, sizeof(zoo_output));

        result->_optype = ZOO_OPINVALID;
        result->_callback_function = nullptr;
        result->_priv_session_ref = nullptr;

        result->add_ref();
        return result;
    }

    static void add_ref(zoo_opcontext *op) { op->add_ref(); }
    static void release_ref(zoo_opcontext *op) { op->release_ref(); }
    static const char *string_zoo_operation(ZOO_OPERATION op);
    static const char *string_zoo_event(int zoo_event);
    static const char *string_zoo_state(int zoo_state);

public:
    typedef std::function<void(int)> state_callback;
    zookeeper_session(const service_app_info &info);
    ~zookeeper_session();
    int attach(void *callback_owner, const state_callback &cb);
    void detach(void *callback_owner);

    int session_state() const { return zoo_state(_handle); }
    void visit(zoo_opcontext *op_context);
    void init_non_dsn_thread();

private:
    utils::rw_lock_nr _watcher_lock;
    struct watcher_object
    {
        std::string watcher_path;
        void *callback_owner;
        state_callback watcher_callback;
    };
    std::list<watcher_object> _watchers;
    service_app_info _srv_node;
    zhandle_t *_handle;

    void dispatch_event(int type, int zstate, const char *path);
    static void global_watcher(zhandle_t *handle, int type, int state, const char *path, void *ctx);
    static void global_string_completion(int rc, const char *name, const void *data);
    static void global_data_completion(
        int rc, const char *value, int value_length, const struct Stat *stat, const void *data);
    static void global_state_completion(int rc, const struct Stat *stat, const void *data);
    static void
    global_strings_completion(int rc, const struct String_vector *strings, const void *data);
    static void global_void_completion(int rc, const void *data);
};
}
}

USER_DEFINED_STRUCTURE_FORMATTER(::dsn::dist::zookeeper_session);
