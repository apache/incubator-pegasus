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

/*
 * Description:
 *     application model atop zion in c++ (layer 2)
 *
 * Revision history:
 *     Mar., 2016, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/cpp/service_app.h>

namespace dsn {
/*!
@addtogroup app-model
@{
*/

class replicated_service_app_type_1 : public service_app
{
public:
    replicated_service_app_type_1(dsn_gpid gpid) : service_app(gpid) {}

    virtual ~replicated_service_app_type_1(void) {}

    //
    // for stateful apps with layer 2 support
    //

    virtual void
    on_batched_write_requests(int64_t decree, int64_t timestamp, dsn_message_t *requests, int count)
    {
    }

    virtual int get_physical_error() { return 0; }

    virtual ::dsn::error_code sync_checkpoint(int64_t last_commit) { return ERR_NOT_IMPLEMENTED; }

    virtual ::dsn::error_code async_checkpoint(int64_t last_commit, bool is_emergency)
    {
        return ERR_NOT_IMPLEMENTED;
    }

    virtual int64_t get_last_checkpoint_decree() { return 0; }
    //
    // prepare an app-specific learning request (on learner, to be sent to learneee
    // and used by method get_checkpoint), so that the learning process is more efficient
    //
    // return value:
    //   ERR_OK: get succeed, and actually used buffer size is stored in "occupied"
    //   ERR_CAPACITY_EXCEEDED: buffer capacity is not enough, and actually neede size is stored in
    //   "occupied".
    //
    virtual ::dsn::error_code
    prepare_get_checkpoint(void *buffer, int capacity, /*out*/ int *occupied)
    {
        *occupied = 0;
        return ERR_OK;
    }

    //
    // Learn [start, infinite) from remote replicas (learner)
    //
    // Must be thread safe.
    //
    // The learned checkpoint can be a complete checkpoint (0, infinite), or a delta checkpoint
    // [start, infinite), depending on the capability of the underlying implementation.
    //
    // Note the files in learn_state are copied from dir /replica@remote/data to dir
    // /replica@local/learn,
    // so when apply the learned file state, make sure using learn_dir() instead of data_dir() to
    // get the
    // full path of the files.
    //
    // the given state is a plain buffer where applications should fill it carefully for all the
    // learn-state
    // into this buffer, with the total written size in total-learn-state-size.
    // if the capcity is not enough, applications should return ERR_CAPACITY_EXCEEDED and put
    // required
    // buffer size in total-learn-state-size field, and our replication frameworks will retry with
    // this new size
    //
    struct app_learn_state
    {
        int64_t from_decree_excluded;
        int64_t to_decree_included;
        blob meta_state;
        std::vector<std::string> files;

        ::dsn::error_code to_c_state(dsn_app_learn_state &state, int state_capacity)
        {
            bool succ = true;

            state.from_decree_excluded = from_decree_excluded;
            state.to_decree_included = to_decree_included;
            state.meta_state_size = meta_state.length();
            state.file_state_count = static_cast<int>(files.size());

            char *ptr = (char *)&state + sizeof(dsn_app_learn_state);
            char *end = (char *)&state + state_capacity;
            if (state.meta_state_size > 0) {
                if (ptr + state.meta_state_size > end) {
                    succ = false;
                }
                if (succ) {
                    state.meta_state_ptr = ptr;
                    memcpy(ptr, meta_state.data(), state.meta_state_size);
                }
                ptr += state.meta_state_size;
            } else {
                state.meta_state_ptr = nullptr;
            }

            if (files.size() > 0) {
                if (ptr + sizeof(const char *) * files.size() > end) {
                    succ = false;
                }

                state.files = (const char **)ptr;

                const char **pptr = state.files;
                ptr = ptr + sizeof(const char *) * files.size();
                for (auto &file : files) {
                    if (ptr + file.length() + 1 > end) {
                        succ = false;
                    }

                    if (succ) {
                        *pptr++ = ptr;
                        memcpy(ptr, file.c_str(), file.length());
                        ptr += file.length();
                        *ptr++ = '\0';
                    } else {
                        ptr += file.length() + 1;
                    }
                }
            } else {
                state.files = nullptr;
            }

            state.total_learn_state_size = static_cast<int>(ptr - (char *)&state);
            return succ ? ERR_OK : ERR_CAPACITY_EXCEEDED;
        }
    };

    virtual ::dsn::error_code get_checkpoint(int64_t learn_start,
                                             int64_t local_commit,
                                             void *learn_request,
                                             int learn_request_size,
                                             /*out*/ app_learn_state &state) = 0;

    //
    // [DSN_CHKPT_LEARN]
    // after learn the state from learner, apply the learned state to the local app
    //
    // Or,
    //
    // [DSN_CHKPT_COPY]
    // when an app only implement synchonous checkpoint, the primary replica
    // needs to copy checkpoint from secondaries instead of
    // doing checkpointing by itself, in order to not stall the normal
    // write operations.
    //
    virtual ::dsn::error_code apply_checkpoint(dsn_chkpt_apply_mode mode,
                                               int64_t local_commit,
                                               const dsn_app_learn_state &state) = 0;

    //
    // copy the latest checkpoint to checkpoint_dir, and the decree of the checkpoint
    // copied will be assigned to checkpoint_decree if checkpoint_decree not null
    virtual ::dsn::error_code copy_checkpoint_to_dir(const char *checkpoint_dir,
                                                     /*output*/ int64_t *checkpoint_decree)
    {
        return ERR_NOT_IMPLEMENTED;
    }

public:
    static void app_on_batched_write_requests(
        void *app, int64_t decree, int64_t timestamp, dsn_message_t *requests, int count)
    {
        reinterpret_cast<replicated_service_app_type_1 *>(app)->on_batched_write_requests(
            decree, timestamp, requests, count);
    }

    static int app_get_physical_error(void *app)
    {
        return reinterpret_cast<replicated_service_app_type_1 *>(app)->get_physical_error();
    }

    static dsn_error_t app_sync_checkpoint(void *app, int64_t last_commit)
    {
        return reinterpret_cast<replicated_service_app_type_1 *>(app)->sync_checkpoint(last_commit);
    }

    static dsn_error_t app_async_checkpoint(void *app, int64_t last_commit, bool is_emergency)
    {
        return reinterpret_cast<replicated_service_app_type_1 *>(app)->async_checkpoint(
            last_commit, is_emergency);
    }

    static dsn_error_t
    app_copy_checkpoint_to_dir(void *app, const char *checkpoint_dir, int64_t *last_decree)
    {
        return reinterpret_cast<replicated_service_app_type_1 *>(app)->copy_checkpoint_to_dir(
            checkpoint_dir, last_decree);
    }

    static int64_t app_get_last_checkpoint_decree(void *app)
    {
        return reinterpret_cast<replicated_service_app_type_1 *>(app)->get_last_checkpoint_decree();
    }

    static dsn_error_t
    app_prepare_get_checkpoint(void *app, void *buffer, int capacity, int *occupied)
    {
        return reinterpret_cast<replicated_service_app_type_1 *>(app)->prepare_get_checkpoint(
            buffer, capacity, occupied);
    }

    static dsn_error_t app_get_checkpoint(void *app,
                                          int64_t learn_start,
                                          int64_t local_commit,
                                          void *learn_request,
                                          int learn_request_size,
                                          dsn_app_learn_state *state,
                                          int state_capacity)
    {
        app_learn_state cpp_state;
        auto err = reinterpret_cast<replicated_service_app_type_1 *>(app)->get_checkpoint(
            learn_start, local_commit, learn_request, learn_request_size, cpp_state);
        return err == ERR_OK ? cpp_state.to_c_state(*state, state_capacity) : err;
    }

    static dsn_error_t app_apply_checkpoint(void *app,
                                            dsn_chkpt_apply_mode mode,
                                            int64_t local_commit,
                                            const dsn_app_learn_state *state)
    {
        return reinterpret_cast<replicated_service_app_type_1 *>(app)->apply_checkpoint(
            mode, local_commit, *state);
    }
};

/*! C++ wrapper of the \ref dsn_register_app function for framework hosted apps */
template <typename TServiceApp>
void register_app_with_type_1_replication_support(const char *type_name)
{
    dsn_app app;
    memset(&app, 0, sizeof(app));
    app.mask = DSN_APP_MASK_FRAMEWORK;
    strncpy(app.type_name, type_name, sizeof(app.type_name));
    app.layer1.create = service_app::app_create<TServiceApp>;
    app.layer1.start = service_app::app_start;
    app.layer1.destroy = service_app::app_destroy;

    app.layer2.apps.calls.get_physical_error =
        replicated_service_app_type_1::app_get_physical_error;
    app.layer2.apps.calls.sync_checkpoint = replicated_service_app_type_1::app_sync_checkpoint;
    app.layer2.apps.calls.async_checkpoint = replicated_service_app_type_1::app_async_checkpoint;
    app.layer2.apps.calls.copy_checkpoint_to_dir =
        replicated_service_app_type_1::app_copy_checkpoint_to_dir;
    app.layer2.apps.calls.get_last_checkpoint_decree =
        replicated_service_app_type_1::app_get_last_checkpoint_decree;
    app.layer2.apps.calls.prepare_get_checkpoint =
        replicated_service_app_type_1::app_prepare_get_checkpoint;
    app.layer2.apps.calls.get_checkpoint = replicated_service_app_type_1::app_get_checkpoint;
    app.layer2.apps.calls.apply_checkpoint = replicated_service_app_type_1::app_apply_checkpoint;

    if (std::is_same<decltype(&TServiceApp::on_batched_write_requests),
                     decltype(&replicated_service_app_type_1::on_batched_write_requests)>())
        app.layer2.apps.calls.on_batched_write_requests = nullptr;
    else
        app.layer2.apps.calls.on_batched_write_requests =
            replicated_service_app_type_1::app_on_batched_write_requests;

    dsn_register_app(&app);
}

/*@}*/
} // end namespace dsn::service
