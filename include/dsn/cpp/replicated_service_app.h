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
 *     application model atop of zion in c++ (layer 2)
 *
 * Revision history:
 *     Mar., 2016, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/cpp/service_app.h>

namespace dsn 
{
    /*!
    @addtogroup app-model
    @{
    */

    class replicated_service_app_type_1 : public service_app
    {
    public:
        replicated_service_app_type_1(dsn_gpid gpid) 
            : service_app(gpid) { _physical_error = 0; }

        virtual ~replicated_service_app_type_1(void) {}

        //
        // for stateful apps with layer 2 support
        //
        virtual ::dsn::error_code checkpoint(int64_t version) { return ERR_NOT_IMPLEMENTED; }

        virtual ::dsn::error_code checkpoint_async(int64_t version) { return ERR_NOT_IMPLEMENTED; }

        virtual int64_t get_last_checkpoint_version() const { return 0; }

        //
        // prepare an app-specific learning request (on learner, to be sent to learneee
        // and used by method get_checkpoint), so that the learning process is more efficient
        //
        // return value:
        //   0 - it is unnecessary to prepare a earn request
        //   <= capacity - learn request is prepared ready
        //   > capacity - buffer is not enough, caller should allocate a bigger buffer and try again
        //
        virtual int prepare_get_checkpoint(void* buffer, int capacity) { return 0; }

        // 
        // Learn [start, infinite) from remote replicas (learner)
        //
        // Must be thread safe.
        //
        // The learned checkpoint can be a complete checkpoint (0, infinite), or a delta checkpoint
        // [start, infinite), depending on the capability of the underlying implementation.
        // 
        // Note the files in learn_state are copied from dir /replica@remote/data to dir /replica@local/learn,
        // so when apply the learned file state, make sure using learn_dir() instead of data_dir() to get the
        // full path of the files.
        //
        // the given state is a plain buffer where applications should fill it carefully for all the learn-state
        // into this buffer, with the total written size in total-learn-state-size.
        // if the capcity is not enough, applications should return ERR_CAPACITY_EXCEEDED and put required
        // buffer size in total-learn-state-size field, and our replication frameworks will retry with this new size
        //
        struct app_learn_state
        {
            int64_t from_decree_excluded;
            int64_t to_decree_included;
            blob    meta_state;
            std::vector<std::string> files;

            ::dsn::error_code to_c_state(dsn_app_learn_state& state, int state_capacity)
            {
                bool succ = true;

                state.total_learn_state_size = sizeof(dsn_app_learn_state);
                state.from_decree_excluded = from_decree_excluded;
                state.to_decree_included = to_decree_included;
                state.meta_state_size = meta_state.length();
                state.file_state_count = files.size();

                char* ptr = (char*)&state + sizeof(dsn_app_learn_state);
                char* end = (char*)&state + state_capacity;
                if (state.meta_state_size > 0)
                {
                    if (ptr + state.meta_state_size > end)
                    {
                        succ = false;
                    }
                    state.meta_state_ptr = ptr;
                    if (succ) memcpy(ptr, meta_state.data(), state.meta_state_size);
                    ptr += state.meta_state_size;
                }
                else
                {
                    state.meta_state_ptr = nullptr;
                }

                if (files.size() > 0)
                {
                    if (ptr + sizeof(const char*) * files.size() > end)
                    {
                        succ = false;
                    }

                    state.files = (const char**)ptr;

                    const char** pptr = state.files;
                    ptr = ptr + sizeof(const char*) * files.size();
                    for (auto& file : files)
                    {
                        if (ptr + file.length() + 1 > end)
                        {
                            succ = false;
                        }

                        if (succ)
                        {
                            *pptr++ = ptr;
                            memcpy(ptr, file.c_str(), file.length());
                            ptr += file.length();
                            *ptr++ = '\0';
                        }
                        else
                        {
                            ptr += file.length() + 1;
                        }
                    }
                }
                else
                {
                    state.files = nullptr;
                }

                state.total_learn_state_size = ptr - (char*)&state;
                return succ ? ERR_OK : ERR_CAPACITY_EXCEEDED;
            }
        };

        virtual ::dsn::error_code get_checkpoint(
            int64_t start,
            int64_t local_commit,
            void*   learn_request,
            int     learn_request_size,
            /* inout */ app_learn_state& state
            ) = 0;

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
        // Postconditions:
        // * after apply_checkpoint() done, last_committed_decree() == last_durable_decree()
        // 
        virtual ::dsn::error_code apply_checkpoint(int64_t local_commit, const dsn_app_learn_state& state, dsn_chkpt_apply_mode mode) = 0;

        int get_last_physical_error() const { return _physical_error; }

        void set_physical_error(int err) { _physical_error = err; }

    private:
        int _physical_error;

    public:
        static dsn_error_t app_checkpoint(void* app, int64_t version)
        {
            auto sapp = (replicated_service_app_type_1*)(app);
            return sapp->checkpoint(version);
        }

        static dsn_error_t app_checkpoint_async(void* app, int64_t version)
        {
            auto sapp = (replicated_service_app_type_1*)(app);
            return sapp->checkpoint_async(version);
        }

        static int64_t app_checkpoint_get_version(void* app)
        {
            auto sapp = (replicated_service_app_type_1*)(app);
            return sapp->get_last_checkpoint_version();
        }

        static int app_prepare_get_checkpoint(void* app, void* buffer, int capacity)
        {
            auto sapp = (replicated_service_app_type_1*)(app);
            return sapp->prepare_get_checkpoint(buffer, capacity);
        }
        
        static dsn_error_t app_get_checkpoint(
            void*   app,
            int64_t start_decree,
            int64_t local_commit,
            void*   learn_request,
            int     learn_request_size,
            /* inout */ dsn_app_learn_state* state,
            int state_capacity
            )
        {
            auto sapp = (replicated_service_app_type_1*)(app);
            app_learn_state cpp_state;
            auto err = sapp->get_checkpoint(start_decree, local_commit, learn_request, learn_request_size, cpp_state);
            if (err == ERR_OK)
            {
                return cpp_state.to_c_state(*state, state_capacity);
            }
            else
            {
                return err;
            }
        }
        
        static dsn_error_t app_apply_checkpoint(void* app, int64_t local_commit, const dsn_app_learn_state* state, dsn_chkpt_apply_mode mode)
        {
            auto sapp = (replicated_service_app_type_1*)(app);
            return sapp->apply_checkpoint(local_commit, *state, mode);
        }

        static int app_get_physical_error(void* app)
        {
            auto sapp = (replicated_service_app_type_1*)(app);
            return sapp->get_last_physical_error();
        }
    };

    /*! C++ wrapper of the \ref dsn_register_app function for layer 1 */
    template<typename TServiceApp>
    void register_app_with_type_1_replication_support(const char* type_name)
    {
        dsn_app app;
        memset(&app, 0, sizeof(app));
        app.mask = DSN_APP_MASK_FRAMEWORK;
        strncpy(app.type_name, type_name, sizeof(app.type_name));
        app.layer1.create = service_app::app_create<TServiceApp>;
        app.layer1.start = service_app::app_start;
        app.layer1.destroy = service_app::app_destroy;

        app.layer2.apps.calls.chkpt = replicated_service_app_type_1::app_checkpoint;
        app.layer2.apps.calls.chkpt_async = replicated_service_app_type_1::app_checkpoint_async;
        app.layer2.apps.calls.chkpt_get_version = replicated_service_app_type_1::app_checkpoint_get_version;
        app.layer2.apps.calls.checkpoint_get_prepare = replicated_service_app_type_1::app_prepare_get_checkpoint;
        app.layer2.apps.calls.chkpt_get = replicated_service_app_type_1::app_get_checkpoint;
        app.layer2.apps.calls.chkpt_apply = replicated_service_app_type_1::app_apply_checkpoint;
        app.layer2.apps.calls.physical_error_get = replicated_service_app_type_1::app_get_physical_error;

        dsn_register_app(&app);
    }

    /*@}*/
} // end namespace dsn::service

