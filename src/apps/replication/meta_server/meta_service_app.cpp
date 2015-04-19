/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#include <dsn/dist/replication.h>
#include "server_state.h"
#include "meta_service.h"

namespace dsn {
    namespace service {

        server_state * meta_service_app::_reliable_state = nullptr;

        meta_service_app::meta_service_app(service_app_spec* s, configuration_ptr c)
            : service_app(s, c)
        {
            _service = nullptr;
        }

        meta_service_app::~meta_service_app()
        {

        }

        error_code meta_service_app::start(int argc, char** argv)
        {
            if (nullptr == _reliable_state)
            {
                _reliable_state = new server_state();
            }

            auto cf = config();
            _service = new meta_service(_reliable_state, cf);
            _reliable_state->init_app(cf);
            _reliable_state->add_meta_node(_service->address());
            _service->start();
            return ERR_SUCCESS;
        }

        void meta_service_app::stop(bool cleanup)
        {
            if (_reliable_state != nullptr)
            {
                if (_service != nullptr)
                {
                    _service->stop();
                    _reliable_state->remove_meta_node(_service->address());
                    delete _service;
                    _service = nullptr;

                    end_point primary;
                    if (!_reliable_state->get_meta_server_primary(primary))
                    {
                        delete _reliable_state;
                        _reliable_state = nullptr;
                    }
                }
            }
            else
            {
                dassert(_service == nullptr, "service must be null");
            }
        }
    }
}
