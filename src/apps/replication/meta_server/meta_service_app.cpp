/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

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
#include "meta_service_app.h"
#include "server_state.h"

server_state * meta_service_app::_reliableState = nullptr;

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
    if (nullptr == _reliableState)
    {
        _reliableState = new server_state();
    }

    _service = new meta_service(_reliableState, config());
    _reliableState->AddMetaNode(_service->address());
    _service->start();    
    return ERR_SUCCESS;
}

void meta_service_app::stop(bool cleanup)
{
    if (_reliableState != nullptr)
    {
        if (_service != nullptr)
        {
            _service->stop();
            _reliableState->RemoveMetaNode(_service->address());
            delete _service;
            _service = nullptr;

            end_point primary;
            if (!_reliableState->GetMetaServerPrimary(primary))
            {
                delete _reliableState;
                _reliableState = nullptr;
            }
        }
    }
    else
    {
        rassert(_service == nullptr, "service must be null");
    }
}
