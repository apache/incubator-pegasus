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
# pragma once

# include <dsn/service_api_c.h>
# include <string>
# include <vector>

namespace dsn 
{
    namespace tools 
    {
        class checker
        {
        public:
            checker(const char* name, dsn_app_info* info, int count)
                : _name(name)
            {
                _apps.resize(count);
                for (int i = 0; i < count; i++)
                {
                    _apps[i] = info[i];
                }
            }

            virtual void check() = 0;

            const std::string& name() const { return _name; }

        protected:
            std::vector<dsn_app_info> _apps;

        private:
            std::string _name;

        public:
            template<typename T> // T : public checker
            static void* create(const char* name, dsn_app_info* info, int count)
            {
                auto chker = new T(name, info, count);                
                return dynamic_cast<checker*>(chker);
            }

            static void apply(void* chker)
            {
                checker* c = (checker*)chker;
                c->check();
            }
        };
    }    
} // end namespace dsn::tools

