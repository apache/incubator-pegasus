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
 *     interface of layer2 frameworks
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), first draft
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/service_api_cpp.h>

 /*
  base contract for the layer2 frameworks on server side

 rDSN has three properties at layer 2 for applications to config, namely

 - stateful or not? whether the application is stateful.

 - partitioned or not? whether the application needs to be partitioned.

 - replicated or not? whether the application needs to be replicated.
  */
namespace dsn
{
    namespace dist
    {
        class layer2_handler
        {
        public:
            template <typename T> static layer2_handler* create()
            {
                return new T();
            }

            typedef layer2_handler* (*factory)();

        public:
            virtual error_code initialize(dsn_app* app_type) = 0;

            virtual void redirect(message_ex* msg) = 0;

        public:
            void commit_layer1(message_ex* msg);
        };

        // partition = true, replication = false, stateless 
        class partitioner : public virtual layer2_handler
        {
        public:
            template <typename T> static partitioner* create()
            {
                return new T();
            }

            typedef partitioner* (*factory)();


        public:
            /*
            * initialization work
            */
            virtual error_code initialize(dsn_app* xxxx) = 0;

        private:

        };

        // single node redirector
        

        // many vnodes redirector
        class vnodes_replicator : public layer2_handler
        {
        public:
            template <typename T> static vnodes_replicator* create()
            {
                return new T();
            }

            typedef vnodes_replicator* (*factory)();
            
        public:
            /*
            * initialization work
            */
            void replicate(message_ex* msg);

        public:
            void commit(message_ex* msg);
        };
    }
}
