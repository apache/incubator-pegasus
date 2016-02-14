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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/service_api_cpp.h>
# include <dsn/cpp/auto_codes.h>
# include <unordered_map>
# include <algorithm>
# include <string>
# include <vector>



namespace dsn
{
    namespace dist
    {

        //error code
        DEFINE_ERR_CODE(ERR_RESOURCE_NOT_ENOUGH)

        class machine_pool_mgr
        {
        public:
            machine_pool_mgr(const char* sec);

            /*
            * Each string in the forbidden_list is of the format username@hostname.
            * Each string in the assign_list is of the format username@hostname.
            * The error_code of ERR_OK indicates a successful get,
            * while an ERR_RESOURCE_NOT_ENOUGH indicate there are not enough machines.
            */
            error_code get_machine(
                int count,
                const std::vector<std::string>& forbidden_list,
                /*out*/ std::vector<std::string>& assign_list
                );

            /*each string in the machine_list is at the format username&hostname of a machine*/
            void return_machine(
                const std::vector<std::string>& machine_list
                );

        private:
            struct machine_workload
            {
                int instance;
            };

            struct machine_info
            {
                machine_workload workload;
                std::string identity;
                bool friend operator < (const machine_info &a, const machine_info &b)
                {
                    return a.workload.instance < b.workload.instance;
                }
            };

            std::unordered_map<std::string, machine_info> _machines;

            ::dsn::service::zlock _lock;


            error_code parse_cluster_config_file(
                const std::string& cluster_config_file,
                /*out*/ std::vector<std::string> &machine_id
                );
        };
    }
}
