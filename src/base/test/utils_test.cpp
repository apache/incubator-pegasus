/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <list>
#include <string>

#include "../pegasus_utils.h"
#include "gtest/gtest.h"

namespace pegasus {
namespace utils {

TEST(utils_test, top_n)
{
    {
        std::list<int> data({2, 3, 7, 8, 9, 0, 1, 5, 4, 6});
        std::list<int> result = top_n<int>(data, 5).to();
        ASSERT_EQ(result, std::list<int>({0, 1, 2, 3, 4}));
    }

    {
        std::list<std::string> data({"2", "3", "7", "8", "9", "0", "1", "5", "4", "6"});
        std::list<std::string> result = top_n<std::string>(data, 5).to();
        ASSERT_EQ(result, std::list<std::string>({"0", "1", "2", "3", "4"}));
    }

    {
        struct longer
        {
            inline bool operator()(const std::string &l, const std::string &r)
            {
                return l.length() < r.length();
            }
        };

        std::list<std::string> data({std::string(2, 'a'),
                                     std::string(3, 'a'),
                                     std::string(7, 'a'),
                                     std::string(8, 'a'),
                                     std::string(9, 'a'),
                                     std::string(0, 'a'),
                                     std::string(1, 'a'),
                                     std::string(5, 'a'),
                                     std::string(4, 'a'),
                                     std::string(6, 'a')});
        std::list<std::string> result = top_n<std::string>(data, 5).to();
        ASSERT_EQ(result,
                  std::list<std::string>({std::string(0, 'a'),
                                          std::string(1, 'a'),
                                          std::string(2, 'a'),
                                          std::string(3, 'a'),
                                          std::string(4, 'a')}));
    }
}

} // namespace utils
} // namespace pegasus
