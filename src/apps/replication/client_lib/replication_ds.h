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

#pragma once

# include <dsn/internal/enum_helper.h>
# include <dsn/dist/replication/replication.types.h>

namespace dsn {
    namespace replication {

        ENUM_BEGIN(partition_status, PS_INVALID)
            ENUM_REG(PS_INACTIVE)
            ENUM_REG(PS_ERROR)
            ENUM_REG(PS_PRIMARY)
            ENUM_REG(PS_SECONDARY)
            ENUM_REG(PS_POTENTIAL_SECONDARY)
        ENUM_END(partition_status)

        ENUM_BEGIN(learner_status, Learning_INVALID)
            ENUM_REG(LearningWithoutPrepare)
            ENUM_REG(LearningWithPrepareTransient)
            ENUM_REG(LearningWithPrepare)
            ENUM_REG(LearningSucceeded)
            ENUM_REG(LearningFailed)
        ENUM_END(learner_status)
        
        ENUM_BEGIN(config_type, CT_NONE)
            ENUM_REG(CT_ASSIGN_PRIMARY)
            ENUM_REG(CT_UPGRADE_TO_PRIMARY)
            ENUM_REG(CT_ADD_SECONDARY)
            ENUM_REG(CT_DOWNGRADE_TO_SECONDARY)
            ENUM_REG(CT_DOWNGRADE_TO_INACTIVE)
            ENUM_REG(CT_REMOVE)
            ENUM_REG(CT_UPGRADE_TO_SECONDARY)
        ENUM_END(config_type)
    }
} // end namespace dsn::replication

namespace std
{
    template<>
    struct hash<::dsn::replication::global_partition_id> {
        size_t operator()(const ::dsn::replication::global_partition_id &gpid) const {
            return std::hash<int>()(gpid.app_id) ^ std::hash<int>()(gpid.pidx);
        }
    };
}