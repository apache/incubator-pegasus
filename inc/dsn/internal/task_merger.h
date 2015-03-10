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
# pragma once

# include <zion/internal/task.h>
# include <list>

namespace zion {

    ///*    
    //PREAPRE_ACK -> _
    //PREPARE_ACK -> _ => CLIENT_ACK
    //*/
    //class TaskGather
    //{
    //public:
    //    TaskGather();

    //    void Gather(int index)
    //    {
    //        extensible* ext = new extensible();
    //        for (int i = 0; i < task::get_extension_count(); i++)
    //            ext->set_extension(i, task::get_current_task()->get_extension(i));
    //        
    //        _gathers[index] = (ext);
    //    }

    //    void seal()
    //    {
    //        OnKeyedGather.execute(_gathers, task::get_current_task());
    //    }

    //    static join_point<void, std::map<int, extensible*>&, task_ptr> OnKeyedGather;

    //protected:
    //    std::map<int, extensible*> _gathers;
    //};

    //template<typename TValue>
    //class TaskScatter
    //{
    //public:
    //    TaskScatter
    //    {
    //        // automatic state copy from task::get_current_task() to _current;        
    //    }

    //    virtual void Scatter(TValue key)
    //    {
    //        OnScatter.execute(&_current, task::get_current_task(), key);
    //    }

    //    static join_point<void, extensible*, task*, TValue> OnScatter;

    //protected:
    //    extensible _current;
    //};


    ///*
    //RPC_CLIENT_WRITE -> _                                      RPC_CLIENT_WRITE_ACK
    //RPC_CLIENT_WRITE -> _ => RPC_PREPARE => PRC_PREPARE_ACK => PRC_CLIENT_WRITE_ACK
    //*/
    //template<typename TValue>
    //class Batch : public TaskGather<TValue>, public TaskScatter<TValue>
    //{
    //public:
    //    //static join_point<void, ExtentionObject*, TaskHolder*> OnBatch;
    //    //static join_point<void, TaskHolder*, ExtentionObject*> OnDebatch;

    //    virtual void seal()
    //    {
    //        // copy _gathers to Batch<TValue>::Helper::set(task::CurrentTask(), _gathers);
    //    }

    //    virtual void Scatter(TValue key)
    //    {

    //    }
    //};

} // end namespace zion
