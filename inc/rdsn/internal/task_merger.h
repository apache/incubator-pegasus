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
