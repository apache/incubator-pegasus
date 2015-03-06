# include "shared_io_service.h"
# include "simple_task_queue.h"


# define __TITLE__ "task.queue.simple"

namespace rdsn {
    namespace tools{
        simple_task_queue::simple_task_queue(task_worker_pool* pool, int index, task_queue* inner_provider)
            : task_queue(pool, index, inner_provider), _queue("")
        {
        }

        void simple_task_queue::enqueue(task_ptr& task)
        {
            if (task->delay_milliseconds() == 0)
                _queue.enqueue(task, task->spec().priority);
            else
            {
                boost::shared_ptr<boost::asio::deadline_timer> timer(new boost::asio::deadline_timer(shared_io_service::instance().ios));
                timer->expires_from_now(boost::posix_time::milliseconds(task->delay_milliseconds()));
                task->set_delay(0);

                timer->async_wait([this, task, timer](const boost::system::error_code& ec) 
                { 
                    if (!ec)
                        this->enqueue((task_ptr&)task);
                    else
                    {
                        rdsn_fatal("delayed execution failed for task %s, err = %u",
                            task->spec().name, ec.value());
                    }
                });
            }
        }

        task_ptr simple_task_queue::dequeue()
        {
            int c = 0;
            return _queue.dequeue(c);
        }

        int      simple_task_queue::count() const
        {
            return _queue.count();
        }
    }
}