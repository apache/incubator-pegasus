#pragma once

# include <rdsn/tool_api.h>
# include <rdsn/internal/priority_queue.h>

namespace rdsn {
    namespace tools {
        class simple_task_queue : public task_queue
        {
        public:
            simple_task_queue(task_worker_pool* pool, int index, task_queue* inner_provider);

            virtual void     enqueue(task_ptr& task);
            virtual task_ptr dequeue();
            virtual int      count() const;

        private:
            typedef utils::blocking_priority_queue<task_ptr, TASK_PRIORITY_COUNT> tqueue;
            tqueue _queue;
        };
    }
}