#include "mutation.h"


namespace rdsn { namespace replication {

mutation::mutation()
{
    _memorySize = sizeof(MutationHeader);
    _private0 = 0; 
    _notLogged = 1;
}

mutation::~mutation()
{
    clear_log_task();
}

void mutation::add_client_request(message_ptr& request)
{
    rdsn::utils::blob buffer(request->get_input_remaining_buffer());
    auto buf = buffer.buffer();
    utils::blob bb(buf, static_cast<int>(buffer.data() - buffer.buffer().get()), buffer.length());

    client_requests.push_back(request);
    data.updates.push_back(bb);
    _memorySize += request->total_size();
}

/*static*/ mutation_ptr mutation::read_from(message_ptr& reader)
{
    mutation_ptr mu(new mutation());
    unmarshall(reader, mu->data);

    for (auto it = mu->data.updates.begin(); it != mu->data.updates.end(); it++)
    {        
        void * buf = malloc(it->length());
        memcpy(buf, it->data(), it->length());                              
        rdsn::utils::blob bb((const char *)buf, 0, it->length());
        message_ptr msg(new message(bb, false));
        mu->client_requests.push_back(msg);
        mu->_memorySize += msg->total_size();
    }

    mu->_fromMessage = reader;
    sprintf (mu->_name, "%llu.%llu", mu->data.header.ballot, mu->data.header.decree);
    return mu;
}

void mutation::write_to(message_ptr& writer)
{
    marshall(writer, data);
}

int mutation::clear_prepare_or_commit_tasks()
{
    int c = 0;
    for (auto it = _prepareOrCommitTasks.begin(); it != _prepareOrCommitTasks.end(); it++)
    {
        it->second->cancel(true);
        c++;
    }

    _prepareOrCommitTasks.clear();
    return c;
}

int mutation::clear_log_task()
{
    if (_logTask != nullptr)
    {
        _logTask->cancel(true);
        _logTask = nullptr;
        return 1;
    }
    else
    {
        return 0;
    }
}

}} // namespace end