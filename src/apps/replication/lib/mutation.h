#pragma once


#include "replication_common.h"
#include <list>

#pragma warning(disable: 4201)

namespace rdsn { namespace replication {

class mutation : public ref_object
{
public:
    mutation();
    virtual ~mutation();

    // state inquery
    const char* name() const { return _name; }
    long memory_size() const { return _memorySize; }           
    bool is_logged() const { return _notLogged == 0; }
    bool is_prepared() const { return _notLogged == 0; }
    bool is_ready_for_commit() const { return _private0 == 0; }
    message_ptr& owner_message() { return _fromMessage; }
    unsigned int left_secondary_ack_count() const { return _leftSecondaryAckCount; }
    unsigned int left_potential_secondary_ack_count() const { return _leftPotentialSecondaryAckCount; }
    uint64_t& start_time_milliseconds() { return _startTimeMs; }
    task_ptr& log_task() { return _logTask; }
    node_tasks& remote_tasks() { return _prepareOrCommitTasks; }

    // state change
    void set_id(ballot b, decree c);
    void add_client_request(message_ptr& request);
    void set_logged() { rassert(!is_logged(), ""); _notLogged = 0; }
    unsigned int decrease_left_secondary_ack_count() { return --_leftSecondaryAckCount; }
    unsigned int decrease_left_potential_secondary_ack_count() { return --_leftPotentialSecondaryAckCount; }
    void set_left_secondary_ack_count(unsigned int count) { _leftSecondaryAckCount = count; }
    void set_left_potential_secondary_ack_count(unsigned int count) { _leftPotentialSecondaryAckCount = count; }
    int  clear_prepare_or_commit_tasks();
    int  clear_log_task();
    
    // reader & writer
    static mutation_ptr read_from(message_ptr& reader);
    void write_to(message_ptr& writer);

    // data
    mutation_data          data;
    std::list<message_ptr> client_requests;
        
private:
    union
    {
    struct 
    {
    unsigned int _notLogged : 1;
    unsigned int _leftSecondaryAckCount : 7;
    unsigned int _leftPotentialSecondaryAckCount : 8;
    };
    uint16_t       _private0;
    };

    node_tasks    _prepareOrCommitTasks;
    task_ptr      _logTask;

    uint64_t       _startTimeMs;
    int          _memorySize;

    message_ptr   _fromMessage;
    char         _name[40]; // ballot.decree
};

DEFINE_REF_OBJECT(mutation)

// ---------------------- inline implementation ----------------------------
inline void mutation::set_id(ballot b, decree c)
{
    data.header.ballot = b;
    data.header.decree = c;
    sprintf (_name, "%llu.%llu", b, c);
}

}} // namespace

#pragma warning(default: 4201)
