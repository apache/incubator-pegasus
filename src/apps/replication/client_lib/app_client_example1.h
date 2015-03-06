#pragma once

#include "replication_app_client_base.h"

namespace rdsn { namespace replication {

class app_client_example1 : public replication_app_client_base
{
public:
    app_client_example1(const std::vector<end_point>& meta_servers);
    ~app_client_example1(void);

    // async ops
    void read(const std::string& key, rpc_reply_handler callback);
    void Update(const std::string& key, const std::string& value, rpc_reply_handler callback);
    void append(const std::string& key, const std::string& appendValue, rpc_reply_handler callback);

    // sync ops
    int read(const std::string& key, __out std::string& value);
    int Update(const std::string& key, const std::string& value);
    int append(const std::string& key, const std::string& appendValue);
    
private:
    int KeyToPartitionIndex(const std::string& key);
    int HandleResponse(rpc_response_task_ptr& resp, std::string* pvalue = nullptr);

private:
    int _timeoutMilliesecondsRead;
    int _timeoutMilliesecondsWrite;
};


}} // namespace
