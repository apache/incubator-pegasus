#pragma once

# include <string>
# include <map>
# include <dsn/service_api_cpp.h>
# include <iostream>

#define TEST_ADD_ARGS_OPERAND 23321
#define TEST_ADD_ARGS_NAME "rdsn counter add"
#define TEST_ADD_RESULT 1235
#define TEST_READ_ARGS_NAME "rdsn counter read"
#define TEST_READ_RESULT 0

class counter_test_helper{
public:
    counter_test_helper(int total_test) : _total_test(total_test), _finished(false) {}
    void add_result(std::string test_name, bool result)
    {
        ::dsn::service::zauto_lock l(_lock);
    //    std::cout << test_name << " " << result << std::endl;
        if (_result.find(test_name) == _result.end())
        {
            _result[test_name] = result;
        } else
        {
            _result[test_name] = _result[test_name] && result;
        }
        if (_result.size() == _total_test)
        {
            _finished = true;
        }
    }
    bool finished()
    {
        return _finished;
    }
    std::map<std::string, bool> result()
    {
        return _result;
    }
private:
    int _total_test;
    bool _finished;
    std::map<std::string, bool> _result;
    ::dsn::service::zlock _lock;
};

extern counter_test_helper* witness;