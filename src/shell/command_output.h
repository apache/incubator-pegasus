#include "shell/commands.h"

class command_output
{
public:
    explicit command_output(const std::string &file_name) : _file_name(file_name)
    {
        if (!file_name.empty()) {
            _file_stream = std::make_unique<std::ofstream>(_file_name);
        }
    }
    std::ostream *stream() const
    {
        if (_file_stream && !_file_stream->is_open()) {
            fmt::print(stderr, "open output file {} failed!\n", _file_name);
            return nullptr;
        }
        return _file_stream ? _file_stream.get() : &std::cout;
    }

private:
    std::string _file_name;
    std::unique_ptr<std::ofstream> _file_stream;
};