#pragma once

#include <string>
#include <vector>
#include <list>

namespace dsn {
namespace utils {

void split_args(const char *args, /*out*/ std::vector<std::string> &sargs, char splitter = ' ');

void split_args(const char *args, /*out*/ std::list<std::string> &sargs, char splitter = ' ');

std::string
replace_string(std::string subject, const std::string &search, const std::string &replace);

std::string get_last_component(const std::string &input, const char splitters[]);

char *trim_string(char *s);
}
}
