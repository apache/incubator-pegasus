#include <cstring>
#include <dsn/utility/strings.h>

namespace dsn {
namespace utils {

std::string get_last_component(const std::string &input, const char splitters[])
{
    int index = -1;
    const char *s = splitters;

    while (*s != 0) {
        auto pos = input.find_last_of(*s);
        if (pos != std::string::npos && (static_cast<int>(pos) > index))
            index = static_cast<int>(pos);
        s++;
    }

    if (index != -1)
        return input.substr(index + 1);
    else
        return input;
}

void split_args(const char *args, /*out*/ std::vector<std::string> &sargs, char splitter)
{
    sargs.clear();

    std::string v(args);

    int lastPos = 0;
    while (true) {
        auto pos = v.find(splitter, lastPos);
        if (pos != std::string::npos) {
            std::string s = v.substr(lastPos, pos - lastPos);
            if (s.length() > 0) {
                std::string s2 = trim_string((char *)s.c_str());
                if (s2.length() > 0)
                    sargs.push_back(s2);
            }
            lastPos = static_cast<int>(pos + 1);
        } else {
            std::string s = v.substr(lastPos);
            if (s.length() > 0) {
                std::string s2 = trim_string((char *)s.c_str());
                if (s2.length() > 0)
                    sargs.push_back(s2);
            }
            break;
        }
    }
}

void split_args(const char *args, /*out*/ std::list<std::string> &sargs, char splitter)
{
    sargs.clear();

    std::string v(args);

    int lastPos = 0;
    while (true) {
        auto pos = v.find(splitter, lastPos);
        if (pos != std::string::npos) {
            std::string s = v.substr(lastPos, pos - lastPos);
            if (s.length() > 0) {
                std::string s2 = trim_string((char *)s.c_str());
                if (s2.length() > 0)
                    sargs.push_back(s2);
            }
            lastPos = static_cast<int>(pos + 1);
        } else {
            std::string s = v.substr(lastPos);
            if (s.length() > 0) {
                std::string s2 = trim_string((char *)s.c_str());
                if (s2.length() > 0)
                    sargs.push_back(s2);
            }
            break;
        }
    }
}

std::string
replace_string(std::string subject, const std::string &search, const std::string &replace)
{
    size_t pos = 0;
    while ((pos = subject.find(search, pos)) != std::string::npos) {
        subject.replace(pos, search.length(), replace);
        pos += replace.length();
    }
    return subject;
}

char *trim_string(char *s)
{
    while (*s != '\0' && (*s == ' ' || *s == '\t')) {
        s++;
    }
    char *r = s;
    s += strlen(s);
    while (s >= r && (*s == '\0' || *s == ' ' || *s == '\t' || *s == '\r' || *s == '\n')) {
        *s = '\0';
        s--;
    }
    return r;
}
}
}
