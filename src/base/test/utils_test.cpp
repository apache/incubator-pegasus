#include "../pegasus_utils.h"
#include <gtest/gtest.h>

namespace pegasus {
namespace utils {

TEST(utils_test, top_n)
{
    {
        std::list<int> data({2, 3, 7, 8, 9, 0, 1, 5, 4, 6});
        std::list<int> result = top_n<int>(data, 5).to();
        ASSERT_EQ(result, std::list<int>({0, 1, 2, 3, 4}));
    }

    {
        std::list<std::string> data({"2", "3", "7", "8", "9", "0", "1", "5", "4", "6"});
        std::list<std::string> result = top_n<std::string>(data, 5).to();
        ASSERT_EQ(result, std::list<std::string>({"0", "1", "2", "3", "4"}));
    }

    {
        struct longer
        {
            inline bool operator()(const std::string &l, const std::string &r)
            {
                return l.length() < r.length();
            }
        };

        std::list<std::string> data({std::string(2, 'a'),
                                     std::string(3, 'a'),
                                     std::string(7, 'a'),
                                     std::string(8, 'a'),
                                     std::string(9, 'a'),
                                     std::string(0, 'a'),
                                     std::string(1, 'a'),
                                     std::string(5, 'a'),
                                     std::string(4, 'a'),
                                     std::string(6, 'a')});
        std::list<std::string> result = top_n<std::string>(data, 5).to();
        ASSERT_EQ(result,
                  std::list<std::string>({std::string(0, 'a'),
                                          std::string(1, 'a'),
                                          std::string(2, 'a'),
                                          std::string(3, 'a'),
                                          std::string(4, 'a')}));
    }
}

} // namespace utils
} // namespace pegasus
