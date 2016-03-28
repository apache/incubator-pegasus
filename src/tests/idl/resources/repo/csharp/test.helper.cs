using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dsn.example
{
    public static class testHelper
    {
        static Dictionary<string, bool> _result;
        public static void add_result(string test_name, bool result)
        {
            lock(_result)
            {
                if (!_result.ContainsKey(test_name))
                {
                    _result[test_name] = result;
                } else
                {
                    _result[test_name] = _result[test_name] && result;
                }
            }
        }

        public static int finished_test()
        {
            lock(_result)
            {
                return _result.Count();
            }
        }

        public static Dictionary<string, bool> result()
        {
            lock(_result)
            {
                Dictionary<string, bool> copy = new Dictionary<string, bool>(_result);
                return copy;
            }
        }

        public static int add_operand;

        public static string add_name;

        public static string read_name;

        public static int add_ret;

        public static int read_ret;

        public static void init()
        {
            add_operand = 123;
            add_name = "counter add";
            read_name = "counter read";
            add_ret = 0;
            read_ret = 23423;
            _result = new Dictionary<string, bool>();
        }
    }
}
