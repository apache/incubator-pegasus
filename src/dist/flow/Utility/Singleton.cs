using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace rDSN.Tron.Utility
{
    public class Singleton<T>:MarshalByRefObject
           where T : new()
    {
        private static T _instance = default(T);
        private static object _lock = new object();
        public static T Instance()
        {
            if (_instance == null)
            {
                lock (_lock)
                {
                    if (_instance == null)
                    {
                        T tmp = new T();
                        _instance = tmp;
                    }
                }
            }
            return _instance;
        }

        public static void SetInstance(T instance)
        {
            lock (_lock)
            {
                _instance = instance;
            }
        }
    }
}
