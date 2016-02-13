using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rDSN.Tron.Contract
{
    public struct KeyValue
    {
        public string Key;
        public string Value;
    }
    
    public interface SampleServiceInterfaceSpec
    {
        /// <summary>
        /// put key, value pair
        /// </summary>
        /// <param name="kv"> key and value pair </param>
        /// <returns> error code </returns>
        int    Put(KeyValue kv);

        /// <summary>
        /// retrive value by key
        /// </summary>
        /// <param name="key"></param>
        /// <returns>value</returns>
        string Get(string key);
    }

    public interface SampleServiceInterfaceSpec2
    {
        /// <summary>
        /// put key, value pair
        /// </summary>
        /// <param name="kv"> key and value pair </param>
        /// <returns> error code </returns>
        int Put(KeyValue kv);

        /// <summary>
        /// retrive value by key
        /// </summary>
        /// <param name="key"></param>
        /// <returns>value</returns>
        string Get(string key);
    } 
}
