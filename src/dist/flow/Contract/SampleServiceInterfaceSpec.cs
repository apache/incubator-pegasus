/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus (rDSN) -=- 
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), done in Tron project and copied here
 *     xxxx-xx-xx, author, fix bug about xxx
 */
 
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
