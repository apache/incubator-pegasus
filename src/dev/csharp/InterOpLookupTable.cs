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
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), done in rDSN.CSharp project and copied here
 *     xxxx-xx-xx, author, fix bug about xxx
 */

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace dsn.dev.csharp
{
    /// <summary>
    /// TODO: optimization - use TLS slots for quick Put and Get in most cases
    /// </summary>
    public static class GlobalInterOpLookupTable
    {
        public static int Put(object obj)
        {
            int table_id = Thread.CurrentThread.ManagedThreadId % _table_count;
            int idx = _tables[table_id].Put(obj);
            Logging.dassert(idx <= 0x07ffffff, "too many concurrent objects in global lookup table now");
            return (table_id << 27) + idx;
        }

        public static object Get(int index)
        {
            int table_id = index >> 27;
            int idx = index & 0x07ffffff;
            return _tables[table_id].Get(idx);
        }

        public static object GetRelease(int index)
        {
            int table_id = index >> 27;
            int idx = index & 0x07ffffff;
            return _tables[table_id].GetRelease(idx);
        }

        private static InterOpLookupTable[] _tables = InitTables(100, 997);
        private static int _table_count;

        private static InterOpLookupTable[] InitTables(int init_slot_count_per_table, int table_count)
        {
            _table_count = table_count;

            List<InterOpLookupTable> tables = new List<InterOpLookupTable>();
            for (int i = 0; i < table_count; i++)
            {
                var table = new InterOpLookupTable(init_slot_count_per_table);
                tables.Add(table);
            }
            return tables.ToArray();
        }
    }

    public class InterOpLookupTable
    {
        public InterOpLookupTable(int init_count)
        {
            _objects = new List<object>();
            _free_objects = new Queue<int>();
            for (int i = 0; i < _objects.Count; i++)
            {
                _objects.Add(null);
                _free_objects.Enqueue(i);
            }

        }
        public int Put(object obj)
        {
            lock(_free_objects)
            {
                int idx;
                if (_free_objects.Count > 0)
                {
                    idx = _free_objects.Dequeue();
                    _objects[idx] = obj;
                }
                else
                {
                    idx = _objects.Count;
                    _objects.Add(obj);                    
                }
                return idx;
            }
        }

        public object Get(int index)
        {
            lock(_free_objects)
            {
                return _objects[index];
            }
        }

        public object GetRelease(int index)
        {
            lock (_free_objects)
            {
                var obj = _objects[index];
                _objects[index] = null;
                _free_objects.Enqueue(index);
                return obj;
            }
        }

        private List<object> _objects;
        private Queue<int> _free_objects;
    }
}
