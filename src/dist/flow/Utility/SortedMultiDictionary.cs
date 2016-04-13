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

using System.Collections.Generic;
using System.Linq;

namespace rDSN.Tron.Utility
{
    public class SortedMultiDictionary<TKey, TValue>
    {
        public void Put(TKey key, TValue val)
        {
            List<TValue> vs;
            if (!_dict.TryGetValue(key, out vs))
            {
                vs = new List<TValue>();
                _dict.Add(key, vs);
            }
            vs.Add(val);
        }

        public TValue Get(TKey key, bool remove)
        {
            List<TValue> vs;
            if (!_dict.TryGetValue(key, out vs)) return default(TValue);
            TValue v;
            if (remove)
            {
                v = vs.First();
                vs.RemoveAt(0);

                if (vs.Count == 0)
                {
                    _dict.Remove(key);
                }
            }
            else
            {
                v = vs.First();
            }

            return v;
        }

        public TValue Min(out TKey key, bool remove)
        {
            if (_dict.Count > 0)
            {
                var vs = _dict.First();
                key = vs.Key;

                if (!remove)
                {
                    return vs.Value.First();
                }
                var v = vs.Value.First();
                vs.Value.RemoveAt(0);
                if (vs.Value.Count == 0)
                {
                    _dict.Remove(vs.Key);
                }
                return v;
            }
            key = default(TKey);
            return default(TValue);
        }

        public TValue Max(out TKey key, bool remove)
        {
            if (_dict.Count > 0)
            {
                var vs = _dict.Last();
                key = vs.Key;

                if (!remove)
                {
                    return vs.Value.First();
                }
                var v = vs.Value.First();
                vs.Value.RemoveAt(0);
                if (vs.Value.Count == 0)
                {
                    _dict.Remove(vs.Key);
                }
                return v;
            }
            key = default(TKey);
            return default(TValue);
        }

        public int Count => _dict.Count;

        private SortedDictionary<TKey, List<TValue>> _dict = new SortedDictionary<TKey, List<TValue>>();
    }
}
