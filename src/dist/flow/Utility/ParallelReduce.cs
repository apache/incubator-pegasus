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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace rDSN.Tron.Utility
{
    public class UnTypedReducer<TSource, TResult>
    {
        public UnTypedReducer(Func<IEnumerable<TSource>, TResult> reducer)
        {

            _reducer = reducer;
        }

        public object Invoke(IEnumerable<TSource> source)
        {
            return _reducer(source);
        }

        private Func<IEnumerable<TSource>, TResult> _reducer;
    }

    public delegate object ParallelReducer<in TSource>(IEnumerable<TSource> source);

    public static class ParallelReducerHelper
    {
        private class EnumerableMulticastServer<T>
        {
            public EnumerableMulticastServer(IEnumerable<T> enums, int clientCount)
            {
                _clients = new EnumerableMulticastClient<T>[clientCount];
                for (var i = 0; i < clientCount; i++)
                {
                    _clients[i] = new EnumerableMulticastClient<T>();
                }
                _enums = enums;
            }

            public void Run()
            {
                foreach (var o in _enums)
                {
                    foreach (var client in _clients)
                    {
                        client.Put(o);
                    }
                }

                foreach (var client in _clients)
                {
                    client.SetEnd();
                }
            }

            public EnumerableMulticastClient<T> GetClient(int index)
            {
                if (index >= 0 && index < _clients.Length)
                {
                    return _clients[index];
                }

                return null;
            }

            private EnumerableMulticastClient<T>[] _clients;
            private IEnumerable<T> _enums;
        }

        private class EnumerableMulticastClient<T>
        {
            public void Put(T o)
            {
                _q.Add(o);
            }

            public void SetEnd()
            {
                _end = true;
                Put(default(T));
            }

            public IEnumerable<T> Get()
            {
                while (true)
                {
                    var o = _q.Take();
                    if (_end && _q.Count == 0)
                        break;

                    yield return o;
                }
            }

            private BlockingCollection<T> _q = new BlockingCollection<T>();
            private bool _end;
        }
        
        public static object[] ParallelReduce<TSource>(this IEnumerable<TSource> source, ParallelReducer<TSource>[] reducers)
        {
            var multicastServer = new EnumerableMulticastServer<TSource>(source, reducers.Length);

            var tasks = new List<Task<object>>();
            for (var index = 0; index < reducers.Length; index++)
            {
                var param = new KeyValuePair<ParallelReducer<TSource>, EnumerableMulticastClient<TSource>> (
                        reducers[index],
                        multicastServer.GetClient(index)
                        );

                var task = new Task<object>(
                    obj => 
                    {
                        var p = (KeyValuePair<ParallelReducer<TSource>, EnumerableMulticastClient<TSource>>)obj;
                        return p.Key(p.Value.Get());
                    },
                    param
                    );

                tasks.Add(task);
                task.Start();
            }

            multicastServer.Run();

            return tasks.Select(t => { t.Wait(); return t.Result; }).ToArray();
        }
    }
}
