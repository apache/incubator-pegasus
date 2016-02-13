using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;

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
            return (object)_reducer(source);
        }

        private Func<IEnumerable<TSource>, TResult> _reducer;
    }

    public delegate object ParallelReducer<TSource>(IEnumerable<TSource> source);

    public static class ParallelReducerHelper
    {
        private class EnumerableMulticastServer<T>
        {
            public EnumerableMulticastServer(IEnumerable<T> enums, int clientCount)
            {
                _clients = new EnumerableMulticastClient<T>[clientCount];
                for (int i = 0; i < clientCount; i++)
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

                else
                {
                    return null;
                }
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
                    T o = _q.Take();
                    if (_end && _q.Count == 0)
                        break;

                    yield return o;
                }
            }

            private BlockingCollection<T> _q = new BlockingCollection<T>();
            private bool _end = false;
        }
        
        public static object[] ParallelReduce<TSource>(this IEnumerable<TSource> source, ParallelReducer<TSource>[] reducers)
        {
            EnumerableMulticastServer<TSource> multicastServer = new EnumerableMulticastServer<TSource>(source, reducers.Length);

            List<Task<object>> tasks = new List<Task<object>>();
            for (int index = 0; index < reducers.Length; index++)
            {
                var param = new KeyValuePair<ParallelReducer<TSource>, EnumerableMulticastClient<TSource>> (
                        reducers[index],
                        multicastServer.GetClient(index)
                        );

                Task<object> task = new Task<object>(
                    (obj) => 
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
