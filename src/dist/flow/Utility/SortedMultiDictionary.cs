using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace rDSN.Tron.Utility
{
    public class SortedMultiDictionary<TKey, TValue>
    {
        public void Put(TKey key, TValue val)
        {
            List<TValue> vs = null;
            if (!_dict.TryGetValue(key, out vs))
            {
                vs = new List<TValue>();
                _dict.Add(key, vs);
            }
            vs.Add(val);
        }

        public TValue Get(TKey key, bool remove)
        {
            List<TValue> vs = null;
            if (_dict.TryGetValue(key, out vs))
            {
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
            else
                return default(TValue);
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
                else
                {
                    var v = vs.Value.First();
                    vs.Value.RemoveAt(0);
                    if (vs.Value.Count == 0)
                    {
                        _dict.Remove(vs.Key);
                    }
                    return v;
                }
            }
            else
            {
                key = default(TKey);
                return default(TValue);
            }
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
                else
                {
                    var v = vs.Value.First();
                    vs.Value.RemoveAt(0);
                    if (vs.Value.Count == 0)
                    {
                        _dict.Remove(vs.Key);
                    }
                    return v;
                }
            }
            else
            {
                key = default(TKey);
                return default(TValue);
            }
        }

        public int Count { get { return _dict.Count; } }

        private SortedDictionary<TKey, List<TValue>> _dict = new SortedDictionary<TKey, List<TValue>>();
    }
}
