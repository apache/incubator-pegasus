using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;

namespace rDSN.Tron.Utility
{
    public class IEnumerableConverter<TSource, TResult> : IEnumerable<TResult>, IEnumerable
    {
        public delegate void PostEnumerationCallback(object arg);
        public IEnumerableConverter(IEnumerable<TSource> enums, PostEnumerationCallback cb, object cbArg)
        {
            _enums = enums;
            _cb = cb;
            _cbArg = cbArg;
            _converter = new TypeConverter(typeof(TSource), typeof(TResult));
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return (IEnumerator)GetEnumerator();
        }

        public IEnumerator<TResult> GetEnumerator()
        {
            if (_enums != null)
            {
                foreach (var o in _enums)
                {
                    yield return (TResult)_converter.Convert(o);
                }

                if (_cb != null)
                {
                    _cb(_cbArg);
                }
            }
        }

        private IEnumerable<TSource> _enums;
        private TypeConverter _converter;
        private PostEnumerationCallback _cb;
        private object _cbArg;
    }
}
