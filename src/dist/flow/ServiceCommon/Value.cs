using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections;

namespace rDSN.Tron.Runtime
{
    public class IValue
    { }

    public class IValue<TValue> : IValue
    {
        private TValue _value;

        public IValue()
            : base()
        {
            _value = default(TValue);
        }

        public IValue(TValue v)
            : base()
        {
            _value = v;
        }

        public static implicit operator TValue(IValue<TValue> d)
        {
            return d._value;
        }

        //public static implicit operator IValue<TValue>(TValue d)
        //{
        //    return new IValue<TValue>(d);
        //}

        public TValue Value()
        {
            return _value;
        }
    }
}
