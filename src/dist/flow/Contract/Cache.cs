using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace Microsoft.CSQL.Contract
{

    public class Case_Cache<TKey, TValue>
    {
        public class CachedEntity
        {
            public TKey Key;
            public TValue Value;
        }


        public class CachedResult : CachedEntity
        {
            public bool HasValue;
        }
        
        public class TypedCache_Service : Service<TypedCache_Service>
        {
            public TypedCache_Service(string name)
                : base(name)
            { }

            public CachedResult Get(TKey key) { throw new NotImplementedException(); }

            public int Put(CachedEntity entity) { throw new NotImplementedException(); }
        }

        public static TypedCache_Service CacheService = new TypedCache_Service(typeof(TValue).Name + ".Cache");
    }

}
