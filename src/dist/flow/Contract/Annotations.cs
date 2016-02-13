using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Reflection;

namespace rDSN.Tron.Contract
{
    [System.AttributeUsage(AttributeTargets.Interface|AttributeTargets.Class, AllowMultiple = false)]
    public class TronService : System.Attribute
    {
        public TronService()
        {
            Props = new ServiceProperty();
        }

        public ServiceProperty Props { get; set; }
    }

    /// <summary>
    /// whether this method has some side effect
    /// </summary>
    [System.AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public class SideEffect : System.Attribute
    { 
    }

    /// <summary>
    /// whether this method is implemented via service composition
    /// </summary>
    [System.AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public class Composed : System.Attribute
    {
    }

    /// <summary>
    /// whether this method is an upcall (event), default is downcall
    /// </summary>
    [System.AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public class UpCall : System.Attribute
    {
    }

    [System.AttributeUsage(AttributeTargets.Field)]
    public class BondAttr : System.Attribute
    {
        public BondAttr()
        {
            Index = -1;
        }

        public int Index { get; set; }
    }

    
}


namespace IDL
{
    [System.AttributeUsage(AttributeTargets.Enum | AttributeTargets.Class | AttributeTargets.Interface, AllowMultiple = false)]
    public class SchemaAttribute : System.Attribute
    {

        public SchemaAttribute()
        {

        }
        public SchemaAttribute(string n)
        {
            name = n;
        }
        /// <summary>
        /// to represent the source type
        /// </summary>
        /// <value>
        /// bond, proto, thrift, etc.
        /// </value>
        public SchemaAttribute(string n, string v)
        {
            name = n;
            version = v;
        }

        public string name;
        public string version;
    }

    [System.AttributeUsage(AttributeTargets.Field, AllowMultiple = false)]
    public class FieldAttribute : System.Attribute
    {
       
        public FieldAttribute(int i)
        {
            index = i;
            modifier = "optional";
            defaultValue = "";
        }
        public FieldAttribute(int i, string m)
        {
            index = i;
            modifier = m;

        }
        public FieldAttribute(int i, string m, int d)
        {
            index = i;
            modifier = m;
            defaultValue = d.ToString();

        }

        public FieldAttribute(int i, string m, double d)
        {
            index = i;
            modifier = m;
            defaultValue = d.ToString();

        }

        public FieldAttribute(int i, string m, string d)
        {
            index = i;
            modifier = m;
            defaultValue = d;

        }

        public int index { get; set; }

        /// <summary>
        /// to represent: required, optional, repeated,etc.
        /// </summary>
        public string modifier { get; set; }


        public string defaultValue { get; set; }

    }

    [System.AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public class MethodAttribute : System.Attribute
    {

        public MethodAttribute()
        {
            isOneWay = false;

        }

        public MethodAttribute(bool t)
        {
            isOneWay = t;
        }
        public bool isOneWay;

    }

    [System.AttributeUsage(AttributeTargets.All, AllowMultiple = false)]
    public class DocAttribute : System.Attribute
    {
        public DocAttribute(string d)
        {
            doc = d;
        }

        public string doc;
    }



}