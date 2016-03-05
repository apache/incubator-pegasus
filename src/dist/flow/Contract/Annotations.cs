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
        /// proto, thrift, etc.
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