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

namespace rDSN.Tron.Contract
{
    [AttributeUsage(AttributeTargets.Interface|AttributeTargets.Class)]
    public class TronService : Attribute
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
    [AttributeUsage(AttributeTargets.Method)]
    public class SideEffect : Attribute
    { 
    }

    /// <summary>
    /// whether this method is implemented via service composition
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    public class Composed : Attribute
    {
    }

    /// <summary>
    /// whether this method is an upcall (event), default is downcall
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    public class UpCall : Attribute
    {
    }
  
}


namespace IDL
{
    [AttributeUsage(AttributeTargets.Enum | AttributeTargets.Class | AttributeTargets.Interface)]
    public class SchemaAttribute : Attribute
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

    [AttributeUsage(AttributeTargets.Field)]
    public class FieldAttribute : Attribute
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

    [AttributeUsage(AttributeTargets.Method)]
    public class MethodAttribute : Attribute
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

    [AttributeUsage(AttributeTargets.All)]
    public class DocAttribute : Attribute
    {
        public DocAttribute(string d)
        {
            doc = d;
        }

        public string doc;
    }



}