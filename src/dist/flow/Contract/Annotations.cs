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
            Name = n;
        }
        /// <summary>
        /// to represent the source type
        /// </summary>
        /// <value>
        /// proto, thrift, etc.
        /// </value>
        public SchemaAttribute(string n, string v)
        {
            Name = n;
            Version = v;
        }

        public string Name;
        public string Version;
    }

    [AttributeUsage(AttributeTargets.Field)]
    public class FieldAttribute : Attribute
    {
       
        public FieldAttribute(int i)
        {
            Index = i;
            Modifier = "optional";
            DefaultValue = "";
        }
        public FieldAttribute(int i, string m)
        {
            Index = i;
            Modifier = m;

        }
        public FieldAttribute(int i, string m, int d)
        {
            Index = i;
            Modifier = m;
            DefaultValue = d.ToString();

        }

        public FieldAttribute(int i, string m, double d)
        {
            Index = i;
            Modifier = m;
            DefaultValue = d.ToString();

        }

        public FieldAttribute(int i, string m, string d)
        {
            Index = i;
            Modifier = m;
            DefaultValue = d;

        }

        public int Index { get; set; }

        /// <summary>
        /// to represent: required, optional, repeated,etc.
        /// </summary>
        public string Modifier { get; set; }


        public string DefaultValue { get; set; }

    }

    [AttributeUsage(AttributeTargets.Method)]
    public class MethodAttribute : Attribute
    {

        public MethodAttribute()
        {
            IsOneWay = false;

        }

        public MethodAttribute(bool t)
        {
            IsOneWay = t;
        }
        public bool IsOneWay;

    }

    [AttributeUsage(AttributeTargets.All)]
    public class DocAttribute : Attribute
    {
        public DocAttribute(string d)
        {
            Doc = d;
        }

        public string Doc;
    }



}