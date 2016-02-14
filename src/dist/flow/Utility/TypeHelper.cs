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
using System.Reflection;
using System.Diagnostics;

namespace rDSN.Tron.Utility
{
    public static class TypeHelper
    {
        public static bool IsEnumerable(this Type type)
        {
            return type.GetInterface("IEnumerable") != null;
        }

        public static bool IsSymbols(this Type type)
        {
            return type.Name == "ISymbolCollection`1";
        }

        public static bool IsSymbol(this Type type)
        {
            return type.Name == "ISymbol`1";
        }
        
        public static bool IsAnonymous(this Type type)
        {
            return type.Name.StartsWith("<>f__AnonymousType");
        }

        public static bool IsSimpleType(this Type type)
        {
            return type.IsPrimitive 
                || type == typeof(Guid) 
                || type == typeof(DateTime) 
                || type == typeof(TimeSpan)
                || type == typeof(string)
                ;
        }

        public static string GetCompilableTypeName(this string typeName)
        {
            return typeName.Replace("::", ".");
        }

        private static void EchoLine(int indent, string line)
        {
            for (int i = 0; i < indent; i++)
                Console.Write("  ");

            Console.WriteLine(line);
        }

        public static void Echo(this object o, string name, int indent = 0, int maxdepth = int.MaxValue)
        {
            if (indent > maxdepth) return;

            if (o == null)
            {
                EchoLine(indent, name + " = [(null)]");
            }
            else
            {
                Type type = o.GetType();            

                EchoLine(indent, type.FullName + " " + name + " = [");
                foreach (var m in type.GetFields())
                {
                    if (m.FieldType.IsSimpleType())
                    {
                        EchoLine(indent + 1, "(field)" + m.Name + " = " + m.GetValue(o));
                    }
                    else
                    {
                        m.GetValue(o).Echo(m.Name, indent + 1, maxdepth);
                    }
                }

                foreach (var p in type.GetProperties())
                {
                    if (p.PropertyType.IsSimpleType())
                    {
                        try
                        {
                            EchoLine(indent + 1, "(prop)" + p.Name + " = " + p.GetValue(o, new object[] { }));
                        }
                        catch (Exception)
                        {
                            EchoLine(indent + 1, "(prop)" + p.Name + " = ...");
                        }
                    }
                    else
                    {
                        p.GetValue(o, new object[] { }).Echo(p.Name, indent + 1, maxdepth);
                    }
                }
                EchoLine(indent, "]");
            }
        }

        public static bool IsInheritedTypeOf(this Type type, Type baseType)
        {
            while (type != null)
            {   
                if (type == baseType || 
                    (baseType.IsGenericTypeDefinition && type.Name == baseType.Name)
                    )
                    return true;

                type = type.BaseType;
            }
            return false;
        }

        public static FieldInfo GetFieldEx(this Type type, string name, BindingFlags flags)
        {
            do
            {
                var fld = type.GetField(name, flags);
                if (fld != null)
                    return fld;

                type = type.BaseType;               
            }
            while (type != null);
            return null;
        }
        
        public static PropertyInfo GetPropertyEx(this Type type, string name, BindingFlags flags)
        {
            do
            {
                var prop = type.GetProperty(name, flags);
                if (prop != null)
                    return prop;

                type = type.BaseType;
            }
            while (type != null);
            return null;
        }

        public static object GetMemberValue(this object o, MemberInfo member)
        {
            if (member.MemberType == MemberTypes.Field)
            {
                FieldInfo fld = member.DeclaringType.GetFieldEx(member.Name, BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
                Trace.Assert(fld != null);
                if (o != null)
                {
                    return fld.GetValue(o);
                }
                else
                {   
                    if (fld.IsStatic)
                        return fld.GetValue(null);
                    else
                        return null;
                }
            }
            else if (member.MemberType == MemberTypes.Property)
            {
                PropertyInfo prop = member.DeclaringType.GetPropertyEx(member.Name, BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
                Trace.Assert(prop != null);
                return prop.GetValue(o, new object[] { });
            }
            else
            {
                throw new Exception("member type '" + member.MemberType + "' for '" + member.Name + "' is not supported yet");
            }
        }

        public static Type GetElementType(Type seqType)
        {
            if (seqType.IsEnumerable())
            {
                Type ienum = FindIEnumerable(seqType);
                if (ienum == null) return seqType;
                return ienum.GetGenericArguments()[0];
            }
            else if (seqType.IsSymbols())
            {
                return seqType.GetGenericArguments()[0];
            }
            else if (seqType.IsSymbol())
            {
                return seqType.GetGenericArguments()[0];
            }
            else
            {
                return seqType;
            }
        }

        private static Type FindIEnumerable(Type seqType)
        {
            if (seqType == null || seqType == typeof(string))
                return null;

            if (seqType.IsArray)
                return typeof(IEnumerable<>).MakeGenericType(seqType.GetElementType());

            if (seqType.IsGenericType)
            {
                foreach (Type arg in seqType.GetGenericArguments())
                {
                    Type ienum = typeof(IEnumerable<>).MakeGenericType(arg);
                    if (ienum.IsAssignableFrom(seqType))
                    {
                        return ienum;
                    }
                }
            }

            Type[] ifaces = seqType.GetInterfaces();
            if (ifaces != null && ifaces.Length > 0)
            {
                foreach (Type iface in ifaces)
                {
                    Type ienum = FindIEnumerable(iface);
                    if (ienum != null) return ienum;
                }
            }

            if (seqType.BaseType != null && seqType.BaseType != typeof(object))
            {
                return FindIEnumerable(seqType.BaseType);
            }

            return null;
        }


    }
}
