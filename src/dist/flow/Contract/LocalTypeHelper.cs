using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

using rDSN.Tron.Utility;


namespace rDSN.Tron.Contract
{
    public static class LocalTypeHelper
    {
        public static bool IsQueryable(this Type type)
        {
            return type.GetInterface("IQueryable") != null;

            //if (type.Name == "IQueryable`1" || type.Name == "IOrderedQueryable`1")
            //    return true;
            //else
            //    return false;
        }

        public static bool IsDirectEnumerable(this Type type)
        {
            return type.Name.StartsWith("IEnumerable`1");
        }

        public static bool IsDirectQueryable(this Type type)
        {
            return type.Name.StartsWith("IQueryable`1") || type.Name.StartsWith("IOrderedQueryable`1");
        }

        public static bool IsGrouping(this Type type)
        {
            return (type.GetInterface("IGrouping") != null) || type.Name.StartsWith("IGrouping`");
        }

        public static bool IsTuple(this Type type)
        {
            return type.Name.StartsWith("Tuple`");
        }

        public static bool IsPublicType(this Type type)
        {
            if (type.IsPublic) return true;
            else if (type.IsNestedPublic) return type.DeclaringType.IsPublicType();
            else return false;
        }

        public static Type FromQueryType2RunningType(this Type t)
        {
            if (t.IsQueryable() || t.IsSymbols())
            {
                return typeof(IEnumerable<>).MakeGenericType(TypeHelper.GetElementType(t));
            }
            else
            {
                return t;
            }
        }
        
        public static bool IsSerializable(this Type t)
        {
            if (t.IsPrimitive || t.IsEnum || t == typeof(string) || t == typeof(DateTime) || t == typeof(TimeSpan) || t == typeof(Guid))
            {
                return true;
            }

            else if (t.IsTuple())
            {
                Trace.Assert(t.IsGenericType);
                foreach (var item in t.GetGenericArguments())
                {
                    if (!IsSerializable(item))
                    {
                        return false;
                    }
                }
                return true;
            }

            else if (t.IsAnonymous())
            {
                foreach (var prop in t.GetProperties())
                {
                    if (!IsSerializable(prop.PropertyType))
                    {
                        return false;
                    }
                }
                return true;
            }

            else if (t.IsGrouping())
            {
                return IsSerializable(t.GetGenericArguments()[0]) && IsSerializable(t.GetGenericArguments()[1]);
            }

            else if (t.IsDirectEnumerable() || t.IsDirectQueryable() || t.IsArray)
            {
                return IsSerializable(TypeHelper.GetElementType(t));
            }

            else
            {
                if (!t.IsPublicType())
                {
                    throw new Exception("User defined type '" + t.FullName + "' must be public");
                }

                if (t.GetInterface("ISerializable") == null)
                {
                    throw new Exception("User defined type '" + t.FullName + "' must implement ISerializable interface");
                }

                if (t.GetConstructor(Type.EmptyTypes) == null)
                {
                    throw new Exception("User defined type '" + t.FullName + "' must have a constructor '" + t.Name + "()'");
                }

                return true;
            }
        }

        public static void CollectSubTypes(this Type t, HashSet<Type> types)
        {
            if (t.IsPrimitive || t.IsEnum || t == typeof(string) || t == typeof(DateTime) || t == typeof(TimeSpan) || t == typeof(Guid))
            {
                types.Add(t);
            }

            else if (t.IsTuple())
            {
                Trace.Assert(t.IsGenericType);

                types.Add(t);
                foreach (var item in t.GetGenericArguments())
                {
                    CollectSubTypes(item, types);
                }
            }

            else if (t.IsAnonymous())
            {
                types.Add(t);

                foreach (var prop in t.GetProperties())
                {
                    CollectSubTypes(prop.PropertyType, types);
                }
            }

            else if (t.IsGrouping())
            {
                types.Add(t);
                CollectSubTypes(t.GetGenericArguments()[0], types);
                CollectSubTypes(t.GetGenericArguments()[1], types);
            }

            else if (t.IsDirectEnumerable() || t.IsDirectQueryable() || t.IsArray)
            {
                types.Add(t);
                CollectSubTypes(TypeHelper.GetElementType(t), types);
            }

            else
            {
                types.Add(t);
            }
        }

        public static string GetCompilableTypeName(this Type t, Dictionary<Type, string> rewrittenTypes)
        {
            if (rewrittenTypes.ContainsKey(t))
            {
                return rewrittenTypes[t];
            }

            else if (t.IsPrimitive || t == typeof(string) || t == typeof(DateTime) || t == typeof(TimeSpan) || t == typeof(Guid))
            {
                return t.Name;
            }

            else if (t.IsEnum)
            {
                return "byte";
            }

            else if (t.IsTuple())
            {
                Trace.Assert(t.IsGenericType);
                StringBuilder typeName = new StringBuilder("Tuple<");
                foreach (var parameterType in t.GetGenericArguments())
                {
                    typeName = typeName.Append(GetCompilableTypeName(parameterType, rewrittenTypes) + ", ");
                }
                typeName.Remove(typeName.Length - 2, 2);
                typeName.Append(">");
                return typeName.ToString();
            }

            else if (t.IsArray)
            {
                return GetCompilableTypeName(TypeHelper.GetElementType(t), rewrittenTypes) + "[]";
            }

            else if (t.IsGrouping())
            {
                return "IGrouping<" + GetCompilableTypeName(t.GetGenericArguments()[0], rewrittenTypes)
                    + ", " + GetCompilableTypeName(t.GetGenericArguments()[1], rewrittenTypes)
                    + ">";
            }

            else if (t.IsDirectEnumerable() || t.IsDirectQueryable())
            {
                return "IEnumerable<" + GetCompilableTypeName(TypeHelper.GetElementType(t), rewrittenTypes) + ">";
            }

            else if (t.IsSymbols())
            {
                return "IEnumerable<" 
                    + GetCompilableTypeName(t.GetGenericArguments()[0], rewrittenTypes)
                    + ">";
            }

            else if (t.IsSymbol())
            {
                return "IValue<"
                        + GetCompilableTypeName(t.GetGenericArguments()[0], rewrittenTypes)
                        + ">";
            }

            else
            {
                return t.FullName.Replace('+', '.');
            }
        }

        public static string GetTypeNameAsFunctionName(string typeName)
        {
            return typeName.Replace('.', '_')
                .Replace('<', '_')
                .Replace('>', '_')
                .Replace('[', '_')
                .Replace(']', '_')
                .Replace(',', '_')
                .Replace(' ', '_')
                ;
        }

        public static string GetTypeNameAsFunctionName(this Type t, Dictionary<Type, string> rewrittenTypes)
        {
            return GetTypeNameAsFunctionName(GetCompilableTypeName(t, rewrittenTypes));
        }

        public static bool ConstantValue2String(object o, out string val)
        {
            val = "undefined";
            try
            {
                val = ConstantValue2StringInternal(o);
                return true;
            }
            catch(Exception)
            {
                return false;
            }
        }

        public static string ConstantValue2StringInternal(object o)
        {
            //if (o == null)
            //{
            //    return "null";
            //}
            //else 
            if (o is bool)
            {
                return ((bool)o) ? "true" : "false";
            }
            else if (o is string)
            {
                return "\"" + o + "\"";
            }
            else if (o is DateTime)
            {
                return "new DateTime(" + ((DateTime)o).Ticks + ")";
            }
            else if (o is TimeSpan)
            {
                return "new TimeSpan(" + ((TimeSpan)o).Ticks + ")";
            }
            else if (o.GetType().IsPrimitive)
            {
                return o.ToString();
            }
            else if (o.GetType().IsEnum)
            {
                return ((int)o).ToString();
            }
            else
            {
                throw new Exception("non primitive variable '" + o.ToString() + "' reference is not supported yet!");
            }
        }

        public static void BuildReader(this Type t, CodeBuilder builder, Dictionary<Type, string> rewrittenTypes)
        {
            string typeName = GetCompilableTypeName(t, rewrittenTypes);
            string cTypeName = GetTypeNameAsFunctionName(typeName);

            builder.AppendLine("private static " + typeName + " Read_" + cTypeName + "(BinaryReader reader)");
            builder.AppendLine("{");
            builder++;

            if (t.IsPrimitive || t == typeof(string) || t == typeof(DateTime) || t == typeof(TimeSpan))
            {
                builder.AppendLine("return reader.Read" + typeName + "();");
            }

            else if (t.IsEnum)
            {
                builder.AppendLine("return (" + typeName + ")reader.ReadByte();");
            }

            else if (t == typeof(Guid))
            {
                builder.AppendLine("return new Guid(reader.ReadString());");
            }

            else if (t.IsTuple())
            {
                Trace.Assert(t.IsGenericType);
                StringBuilder temp = new StringBuilder("return Tuple.Create(");
                foreach (var parameterType in t.GetGenericArguments())
                {
                    temp = temp.Append("Read_" + GetTypeNameAsFunctionName(parameterType, rewrittenTypes) + "(reader), ");
                }
                temp.Remove(temp.Length - 2, 2);
                temp.Append(");");
                builder.AppendLine(temp.ToString());
            }

            else if (t.IsAnonymous())
            {
                StringBuilder tempObject = new StringBuilder("{ ");
                foreach (var propertyType in t.GetProperties())
                {
                    tempObject.Append(propertyType.Name + " = Read_" + GetTypeNameAsFunctionName(propertyType.PropertyType, rewrittenTypes) + "(reader), ");
                }
                tempObject.Remove(tempObject.Length - 2, 2);
                tempObject.Append("}");
                builder.AppendLine("return new " + typeName + "() " + tempObject.ToString() + ";");
            }

            else if (t.IsGrouping())
            {
                Type keyType = t.GetGenericArguments()[0];
                Type elementType = t.GetGenericArguments()[1];

                builder.AppendLine("var temp = new SimpleGrouping<"
                    + GetCompilableTypeName(keyType, rewrittenTypes) + ", "
                    + GetCompilableTypeName(elementType, rewrittenTypes) + ">();");
                builder.AppendLine("temp.Key = Read_" + GetTypeNameAsFunctionName(keyType, rewrittenTypes) + "(reader);");
                builder.AppendLine("int count = reader.ReadInt32();");
                builder.AppendLine("for(int i = 0; i< count; i++)");
                builder.AppendLine("{");
                builder++;
                builder.AppendLine("temp.Add(Read_" + GetTypeNameAsFunctionName(elementType, rewrittenTypes) + "(reader));");
                builder--;
                builder.AppendLine("}");
                builder.AppendLine("return temp;");
            }

            else if (t.IsDirectEnumerable() || t.IsDirectQueryable() || t.IsArray)
            {
                Type elementType = TypeHelper.GetElementType(t);
                builder.AppendLine("int count = reader.ReadInt32();");
                builder.AppendLine("var temp = new List<" + GetCompilableTypeName(elementType, rewrittenTypes) + ">();");
                builder.AppendLine("for(int i = 0; i< count; i++)");
                builder.AppendLine("{");
                builder++;
                builder.AppendLine("temp.Add(Read_" + GetTypeNameAsFunctionName(elementType, rewrittenTypes) + "(reader));");
                builder--;
                builder.AppendLine("}");
                if (t.IsArray)
                {
                    builder.AppendLine("return temp.ToArray();");
                }
                else
                {
                    builder.AppendLine("return temp;");
                }
            }

            else
            {
                builder.AppendLine("var temp = new " + typeName + "();");
                builder.AppendLine("if (temp.Read(reader)) return temp;");
                builder.AppendLine("else return null;");
            }

            builder--;
            builder.AppendLine("}");
        }

        public static void BuildWriter(this Type t, CodeBuilder builder, Dictionary<Type, string> rewrittenTypes)
        {
            string typeName = t.GetCompilableTypeName(rewrittenTypes);
            string cTypeName = GetTypeNameAsFunctionName(typeName);

            builder.AppendLine("private static void Write_" + cTypeName + "(BinaryWriter writer, " + typeName + " obj)");
            builder.AppendLine("{");
            builder++;

            if (t.IsPrimitive || t == typeof(string) || t == typeof(DateTime) || t == typeof(TimeSpan))
            {
                builder.AppendLine("writer.Write(obj);");
            }

            else if (t.IsEnum)
            {
                builder.AppendLine("writer.Write((byte)(int)obj);");
            }

            else if (t == typeof(Guid))
            {
                builder.AppendLine("writer.Write(obj.ToString());");
            }

            else if (t.IsTuple())
            {
                Trace.Assert(t.IsGenericType);
                int itemNum = 0;
                foreach (var parameterType in t.GetGenericArguments())
                {
                    itemNum++;
                    builder.AppendLine("Write_" + GetTypeNameAsFunctionName(parameterType, rewrittenTypes) + "(writer, obj.Item" + itemNum + ");");
                }
            }

            else if (t.IsAnonymous())
            {
                foreach (var propertyType in t.GetProperties())
                {
                    builder.AppendLine("Write_" + GetTypeNameAsFunctionName(propertyType.PropertyType, rewrittenTypes) + "(writer, obj." + propertyType.Name + ");");
                }
            }

            else if (t.IsGrouping())
            {
                Type keyType = t.GetGenericArguments()[0];
                Type elementType = t.GetGenericArguments()[1];

                builder.AppendLine("Write_" + GetTypeNameAsFunctionName(keyType, rewrittenTypes) + "(writer, obj.Key);");
                builder.AppendLine("int count = obj.Count();");
                builder.AppendLine("writer.Write(count);");
                builder.AppendLine("foreach (var temp in obj)");
                builder.AppendLine("{");
                builder++;
                builder.AppendLine("Write_" + GetTypeNameAsFunctionName(elementType, rewrittenTypes) + "(writer, temp);");
                builder--;
                builder.AppendLine("}");
            }

            else if (t.IsDirectEnumerable() || t.IsDirectQueryable() || t.IsArray)
            {
                Type elementType = TypeHelper.GetElementType(t);
                builder.AppendLine("int count = obj.Count();");
                builder.AppendLine("writer.Write(count);");
                builder.AppendLine("foreach (var temp in obj)");
                builder.AppendLine("{");
                builder++;
                builder.AppendLine("Write_" + GetTypeNameAsFunctionName(elementType, rewrittenTypes) + "(writer, temp);");
                builder--;
                builder.AppendLine("}");
            }

            else
            {
                Trace.Assert(t.IsClass && t.GetInterface("ISerializable") != null);
                builder.AppendLine("obj.Write(writer);");
            }

            builder--;
            builder.AppendLine("}");
        }
    }
}
