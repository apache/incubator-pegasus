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
using System.Diagnostics;

namespace rDSN.Tron.Utility
{
    public class TypeConverter
    {
        public enum ConversionType
        {
            Same, // same source and destination type
            Base, // dest is the base class of source
            Copy, // layer out is the same so it is ok, though the type name is not the same
            Invalid
        }

        public TypeConverter(Type SourceT, Type DestinationT)
        {
            SourceType = SourceT;
            DestinationType = DestinationT;
            Type = Check(SourceType, DestinationType);
        }

        public object Convert(object sourceObject)
        {
            Trace.Assert(sourceObject.GetType() == SourceType);

            if (Type == ConversionType.Same || Type == ConversionType.Base)
                return sourceObject;

            else if (Type == ConversionType.Copy)
            {
                var construc = DestinationType.GetConstructors();
                List<object> arg = new List<object>();
                foreach (var prop in SourceType.GetProperties())
                    arg.Add(prop.GetValue(sourceObject, new object[0]));
                return construc[0].Invoke(arg.ToArray());
            }
            else
            {
                throw new Exception("invalid object conversion");
            }
        }

        private static ConversionType Check(Type SourceT, Type DestinationT)
        {
            if (SourceT == DestinationT)
            {
                return ConversionType.Same;
            }

            else if (DestinationT.IsEnum && SourceT == typeof(byte))
            {
                return ConversionType.Copy;
            }

            else if (SourceT.IsSubclassOf(DestinationT))
            {
                return ConversionType.Base;
            }

            else if (SourceT.Name.StartsWith("RewrittenType_") && DestinationT.IsAnonymous())
            {
                if (SourceT.GetProperties().Count() != DestinationT.GetProperties().Count())
                {
                    return ConversionType.Invalid;
                }

                foreach (var prop in SourceT.GetProperties())
                {
                    var prop2 = DestinationT.GetProperty(prop.Name);
                    if (prop2 == null)
                    {
                        return ConversionType.Invalid;
                    }

                    var ct = Check(prop.PropertyType, prop2.PropertyType);
                    if (ct != ConversionType.Same && ct != ConversionType.Copy)
                    {
                        return ConversionType.Invalid;
                    }
                }

                return ConversionType.Copy;
            }

            else
            {
                return ConversionType.Invalid;
            }
        }

        public Type SourceType { get; private set; }
        public Type DestinationType { get; private set; }
        public ConversionType Type { get; private set; }
    }
}
