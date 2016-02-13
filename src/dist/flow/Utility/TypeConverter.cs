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
