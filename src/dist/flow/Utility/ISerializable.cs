using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace rDSN.Tron.Utility
{
    public interface ISerializable
    {
        bool Write(BinaryWriter writer);
        bool Read(BinaryReader reader);
    }
}
