using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace rDSN.Tron
{    
    public partial class NodeAddress
    {
        public override int GetHashCode()
        {
            return Host.GetHashCode() ^ Port.GetHashCode();
            //return ToString(true, ':').GetHashCode();
        }

        public override bool Equals(object obj)
        {
            NodeAddress that = obj as NodeAddress;
            return Host == that.Host && Port == that.Port;
        }

    }
}
