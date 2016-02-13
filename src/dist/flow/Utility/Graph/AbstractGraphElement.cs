using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace rDSN.Tron.Utility
{
    public abstract class AbstractGraphElement<instantiatedClassT>
    {
        public string Name { get; set; }
        public string Description { get; set; }

        public AbstractGraphElement()
        {

        }
    }
}
