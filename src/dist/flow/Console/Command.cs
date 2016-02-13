using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace rDSN.Tron.ControlPanel
{
    public abstract class Command
    {
        public abstract bool Execute(List<string> args);
        public abstract string Help();
        public virtual string Usage() { return Help(); }
    }
}

