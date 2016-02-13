using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rDSN.Tron.Contract
{
    public class ServicePlan
    {
        public Service[] DependentServices { get; set; }

        public ServicePackage Package { get; set; }
    }
}
