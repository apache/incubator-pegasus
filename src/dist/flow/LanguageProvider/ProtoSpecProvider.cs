using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using System.IO;
using System.Diagnostics;
using System.Runtime.InteropServices;

using rDSN.Tron.Utility;
using rDSN.Tron.Contract;

namespace rDSN.Tron.LanguageProvider
{

    class ProtoSpecProvider : ISpecProvider
    {
        public new ServiceSpecType GetType()
        {
            return ServiceSpecType.Proto_Buffer_1_0;

        }

        public string[] ToCommonSpec(ServiceSpec spec, string dir)
        {
            throw new NotImplementedException();

        }

        //public ServiceSpec FromCommonSpec(string spec, ServiceSpecType targetType)
        //{
        //    var generator = IdlGenerator.GetInstance(targetType);
        //    Assembly asm = Assembly.LoadFrom(spec);
        //    generator.Generate(asm);
        //    ServiceSpec serviceSpec = new ServiceSpec();
        //    // FIXME
        //    serviceSpec.SType = targetType;
        //    serviceSpec.Directory = ".";
        //    serviceSpec.MainSpecFile = "";
        //    serviceSpec.ReferencedSpecFiles = null;
        //    return serviceSpec;
        //}


        public ErrorCode GenerateServiceClient(
            ServiceSpec spec,
            string dir,
            ClientLanguage lang,
            ClientPlatform platform,
            out LinkageInfo linkInfo
            )
        {
            linkInfo = null;
            return ErrorCode.Success;
        }


        public ErrorCode GenerateServiceSketch(
            ServiceSpec spec,
            string dir,
            ClientLanguage lang,
            ClientPlatform platform,
            out LinkageInfo linkInfo
            )
        {
            linkInfo = null;
            return ErrorCode.Success;
        }
    }

}
