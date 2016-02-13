using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

using rDSN.Tron.Utility;
using rDSN.Tron.Contract;

namespace rDSN.Tron.LanguageProvider
{
   public class LanguageHelper
   {
       public static string GetSourceExtension(ClientLanguage lang)
       {
           Dictionary<ClientLanguage, string> map = new Dictionary<ClientLanguage, string>()
            {
                {ClientLanguage.Client_CPlusPlus, "cpp"},
                {ClientLanguage.Client_CSharp, "cs"},
                {ClientLanguage.Client_Java, "java"},
                {ClientLanguage.Client_Python, "py"},
                {ClientLanguage.Client_Javascript, "js"},
            };
           if (map.ContainsKey(lang))
           {
               return map[lang];
           }
           else
           {
               return "";
           }
       }

       // TODO: return the correct path of compiler in Linux platform
       public static string GetCompilerPath(ServiceSpecType t, ClientPlatform p = ClientPlatform.Windows)
       {
           var path = "";
           var prefix = "..\\..\\external\\";
           switch(t)
           {
               case ServiceSpecType.Bond_3_0:
                   path = prefix + "bond\\amd64\\bondc.exe";
                   break;
               case ServiceSpecType.Proto_Buffer_1_0:
                   path = prefix + "protobuf\\protoc.exe";
                   break;
               case ServiceSpecType.Thrift_0_9:
                   path = prefix + "thrift\\thrift.exe";
                   break;
               default:
                   break;
           }
           if (File.Exists(path))
           {
               return path;
           }
           else
           {
               Console.WriteLine("Cannot find thrift compiler at path: {0}!", path);
               return "";
           }
       }
   }
  
}
