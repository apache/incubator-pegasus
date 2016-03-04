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
