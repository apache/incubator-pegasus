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

using Microsoft.Tron.Utility;
using Microsoft.Tron.Contract;

namespace Microsoft.Tron.Compiler
{
    public enum ClientLanguage
    { 
        Client_CSharp,
        Client_CPlusPlus,
        Client_Python,
        Client_Javascript
    }

    public enum ClientPlatform
    { 
        Windows,
        Linux
    }
    
    public class ServiceSpec
    {
        public ServiceSpecType SType;
        public string Directory;
        public string MainSpecFile;
        public List<string> ReferencedSpecFiles;
    }

    public class LinkageInfo
    {
        public List<string> IncludeDirectories;
        public List<string> LibraryPaths;
        public List<string> StaticLibraries;
        public List<string> DynamicLibraries;
    }

    /// <summary>
    /// each spec lang (e.g., bond/protobuf/thrift) needs to implement ISpecProvider and register it
    /// to SpecProviderManager.
    /// </summary>
    public interface ISpecProvider
    {
        ServiceSpecType GetType();

        /// <summary>
        /// convert service spec file to our common spec (compiled into an assembly)
        /// </summary>
        /// <param name="spec"> given spec </param>
        /// <returns> result spec </returns>
        string ToCommonSpec(ServiceSpec spec);

        /// <summary>
        /// convert our common spec back to target service spec
        /// note we don't support convert common spec to other spec type except the original type
        /// e.g., you cannot convert protobuf to common, and then to thrift (only protobuf is allowed)
        /// </summary>
        /// <param name="spec"> common spec assembly </param>
        /// <param name="targetType"> original spec </param>
        /// <returns></returns>
        ServiceSpec FromCommonSpec(string spec, ServiceSpecType targetType);

        /// <summary>
        /// generate service invocation client code, save them into %fileName%
        /// namespace service.namespace
        /// public class service.name##_Client
        /// {
        /// public service.name##_Client(ip, port) {}
        /// public TResponse Method1(TRequest request) {}
        /// public ErrorCode Method1Async(TRequest request, callback, timeout) {}
        /// };
        /// </summary>
        /// <param name="service"> service type </param>
        /// <param name="fileName"> save client code into this file </param>
        /// <param name="lang"> specified language type for generated code</param>
        /// <param name="lang"> platform where the client code will run on </param>
        /// <returns> error code </returns>
        ErrorCode GenerateServiceClient(
            Type service, 
            string fileName, 
            ClientLanguage lang, 
            ClientPlatform platform,
            out LinkageInfo linkInfo
            );
    }

    public class SpecProviderManager : Singleton<SpecProviderManager>
    {
        public void Register(ISpecProvider provider)
        {
            _providers.Add(provider.GetType(), provider);
        }

        private Dictionary<ServiceSpecType, ISpecProvider> _providers = new Dictionary<ServiceSpecType, ISpecProvider>();
    }
}
