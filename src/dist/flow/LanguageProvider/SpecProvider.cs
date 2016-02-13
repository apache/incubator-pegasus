using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using rDSN.Tron.Utility;
using rDSN.Tron.Contract;

namespace rDSN.Tron.LanguageProvider
{
   
    public enum ClientLanguage
    {
        Client_CSharp,
        Client_CPlusPlus,
        Client_Python,
        Client_Javascript,
        Client_Java,
    }

    public enum ClientPlatform
    {
        Windows,
        Linux
    }

    /// <summary>
    /// information needed for generated service client code linked against the composed serivce program
    /// </summary>
    public class LinkageInfo
    {
        public LinkageInfo()
        {
            Sources = new List<string>();
            IncludeDirectories = new List<string>();
            LibraryPaths = new List<string>();
            StaticLibraries = new List<string>();
            DynamicLibraries = new List<string>();
        }
        /// <summary>
        /// source files, e.g., CDG_service_client.cpp
        /// </summary>
        public List<string> Sources;

        /// <summary>
        /// where are the header files (if necessary), e.g., d:/bond/Cpp/Include
        /// </summary>
        public List<string> IncludeDirectories;

        /// <summary>
        /// where are the libraries, e.g., d:/bond/Cpp/Library/x64
        /// </summary>
        public List<string> LibraryPaths;

        /// <summary>
        /// static linked library names, without path (path should be designated using LibraryPaths above)
        /// e.g., Bond.lib (bond.a)
        /// </summary>
        public List<string> StaticLibraries;

        /// <summary>
        /// dynamic linked library names, without path, e.g., bond.dll (bond.so)
        /// </summary>
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
        /// convert service spec file to our common spec
        /// </summary>
        /// <param name="spec"> given spec </param>
        /// <param name="dir"> dir to place result files </param>
        /// <returns> result spec as a set of .cs files </returns>
        string[] ToCommonSpec(ServiceSpec spec, string dir);
        
        /// <summary>
        /// generate service invocation client code, save them into %dir%
        /// namespace service.namespace
        /// public class service.name##_Client
        /// {
        /// public service.name##_Client(ip, port) {}
        /// public TResponse Method1(TRequest request) {}
        /// public ErrorCode Method1Async(TRequest request, callback, timeout) {}
        /// };
        /// </summary>
        /// <param name="spec"> given package spec info </param>
        /// <param name="dir"> save generated code in this dir </param>
        /// <param name="lang"> specified language type for generated code</param>
        /// <param name="lang"> platform where the client code will run on </param>
        /// <returns> error code </returns>
        ErrorCode GenerateServiceClient(
            ServiceSpec spec,
            string dir,
            ClientLanguage lang,
            ClientPlatform platform,
            out LinkageInfo linkInfo
            );

        /// <summary>
        /// generate service implementation sketch code, save them into %dir%
        /// </summary>
        /// <param name="spec"> given package spec info </param>
        /// <param name="dir"> save generated code in this dir </param>
        /// <param name="lang"> specified language type for generated code</param>
        /// <param name="lang"> platform where the service code will run on </param>
        /// <returns> error code </returns>
        ErrorCode GenerateServiceSketch(
            ServiceSpec spec,
            string dir,
            ClientLanguage lang,
            ClientPlatform platform,
            out LinkageInfo linkInfo
            );

        /// <summary>
        /// get the specific spec compiler path
        /// </summary>
        /// <returns>compiler path</returns>
        //string GetCompilerPath();
    }

    public class SpecProviderManager : Singleton<SpecProviderManager>
    {
        public SpecProviderManager()
        {
            Register(new BondSpecProvider());
            Register(new ThriftSpecProvider());
        }

        public void Register(ISpecProvider provider)
        {
            _providers.Add(provider.GetType(), provider);
        }

        public ISpecProvider GetProvider(ServiceSpecType type)
        {
            ISpecProvider spec = null;
            _providers.TryGetValue(type, out spec);
            return spec;
        }

        private Dictionary<ServiceSpecType, ISpecProvider> _providers = new Dictionary<ServiceSpecType, ISpecProvider>();
    }
    
}
