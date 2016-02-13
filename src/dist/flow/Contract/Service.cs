using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Reflection;

using rDSN.Tron.Utility;

namespace rDSN.Tron.Contract
{
    public enum ConsistencyLevel
    {
        Any,
        Eventual,
        Causal,
        Strong  // Primary-backup or Quorum
    }

    public enum PartitionType
    { 
        None,
        Fixed,
        Dynamic
    }

    public class ServiceProperty
    {
        /// <summary>
        /// whether the service needs to be deployed by our infrastructure, or it is an existing service that we can invoke directly
        /// </summary>
        public bool? IsDeployedAlready { get; set; }

        /// <summary>
        /// whether the service is a primtive service or a service composed in TRON
        /// </summary>
        public bool? IsPrimitive { get; set; }

        /// <summary>
        /// whether the service is partitioned and will be deployed on multiple machines
        /// </summary>
        public bool? IsPartitioned { get; set; }

        /// <summary>
        /// whether the service is stateful or stateless. A stateless service can lose its state safely.
        /// </summary>
        public bool? IsStateful { get; set; }

        /// <summary>
        /// whether the service is replicated (multiple copies)
        /// </summary>
        public bool? IsReplicated { get; set; }
    }

    /// <summary>
    /// service description
    /// </summary>
    public class Service
    {
        public Service(Type schema, string package, string url, string name = "")
        {
            Schema = schema;
            PackageName = package;
            URL = url;
            Name = ((name != null && name != "") ? name : url);

            Properties = new ServiceProperty();
            Spec = new ServiceSpec();
        }

        /// <summary>
        /// schema of the service, with which we know what methods and streams it provides.
        /// </summary>
        public Type Schema { get; private set; }

        /// <summary>
        /// package name for this service (the package is published and stored in a service store)
        /// with the package, the service can be deployed with a deployment service
        /// </summary>
        public string PackageName { get; private set; }
        
        /// <summary>
        /// universal remote link for the service, which is used for service address resolve
        /// example:  http://service-manager-url/service-name
        /// </summary>
        public string URL { get; private set; }

        /// <summary>
        /// service name for print and RPC resolution (e.g., service-name.method-name for invoking a RPC)
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// service properties
        /// </summary>
        public ServiceProperty Properties { get; set; }
        
        /// <summary>
        /// spec info
        /// </summary>
        public ServiceSpec Spec { get; private set; }

        public ServiceSpec ExtractSpec()
        {
            if (Spec.Directory == "")
            {
                Spec.Directory = PackageName + ".Spec";
                if (!Directory.Exists(Spec.Directory))
                {
                    Directory.CreateDirectory(Spec.Directory);
                }

                List<string> files = new List<string>();
                files.Add(Spec.MainSpecFile);
                foreach (var ds in Spec.ReferencedSpecFiles)
                    files.Add(ds);

                foreach (var f in files)
                {
                    if (!File.Exists(Path.Combine(Spec.Directory, f)))
                    {
                        var stream = Schema.Assembly.GetManifestResourceStream(f);
                        using (Stream file = File.Create(Path.Combine(Spec.Directory, f)))
                        {
                            int len;
                            byte[] buffer = new byte[8 * 1024];                            
                            while ((len = stream.Read(buffer, 0, buffer.Length)) > 0)
                            {
                                file.Write(buffer, 0, len);
                            }
                        }
                    }
                }
            }
            return Spec;
        }
    }

    public class PrimitiveService
    {
        public PrimitiveService(string name, string classFullName, string classShortName)
        {
            Name = name;
            ServiceClassFullName = classFullName;
            ServiceClassShortName = classShortName;

            ReadConsistency = ConsistencyLevel.Any;
            WriteConsistency = ConsistencyLevel.Any;

            PartitionKey = null;
            PartitionType = PartitionType.None;
            PartitionCount = 1;
        }

        protected PrimitiveService Replicate(
            int minDegree, 
            int maxDegree, 
            ConsistencyLevel readConsistency = ConsistencyLevel.Any, 
            ConsistencyLevel writeConsistency = ConsistencyLevel.Any
            )
        {
            ReplicateMinDegree = minDegree;
            ReplicateMaxDegree = maxDegree;
            ReadConsistency = readConsistency;
            WriteConsistency = writeConsistency;

            return this;
        }

        protected PrimitiveService Partition(Type key, PartitionType type, int partitionCount = 1)
        {
            PartitionKey = key;
            PartitionType = type;
            PartitionCount = partitionCount;
            
            return this;
        }

        protected PrimitiveService SetDataSource(string dataSource) // e.g., cosmos structured stream
        {
            DataSource = dataSource;
            return this;
        }

        protected PrimitiveService SetConfiguration(string uri)
        {
            Configuration = uri;
            return this;
        }

        public Type PartitionKey { get; private set; }
        public PartitionType PartitionType { get; private set; }
        public int PartitionCount { get; private set; }

        public int ReplicateMinDegree { get; private set; }
        public int ReplicateMaxDegree { get; private set; }
        public ConsistencyLevel ReadConsistency { get; private set; }
        public ConsistencyLevel WriteConsistency { get; private set; }

        public string DataSource { get; private set; }
        public string Configuration { get; private set; }
        public string Name { get; private set; }
        public string ServiceClassFullName { get; private set; }
        public string ServiceClassShortName { get; private set; }
    }

    public class PrimitiveService<TSelf> : PrimitiveService
        where TSelf : PrimitiveService<TSelf>
    {
        public PrimitiveService(string name, string classFullName)
            : base(name, classFullName, classFullName.Substring(classFullName.LastIndexOfAny(new char[]{':', '.'}) + 1))
        {
        }

        public new TSelf Replicate(
            int minDegree,
            int maxDegree,
            ConsistencyLevel readConsistency = ConsistencyLevel.Any,
            ConsistencyLevel writeConsistency = ConsistencyLevel.Any
            )
        {
            return base.Replicate(minDegree, maxDegree, readConsistency, writeConsistency) as TSelf;
        }

        public new TSelf Partition(Type key, PartitionType type = PartitionType.Dynamic, int partitionCount = 1)
        {
            return base.Partition(key, type, partitionCount) as TSelf;
        }

        public new TSelf DataSource(string dataSource) // e.g., cosmos structured stream
        {
            return base.SetDataSource(dataSource) as TSelf;
        }

        public new TSelf Configuration(string uri)
        {
            return base.SetConfiguration(uri) as TSelf;
        }
    }

    public class SLA
    {
        public enum Metric
        { 
            Latency99Percentile,
            Latency95Percentile,
            Latency90Percentile,
            Latency50Percentile,

            WorkflowConsistency,
        }

        public enum WorkflowConsistencyLevel
        { 
            Any,
            Atomic,
            ACID,
        }

        public SLA Add<TValue>(Metric prop, TValue value)
        {
            return Add(prop, value.ToString());
        }

        public SLA Add(Metric prop, string value)
        {
            m_properties.Add(prop, value);
            return this;
        }

        public string Get(Metric prop)
        {
            string v = "";
            m_properties.TryGetValue(prop, out v);
            return v;
        }

        private Dictionary<Metric, string> m_properties = new Dictionary<Metric, string>();
    }

    public interface IPartitionableService<TPartitionKey>
    { 
    }

    public interface IDynamicPartitionableService<TPartitionKey> : IPartitionableService<TPartitionKey>
    {
        UInt64 PartitionKeyHash(TPartitionKey key);
        void Merge();
        void Split();
    }

    public class LearnRequest
    {}

    public class LearnState
    {}

    public interface IReplicationableService
    { 
        int Checkpoint(bool force);
        void PrepareLearningRequest(LearnRequest request);
        void GetLearnState(LearnRequest request, LearnState state);
        int  ApplyLearnState(LearnState state);
    }
}
