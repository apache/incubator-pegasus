using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using rDSN.Tron.Utility;
using rDSN.Tron.Contract;

namespace rDSN.Tron.Runtime
{
    interface IContainer
    {
        ErrorCode Start(
            string command,
            string resourceDir,
            string workingDir
            );

        void Destroy();

        void RegisterExitNotification(Action<IContainer> notifier);
    }

    interface IFailureDetector
    {
        ErrorCode RegisterMaster(NodeAddress master, Action<NodeAddress, bool> notificationCallback);
        ErrorCode RegisterSlave(NodeAddress slave, Action<NodeAddress, bool> notificationCallback);
        ErrorCode RemoveMaster(NodeAddress master);
        ErrorCode RemoveSlave(NodeAddress slave);
    }

    interface ISerivceDaemon
    {
        //void SuggestServicePartition(ServicePartitionSuggestRequest request);
        void OnMasterNodeChange(NodeAddress server, bool isAlive);
    }


    delegate void ServiceSlotStateChangedEventHandler(NodeAddress addr, bool isAlive);
    interface IClusterScheduler
    {
        ErrorCode AllocateServiceSlot(out NodeAddress addr);
        ErrorCode FreeServiceSlot(NodeAddress addr);
        event ServiceSlotStateChangedEventHandler ServiceStateChanged;
    }

    class ServiceStatistics {};
    interface IServiceController
    {
        ErrorCode Create(ServiceInfo service, string packageName);
        ErrorCode Remove(string serviceName);

        // from ClusterScheduler
        void OnServiceDisconnected(string serviceName, NodeAddress addr);

        // from LoadBalancer
        void OnStatistics(ServiceStatistics stats);
    }
    
    interface ILoadBalancer
    {
        List<NodeAddress> QueryTargetNodes(UInt64 partitionKeyHash);
    }

    interface IReplicatedStateMachine
    { 
        
    }

    interface RpcMessage<T>
    {
        // T -> protocol -> msg => rpc => transport => rpc => msg => protocol => T
        // thread pool
        // rpc server
    }
}
