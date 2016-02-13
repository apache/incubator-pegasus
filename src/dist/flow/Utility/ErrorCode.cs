using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace rDSN.Tron.Utility
{
    public enum ErrorCode
    {
        Success,
        NotImplemented,
        ResourceLoadingFailure,
        Streaming,
        TopologyLoopError,
        ExceptionError,
        EndStreamAck,
        ChildDisconnected,
        ChildConnected,
        ParentDisconnected,
        ParentConnected,
        TimeOut,
        InvalidParameter,
        LoadFromAssemblyFailed,

        AppServicePackageAlreadyExist,
        AppServicePackageNotFound,
        AppServiceAlreadyExist,
        AppServiceNotFound,
        AppServicePartitionStillAlive,
        AppServerNotFound,
        VersionOutdated,
        ProcessStartFailed,

        SpecCompilerNotFound,
        PlatformNotSupported,

        HttpServiceAlreadyStarted,
        HttpServiceNotStarted,
    }
}
