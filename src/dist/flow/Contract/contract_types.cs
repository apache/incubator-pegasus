
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


namespace rDSN.Tron
{    

public class ServiceMethod
{
	public string Name;
	public string InputTypeFullName;
	public string OutputTypeFullName;
	public string ParameterName;
}

public class ServiceAPI
{
	public string Name;
	public Dictionary<string, ServiceMethod> Methods;
}

public enum ServiceSpecType
{
	Unknown,
	Composition,
	Common,
	Proto_Buffer_1_0,
	Thrift_0_9
}

public class ServiceSpec
{
	public ServiceSpecType SType = ServiceSpecType.Unknown;
	public string MainSpecFile;
	public List<string> ReferencedSpecFiles;
	public string Directory;
    public bool IsRdsnRpc;
}

public class ServicePackage
{
    public string Name;
	public string Author;
	public double Version;
	public Int64  PublishTime;
	public string Description;
	public string IconFileName;
	public byte[]   PackageZip;
    public ServiceSpec Spec;
	public string MainExecutableName;
	public string Arguments; // %port% is used for RPC port, which will be replaced by our scheduler at runtime
	public string MainSpec; // for query
	public byte[]   CompositionAssemblyContent; // for composition
}

public class ServiceDsptr
{
	public string Name;
	public string Author;
	public double Version;
	public string PublishTime;
	public string Description;
	public string IconFileName;
	public string MainSpec;
}

public class RpcError
{
	public  Int32 Code;
}

public class RpcResponse<T> : RpcError
{
	public T Value;
}

public class Name
{
	public string Value;
}

public class NameList
{
	public List<string> Names;
}


public partial class NodeAddress
{
	public string Host;
	public Int32  Port;
}

public class ServiceInfo
{
	public string Name;
	public Int32  InternalServiceSequenceId;
	public Int32  PartitionCount;	
	public string ServicePackageName;
};

public class ServicePartitionInfo
{
    public Int32       InternalServiceSequenceId;
	public Int32       Index;
	public UInt64      GlobalPartitionId; // InternalServiceSequenceId ## Index
	public string      Name;
	public Int32       ConfigurationVersion;
	public Int32       ServicePort;
	public NodeAddress ManagerAddress;	
}

public class ServiceInfoEx : ServiceInfo
{
	public List<ServicePartitionInfo> Partitions;
}

public class AppServerInfo
{
    public string                     Host;
	public NodeAddress                Address;
	// temp fix from Dictionary<UInt64, ...> to ensure converting to json can work in C#
	public Dictionary<string, ServicePartitionInfo> Services;
	public bool                       IsAlive;
}

public enum ServicePartitionAction
{
	PUT,
	REMOVE
}
}