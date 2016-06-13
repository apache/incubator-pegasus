
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

using System.Collections.Generic;

namespace rDSN.Tron
{    

public class ServiceMethod
{
	public string Name{ get; set; } 
	public string InputTypeFullName{ get; set; } 
	public string OutputTypeFullName{ get; set; } 
	public string ParameterName{ get; set; } 
}

public class ServiceApi
{
	public string Name{ get; set; } 
	public Dictionary<string, ServiceMethod> Methods{ get; set; } 
}

public enum ServiceSpecType
{
	Unknown,
	Composition,
	Common,
	Proto,
	Thrift,
    Bond30
}

public class ServiceSpec
{
    public ServiceSpecType SType{ get; set; } 
	public string MainSpecFile{ get; set; } 
	public List<string> ReferencedSpecFiles{ get; set; } 
	public string Directory{ get; set; } 
    public bool IsRdsnRpc{ get; set; } 

    public ServiceSpec()
    {
        Directory = "";
        ReferencedSpecFiles = new List<string>();
    }
}

public class ServicePackage
{
    public string Name{ get; set; } 
	public string Author{ get; set; } 
	public double Version{ get; set; } 
	public long  PublishTime{ get; set; } 
	public string Description{ get; set; } 
	public string IconFileName{ get; set; } 
	public byte[]   PackageZip{ get; set; } 
    public ServiceSpec Spec{ get; set; } 
	public string MainExecutableName{ get; set; } 
	public string Arguments{ get; set; }  // %port% is used for RPC port, which will be replaced by our scheduler at runtime
	public string MainSpec{ get; set; }  // for query
	public byte[]   CompositionAssemblyContent{ get; set; }  // for composition

    public ServicePackage()
    {
        Spec = new ServiceSpec();
    }
}

public class ServiceDsptr
{
	public string Name{ get; set; } 
	public string Author{ get; set; } 
	public double Version{ get; set; } 
	public string PublishTime{ get; set; } 
	public string Description{ get; set; } 
	public string IconFileName{ get; set; } 
	public string MainSpec{ get; set; } 
}

public class RpcError
{
	public  int Code{ get; set; } 
}

public class RpcResponse<T> : RpcError
{
	public T Value{ get; set; } 
}

public class Name
{
	public string Value{ get; set; } 
}

public class NameList
{
	public List<string> Names{ get; set; } 
}


public partial class NodeAddress
{
	public string Host{ get; set; } 
	public int Port { get; set; } 
}

public class ServiceInfo
{
	public string Name{ get; set; } 
	public int InternalServiceSequenceId { get; set; } 
	public int PartitionCount { get; set; } 	
	public string ServicePackageName{ get; set; } 
}

public class ServicePartitionInfo
{
    public int InternalServiceSequenceId { get; set; } 
	public int Index { get; set; } 
	public ulong GlobalPartitionId { get; set; }  // InternalServiceSequenceId ## Index
	public string      Name{ get; set; } 
	public int ConfigurationVersion { get; set; } 
	public int ServicePort { get; set; } 
	public NodeAddress ManagerAddress{ get; set; } 	
}

public class ServiceInfoEx : ServiceInfo
{
	public List<ServicePartitionInfo> Partitions{ get; set; } 
}

public class AppServerInfo
{
    public string                     Host{ get; set; } 
	public NodeAddress                Address{ get; set; } 
	// temp fix from Dictionary<UInt64, ...> to ensure converting to json can work in C#
	public Dictionary<string, ServicePartitionInfo> Services{ get; set; } 
	public bool                       IsAlive{ get; set; } 
}

public enum ServicePartitionAction
{
	Put,
	Remove
}
}