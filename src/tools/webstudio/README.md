
#rDSN.WebStudio

##Overview

**rDSN.WebStudio** (previously known as **rDSN.Monitor**) is a lightweight and powerful toolkit for rDSN application profiling and deployment.

![Main Screen](https://raw.githubusercontent.com/mcfatealan/rDSN.Screenshots/master/main.png)

##Features

* Node resource overview
* Profiling data visualization
* Online command line interface 
* Remote file editing 
* Service automatic deployment and management 
* Solution wizard for developers //TODO
* Cluster overview  //TODO

##To start

To start rDSN.WebStudio, you should install python 2.7.11+, due to some package dependency, we don't support python 3.

Previously rDSN.WebStudio needs to attach on rDSN process, but now we've already used RPC All you need is python.

##Simple Installation
1. install python 2.7.11+
2. run `python -m pip install -r requirement.txt`

###Open HTTP port for webstudio in rDSN app config
```bash
[apps.meta]
type = meta
dmodule = dsn.meta_server
arguments = 
ports = 34601
run = true
count = 1 
pools = THREAD_POOL_DEFAULT,THREAD_POOL_META_SERVER,THREAD_POOL_FD

```

add new port for webstudio `34602` to ports:
```bash
ports = 34601, 34602
```

add the following line:
```bash
network.server.34602.RPC_CHANNEL_TCP = NET_HDR_HTTP, dsn::tools::asio_network_provider, 65536
```

###Launch target program and http server
```bash
cd .\rDSN.WebStudio
python rDSN.WebStudio.py light 8088
```



