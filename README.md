
#rDSN.WebStudio

##Overview

**rDSN.WebStudio** (previously known as **rDSN.Monitor**) is the web panel for rDSN.

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

To start rDSN.WebStudio, you should install rDSN.Python first. Check [here](https://github.com/rDSN-Projects/rDSN.Python/blob/master/README.md) for more detail.

Please make sure you have all  required dlls (dsn.core.dll,dsn.dev.python_helper.dll) in your `DSN_ROOT`, and you already added the path to environment variable `PATH`.

Before the next step, noticing that comparing to rDSN.Python, there are some other python packages needed for hosting the http server. We recommend you to use [pip](https://pip.pypa.io/en/stable/installing/) to install them. Click the link to see the instructions to install pip.

When you finished installing pip, now run:
```bash
cd .\rDSN.WebStudio
pip install -r requirement.txt
```

##Startup mode selection
rDSN.WebStudio has two modes to run.

* Embedded Mode: rDSN.WebStudio runs as a daemon app together with the target programs. **(RECOMMENDED)** 
* Standalone Mode: rDSN.WebStudio works as the main program, creating a new thread to load target programs. 


##Mode I: Embedded Mode
Here we take `simple_kv` as an example. We want to run `simple_kv` with rDSN.WebStudio enabled.

###Modify config file to enable rDSN.WebStudio
We added the following lines in the config file of `simple_kv`:
```bash
[apps.webstudio]
name = webstudio
type = webstudio
arguments = 8080
pools = THREAD_POOL_DEFAULT
dmodule = dsn.dev.python_helper
dmodule_bridge_arguments = [PATH_TO_rDSN.WebStudio.py]
```

The 'arguments' is the port number http server will use. 

###Launch target program
Now you can directly run your target program!

Take `simple_kv` as an example.

```bash
dsn.replication.simple_kv.exe config.ini
```

The target program will automatically startup with rDSN.WebStudio on.

##Mode II: Standalone Mode
This mode is convenient when you're writing new functions for rDSN.WebStudio and want to test it. Here we take "simple_kv" as an example.


###Build dynamic link libraries of target program

Build dsn.replication.simple_kv.module in rDSN, then we get dsn.replication.simple_kv.module.dll, put it under `DSN_ROOT`.

###Modify the config file
In config file, this part is about WebStudio config:
```bash
[apps.webstudio]
name = webstudio
type = webstudio
arguments = 8080
pools = THREAD_POOL_DEFAULT
dmodule = dsn.dev.python_helper
```

For every other app (meta, replica), we should add:
```bash
dmodule = dsn.replication.simple_kv.module
```

###Launch target program and http server
Then run command
```bash
cd .\rDSN.WebStudio
python rDSN.WebStudio.py standalone
```
The Python script will start a thread to run simple_kv and a thread to host the http server.

Now you could visit [here](http://localhost:8080).

##Mode III: Light Mode
In this mode, rDSN.WebStudio will only launch as a python HTTP server, has nothing to do with rDSN process. But this mode also has the lightest dependency, all you need is python.

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



