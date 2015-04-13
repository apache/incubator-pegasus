### Requirements and Dependencies

#### OS

rDSN has been built and tested on Ubuntu 14.04 LTS, Mac OS X Yosemite, and Windows 8.1. Though not tested, we assume rDSN can run on most platforms across Linux, Mac and Windows.

#### Libraries

rDSN requires the boost library, which can be installed by following the instructions [here](http://www.boost.org/doc/libs/1_57_0/more/getting_started/). 

### Building rDSN

Following is an example of building rDSN on Ubuntu 14.04 TLS, and building on other platforms are similar.

```bash
~/projects/rdsn$ vim scripts/makefile.common // Windows: Makefile.nmake.common

// edit in makefile.common
ROOT = $(HOME)/projects/rdsn # root dir where the source is cloned into

~/projects/rdsn$ make -j // Windows: nmake -f Makefile.nmake
```

If there are errors indicating boost header files or library not found, you may need to append SYS_INC and SYS_LIB in scripts/makefile.common to ensure the paths for boost are present correctly.

```bash
SYS_INC = /usr/local/include $(BOOST_INC) # ensure boost header files are there
SYS_LIB = /usr/local/lib $(BOOST_LIB) # ensure boost libraries are there
```

All rDSN header files for further development are in ./inc directory, and all libraries are in ./bin directory.
* libdsn.core.a - this is where the Service API and Tool API are defined and connected.
* libdsn.dev.a - higher level programming abstractions atop of Service API to improve programming agility
* libdsn.tools.common.a - default local runtime providers, including network, aio, lock, etc., and a set of common tools like tracer/profiler/fault-injector.
* libdsn.tools.simulator.a - the simulation tool

If everything is as expected, congratulations, and you are ready to try our [tutorial](https://github.com/imzhenyu/rDSN/wiki/tutorial) to develop your first service with rDSN.


