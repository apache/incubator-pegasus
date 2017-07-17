/*!
@defgroup install Installation

rDSN has been built on the following platforms, and here are the quick summaries.

### Ubuntu 14.04 LTS x86_64 with gcc 4.8.2

```
~$ sudo apt-get install build-essential
~$ sudo apt-get install git
~$ sudo apt-get install cmake
~$ sudo apt-get install php5-cli
~$ sudo apt-get install libboost-all-dev
~$ sudo apt-get install libaio-dev
~/projects$ git clone https://github.com/Microsoft/rDSN.git
~/projects/rdsn$ ./run.sh build
~/projects/rdsn$ ./run.sh test
~/projects/rdsn$ ./run.sh install
```

### Windows 8.1/Server 2012 R2/10 with Visual Studio 2015.
install [Visual Studio
2015](https://www.visualstudio.com/en-us/products/visual-studio-community-vs.aspx)

install [GitHub Windows](https://windows.github.com/)

```
c:\projects> git clone https://github.com/Microsoft/rDSN.git
c:\Projects\rDSN> run.cmd build Debug .\builddbg
c:\Projects\rDSN> run.cmd test
c:\Projects\rDSN> run.cmd build Debug .\builddbg install
```

***

If everything is as expected, congratulations, and you are ready to try our [tutorials](\ref
tutorials).

*/
