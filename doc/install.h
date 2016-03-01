/*!
@defgroup install Installation
 
How to install rDSN? 
 
rDSN 1.0 has been built on the following platforms, and here are the quick summaries.

###### Ubuntu 14.04 LTS x86_64 with gcc 4.8.2
```
~$ sudo apt-get install build-essential
~$ sudo apt-get install git
~$ sudo apt-get install cmake
~$ sudo apt-get install php5-cli
~$ sudo apt-get install libboost-all-dev
~$ sudo apt-get install libaio-dev
~/projects$ git clone https://github.com/Microsoft/rdsn.git
~/projects/rdsn$ ./run.sh build
~/projects/rdsn$ ./run.sh test
```

###### Windows 8.1/Server 2012 R2/10 with Visual Studio 2015.
install [Visual Studio 2015](https://www.visualstudio.com/en-us/products/visual-studio-community-vs.aspx)

install [GitHub Windows](https://windows.github.com/)

install [php](http://windows.php.net/download#php-5.6)

```
c:\projects> git clone https://github.com/Microsoft/rDSN.git
c:\Projects\rDSN> run.cmd build Debug .\builddbg
c:\Projects\rDSN> run.cmd test
```

***

If everything is as expected, congratulations, and you are ready to try our [tutorial](https://github.com/Microsoft/rDSN/wiki/Tutorial:-Build-A-Single-Node-Counter-Service) to develop your first service with rDSN.

*/
