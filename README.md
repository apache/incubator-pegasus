# admin-cli

The command-line tool for the administration of Pegasus.

Thanks to the portability of Go, we have provided binaries of admin-cli for multiple platforms. This is a tremendous advantage
compared to the old "Pegasus-Shell" which is previously written in C++. If you are using the Pegasus-Shell,
we highly recommend you taking a try on the brand new, more user-friendly admin-cli.

## Quick Start

Choose and download a suitable [release](https://github.com/pegasus-kv/admin-cli/releases) for your platform.

## Manual Installation

```sh
make
```

The executable will reside in ./bin/admin-cli. Checkout the usage via `--help`.

```sh
./bin/admin-cli --help
```

## Developer Guide

This tool uses https://github.com/desertbit/grumble for interactive command line parsing.
Please read the details from the library to learn how to develop a new command.
