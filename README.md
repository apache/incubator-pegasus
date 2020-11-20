# admin-cli

The command-line tool for the administration of Pegasus.

## Installation

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
