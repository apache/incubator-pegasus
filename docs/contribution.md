Contribute to Pegasus
===============

## code format

We use "clang-format"(version 3.9) to format our code. For ubuntu users, clang-format-3.9 could be installed via `apt-get`:
```
sudo apt-get install clang-format-3.9
```

After installed clang-format, you can format your code by the ".clang-format" config file in the root of the project.

## C++ development guidelines

Basically, we follow the google-code-style, except for: 

* We prefer to use reference rather than pointers for return value of functions.
* The compilation of headers is controlled by "#progma once"

Reason for these exceptions is that we develop Pegasus based on Microsoft's open-source project [rDSN](https://github.com/Microsoft/rDSN), and we just follow its rules. Currently we fork a new repo on this project, and modification on our repo is hard to merge though we've made lots contributes to it.

## Roadmap

You may want to refer to the [roadmap](roadmap.md).
