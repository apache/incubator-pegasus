# Pegasus C++ Client Doxygen Guide

This document explains how to install Doxygen and generate the Pegasus C++ client API
documentation on **macOS** and **Linux**.

## Directory Layout

- `docs/doxygen/Doxyfile`: Preconfigured Doxygen configuration
- `docs/doxygen/output/html`: Generated HTML output

> If you update the C++ client headers or comments, re-run the generation command to
> refresh the documentation.

## 1. Install Doxygen

### macOS (Homebrew)

```bash
brew install doxygen
```

### Linux (Debian / Ubuntu)

```bash
sudo apt-get update
sudo apt-get install -y doxygen
```

Verify the installation:

```bash
doxygen -v
```

## 2. Generate Documentation

Run this from the repository root:

```bash
doxygen docs/doxygen/Doxyfile
```

The generated HTML entry point is:

```
docs/doxygen/output/html/index.html
```