#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Most of the code are inspired by https://github.com/apache/kudu/blob/856fa3404b00ee02bd3bc1d77d414ede2b2cd02e/build-support/clang_tidy_gerrit.py

import argparse
import collections
import json
import multiprocessing
from multiprocessing.pool import ThreadPool
import os
import re
import subprocess
import sys
import tempfile

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

BUILD_PATH = os.path.join(ROOT, "build", "latest")

def run_tidy(sha="HEAD", is_rev_range=False):
    diff_cmdline = ["git", "diff" if is_rev_range else "show", sha]

    # Figure out which paths changed in the given diff.
    changed_paths = subprocess.check_output(diff_cmdline + ["--name-only", "--pretty=format:"]).splitlines()
    changed_paths = [p for p in changed_paths if p]

    # Produce a separate diff for each file and run clang-tidy-diff on it
    # in parallel.
    #
    # Note: this will incorporate any configuration from .clang-tidy.
    def tidy_on_path(path):
        patch_file = tempfile.NamedTemporaryFile()
        cmd = diff_cmdline + [
            "--src-prefix=%s/" % ROOT,
            "--dst-prefix=%s/" % ROOT,
            "--",
            path]
        subprocess.check_call(cmd, stdout=patch_file, cwd=ROOT)
        # TODO(yingchun): some checks could be disabled before we fix them.
        #  "-checks=-llvm-include-order,-modernize-concat-nested-namespaces,-cppcoreguidelines-macro-usage,-cppcoreguidelines-special-member-functions,-hicpp-special-member-functions,-bugprone-easily-swappable-parameters,-google-readability-avoid-underscore-in-googletest-name,-cppcoreguidelines-avoid-c-arrays,-hicpp-avoid-c-arrays,-modernize-avoid-c-arrays,-llvm-header-guard,-cppcoreguidelines-pro-bounds-pointer-arithmetic",
        cmdline = ["clang-tidy-diff",
                   "-clang-tidy-binary",
                   "clang-tidy",
                   "-p0",
                   "-path", BUILD_PATH,
                   # Disable some checks that are not useful for us now.
                   # They are sorted by names, and should be consistent to .clang-tidy.
                   "-checks=-bugprone-easily-swappable-parameters,"
                           "-bugprone-lambda-function-name,"
                           "-bugprone-macro-parentheses,"
                           "-bugprone-sizeof-expression,"
                           "-cert-err58-cpp,"
                           "-concurrency-mt-unsafe,"
                           "-cppcoreguidelines-avoid-c-arrays,"
                           "-cppcoreguidelines-avoid-magic-numbers,"
                           "-cppcoreguidelines-avoid-non-const-global-variables,"
                           "-cppcoreguidelines-macro-usage,"
                           "-cppcoreguidelines-non-private-member-variables-in-classes,"
                           "-cppcoreguidelines-owning-memory,"
                           "-cppcoreguidelines-pro-bounds-array-to-pointer-decay,"
                           "-cppcoreguidelines-pro-bounds-pointer-arithmetic,"
                           "-cppcoreguidelines-pro-type-const-cast,"
                           "-cppcoreguidelines-pro-type-union-access,"
                           "-fuchsia-default-arguments-calls,"
                           "-fuchsia-multiple-inheritance,"
                           "-fuchsia-overloaded-operator,"
                           "-fuchsia-statically-constructed-objects,"
                           "-google-readability-avoid-underscore-in-googletest-name,"
                           "-hicpp-avoid-c-arrays,"
                           "-hicpp-named-parameter,"
                           "-hicpp-no-array-decay,"
                           "-llvm-include-order,"
                           "-misc-definitions-in-headers,"
                           "-misc-non-private-member-variables-in-classes,"
                           "-misc-unused-parameters,"
                           "-modernize-avoid-bind,"
                           "-modernize-avoid-c-arrays,"
                           "-modernize-replace-disallow-copy-and-assign-macro,"
                           "-modernize-use-trailing-return-type,"
                           "-performance-unnecessary-value-param,"
                           "-readability-function-cognitive-complexity,"
                           "-readability-identifier-length,"
                           "-readability-magic-numbers,"
                           "-readability-named-parameter,"
                           "-readability-suspicious-call-argument",
                   "-extra-arg=-language=c++",
                   "-extra-arg=-std=c++17",
                   "-extra-arg=-Ithirdparty/output/include"]
        return subprocess.check_output(
            cmdline,
            stdin=open(patch_file.name),
            cwd=ROOT).decode()
    pool = ThreadPool(multiprocessing.cpu_count())
    try:
        return "".join(pool.imap(tidy_on_path, changed_paths))
    except KeyboardInterrupt as ki:
        sys.exit(1)
    finally:
        pool.terminate()
        pool.join()


if __name__ == "__main__":
    # Basic setup and argument parsing.
    parser = argparse.ArgumentParser(description="Run clang-tidy on a patch")
    parser.add_argument("--rev-range", action="store_true",
                        default=False,
                        help="Whether the revision specifies the 'rev..' range")
    parser.add_argument('rev', help="The git revision (or range of revisions) to process")
    args = parser.parse_args()

    # Run clang-tidy and parse the output.
    clang_output = run_tidy(args.rev, args.rev_range)
    parsed = re.match(r'.+(warning|error): .+', clang_output, re.MULTILINE | re.DOTALL)
    print(clang_output, file=sys.stderr)
    if not parsed:
        print("No warnings", file=sys.stderr)
        sys.exit(0)
    sys.exit(1)

