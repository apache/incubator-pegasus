#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import os

COPYRIGHT_MARKERS = [
    "Copyright (c) Facebook, Inc",
    "https://github.com/preshing/cpp11-on-multicore/blob/master/LICENSE",
    "Copyright (c) 2011 The Chromium Authors",
    "Copyright (c) 2012 The Chromium Authors",
    "Copyright (c) 2006-2009 The Chromium Authors",
    "Copyright 2017 The Abseil Authors",
    "Copyright (c) 2010-2011, Rob Jansen",
    "Copyright (c) 2017 Guillaume Papin",
    "Copyright (c) 2015 Microsoft Corporation",
]

NO_COPYRIGHT_MARKER = "NO_COPYRIGHT_MARKER"


def mark_file(path):
    with open(path) as f:
        # print(path)
        try:
            for line in f:
                for marker in COPYRIGHT_MARKERS:
                    if marker in line:
                        return marker
        except Exception:
            pass

    return NO_COPYRIGHT_MARKER


def classify_files():
    marked_files = {}

    prj_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for abs_dir, sub_dirs, file_names in os.walk(prj_path):
        rel_dir = os.path.relpath(abs_dir, prj_path)
        for name in file_names:
            path = os.path.join(abs_dir, name)
            marker = mark_file(path)
            files = marked_files.get(marker, set())
            files.add(os.path.join(rel_dir, name))
            marked_files[marker] = files
            # print(rel_dir, sub_dirs, file_names)

    print(marked_files)


def main():
    classify_files()


if __name__ == '__main__':
    main()
