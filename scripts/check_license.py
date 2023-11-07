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

PRJ_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
YML_PATH = os.path.join(PRJ_PATH, '.licenserc.yaml')


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

    for abs_dir, sub_dirs, file_names in os.walk(PRJ_PATH):
        rel_dir = os.path.relpath(abs_dir, PRJ_PATH)
        for name in file_names:
            path = os.path.join(abs_dir, name)
            marker = mark_file(path)
            if marker not in marked_files:
                marked_files[marker] = set()
            marked_files[marker].add(os.path.join(rel_dir, name))
            # print(rel_dir, sub_dirs, file_names)

    return marked_files


def parse_yml():
    marked_files = {}

    with open(YML_PATH) as f:
        current_marker = None
        for line in f:
            for marker in COPYRIGHT_MARKERS:
                if marker in line:
                    current_marker = marker
                    break
            else:
                if not current_marker:
                    continue

                begin_idx = line.find("'")
                if begin_idx < 0:
                    current_marker = None
                    continue

                begin_idx += 1
                end_idx = line.find("'", begin_idx)
                if end_idx < 0:
                    raise ValueError("Invalid file path line in {yml_path}".format(yml_path=YML_PATH))

                path = line[begin_idx:end_idx]
                if current_marker not in marked_files:
                    marked_files[current_marker] = set()
                marked_files[current_marker].add(path)

    return marked_files


def check_diff():
    yml_marked_files = parse_yml()
    marked_files = classify_files()
    for yml_marker, yml_files in yml_marked_files.items():
        if yml_marker not in marked_files:
            print(
                "marker {yml_marker} in {yml_path} not found in any file of the project".format(yml_marker=yml_marker,
                                                                                                yml_path=YML_PATH))
            continue

        files = marked_files[yml_marker]
        yml_plus = yml_files - files
        yml_minus = files - yml_files
        if not yml_plus and not yml_minus:
            print(
                "No diff found for marker '{yml_marker}' in {yml_path}".format(yml_marker=yml_marker,
                                                                               yml_path=YML_PATH))
            continue

        print("Diff found for marker '{yml_marker}' in {yml_path}:".format(yml_marker=yml_marker, yml_path=YML_PATH))
        if yml_plus:
            print("{plus}: {yml_plus}".format(plus='+' * len(yml_plus), yml_marker=yml_marker, yml_plus=yml_plus))
        if yml_minus:
            print("{minus}: {yml_minus}".format(minus='-' * len(yml_minus), yml_minus=yml_minus))

        del marked_files[yml_marker]

    if not marked_files:
        return

    print("markers in some files of the project not found in {yml_path}: {marked_files}".format(
        yml_path=YML_PATH,
        marked_files=marked_files))


def main():
    check_diff()


if __name__ == '__main__':
    main()
