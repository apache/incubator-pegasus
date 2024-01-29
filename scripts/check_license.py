#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
import pprint

PRJ_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
YML_PATH = os.path.join(PRJ_PATH, '.licenserc.yaml')

IGNORED_STARTS_WITH = ['.git/', '.idea/']
IGNORED_ENDS_WITH = ['.swp', '.npmigonre', 'go.sum', '.csv', '.json', '.pdf', '.jpg', '.png']
IGNORED_NAMES = {'.licenserc.yaml', 'LICENSE', 'tags'}

COPYRIGHT_MARKERS = [
    "Copyright (c) 2016, Adi Shavit",
    "Copyright (c) 2010-2016, Salvatore Sanfilippo",
    "Copyright (c) 2010-2014, Salvatore Sanfilippo",
    "Copyright (c) 2006-2015, Salvatore Sanfilippo",
    "Copyright (c) Facebook, Inc",
    "https://github.com/preshing/modern-cpp-threading",
    "https://github.com/preshing/cpp11-on-multicore",
    "Copyright (c) 2011 The Chromium Authors",
    "Copyright (c) 2012 The Chromium Authors",
    "Copyright (c) 2006-2009 The Chromium Authors",
    "Copyright 2017 The Abseil Authors",
    "Copyright (c) 2010-2011, Rob Jansen",
    "Copyright (c) 2017 Guillaume Papin",
    "Copyright (c) 2015 Microsoft Corporation",
]
IGNORED_COPYRIGHT_MARKERS = ["http://www.apache.org/licenses/LICENSE-2.0"]

NO_COPYRIGHT_MARKER_KEY = "NO_COPYRIGHT_MARKER"
IGNORED_COPYRIGHT_MARKER_KEY = "IGNORED_COPYRIGHT_MARKER"


def mark_file(path):
    with open(path) as f:
        try:
            for line in f:
                for marker in IGNORED_COPYRIGHT_MARKERS:
                    if marker in line:
                        return IGNORED_COPYRIGHT_MARKER_KEY

                for marker in COPYRIGHT_MARKERS:
                    if marker in line:
                        return marker
        except UnicodeDecodeError:
            # Ignore UnicodeDecodeError, since some files might be binary.
            pass

    # No marker was found, thus marked with no copyright.
    return NO_COPYRIGHT_MARKER_KEY


def is_path_ignored(path):
    for header in IGNORED_STARTS_WITH:
        if path.startswith(header):
            return True

    for trailer in IGNORED_ENDS_WITH:
        if path.endswith(trailer):
            return True

    return False


def is_name_ignored(name):
    return name in IGNORED_NAMES


def classify_files():
    """
    Scan all the files of the project, mark the ones that have copyright info.
    """
    marked_files = {}

    for abs_dir, sub_dirs, file_names in os.walk(PRJ_PATH):
        rel_dir = os.path.relpath(abs_dir, PRJ_PATH)
        if rel_dir == '.':
            # Drop the possible prefixed './' for the relative paths.
            rel_dir = ''

        for name in file_names:
            # Some kinds of files should be ignored.
            if is_name_ignored(name):
                continue

            rel_path = os.path.join(rel_dir, name)

            # Some kinds of dirs/files should be ignored.
            if is_path_ignored(rel_path):
                continue

            path = os.path.join(abs_dir, name)
            marker = mark_file(path)

            # Some kinds of copyright could be ignored, such as Apache LICENSE-2.0.
            if marker == IGNORED_COPYRIGHT_MARKER_KEY:
                continue

            if marker not in marked_files:
                marked_files[marker] = set()
            marked_files[marker].add(rel_path)

    return marked_files


def parse_yml():
    """
    Scan all the files in .licenserc.yaml, mark the ones that have copyright info.
    """
    marked_files = {}

    with open(YML_PATH) as f:
        # The files without copyright info are marked with the specific key.
        current_marker = NO_COPYRIGHT_MARKER_KEY
        for line in f:
            for marker in COPYRIGHT_MARKERS:
                if marker in line:
                    # Files in following lines would belong to this copyright.
                    current_marker = marker
                    break
            else:
                begin_idx = line.find("'")
                if begin_idx < 0:
                    # There's no file in this line, thus copyright would be reset.
                    current_marker = NO_COPYRIGHT_MARKER_KEY
                    continue

                begin_idx += 1
                end_idx = line.find("'", begin_idx)
                if end_idx < 0:
                    raise ValueError("Invalid file path line in {yml_path}".format(yml_path=YML_PATH))

                path = line[begin_idx:end_idx]

                # Some kinds of dirs/files should be ignored.
                if is_name_ignored(os.path.basename(path)):
                    continue
                if is_path_ignored(path):
                    continue

                if current_marker not in marked_files:
                    marked_files[current_marker] = set()
                marked_files[current_marker].add(path)

    return marked_files


def check_diff():
    """
    Check if .licenserc.yaml is consistent with all real files of the project.
    """
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
            # .licenserc.yaml is consistent with the project.
            print(
                "No diff found for marker '{yml_marker}' in {yml_path}".format(yml_marker=yml_marker,
                                                                               yml_path=YML_PATH))
            del marked_files[yml_marker]
            continue

        print("Diff found for marker '{yml_marker}' in {yml_path}:".format(yml_marker=yml_marker, yml_path=YML_PATH))
        if yml_plus:
            # Files in .licenserc.yaml, but not in the project.
            print("{plus}: {yml_plus}".format(plus='+' * len(yml_plus), yml_marker=yml_marker, yml_plus=yml_plus))
        if yml_minus:
            # Files in the project, but not in .licenserc.yaml.
            print("{minus}: {yml_minus}".format(minus='-' * len(yml_minus), yml_minus=yml_minus))

        del marked_files[yml_marker]

    if not marked_files:
        return

    print("markers in some files of the project not found in {yml_path}:".format(yml_path=YML_PATH))
    pprint.pprint(marked_files)


def main():
    check_diff()


if __name__ == '__main__':
    main()
