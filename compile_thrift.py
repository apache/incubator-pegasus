#!/usr/bin/env python2

import os
import sys
import platform
import re

'''
the default thrift generator
'''

thrift_description = [
    {
        "name": "dsn.layer2",
        "path": "src",
        "include_fix": {
            "_types.h": {
                "add": ["<dsn/cpp/serialization_helper/dsn_types.h>"],
                "remove": ["\"dsn_types.h\""]
            },
            "_types.cpp": {
                "add": ["<dsn/cpp/serialization_helper/dsn.layer2_types.h>"],
                "remove": ["\"dsn.layer2_types.h\""]
            }
        },
        "file_move": {
            "_types.h": "include/dsn/cpp/serialization_helper",
            "_types.cpp": "src/core/core"
        }
    },
    {
        "name": "fd",
        "path": "src/dist/failure_detector",
        "file_move": {
            "_types.h": "include/dsn/dist/failure_detector"
        },
        "include_fix": {
            "_types.h": {
                "add": ["<dsn/service_api_cpp.h>"],
                "remove": ["\"dsn_types.h\""]
            },
            "_types.cpp": {
                "add": ["<dsn/dist/failure_detector/fd_types.h>"],
                "remove": ["\"fd_types.h\""]
            },
        }
    },
    {
        "name": "replication",
        "path": "src/dist/replication",
        "file_move": {
            "_types.h": "include/dsn/dist/replication",
            "_types.cpp": "src/dist/replication/common"
        },
        "include_fix": {
            "_types.h": {
                "add": ["<dsn/cpp/serialization_helper/dsn.layer2_types.h>"],
                "remove": ["\"dsn_types.h\"", "\"dsn.layer2_types.h\""]
            },
            "_types.cpp": {
                "add": ["<dsn/dist/replication/replication_types.h>"],
                "remove": ["\"replication_types.h\""]
            },
        },
    },
    {
        "name": "nfs",
        "path": "src/dist/nfs",
        "include_fix": {
            "_types.h": {
                "add": ["<dsn/service_api_cpp.h>"],
                "remove": ["\"dsn_types.h\""]
            }
        }
    },
    {
        "name": "simple_kv",
        "path": "src/dist/replication/storage_engine/simple_kv"
    },
    {
        "name": "cli",
        "path": "src/dist/cli",
        "file_move": {
            "_types.h": "include/dsn/dist/cli"
        },
        "include_fix": {
            "_types.h": {
                "remove": ["\"dsn_types.h\""]
            },
            "_types.cpp": {
                "add": ["<dsn/dist/cli/cli_types.h>"],
                "remove": ["\"cli_types.h\""]
            },
        }
    }
]

thrift_exe = os.getcwd() + "/bin/Linux/thrift"
root_dir = os.getcwd()
print "thrift_exe = " + thrift_exe
print "root_dir = " + root_dir

if not os.path.isfile(thrift_exe):
    os.system("wget --no-check-certificate "
              "https://github.com/xiaomi/pegasus-common/raw/master/pre-built/ubuntu14.04/thrift "
              "&& chmod u+x thrift "
              "&& mv thrift "+thrift_exe)

class CompileError(Exception):
    """ Raised when dealing with thrift idl have errors"""

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


def fix_include_file(filename, fix_commands):
    tmp_result = filename + ".swapfile"
    from_fd, to_fd = open(filename, "r"), open(tmp_result, "w")

    add_ok = not "add" in fix_commands

    for line in from_fd:
        include_statement = False
        if len(line.strip()) > 0:
            stripped_line = line.strip()
            if stripped_line[0] == "#" and "include" in stripped_line:
                include_statement = True

        if include_statement == True and add_ok == False:
            add_includes = "\n".join(["#include %s" % (s)
                                      for s in fix_commands["add"]])
            to_fd.write(add_includes + "\n")
            add_ok = True

        if include_statement == True and ("remove" in fix_commands):
            if len(filter(lambda x: x in line, fix_commands["remove"])) == 0:
                to_fd.write(line)
        else:
            to_fd.write(line)

    from_fd.close()
    to_fd.close()

    os.remove(filename)
    os.rename(tmp_result, filename)


def fix_include(thrift_name, include_fix_dict):
    # current dir is thrift file dir
    os.chdir("output")

    for pair in include_fix_dict.iteritems():
        filename = thrift_name + pair[0]
        fix_include_file(filename, pair[1])

    os.chdir("..")


def compile_thrift_file(thrift_info):
    thrift_name = thrift_info["name"]
    print "\n>>> compiling thrift file %s.thrift ..." % (thrift_name)

    if "path" not in thrift_info:
        raise CompileError("can't find thrift file")

    # ensure <name>.thrift exists
    os.chdir(root_dir + "/" + thrift_info["path"])
    if os.path.isfile(thrift_name+".thrift") == False:
        raise CompileError("can't find thrift file")

    # create tmp directory: <thrift_info["path"]>/output
    os.system("rm -rf output")
    os.system("mkdir output")
    print "mkdir {}/output".format(os.getcwd())

    # generate files
    cmd = "{} -gen cpp:moveable_types -out output {}.thrift".format(
        thrift_exe, thrift_name)
    os.system(cmd)
    print cmd

    # TODO(wutao1): code format files
    # os.system("clang-format-3.9 -i output/*")

    if "include_fix" in thrift_info:
        fix_include(thrift_name, thrift_info["include_fix"])

    if "hook" in thrift_info:
        os.chdir("output")
        for hook_func, args in thrift_info["hook"]:
            hook_func(args)
        os.chdir("..")

    if "file_move" in thrift_info:
        for pair in thrift_info["file_move"].iteritems():
            dest_path = root_dir + "/" + pair[1]
            for postfix in pair[0].split():
                src_path = "output/%s%s" % (thrift_name, postfix)
                cmd = "mv %s %s" % (src_path, dest_path)
                os.system(cmd)
                print cmd

    os.system("rm -rf output")
    print "rm -rf {}/output".format(os.getcwd())

    os.chdir(root_dir)


# special hooks for thrift, all these are executed in the output dir


def constructor_hook(args):
    generated_fname = args[0]
    class_name = args[1]
    add_code = args[2]

    target_fname = generated_fname + ".swapfile"
    src_fd, dst_fd = open(generated_fname, "r"), open(target_fname, "w")

    in_class = 0
    for line in src_fd:
        if in_class == 1:
            if "public:" in line:
                line = line + add_code + "\n"
            elif "bool operator <" in line:
                line = ""
            # this may not be right
            elif line.startswith("};"):
                in_class = 2
        elif in_class == 0 and line.startswith("class " + class_name + " {"):
            in_class = 1
        dst_fd.write(line)

    src_fd.close()
    dst_fd.close()

    os.remove(generated_fname)
    os.rename(target_fname, generated_fname)


def replace_hook(args):
    generated_fname = args[0]
    replace_map = args[1]

    target_fname = generated_fname + ".swapfile"
    src_fd, dst_fd = open(generated_fname, "r"), open(target_fname, "w")

    for line in src_fd:
        for key, value in replace_map.items():
            line = re.sub(key, value, line)
        dst_fd.write(line)

    src_fd.close()
    dst_fd.close()

    os.remove(generated_fname)
    os.rename(target_fname, generated_fname)


def add_hook(name, path, func, args):
    for i in thrift_description:
        if name == i["name"] and path == i["path"]:
            if "hook" not in i:
                i["hook"] = [(func, args)]
            else:
                i["hook"].append((func, args))


if __name__ == "__main__":
    ctor_kv_pair = "  kv_pair(const std::string& _key, const std::string& _val): key(_key), value(_val) {\n  }"
    ctor_configuration_proposal_action = "  configuration_proposal_action(::dsn::rpc_address t, ::dsn::rpc_address n, config_type::type tp): target(t), node(n), type(tp) {}"
    add_hook("simple_kv", "src/apps/skv", constructor_hook,
             ["simple_kv_types.h", "kv_pair", ctor_kv_pair])
    add_hook("replication", "src/dist/replication", constructor_hook,
             ["replication_types.h", "configuration_proposal_action", ctor_configuration_proposal_action])
    add_hook("dsn.layer2", "src", replace_hook, ["dsn.layer2_types.h", {
             r"dsn\.layer2_TYPES_H": 'dsn_layer2_TYPES_H'}])

    for i in thrift_description:
        compile_thrift_file(i)
