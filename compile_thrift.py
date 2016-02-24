#!/usr/bin/env python2

import os
import sys

'''
some extra work need to be done to make all this compiled
1. remove the enums define in the deploy_svc_types.h,
   coz they have been defined in the cluster_scheduler.h
2. add constructor for global_partition_id in replication_types.h:
   global_partition_id(int app_id, int pidx),
   and remove the "operator <" statement
'''

thrift_description = [
    {
        "name": "deploy_svc", 
        "path": "src/dist/deployment_service",
        "include_fix": {
            "_types.h": {
                "add": ["<dsn/service_api_cpp.h>", "<dsn/dist/cluster_scheduler.h>"],
                "remove": ["\"dsn_types.h\""]
            },
        }
    },
    {
        "name": "fd", 
        "path": "src/dist/failure_detector", 
        "file_move": {
            ".types.h _types.h": "include/dsn/dist/failure_detector"
        },
        "include_fix": {
            "_types.h": {
                "add": ["<dsn/service_api_cpp.h>"],
                "remove": ["\"dsn_types.h\""]
            },
            "_types.cpp": {
                "add": ["<dsn/dist/failure_detector/fd_types.h>"],
                "remove": ["\"fd_types.h\""]
            }
        }
    }, 
    {
        "name": "replication", 
        "path": "src/dist/replication", 
        "file_move": {
            ".types.h _types.h": "include/dsn/dist/replication",
            "_types.cpp": "src/dist/replication/client_lib"
        },
        "include_fix": {
            "_types.h": {
                "add": ["<dsn/service_api_cpp.h>"],
                "remove": ["\"dsn_types.h\""]
            },
            "_types.cpp": {
                "add": ["<dsn/dist/replication/replication_types.h>"],
                "remove": ["\"replication_types.h\""]
            }
        }
    }, 
    {
        "name": "simple_kv", 
        "path": "src/dist/replication/test/simple_kv"
    }, 
    {
        "name": "echo", 
        "path": "src/apps/echo"
    }, 
    {
        "name": "nfs", 
        "path": "src/apps/nfs",
        "include_fix": {
            "_types.h": {
                "add": ["<dsn/service_api_cpp.h>"],
                "remove": ["\"dsn_types.h\""]
            }
        }
    }, 
    {
        "name": "simple_kv", 
        "path": "src/apps/skv"
    }, 
    {
        "name": "cli", 
        "path": "src/apps/cli",
        "file_move": {
            ".types.h _types.h": "include/dsn/tool"
        },
        "include_fix": {
            "_types.h": {
                "add": ["<dsn/service_api_cpp.h>"],
                "remove": ["\"dsn_types.h\""]
            },
            "_types.cpp": {
                "add": ["<dsn/tool/cli_types.h>"],
                "remove": ["\"cli_types.h\""]
            }
        }
    }
]

env_tools = {
    "dsn_gentool": "",
    "thrift_exe": "",
    "root_dir": ""
}

class CompileError(Exception): 
    """ Raised when dealing with thrift idl have errors"""
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

def init_env(): 
    env_tools["dsn_gentool"] = os.getcwd() + "/bin/dsn.cg.sh"
    env_tools["thrift_exe"] = os.getcwd() + "/bin/Linux/thrift-0.9.3"
    env_tools["root_dir"] = os.getcwd()

def find_struct_define(line, enum_class_list):
    if len(line)<7 or line[0:7] != "struct ":
        return ""
    struct_name = line.strip().split()[1]
    if struct_name in enum_class_list:
        return struct_name
    return ""

def modify_struct_define(header_file, enum_class):
    tmp_result = header_file + ".swapfile"
    res_fd = open(tmp_result, "w")
    src_fd = open(header_file, "r")

    in_enum_define = 0

    for line in src_fd:
        struct_name = find_struct_define(line, enum_class)
        if struct_name != "":
            res_fd.write("enum %s {\n"%(struct_name))
            in_enum_define = 1
        elif "enum type" in line and in_enum_define == 1:
            in_enum_define = 2
        elif line.strip() == "};" and in_enum_define == 2:
            in_enum_define = 1
        elif "VALUES_TO_NAMES" in line or "include \"dsn_types.h\"" in line:
            pass
        else:
            res_fd.write(line)
    
    src_fd.close()
    res_fd.close()

    os.system("rm %s"%(header_file))
    os.system("mv %s %s"%(tmp_result, header_file))

def remove_struct_impl(impl_file, enum_class):
    os.system("sed -i \'/VALUES_TO_NAMES/\'d %s"%(impl_file))

def replace_struct_usage(cpp_file, enum_class):
    sed_exp = "sed -i " + " ".join(["-e \'s/%s::type/%s/\'"%(i,i) for i in enum_class]) + " " + cpp_file
    os.system(sed_exp)

def handle_enums(thrift_name):
    enum_class = []
    for line in open(thrift_name + ".thrift"):
        if "enum " == line[0:5]:
            enum_class.append(line.strip().split()[1])

    if len(enum_class) != 0:
        header_file = thrift_name + "_types.h"
        impl_file = thrift_name + "_types.cpp"

        os.chdir("output")

        modify_struct_define(header_file, enum_class)
        remove_struct_impl(impl_file, enum_class)
        
        replace_struct_usage(header_file, enum_class)
        replace_struct_usage(impl_file, enum_class)

        os.chdir("..")

def fix_include_file(filename, fix_commands):
    tmp_result = filename + ".swapfile"
    from_fd = open(filename, "r")
    to_fd = open(tmp_result, "w")

    add_ok = not "add" in fix_commands

    for line in from_fd:
        include_statement = False
        if len(line.strip())>0:
            stripped_line = line.strip()
            if stripped_line[0]=="#" and "include" in stripped_line:
                include_statement = True

        if include_statement==True and add_ok==False:
            add_includes = "\n".join(["#include %s"%(s) for s in fix_commands["add"]])
            to_fd.write(add_includes + "\n")
            add_ok = True

        if include_statement==True and ("remove" in fix_commands):
            if len(filter(lambda x: x in line, fix_commands["remove"]))==0:
                to_fd.write(line)
        else:
            to_fd.write(line)

    from_fd.close()
    to_fd.close()

    os.rename(tmp_result, filename)

def fix_include(thrift_name, include_fix_dict):
    # current dir is thrift file dir
    os.chdir("output")

    for pair in include_fix_dict.iteritems():
        filename = thrift_name + pair[0]
        fix_include_file(filename, pair[1])

    os.chdir("..")

def toggle_serialization_in_cpp(thrift_name):
    # current dir is thrift file dir
    os.chdir("output")
    cpp_file = thrift_name + "_types.cpp"
    new_file = cpp_file + ".swapfile"

    os.system("pwd")
    os.system("echo \"#ifdef DSN_NOT_USE_DEFAULT_SERIALIZATION\" > %s"%(new_file) )
    os.system("cat %s >> %s"%(cpp_file, new_file))
    os.system("echo \"#endif\" >> %s"%(new_file) )

    os.rename(new_file, cpp_file)
    os.chdir("..")

def compile_thrift_file(thrift_info, only_php_flag):
    thrift_name = thrift_info["name"]
    print ">>>compiling thrift file %s.thrift ..."%(thrift_name)

    if "path" not in thrift_info:
        raise CompileError("can't find thrift file")

    os.chdir( env_tools["root_dir"] + "/" + thrift_info["path"])
    if os.path.isfile(thrift_name+".thrift") == False:
        raise CompileError("can't find thrift file")

    ## generate the files
    os.system("rm -rf output")
    os.system("mkdir output")
    #### first generate .types.h
    os.system("%s %s.thrift cpp build replication"%(env_tools["dsn_gentool"], thrift_name))
    os.system("cp build/%s.types.h output"%(thrift_name))

    if not only_php_flag:
        #### then generate _types.h _types.cpp
        thrift_gen = "%s -r --gen cpp -out build %s.thrift"%(env_tools["thrift_exe"], thrift_name)
        print "exec " + thrift_gen
        os.system(thrift_gen)
        os.system("cp build/%s_types.h output"%(thrift_name))
        os.system("cp build/%s_types.cpp output"%(thrift_name))
        os.system("rm -rf build")

        if "include_fix" in thrift_info:
            fix_include(thrift_name, thrift_info["include_fix"])

        handle_enums(thrift_name)
        toggle_serialization_in_cpp(thrift_name)

    if "file_move" in thrift_info:
        for pair in thrift_info["file_move"].iteritems():
            dest_path = env_tools["root_dir"] + "/" + pair[1]
            for postfix in pair[0].split():
                src_path = "output/%s%s"%(thrift_name, postfix)
                cmd = "mv %s %s"%(src_path, dest_path)
                os.system(cmd)

    if len(os.listdir("output"))>0:
        os.system("mv output/* ./")
    os.system("rm -rf output")

    os.chdir( env_tools["root_dir"] )

def find_desc_from_name(name):
    for i in thrift_description:
        if i["name"] == name:
            return i
    return None

if __name__ == "__main__":
    init_env()

    only_php = sys.argv[1]=="only_php"

    if len(sys.argv)>2:
        for i in sys.argv[2:]:
            desc = find_desc_from_name(i)
            if not desc is None:
                compile_thrift_file(desc, only_php)
    else:
        for i in thrift_description:
            compile_thrift_file(i, only_php)
