// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

// for the auxiliary of main function

#include <iostream>
#include <stdio.h>
#include <string>
#include <iomanip>
#include <readline/readline.h>
#include <readline/history.h>
#include <boost/lexical_cast.hpp>
#include <dsn/service_api_c.h>

extern std::string s_last_history;

inline void rl_gets(char *&line_read, bool nextCommand = true)
{
    if (line_read) {
        free(line_read);
        line_read = (char *)NULL;
    }

    if (nextCommand)
        line_read = readline("\n>>>");
    else
        line_read = readline(">>>");

    if (!line_read)
        dsn_exit(0);

    if (line_read && *line_read && s_last_history != line_read) {
        add_history(line_read);
        s_last_history = line_read;
    }
}

// scanf a word
inline bool scanfWord(std::string &str, char endFlag, char *&line_read, int &pos)
{
    char ch;
    bool commandEnd = true;

    if (endFlag == '\'') {
        while ((ch = line_read[pos++]) != endFlag) {
            if (ch == '\0') {
                str += '\n';
                rl_gets(line_read, false);
                pos = 0;
                continue;
            }

            if (ch == '\\') {
                ch = line_read[pos++];
                if (ch == 'x' || ch == 'X')
                    str += '\\';
            }
            str += ch;
        }
    } else if (endFlag == '\"') {
        while ((ch = line_read[pos++]) != endFlag) {
            if (ch == '\0') {
                str += '\n';
                rl_gets(line_read, false);
                pos = 0;
                continue;
            }

            if (ch == '\\') {
                ch = line_read[pos++];
                if (ch == 'x' || ch == 'X')
                    str += '\\';
            }
            str += ch;
        }
    } else {
        ch = line_read[pos++];
        while (!(ch == ' ' || ch == '\0')) {
            str += ch;
            ch = line_read[pos++];
        }
    }
    if (ch == '\0')
        return commandEnd;
    else
        return !commandEnd;
}

inline void scanfCommand(int &Argc, std::string Argv[], int paraNum)
{
    for (int i = 0; i < paraNum; ++i)
        Argv[i].clear();
    char *line_read = NULL;
    rl_gets(line_read);

    if (line_read == NULL) {
        std::cout << std::endl;
        Argc = -1;
        return;
    }

    char ch;
    int index;
    int pos;
    Argc = 0;
    for (pos = 0, index = 0; index < paraNum; ++index) {
        while ((ch = line_read[pos++]) == ' ')
            ;

        if (ch == '\'')
            scanfWord(Argv[index], '\'', line_read, pos);
        else if (ch == '\"')
            scanfWord(Argv[index], '\"', line_read, pos);
        else if (ch == '\0')
            return;
        else {
            Argv[index] = ch;
            if (scanfWord(Argv[index], ' ', line_read, pos)) {
                Argc++;
                return;
            }
        }

        Argc++;
    }

    for (index = 0; index < Argc; ++index)
        std::cout << Argv[index] << ' ';
    free(line_read);
    line_read = NULL;
}
