#!/bin/bash

/usr/sbin/sshd -D &

/pegasus/bin/pegasus_server /pegasus/bin/config.ini -app_list "$1"
