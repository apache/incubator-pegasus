exit_if_fail() {
    if [ $1 != 0 ]; then
        echo $2
        exit 1
    fi
}

# $1: string
# print 1 for success, 0 for fail
function is_ipv4() 
{
    if [[ $1 =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "1"
    else
        echo "0"
    fi
}

# $1: an ip_addr
function is_site_local_addr()
{
    # 10.*.*.*
    if [[ $1 =~ ^10\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "1"
    # 172.16.*.*
    elif [[ $1 =~ ^172\.16\.[0-9]+\.[0-9]+$ ]]; then
        echo "1"
    # 192.168.*.*
    elif [[ $1 =~ ^192\.168\.[0-9]+\.[0-9]+$ ]]; then
        echo "1"
    else
        echo "0"
    fi
}

function get_local_ip()
{
    got_result="0"
    for ip in `hostname -I`; do
        if [ "1" == `is_site_local_addr $ip` ]; then
            echo $ip
            got_result="1"
            break
        fi
    done
    if [ "0" == $got_result ]; then
        echo "127.0.0.1"
    fi
}


LOCAL_IP=`get_local_ip`

echo ${LOCAL_IP}

sed -i "s/@LOCAL_IP@/${LOCAL_IP}/g"  config.ini


./pegasus_unit_test

exit_if_fail $? "run unit test failed"