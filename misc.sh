function find_matchfile()
{
    dir=`pwd`
    while [ "$dir" != "/" ]; do
        if [ -f "$dir/.matchfile" ]; then
            echo "$dir/.matchfile"
            return
        fi
        dir=`dirname $dir`
    done
}

function goto_dest_dir()
{
    cd $3
}

function goto_source_dir()
{
    cd $2
}

function g()
{
    matchfile=`find_matchfile`
    if [ -z $matchfile ]; then
        echo "You need to be in rDSN project and build the project first"
        return
    fi

    proj_dir=`pwd`
    result=`cat $matchfile | grep -F "$proj_dir " | wc -l`
    if [ $result -ne 1 ]; then
        echo "You are not in any module src/binary dir, try 'lst' command to list modules"
    else
        proj_line=`cat $matchfile | grep -F "$proj_dir "`
        src_dir=`echo $proj_line | awk '{print $2}'`
        if [ "$proj_dir" = "$src_dir" ]; then
            goto_dest_dir $proj_line
        else
            goto_source_dir $proj_line
        fi
    fi
}

function src()
{
    matchfile=`find_matchfile`
    if [ -z $matchfile ]; then
        echo "You need to be in rDSN project and build the project first"
        return
    fi
    
    result=`cat $matchfile | grep -F "$1 " | wc -l`
    if [ $result -eq 1 ]; then
        proj_line=`cat $matchfile | grep -F "$1 "`
        goto_source_dir $proj_line
        return
    fi

    result=`cat $matchfile | grep -F $1 | wc -l`
    if [ $result -ne 1 ]; then
        echo "Can not identify unique module name, candidates are:"
        cat $matchfile | grep -F $1
    else
        proj_line=`cat $matchfile | grep -F $1`
        goto_source_dir $proj_line
    fi
}

function dst()
{
    matchfile=`find_matchfile`
    if [ -z $matchfile ]; then
        echo "You need to be in rDSN project and build the project first"
        return
    fi
    
    result=`cat $matchfile | grep -F "$1 " | wc -l`
    if [ $result -eq 1 ]; then
        proj_line=`cat $matchfile | grep -F "$1 "`
        goto_dest_dir $proj_line
        return
    fi

    result=`cat $matchfile | grep -F $1 | wc -l`
    if [ $result -ne 1 ]; then
        echo "Can not identify unique module name, candidates are:"
        cat $matchfile | grep -F $1
    else
        proj_line=`cat $matchfile | grep -F $1`
        goto_dest_dir $proj_line
    fi
}

function gcp()
{
    matchfile=`find_matchfile`
    if [ -z $matchfile ]; then
        echo "You need to be in rDSN project and build the project first"
        return
    fi

    if [ $# -gt 0 ]; then
        proj_dir=`pwd`
        result=`cat $matchfile | grep -F "$proj_dir " | wc -l`
        if [ $result -ne 1 ]; then
            echo "You are not in any module src/binary dir, try 'lst' command to list modules"
        else
            proj_line=`cat $matchfile | grep -F "$proj_dir "`
            src_dir=`echo $proj_line | awk '{print $2}'`
            dest_dir=`echo $proj_line | awk '{print $3}'`
            if [ "$proj_dir" = "$src_dir" ]; then
                cp -v $* $dest_dir
            else
                cp -v $* $src_dir
            fi
        fi
    else
        echo "No file specified to copy"
    fi
}

function r()
{
    matchfile=`find_matchfile`
    if [ -z $matchfile ]; then
        echo "You need to be in rDSN project and build the project first"
        return
    fi

    cd `dirname $matchfile`
}

function lst()
{
    matchfile=`find_matchfile`
    if [ -z $matchfile ]; then
        echo "You need to be in rDSN project and build the project first"
        return
    fi

    cat $matchfile | awk '{print $1}'
    echo
    echo "Try 'src <module>' or 'dst <module>' command to go to src/binary dirtory"
}
