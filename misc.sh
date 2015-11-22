export BUILD_ROOT=$PROJ_ROOT/builder
export BIN_ROOT=$BUILD_ROOT/bin

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
    if [ ! -f $PROJ_ROOT/.matchfile ]; then
        echo "you need to build the project first"
        return
    fi

    proj_dir=`pwd`
    result=`cat $PROJ_ROOT/.matchfile | grep -F "$proj_dir " | wc -l`
    if [ $result -ne 1 ]; then
        echo "not in a executable src/binary dir, try src or dst command"
    else
        proj_line=`cat $PROJ_ROOT/.matchfile | grep -F "$proj_dir "`
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
    if [ ! -f $PROJ_ROOT/.matchfile ]; then
        echo "you need to build the project first"
        return
    fi
    
    result=`cat $PROJ_ROOT/.matchfile | grep -F $1 | wc -l`
    if [ $result -ne 1 ]; then
        echo "can't identify the unique project"
        cat $PROJ_ROOT/.matchfile | grep -F $1
    else
        proj_line=`cat $PROJ_ROOT/.matchfile | grep -F $1`
        goto_source_dir $proj_line
    fi
}

function dst()
{
    if [ ! -f $PROJ_ROOT/.matchfile ]; then
        echo "you need to build the project first"
        return
    fi
    
    result=`cat $PROJ_ROOT/.matchfile | grep -F $1 | wc -l`
    if [ $result -ne 1 ]; then
        echo "can't identify the unique project"
        cat $PROJ_ROOT/.matchfile | grep -F $1
    else
        proj_line=`cat $PROJ_ROOT/.matchfile | grep -F $1`
        goto_dest_dir $proj_line
    fi
}

function gcp()
{
    if [ ! -f $PROJ_ROOT/.matchfile ]; then
        echo "you need to build the project first"
        return
    fi

    if [ $# -gt 0 ]; then
        proj_dir=`pwd`
        result=`cat $PROJ_ROOT/.matchfile | grep -F "$proj_dir " | wc -l`
        if [ $result -ne 1 ]; then
            echo "not in a executable src/binary dir, try src or dst command"
        else
            proj_line=`cat $PROJ_ROOT/.matchfile | grep -F "$proj_dir "`
            src_dir=`echo $proj_line | awk '{print $2}'`
            dest_dir=`echo $proj_line | awk '{print $3}'`
            if [ "$proj_dir" = "$src_dir" ]; then
                cp $* $dest_dir
            else
                cp $* $src_dir
            fi
        fi
    else
        echo "nothing to copy between src and dst"
    fi
}

function r()
{
    cd $PROJ_ROOT
}

function b()
{
    cd $BUILD_ROOT
}

function lst()
{
    if [ ! -f $PROJ_ROOT/.matchfile ]; then
        echo "you need to build the project first"
        return
    fi
    cat $PROJ_ROOT/.matchfile | awk '{print $1}'
}
