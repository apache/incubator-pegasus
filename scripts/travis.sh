# ensure source files are well formatted
./scripts/format_files.sh

modified=$(git status -s --ignore-submodules=dirty)
if [ "$modified" ]; then
    echo "$modified"
    echo "please format the above files before commit"
    exit 1
fi

./run.sh build -c --skip_thirdparty --disable_gperf && ./run.sh test

if [ $? ]; then
    echo "travis failed with exit code $?"
fi
