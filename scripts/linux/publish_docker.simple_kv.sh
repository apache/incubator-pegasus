#!/bin/bash

set -e

b_dir=$1
t_dir=$2

applist="meta replica client client.perf.test"
echo $applist > $t_dir/applist

cp $b_dir/bin/dsn.replication.simple_kv/dsn.replication.simple_kv $t_dir/simple_kv
cp $b_dir/bin/dsn.replication.simple_kv/config.ini $t_dir/

echo "please customize your config.ini"
sleep 5
vim $t_dir/config.ini

sed -e "s/{{ placeholder\['deploy_name'\] }}/simple_kv/g" $scripts_dir/deploy/run.sh > $t_dir/run.sh

chmod a+x $t_dir/run.sh

cat << EOF > $t_dir/Dockerfile
FROM ${REPO}/rdsn
COPY simple_kv /home/rdsn/ 
COPY config.ini /home/rdsn/
COPY run.sh /home/rdsn/
RUN mkdir -p /home/rdsn/data

ENV LD_LIBRARY_PATH=/home/rdsn/lib

CMD ["./run.sh"]
EOF

docker build -t ${REPO}/simple_kv $t_dir
docker push ${REPO}/simple_kv

