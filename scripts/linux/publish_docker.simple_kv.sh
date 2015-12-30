#!/bin/bash

set -e

b_dir=$1
t_dir=$2

applist="meta replica client client.perf.test"
echo $applist > $t_dir/applist

cp $b_dir/bin/dsn.replication.simple_kv/dsn.replication.simple_kv $t_dir/simple_kv
cp $scripts_dir/deploy/configtool $t_dir/
cp $DSN_ROOT/lib/libdsn.core.so $t_dir/
cp $scripts_dir/deploy/genrcyaml.sh $t_dir
cp $scripts_dir/deploy/gensvcyaml.sh $t_dir
cp $scripts_dir/deploy/rdsn-rc.yaml.in $t_dir
cp $scripts_dir/deploy/rdsn-service.yaml.in $t_dir

if [ ! -f $t_dir/config.ini ];then
    cp $b_dir/bin/dsn.replication.simple_kv/config.ini $t_dir/
    echo "please customize your config.ini"
    sleep 5
    vim $t_dir/config.ini
fi
sed -e "s/{{ placeholder\['deploy_name'\] }}/simple_kv/g" $scripts_dir/deploy/run.sh > $t_dir/run.sh

chmod a+x $t_dir/run.sh

cat << EOF > $t_dir/Dockerfile
FROM ${REPO}/rdsn
COPY simple_kv /home/rdsn/ 
COPY config.ini /home/rdsn/
COPY run.sh /home/rdsn/
RUN mkdir -p /home/rdsn/data
RUN chown -R rdsn:rdsn /home/rdsn 
ENV LD_LIBRARY_PATH=/home/rdsn/lib
CMD ["./run.sh"]
EOF

docker build -t ${REPO}/simple_kv $t_dir
docker push ${REPO}/simple_kv


#cp general start_docker.sh to dir
sed -e "s/{{ placeholder\['image_name'\] }}/${REPO}\/simple_kv/g" $scripts_dir/deploy/start_docker.sh > $t_dir/start.sh

chmod a+x $t_dir/start.sh
#publish_docker

function publish_app_docker(){
    cp $t_dir/config.ini $t_dir/$1
    cp $t_dir/configtool $t_dir/$1
    cp $t_dir/start.sh  $t_dir/$1
    cp $t_dir/libdsn.core.so $t_dir/$1
}


for app in $applist; do
    mkdir -p $t_dir/$app
    publish_app_docker $app
done
