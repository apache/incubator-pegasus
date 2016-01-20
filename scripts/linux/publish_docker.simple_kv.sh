#!/bin/bash

set -e

b_dir=$1
t_dir=$2
if [ "$#" -eq "3" ];then
    repub="x"
fi
if [ -z $DOCKER_REPO ];then
    echo "please specify docker repository using export DOCKER_REPO=<repo name>"
    exit -1
fi

applist="meta replica client client.perf.test"
echo $applist > $t_dir/applist
echo "simple_kv" > $t_dir/d_unit
cp $b_dir/bin/dsn.replication.simple_kv/dsn.replication.simple_kv $t_dir/simple_kv
cp $scripts_dir/deploy/configtool $t_dir/
cp $DSN_ROOT/lib/libdsn.core.so $t_dir/
#cp $scripts_dir/deploy/genrcyaml.sh $t_dir
#cp $scripts_dir/deploy/gensvcyaml.sh $t_dir
sed -e "s/{{ placeholder\['deploy_name'\] }}/simple_kv/g" $scripts_dir/deploy/genrcyaml.sh | \
    sed -e "s/{{ placeholder\['image_name'\] }}/${DOCKER_REPO}\/simple_kv/g" > $t_dir/genrcyaml.sh
chmod a+x $t_dir/genrcyaml.sh

sed -e "s/{{ placeholder\['deploy_name'\] }}/simple_kv/g" $scripts_dir/deploy/gensvcyaml.sh > $t_dir/gensvcyaml.sh
chmod a+x $t_dir/gensvcyaml.sh

cp $scripts_dir/deploy/rdsn-rc.yaml.in $t_dir
cp $scripts_dir/deploy/rdsn-service.yaml.in $t_dir
cp $scripts_dir/deploy/stop_docker.sh $t_dir/stop.sh
if [ -z $repub ] || [ ! -f $t_dir/config.ini ];then
    cp $b_dir/bin/dsn.replication.simple_kv/config-docker.ini $t_dir/config.ini
    echo "please customize your config.ini"
    sleep 5
    vim $t_dir/config.ini
fi
sed -e "s/{{ placeholder\['deploy_name'\] }}/simple_kv/g" $scripts_dir/deploy/run.sh > $t_dir/run.sh

chmod a+x $t_dir/run.sh

cat << EOF > $t_dir/Dockerfile
FROM ${DOCKER_REPO}/rdsn
COPY simple_kv /home/rdsn/ 
COPY config.ini /home/rdsn/
COPY run.sh /home/rdsn/
RUN mkdir -p /home/rdsn/data
RUN chown -R rdsn:rdsn /home/rdsn 
ENV LD_LIBRARY_PATH=/home/rdsn/lib
CMD ["./run.sh"]
EOF

docker build -t ${DOCKER_REPO}/simple_kv $t_dir
docker push ${DOCKER_REPO}/simple_kv


#cp general start_docker.sh to dir
sed -e "s/{{ placeholder\['image_name'\] }}/${DOCKER_REPO}\/simple_kv/g" $scripts_dir/deploy/start_docker.sh > $t_dir/start.sh

chmod a+x $t_dir/start.sh
#publish_docker

function publish_app_docker(){
    cp $t_dir/config.ini $t_dir/$1
    cp $t_dir/configtool $t_dir/$1
    cp $t_dir/start.sh  $t_dir/$1
    cp $t_dir/libdsn.core.so $t_dir/$1
    cp $t_dir/stop.sh $t_dir/$1
}

if [ -z $repub ] || [ ! -f $t_dir/metalist ] || [ ! -f $t_dir/replicalist ] || [ ! -f $t_dir/clientlist ] || [ ! -f $t_dir/client.perf.clientlist ];then
    rm -f $t_dir/{meta,replica,client,client.perf.test}list 
    touch $t_dir/metalist $t_dir/replicalist $t_dir/clientlist $t_dir/client.perf.testlist
fi


for app in $applist; do
    mkdir -p $t_dir/$app
    publish_app_docker $app
done
