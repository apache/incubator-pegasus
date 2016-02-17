#!/bin/bash


configtool=./configtool
svclist=${svclist:-"meta replica"}
svcnums=${svcnums:-"1 3"}
svcnums=($svcnums)
svctemplate=rdsn-service.yaml.in
d_unit=${d_unit:-"{{ placeholder['deploy_name'] }}"}
instance_name=${instance_name:-""}
ii=0
for svc in $svclist;do
    num=${svcnums[${ii}]}
    for (( i=1; i<=${num}; i++));do
        port=`${configtool} config.ini apps.${svc} ports`
        app_name=$svc
        server_name=${app_name}-${i}

        cat ${svctemplate} | sed -e "s/{{ placeholder\['app_name'\] }}/${app_name}/g" | \
            sed -e "s/{{ placeholder\['server_name'\] }}/${server_name}/g" | \
            sed -e "s/{{ placeholder\['instance_name'\] }}/${instance_name}/g" | \
            sed -e "s/{{ placeholder\['port'\] }}/${port}/g" | \
            sed -e "s/{{ placeholder\['deploy_name'\] }}/${d_unit}/g" | \
            sed -e "s/{{ placeholder\['num'\] }}/${i}/g" > "rdsn-${app_name}${i}-service.yaml"
        echo "rdsn-${app_name}${i}-service.yaml"
    done

    ((ii=ii+1))
done
