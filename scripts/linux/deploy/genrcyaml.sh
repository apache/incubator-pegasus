#!/bin/bash


configtool=./configtool
rclist=${rclist:-"meta replica client client.perf.test"}
rcnums=${rcnums:-"1 3 1 1"}
rcnums=($rcnums)
rctemplate=rdsn-rc.yaml.in
image_name=${image_name:-"{{ placeholder['image_name'] }}"}
image_name=`echo $image_name | sed 's|\/|\\\/|g'`
d_unit=${d_unit:-"{{ placeholder['deploy_name'] }}"}
instance_name=${instance_name:-""}
ii=0
for rc in $rclist;do
    num=${rcnums[${ii}]}
    for (( i=1; i<=${num}; i++ ));do
        app_name=$rc
        rc_name="$rc-$i-rc"
        rc_name=${rc_name//.}
        app_data="$rc-$i-data"
        app_data="${app_data//.}"

        cat $rctemplate | sed -e "s/{{ placeholder\['rc_name'\] }}/${rc_name}/g" | \
            sed -e "s/{{ placeholder\['num'\] }}/${i}/g" | \
            sed -e "s/{{ placeholder\['app_name'\] }}/${app_name}/g" | \
            sed -e "s/{{ placeholder\['instance_name'\] }}/${instance_name}/g" | \
            sed -e "s/{{ placeholder\['app_data'\] }}/${app_data}/g" | \
            sed -e "s/{{ placeholder\['deploy_name'\] }}/${d_unit}/g" | \
            sed -e "s/{{ placeholder\['image_name'\] }}/${image_name}/g" > "rdsn-${app_name}${i}-rc.yaml"
        echo "rdsn-${app_name}${i}-rc.yaml"

    done
    ((ii=ii+1))

done
