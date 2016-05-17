#!/bin/sh
###########################################
####  jacket project init shell file    ###
####                                    ###
####  2016/5/17                         ###
###########################################


LOG_DIR=/var/log/fusionsphere/component/jacket
LOG_FILE=${LOG_DIR}/install.log

#source code file directory
CODE_DIR=/usr/lib64/python/site-packages

TIME_CMD=`date '+%Y-%m-%d %H:%M:%S'`

copy_files_to_dir()
{
    #copy vcloud source code
    if [ -d ${CODE_DIR}/nova/virt/jacket ]; then
        echo rm -rf ${CODE_DIR}/nova/virt/jacket
        rm -rf ${CODE_DIR}/nova/virt/jacket
    fi
    echo cp -r ./jacket/nova/virt/jacket ${CODE_DIR}/nova/virt/
    cp -r ./jacket/nova/virt/jacket ${CODE_DIR}/nova/virt/

    if [ -d ${CODE_DIR}/cinder/volume/drivers/jacket ]; then
        echo rm -rf ${CODE_DIR}/cinder/volume/drivers/jacket
        rm -rf ${CODE_DIR}/cinder/volume/drivers/jacket
    fi
    echo cp -r ./jacket/cinder/volume/drivers/jacket ${CODE_DIR}/cinder/volume/drivers
    cp -r ./jacket/cinder/volume/drivers/jacket ${CODE_DIR}/cinder/volume/drivers

    #copy aws source code
    if [ -d ${CODE_DIR}/nova/virt/aws ]; then
        echo rm -rf ${CODE_DIR}/nova/virt/aws
        rm -rf ${CODE_DIR}/nova/virt/aws
    fi
    echo cp -r ./jacket/nova/virt/aws ${CODE_DIR}/nova/virt/
    cp -r ./jacket/nova/virt/aws ${CODE_DIR}/nova/virt/

    if [ -d ${CODE_DIR}/cinder/volume/drivers/ec2 ]; then
        echo rm -rf ${CODE_DIR}/cinder/volume/drivers/ec2
        rm -rf ${CODE_DIR}/cinder/volume/drivers/ec2
    fi
    echo cp -r ./jacket/cinder/volume/drivers/ec2 ${CODE_DIR}/cinder/volume/drivers
    cp -r ./jacket/cinder/volume/drivers/ec2 ${CODE_DIR}/cinder/volume/drivers

    echo ""
}

clear_files()
{
    #remove source code files
    echo rm -rf ${CODE_DIR}/nova/virt/jacket
    rm -rf ${CODE_DIR}/nova/virt/jacket

    echo rm -rf ${CODE_DIR}/cinder/volume/drivers/jacket
    rm -rf ${CODE_DIR}/cinder/volume/drivers/jacket

    echo rm -rf ${CODE_DIR}/nova/virt/aws
    rm -rf ${CODE_DIR}/nova/virt/aws

    echo rm -rf ${CODE_DIR}/cinder/volume/drivers/ec2
    rm -rf ${CODE_DIR}/cinder/volume/drivers/ec2

    echo ""
}

# create log directory
mkdir -p ${LOG_DIR}
chown -R openstack:openstack ${LOG_DIR}

init() {
:
}

start() {
    echo ""
    echo "...cps host-template-instance-operate --action start --service nova nova-compute..."
    cps host-template-instance-operate --action start --service nova nova-compute

    echo ""
    echo "...cps host-template-instance-operate --action start --service cinder cinder-volume..."
    cps host-template-instance-operate --action start --service cinder cinder-volume

    echo ""
    echo "...cps host-template-instance-operate --action start --service cinder cinder-backup..."
    cps host-template-instance-operate --action start --service cinder cinder-backup
}


stop() {
    echo ""
    echo "...cps host-template-instance-operate --action stop --service nova nova-compute..."
    cps host-template-instance-operate --action stop --service nova nova-compute

    echo ""
    echo "...host-template-instance-operate --action stop --service cinder cinder-volume..."
    cps host-template-instance-operate --action stop --service cinder cinder-volume

    echo ""
    echo "...cps host-template-instance-operate --action stop --service cinder cinder-backup..."
    cps host-template-instance-operate --action stop --service cinder cinder-backup
}
restart() {
    stop; sleep 1; start;
}

install() {
     echo "install jacket..."
     init

     #copy source_code
     echo "...copy_files_to_dir..."
     copy_files_to_dir

     #start service
     echo restart service
     restart

     echo ""
}

uninstall() {
     #stop service
     echo stop service
     stop

     #clear source_code bin config_file
     echo ""
     echo "...clear_files..."
     clear_files

     echo ""
}

echo uninstall install start stop restart | grep -w ${1:-NOT} >/dev/null && $@  >> "${LOG_FILE}" 2>&1

