#!/usr/bin/env python
# Copyright 2011 OpenStack, LLC
# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.


"""Starter script for jacket.

This script attempts to re-start cinder volume and nova compute service to enable jacket drivers.

"""

import subprocess
import time


def restart_on_opentack():
    if 0!=subprocess.call("service cinder-volume restart",shell=True) :
        raise Exception("restart cinder volume service failed")

    if 0!=subprocess.call("service nova-compute restart",shell=True) :
        raise Exception("restart nova compute service failed")


#todo: scripts for devstack
def restart_on_devstack():
    pass


def restart_on_fs():
    if 0!=subprocess.call("cps host-template-instance-operate --action stop --service cinder cinder-volume",shell=True) :
        raise Exception("Stop cinder volume service failed")
    time.sleep(1)

    if 0!=subprocess.call("cps host-template-instance-operate --action start --service cinder cinder-volume",shell=True):
        raise Exception("Start cinder volume service failed")

    if 0!=subprocess.call("cps host-template-instance-operate --action stop --service nova nova-compute",shell=True):
        raise Exception("Stop nova compute service failed")

    time.sleep(1)
    if 0!=subprocess.call("cps host-template-instance-operate --action start --service nova nova-compute",shell=True):
        raise Exception("Stop nova compute service failed")


def restart(distribution='fusionsphere'):

    if distribution == 'openstack':
        restart_on_devstack()
    elif distribution == 'fusionsphere':
        restart_on_fs()
    elif distribution == 'devstack':
        restart_on_devstack()
    else:
        pass