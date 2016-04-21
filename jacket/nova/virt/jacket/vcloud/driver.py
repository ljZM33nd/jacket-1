# Copyright 2011 OpenStack Foundation
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
A connection to the VMware vCloud platform.
"""

import os
import time
import shutil
import urllib2
import sshclient
import eventlet
import subprocess
from oslo.config import cfg

from nova import utils
from nova import image
from nova.i18n import _
from nova import exception
from nova.virt import driver
from nova.network import neutronv2
from nova.image import glance
from nova.image import image_utils
from nova.compute import power_state
from nova.compute import task_states
from nova.context import RequestContext
from nova.openstack.common import excutils
from nova.openstack.common import fileutils
from nova.openstack.common import jsonutils
from nova.compute import utils as compute_utils
from nova.volume.cinder import API as cinder_api
from nova.openstack.common import log as logging

from nova.virt.jacket.vcloud import util
from nova.virt.jacket.vcloud import constants
from nova.virt.jacket.common import common_tools
from nova.virt.jacket.vcloud import hyper_agent_api
from nova.virt.jacket.vcloud.vcloud import VCLOUD_STATUS
from nova.virt.jacket.vcloud.vcloud_client import VCloudClient

from wormholeclient import errors
from wormholeclient.client import Client
from wormholeclient import constants as client_constants


vcloudapi_opts = [

    cfg.StrOpt('vcloud_node_name',
               default='vcloud_node_01',
               help='node name, which a node is a vcloud vcd'
               'host.'),
    cfg.StrOpt('vcloud_host_ip',
               help='Hostname or IP address for connection to VMware VCD '
               'host.'),
    cfg.IntOpt('vcloud_host_port',
               default=443,
               help='Host port for cnnection to VMware VCD '
               'host.'),
    cfg.StrOpt('vcloud_host_username',
               help='Host username for connection to VMware VCD '
               'host.'),
    cfg.StrOpt('vcloud_host_password',
               help='Host password for connection to VMware VCD '
               'host.'),
    cfg.StrOpt('vcloud_org',
               help='User org for connection to VMware VCD '
               'host.'),
    cfg.StrOpt('vcloud_vdc',
               help='Vdc for connection to VMware VCD '
               'host.'),
    cfg.StrOpt('vcloud_version',
               default='5.5',
               help='Version for connection to VMware VCD '
               'host.'),
    cfg.StrOpt('vcloud_service',
               default='85-719',
               help='Service for connection to VMware VCD '
               'host.'),
    cfg.BoolOpt('vcloud_verify',
                default=False,
                help='Verify for connection to VMware VCD '
                'host.'),
    cfg.BoolOpt('use_link_clone',
                default=True,
                help='Use link clone or not '),
    cfg.StrOpt('vcloud_service_type',
               default='vcd',
               help='Service type for connection to VMware VCD '
               'host.'),
    cfg.IntOpt('vcloud_api_retry_count',
               default=12,
               help='Api retry count for connection to VMware VCD '
               'host.'),
    cfg.StrOpt('vcloud_conversion_dir',
               default='/vcloud/convert_tmp',
               help='the directory where images are converted in '),
    cfg.StrOpt('vcloud_volumes_dir',
               default='/vcloud/volumes',
               help='the directory of volume files'),
    cfg.StrOpt('vcloud_vm_naming_rule',
               default='openstack_vm_id',
               help='the rule to name vcloud VMs, valid options:'
               'openstack_vm_id, openstack_vm_name, cascaded_openstack_rule'),
    cfg.DictOpt('vcloud_flavor_map',
                default={
                    'm1.tiny': '1',
                    'm1.small': '2',
                    'm1.medium': '3',
                    'm1.large': '4',
                    'm1.xlarge': '5'},
                help='map nova flavor name to vcloud vm specification id'),
    cfg.StrOpt('metadata_iso_catalog',
               default='metadata-isos',
               help='The metadata iso cotalog.'),
    cfg.StrOpt('provider_base_network_name',
               help='The provider network name which base provider network use.'),
    cfg.StrOpt('provider_tunnel_network_name',
               help='The provider network name which tunnel provider network use.'),
    cfg.StrOpt('image_user',
               default='',
               help=''),
    cfg.StrOpt('image_password',
               default='',
               help=''),
    cfg.StrOpt('tunnel_cidr',
               help='The tunnel cidr of provider network.'),
    cfg.StrOpt('route_gw',
               help='The route gw of the provider network.'),

    cfg.StrOpt('dst_path',
               default = '/home/neutron_agent_conf.txt',
               help='The config location for hybrid vm.'),
    cfg.StrOpt('hybrid_service_port',
               default = '7127',
               help='The port of the hybrid service.')
]

status_dict_vapp_to_instance = {
    VCLOUD_STATUS.FAILED_CREATION: power_state.CRASHED,
    VCLOUD_STATUS.UNRESOLVED: power_state.NOSTATE,
    VCLOUD_STATUS.RESOLVED: power_state.NOSTATE,
    VCLOUD_STATUS.DEPLOYED: power_state.NOSTATE,
    VCLOUD_STATUS.SUSPENDED: power_state.SUSPENDED,
    VCLOUD_STATUS.POWERED_ON: power_state.RUNNING,
    VCLOUD_STATUS.WAITING_FOR_INPUT: power_state.NOSTATE,
    VCLOUD_STATUS.UNKNOWN: power_state.NOSTATE,
    VCLOUD_STATUS.UNRECOGNIZED: power_state.NOSTATE,
    VCLOUD_STATUS.POWERED_OFF: power_state.SHUTDOWN,
    VCLOUD_STATUS.INCONSISTENT_STATE: power_state.NOSTATE,
    VCLOUD_STATUS.MIXED: power_state.NOSTATE,
    VCLOUD_STATUS.DESCRIPTOR_PENDING: power_state.NOSTATE,
    VCLOUD_STATUS.COPYING_CONTENTS: power_state.NOSTATE,
    VCLOUD_STATUS.DISK_CONTENTS_PENDING: power_state.NOSTATE,
    VCLOUD_STATUS.QUARANTINED: power_state.NOSTATE,
    VCLOUD_STATUS.QUARANTINE_EXPIRED: power_state.NOSTATE,
    VCLOUD_STATUS.REJECTED: power_state.NOSTATE,
    VCLOUD_STATUS.TRANSFER_TIMEOUT: power_state.NOSTATE,
    VCLOUD_STATUS.VAPP_UNDEPLOYED: power_state.NOSTATE,
    VCLOUD_STATUS.VAPP_PARTIALLY_DEPLOYED: power_state.NOSTATE,
}


CONF = cfg.CONF
CONF.register_opts(vcloudapi_opts, 'vcloud')

LOG = logging.getLogger(__name__)


IMAGE_API = image.API()

def _retry_decorator(max_retry_count=-1, inc_sleep_time=10, max_sleep_time=10, exceptions=()):
    def handle_func(func):
        def handle_args(*args, **kwargs):
            retry_count = 0
            sleep_time = 0
            def _sleep(retry_count, sleep_time):
                retry_count += 1
                if max_retry_count == -1 or retry_count < max_retry_count:
                    sleep_time += inc_sleep_time
                    if sleep_time > max_sleep_time:
                        sleep_time = max_sleep_time

                    LOG.debug('_retry_decorator func %s times %s sleep time %s', func, retry_count, sleep_time)
                    eventlet.greenthread.sleep(sleep_time)
                    return retry_count, sleep_time
                else:
                    return retry_count, sleep_time
            while (max_retry_count == -1 or retry_count < max_retry_count):
                try:
                    result = func(*args, **kwargs)
                    if not result:
                        retry_count, sleep_time = _sleep(retry_count, sleep_time)
                    else:
                        return result
                except exceptions:
                    with excutils.save_and_reraise_exception() as ctxt:
                        retry_count, sleep_time = _sleep(retry_count, sleep_time)
                        if max_retry_count == -1 or retry_count < max_retry_count:
                            ctxt.reraise = False

            if max_retry_count != -1 and retry_count >= max_retry_count:
                LOG.error(_("func (%(name)s) exec failed since retry count (%(retry_count)d) reached max retry count (%(max_retry_count)d)."),
                                  {'name': func, 'retry_count': retry_count, 'max_retry_count': max_retry_count})
        return handle_args
    return handle_func

def make_step_decorator(context, instance, update_instance_progress,
                        total_offset=0):
    """Factory to create a decorator that records instance progress as a series
    of discrete steps.

    Each time the decorator is invoked we bump the total-step-count, so after::

        @step
        def step1():
            ...

        @step
        def step2():
            ...

    we have a total-step-count of 2.

    Each time the step-function (not the step-decorator!) is invoked, we bump
    the current-step-count by 1, so after::

        step1()

    the current-step-count would be 1 giving a progress of ``1 / 2 *
    100`` or 50%.
    """
    step_info = dict(total=total_offset, current=0)

    def bump_progress():
        step_info['current'] += 1
        update_instance_progress(context, instance,
                                 step_info['current'], step_info['total'])

    def step_decorator(f):
        step_info['total'] += 1

        @functools.wraps(f)
        def inner(*args, **kwargs):
            rv = f(*args, **kwargs)
            bump_progress()
            return rv

        return inner

    return step_decorator

class VCloudDriver(driver.ComputeDriver):
    """The VCloud host connection object."""

    def __init__(self, virtapi, scheme="https"):
        #to do, read from db when boot
        self.instances = {}

        self._node_name = CONF.vcloud.vcloud_node_name
        self._vcloud_client = VCloudClient(scheme=scheme)
        self._cinder_api = cinder_api()
        self._image_api = image.API()

        if not os.path.exists(CONF.vcloud.vcloud_conversion_dir):
            os.makedirs(CONF.vcloud.vcloud_conversion_dir)

        if not os.path.exists(CONF.vcloud.vcloud_volumes_dir):
            os.makedirs(CONF.vcloud.vcloud_volumes_dir)

        self.hyper_agent_api = hyper_agent_api.HyperAgentAPI()
        super(VCloudDriver, self).__init__(virtapi)

        #begin for cleaning garbage disk
        disk_refs = self._vcloud_client._invoke_api('get_diskRefs',
                                     self._vcloud_client._get_vcloud_vdc())
        for disk_ref in disk_refs:
            #self._vcloud_client.delete_volume(disk_ref.get_name())
            pass
        #end for cleaning garbage disk

    def init_host(self, host):
        LOG.debug("init_host %s", host)

    def cleanup_host(self, host):
        """Clean up anything that is necessary for the driver gracefully stop,
        including ending remote sessions. This is optional.
        """
        LOG.debug("cleanup_host %s", host)

    def get_info(self, instance):
        state = power_state.NOSTATE
        try:
            vapp_name = self._get_vcloud_vapp_name(instance)
            vapp_status = self._vcloud_client.get_vcloud_vapp_status(vapp_name)
            state = status_dict_vapp_to_instance.get(vapp_status)
        except Exception as e:
            LOG.info('can not find the vapp %s' % e)

        return {'state': state,
                'max_mem': 0,
                'mem': 0,
                'num_cpu': 1,
                'cpu_time': 0}

    def get_num_instances(self):
        """Return the total number of virtual machines.

        Return the number of virtual machines that the hypervisor knows
        about.

        .. note::

            This implementation works for all drivers, but it is
            not particularly efficient. Maintainers of the virt drivers are
            encouraged to override this method with something more
            efficient.
        """
        return len(self.list_instances())

    def instance_exists(self, instance):
        """Checks existence of an instance on the host.

        :param instance: The instance to lookup

        Returns True if an instance with the supplied ID exists on
        the host, False otherwise.

        .. note::

            This implementation works for all drivers, but it is
            not particularly efficient. Maintainers of the virt drivers are
            encouraged to override this method with something more
            efficient.
        """
        try:
            return instance.uuid in self.list_instance_uuids()
        except NotImplementedError:
            return instance.name in self.list_instances()

    def estimate_instance_overhead(self, instance_info):
        """Estimate the virtualization overhead required to build an instance
        of the given flavor.

        Defaults to zero, drivers should override if per-instance overhead
        calculations are desired.

        :param instance_info: Instance/flavor to calculate overhead for.
        :returns: Dict of estimated overhead values.
        """
        return {'memory_mb': 0}

    def list_instances(self):
        name_labels = []
        for instance in self.instances.values():
            name_labels.append(instance.name)
        return name_labels

    def list_instance_uuids(self):
        """Return the UUIDS of all the instances known to the virtualization
        layer, as a list.
        """
        return self.instances.keys()

    def _get_vcloud_vapp_name(self, instance):
        if CONF.vcloud.vcloud_vm_naming_rule == 'openstack_vm_id':
            return instance.uuid
        elif CONF.vcloud.vcloud_vm_naming_rule == 'openstack_vm_name':
            return instance.display_name
        elif CONF.vcloud.vcloud_vm_naming_rule == 'cascaded_openstack_rule':
            return instance.display_name
        else:
            return instance.uuid

    def _get_vcloud_volume_name(self, volume_id, volume_name):
        prefix = 'volume@'
        if volume_name.startswith(prefix):
            vcloud_volume_name = volume_name[len(prefix):]
        else:
            vcloud_volume_name = volume_id

        return vcloud_volume_name

    def _get_root_bdm_from_bdms(self, bdms, root_device_name):
        for bdm in bdms:
            if bdm['mount_device'] == root_device_name:
                return bdm

        return None

    @staticmethod
    def _binding_host(context, network_info, host_id):
        neutron = neutronv2.get_client(context, admin=True)
        port_req_body = {'port': {'binding:host_id': host_id}}
        for vif in network_info:
            neutron.update_port(vif.get('id'), port_req_body)

    @staticmethod
    def _binding_host_vif(vif, host_id):
        context = RequestContext('user_id', 'project_id')
        neutron = neutronv2.get_client(context, admin=True)
        port_req_body = {'port': {'binding:host_id': host_id}}
        neutron.update_port(vif.get('id'), port_req_body)

    def _spawn_from_image(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        """
        """
        def _del_vapp():
            self._vcloud_client.delete_vapp(vapp_name)

        def _delete_volume():
            self._vcloud_client.delete_volume(disk_name)

        def _detach_disks_to_vm():
            for attached_disk_name in attached_disk_names:
                result, disk_ref = self._vcloud_client.get_disk_ref(attached_disk_name)
                if result:
                    self._vcloud_client.detach_disk_from_vm(vapp_name, disk_ref)

        def _power_off_vapp():
            self._vcloud_client.power_off_vapp(vapp_name)

        undo_mgr = utils.UndoManager()
        attached_disk_names = []

        try:
            image_cache_dir = CONF.vcloud.vcloud_conversion_dir

            if 'container_format' in image_meta and image_meta['container_format'] == constants.HYBRID_VM:
                is_hybrid_vm = True

            # update port bind host
            self._binding_host(context, network_info, instance.uuid)

            vapp_name = self._get_vcloud_vapp_name(instance)

            # 0.get vorg, user name,password vdc  from configuration file (only one org)

            # 1.1 get image id, vm info ,flavor info
            # image_uuid = instance.image_ref
            if 'id' in image_meta:
                # create from image
                image_uuid = image_meta['id']
            else:
                # create from volume
                image_uuid = image_meta['properties']['image_id']

            #NOTE(nkapotoxin): create vapp with vapptemplate
            network_names = [CONF.vcloud.provider_tunnel_network_name, CONF.vcloud.provider_base_network_name]
            network_configs = self._vcloud_client.get_network_configs(network_names)

            # create vapp
            if is_hybrid_vm:
                vapp = self._vcloud_client.create_vapp(vapp_name, image_uuid , network_configs)
            else:
                vapp = self._vcloud_client.create_vapp(vapp_name,image_uuid , network_configs,
                                                       root_gb=instance.get_flavor().root_gb)
            undo_mgr.undo_with(_del_vapp)
            LOG.debug("Create vapp %s successful", vapp_name)

            # generate the network_connection
            network_connections = self._vcloud_client.get_network_connections(vapp, network_names)

            # update network
            self._vcloud_client.update_vms_connections(vapp, network_connections)

            # update vm specification
            self._vcloud_client.modify_vm_cpu(vapp, instance.get_flavor().vcpus)
            self._vcloud_client.modify_vm_memory(vapp, instance.get_flavor().memory_mb)
            LOG.debug("Config vapp %s successful", vapp_name)

            rabbit_host = CONF.rabbit_host
            if 'localhost' in rabbit_host or '127.0.0.1' in rabbit_host:
                rabbit_host = CONF.rabbit_hosts[0]
            if ':' in rabbit_host:
                rabbit_host = rabbit_host[0:rabbit_host.find(':')]

            if not is_hybrid_vm:
                this_conversion_dir = '%s/%s' % (CONF.vcloud.vcloud_conversion_dir, 
                                                instance.uuid)
                fileutils.ensure_tree(this_conversion_dir)
                os.chdir(this_conversion_dir)
                #0: create metadata iso and upload to vcloud
                iso_file = common_tools.create_user_data_iso(
                    "userdata.iso",
                    {"rabbit_userid": CONF.rabbit_userid,
                     "rabbit_password": CONF.rabbit_password,
                     "rabbit_host": rabbit_host,
                     "host": instance.uuid,
                     "tunnel_cidr": CONF.vcloud.tunnel_cidr,
                     "route_gw": CONF.vcloud.route_gw},
                    this_conversion_dir)
                metadata_iso = self._vcloud_client.upload_metadata_iso(iso_file,
                                                                       vapp_name)
                # mount it
                self._vcloud_client.insert_media(vapp_name, metadata_iso)

                # 7. clean up
                shutil.rmtree(this_conversion_dir, ignore_errors=True)
            else:
                #when spawn from image, root_device doesn't exist in bdm, so create local disk as root device
                if vapp_name.startswith('server@'):
                    disk_name = 'Local@%s' % vapp_name[len('server@'):]
                else:
                    disk_name = 'Local@%s' % vapp_name
                self._vcloud_client.create_volume(disk_name, instance.get_flavor().root_gb)
                undo_mgr.undo_with(_delete_volume)
                LOG.debug("Create local disk %s for app %s successful", disk_name, vapp_name)

                result, disk_ref = self._vcloud_client.get_disk_ref(disk_name)
                if result:
                    self._vcloud_client.attach_disk_to_vm(vapp_name, disk_ref)
                else:
                    msg = _('Unable to find volume %s to instance') % disk_name
                    LOG.error(msg)
                    raise exception.NovaException(msg)

                attached_disk_names.append(disk_name)
                undo_mgr.undo_with(_detach_disks_to_vm)

            # power on it
            self._vcloud_client.power_on_vapp(vapp_name)
            undo_mgr.undo_with(_power_off_vapp)

            if is_hybrid_vm:
                vapp_ip = self.get_vapp_ip(vapp_name)
                client = Client(vapp_ip, CONF.vcloud.hybrid_service_port)
                self._wait_hybrid_service_up(vapp_ip, CONF.vcloud.hybrid_service_port)
                LOG.debug("vapp %s(ip %s) hybrid service has been up", vapp_name, vapp_ip)

                LOG.info("To inject file %s for vapp %s", CONF.vcloud.dst_path, vapp_name)
                file_data = 'rabbit_userid=%s\nrabbit_password=%s\nrabbit_host=%s\n' % (CONF.rabbit_userid, CONF.rabbit_password, rabbit_host)
                file_data += 'host=%s\ntunnel_cidr=%s\nroute_gw=%s\n' % (instance.uuid,CONF.vcloud.tunnel_cidr,CONF.vcloud.route_gw)
                client.inject_file(CONF.vcloud.dst_path, file_data = file_data)

                bdms = block_device_info.get('block_device_mapping')
                root_device_name = block_device_info.get('root_device_name')

                for bdm in bdms:
                    mount_device = bdm['mount_device']
                    if mount_device == root_device_name:
                        continue

                    volume_id = bdm['connection_info']['data']['volume_id']
                    volume_name = bdm['connection_info']['data']['display_name']
                    vcloud_volume_name = self._get_vcloud_volume_name(volume_id, volume_name)
                    result,disk_ref = self._vcloud_client.get_disk_ref(vcloud_volume_name)

                    if result:
                        LOG.debug("Find volume successful, disk name is: %(disk_name)s"
                                  "disk ref's href is: %(disk_href)s.",
                                  {'disk_name': vcloud_volume_name,
                                   'disk_href': disk_ref.href})

                        odevs = set(client.list_volume()['devices'])
                        if self._vcloud_client.attach_disk_to_vm(vapp_name, disk_ref):
                            attached_disk_names.append(vcloud_volume_name)
                            LOG.debug("Volume %(volume_name)s attached to: %(instance_name)s",
                                     {'volume_name': vcloud_volume_name,
                                      'instance_name': vapp_name})

                        ndevs = set(client.list_volume()['devices'])
                        devs = ndevs - odevs

                        volume = self._cinder_api.get(context, volume_id)
                        bdm['real_device'] = devs[0]
                        bdm['size'] = volume['size']
                    else:
                        msg = _('Unable to find volume %s to instance') % vcloud_volume_name
                        LOG.error(msg)
                        raise exception.NovaException(msg)

                LOG.info("To create container %s for vapp %s time %s", image_meta.get('name'), vapp_name,
                                        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                task = client.create_container(image_meta.get('name'), image_uuid,
                                        inject_files=injected_files,
                                        admin_password=admin_password,
                                        network_info=network_info,
                                        block_device_info=block_device_info)
                while task['code'] == client_constants.TASK_DOING:
                    eventlet.greenthread.sleep(10)
                    task = client.query_task(task)

                if task['code'] != client_constants.TASK_SUCCESS:
                    LOG.error(task['message'])
                    raise exception.NovaException(task['message'])
                else:
                    LOG.debug('end time of create container is %s', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

                LOG.info("To start container network:%s, block_device_info:%s for instance %s", network_info, block_device_info, vapp_name)
                client.start_container(network_info=network_info, block_device_info=block_device_info)

            # update port bind host
            self._binding_host(context, network_info, instance.uuid)
            self.instances[instance.uuid] = instance
        except Exception as e:
            msg = _("Failed to spawn reason %s, rolling back") % e
            LOG.error(msg)
            undo_mgr.rollback_and_reraise(msg=msg, instance=instance)

    def _spawn_from_volume(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        """
        """

        def _del_vapp():
            self._vcloud_client.delete_vapp(vapp_name)

        def _detach_disks_to_vm():
            for attached_disk_name in attached_disk_names:
                result, disk_ref = self._vcloud_client.get_disk_ref(attached_disk_name)
                if result:
                    self._vcloud_client.detach_disk_from_vm(vapp_name, disk_ref)

        def _power_off_vapp():
            self._vcloud_client.power_off_vapp(vapp_name)

        undo_mgr = utils.UndoManager()
        attached_disk_names = []

        try:
            bdms = block_device_info.get('block_device_mapping')
            root_device_name = block_device_info.get('root_device_name')

            root_bdm = self._get_root_bdm_from_bdms(bdms, root_device_name)

            root_volume_id = root_bdm['connection_info']['data']['volume_id']
            root_volume = self._cinder_api.get(context, root_volume_id)
            volume_image_metadata = root_volume['volume_image_metadata']

            rabbit_host = CONF.rabbit_host
            if 'localhost' in rabbit_host or '127.0.0.1' in rabbit_host:
                rabbit_host = CONF.rabbit_hosts[0]
            if ':' in rabbit_host:
                rabbit_host = rabbit_host[0:rabbit_host.find(':')]

            if volume_image_metadata['container_format'] == constants.HYBRID_VM:
                self._binding_host(context, network_info, instance.uuid)

                network_names = [CONF.vcloud.provider_tunnel_network_name, CONF.vcloud.provider_base_network_name]
                network_configs = self._vcloud_client.get_network_configs(network_names)

                vapp_name = self._get_vcloud_vapp_name(instance)
                vapp = self._vcloud_client.create_vapp(vapp_name, volume_image_metadata['image_id'], network_configs)

                LOG.debug("Create vapp %s successful", vapp_name)
                undo_mgr.undo_with(_del_vapp)

                # generate the network_connection
                network_connections = self._vcloud_client.get_network_connections(vapp, network_names)
                # update network
                self._vcloud_client.update_vms_connections(vapp, network_connections)

                # update vm specification
                self._vcloud_client.modify_vm_cpu(vapp, instance.get_flavor().vcpus)
                self._vcloud_client.modify_vm_memory(vapp, instance.get_flavor().memory_mb)
                LOG.debug("Config vapp %s successful", vapp_name)

                root_volume_name = root_bdm['connection_info']['data']['display_name']
                vcloud_volume_name = self._get_vcloud_volume_name(root_volume_id, root_volume_name)
                result,disk_ref = self._vcloud_client.get_disk_ref(vcloud_volume_name)

                if result:
                    LOG.debug("Find volume successful, disk name is: %(disk_name)s"
                              "disk ref's href is: %(disk_href)s.",
                              {'disk_name': vcloud_volume_name,
                               'disk_href': disk_ref.href})

                    if self._vcloud_client.attach_disk_to_vm(vapp_name, disk_ref):
                        LOG.debug("Volume %(volume_name)s attached to: %(instance_name)s",
                                 {'volume_name': vcloud_volume_name,
                                  'instance_name': vapp_name})

                    root_bdm['size'] = root_volume['size']

                    attached_disk_names.append(vcloud_volume_name)
                    undo_mgr.undo_with(_detach_disks_to_vm)
                else:
                    msg = _('Unable to find volume %s to instance')% vcloud_volume_name
                    LOG.error(msg)
                    raise exception.NovaException(msg)

                # power on it
                self._vcloud_client.power_on_vapp(vapp_name)
                undo_mgr.undo_with(_power_off_vapp)

                vapp_ip = self.get_vapp_ip(vapp_name)
                client = Client(vapp_ip, CONF.vcloud.hybrid_service_port)
                self._wait_hybrid_service_up(vapp_ip, CONF.vcloud.hybrid_service_port)
                LOG.debug("vapp %s(ip:%s) hybrid service has been up", vapp_name, vapp_ip)

                for bdm in bdms:
                    mount_device = bdm['mount_device']
                    if mount_device == root_device_name:
                        continue

                    volume_id = bdm['connection_info']['data']['volume_id']
                    volume_name = bdm['connection_info']['data']['display_name']
                    vcloud_volume_name = self._get_vcloud_volume_name(volume_id, volume_name)
                    result,disk_ref = self._vcloud_client.get_disk_ref(vcloud_volume_name)

                    if result:
                        LOG.debug("Find volume successful, disk name is: %(disk_name)s"
                                  "disk ref's href is: %(disk_href)s.",
                                  {'disk_name': vcloud_volume_name, 'disk_href': disk_ref.href})

                        odevs = set(client.list_volume()['devices'])
                        if self._vcloud_client.attach_disk_to_vm(vapp_name, disk_ref):
                            attached_disk_names.append(vcloud_volume_name)
                            LOG.debug("Volume %(volume_name)s attached to: %(instance_name)s",
                                     {'volume_name': vcloud_volume_name, 'instance_name': vapp_name})

                        ndevs = set(client.list_volume()['devices'])
                        devs = ndevs - odevs

                        volume = self._cinder_api.get(context, volume_id)

                        bdm['real_device'] = devs[0]
                        bdm['size'] = volume['size']
                    else:
                        msg = _('Unable to find volume %s to instance') % vcloud_volume_name
                        LOG.error(msg)
                        raise exception.NovaException(msg)

                LOG.info("To inject file %s for vapp %s", CONF.vcloud.dst_path, vapp_name)
                file_data = 'rabbit_userid=%s\nrabbit_password=%s\nrabbit_host=%s\n' % (CONF.rabbit_userid, CONF.rabbit_password, rabbit_host)
                file_data += 'host=%s\ntunnel_cidr=%s\nroute_gw=%s\n' % (instance.uuid,CONF.vcloud.tunnel_cidr,CONF.vcloud.route_gw)
                client.inject_file(CONF.vcloud.dst_path, file_data = file_data)

                LOG.info("To create container %s for vapp %s time %s", volume_image_metadata.get('image_name'), vapp_name,
                                             time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                task = client.create_container(volume_image_metadata['image_name'],
                                        volume_image_metadata['image_id'],
                                        root_volume_id=root_volume_id,
                                        inject_files=injected_files,
                                        admin_password=admin_password,
                                        network_info=network_info,
                                        block_device_info=block_device_info)

                while task['code'] == client_constants.TASK_DOING:
                    eventlet.greenthread.sleep(10)
                    task = client.query_task(task)

                if task['code'] != client_constants.TASK_SUCCESS:
                    LOG.error(task['message'])
                    raise exception.NovaException(task['message'])
                else:
                    LOG.debug('end time of create container is %s', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

                LOG.info("To start container network:%s, block_device_info:%s for instance %s", network_info, block_device_info, vapp_name)
                client.start_container(network_info=network_info, block_device_info=block_device_info)

                # update port bind host
                self._binding_host(context, network_info, instance.uuid)

                self.instances[instance.uuid] = instance
            else:
                msg = _("boot from volume for normal vm not supported!")
                LOG.error(msg)
                raise exception.NovaException(msg)
        except Exception as e:
            msg = _("Failed to spawn reason %s, rolling back") % e
            LOG.error(msg)
            undo_mgr.rollback_and_reraise(msg=msg, instance=instance)

    def spawn(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        """Create VM instance."""
        LOG.debug(_("image meta is:%s") % image_meta)
        LOG.debug(_("instance is:%s") % instance)
        LOG.debug(_("network_info is %s") % network_info)
        LOG.debug(_("block_device_info is %s") % block_device_info)

        LOG.info('begin time of vcloud create vm is %s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

        if not instance.image_ref:
            self._spawn_from_volume(context, instance, image_meta, injected_files,
                                    admin_password, network_info, block_device_info)
        else:
            self._spawn_from_image(context, instance, image_meta, injected_files,
                                    admin_password, network_info, block_device_info)
        LOG.info('end time of vcloud create vm is %s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    def _update_instance_progress(self, context, instance, step, total_steps):
        """Update instance progress percent to reflect current step number
        """
        # FIXME(sirp): for now we're taking a KISS approach to instance
        # progress:
        # Divide the action's workflow into discrete steps and "bump" the
        # instance's progress field as each step is completed.
        #
        # For a first cut this should be fine, however, for large VM images,
        # the get_vdis_for_instance step begins to dominate the equation. A
        # better approximation would use the percentage of the VM image that
        # has been streamed to the destination host.
        progress = round(float(step) / total_steps * 100)
        LOG.debug("Updating progress to %d", progress,
                  instance=instance)
        instance.progress = progress
        instance.save()

    def _update_vm_task_state(self, instance, task_state):
        instance.task_state = task_state
        instance.save()

    def _do_destroy_vm(self, context, instance, network_info, block_device_info=None,
                       destroy_disks=True, migrate_data=None):
        try:
            vapp_name = self._get_vcloud_vapp_name(instance)
            self._vcloud_client.power_off_vapp(vapp_name)
            LOG.debug("power off vapp %s successful", vapp_name)

            vm_task_state = instance.task_state
            self._update_vm_task_state(instance, vm_task_state)

            if instance.system_metadata.get('image_container_format') == constants.HYBRID_VM:
                if vapp_name.startswith('server@'):
                    disk_name = 'Local@%s' % vapp_name[len('server@'):]
                else:
                    disk_name = 'Local@%s' % vapp_name

                result, disk_ref = self._vcloud_client.get_disk_ref(disk_name)
                #spawn from image
                if result:
                    self._vcloud_client.detach_disk_from_vm(vapp_name, disk_ref)
                    self._vcloud_client.delete_volume(disk_name)
                    LOG.debug("delete volume %s for vapp %s successful", vapp_name, disk_name)

            self._vcloud_client.delete_vapp(vapp_name)
            LOG.debug("delete vapp %s successful", vapp_name)

            if instance.system_metadata.get('image_container_format') != constants.HYBRID_VM:
                self._vcloud_client.delete_metadata_iso(vapp_name)
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error('_do_destroy_vm failed,reason %s' % e)

    def destroy(self, context, instance, network_info, block_device_info=None,
               destroy_disks=True, migrate_data=None):
        LOG.debug('[vcloud nova driver] destroy: %s' % instance.uuid)
        self._do_destroy_vm(context, instance, network_info, block_device_info,
                            destroy_disks, migrate_data)

        self.cleanup(context, instance, network_info, block_device_info,
                    destroy_disks, migrate_data)

        self.instances.pop(instance.uuid, None)

        # delete agent
        instance_id = instance.uuid
        neutron_client = neutronv2.get_client(context=None, admin=True)
        agent = neutron_client.list_agents(host=instance_id)
        if len(agent['agents']) == 1:
            neutron_client.delete_agent(agent['agents'][0]['id'])

    def cleanup(self, context, instance, network_info, block_device_info=None,
                destroy_disks=True, migrate_data=None, destroy_vifs=True):
        if destroy_vifs:
            self.unplug_vifs(instance, network_info)

        LOG.debug("Cleanup network finished", instance=instance)

    def reboot(self, context, instance, network_info, reboot_type,
               block_device_info=None, bad_volumes_callback=None):
        LOG.debug('[vcloud nova driver] begin reboot instance: %s' % instance.uuid)

        try:
            vapp_name = self._get_vcloud_vapp_name(instance)
            if instance.system_metadata.get('image_container_format') != constants.HYBRID_VM:
                self._vcloud_client.reboot_vapp(vapp_name)
            else:
                vapp_ip = self.get_vapp_ip(vapp_name)
                client = Client(vapp_ip, port = CONF.vcloud.hybrid_service_port)
                self._wait_hybrid_service_up(vapp_ip, CONF.vcloud.hybrid_service_port)
                client.restart_container(network_info=network_info, block_device_info=block_device_info)
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error('reboot failed,reason %s' % e)

    def get_console_pool_info(self, console_type):
        LOG.debug("get_console_pool_info")

    def get_console_output(self, context, instance):
        try:
            vapp_name = self._get_vcloud_vapp_name(instance)
            if instance.system_metadata.get('image_container_format') == constants.HYBRID_VM:
                vapp_ip = self.get_vapp_ip(vapp_name)
                client = Client(vapp_ip, CONF.vcloud.hybrid_service_port)
                self._wait_hybrid_service_up(vapp_ip, CONF.vcloud.hybrid_service_port)
                response = client.get_console_output()
                if response['logs']:
                    return response['logs']
                else:
                    return 'console output empty'
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error('get_console_output failed,reason %s' % e)

    def get_vnc_console(self, context, instance):
        LOG.debug("get_vnc_console")

    def get_spice_console(self, context, instance):
        LOG.debug("get_spice_console")

    def get_rdp_console(self, context, instance):
        LOG.debug("get_rdp_console")

    def get_serial_console(self, context, instance):
        LOG.debug("get_serial_console")

    def get_diagnostics(self, instance_name):
        LOG.debug("get_diagnostics")

    def get_instance_diagnostics(self, instance_name):
        LOG.debug("get_instance_diagnostics")

    def get_all_bw_counters(self, instances):
        LOG.debug("get_all_bw_counters")
        return []

    def get_all_volume_usage(self, context, compute_host_bdms):
        LOG.debug("get_all_volume_usage")
        return []

    def get_host_ip_addr(self):
        """Retrieves the IP address of the dom0
        """
        # TODO(Vek): Need to pass context in for access to auth_token
        LOG.debug("get_host_ip_addr")

    def _attach_volume_iscsi(self, instance, connection_info):
        user = CONF.vcloud.image_user
        pwd = CONF.vcloud.image_password
        vapp_ip = self._get_vapp_ip(instance)
        if vapp_ip:
            host = vapp_ip
        else:
            LOG.error("vapp_ip is None ,attach volume failed")
            raise Exception(_("vapp_ip is None ,attach volume failed"))

        ssh_client = sshclient.SSH(user, host, password=pwd)

        target_iqn = connection_info['data']['target_iqn']
        target_portal = connection_info['data']['target_portal']
        cmd1 = "sudo iscsiadm -m node -T %s -p %s" % (target_iqn, target_portal)
        while True:
            try:
                cmd1_status, cmd1_out, cmd1_err = ssh_client.execute(cmd1)
                LOG.debug("sudo cmd1 info status=%s ,out=%s, err=%s " % (cmd1_status, cmd1_out, cmd1_err))
                if cmd1_status in [21, 255]:
                    cmd2 = "sudo iscsiadm -m node -T %s -p %s --op new" % (target_iqn, target_portal)
                    cmd2_status, cmd2_out, cmd2_err = ssh_client.execute(cmd2)
                    LOG.debug("sudo cmd2 info status=%s ,out=%s, err=%s " % (cmd2_status, cmd2_out, cmd2_err))
                break
            except sshclient.SSHError:
                LOG.debug("wait for vm to initialize network")
                eventlet.greenthread.sleep(5)

        cmd3 = "sudo iscsiadm -m session"
        cmd3_status, cmd3_out, cmd3_err = ssh_client.execute(cmd3)
        portals = [{'portal': p.split(" ")[2], 'iqn': p.split(" ")[3]}
                   for p in cmd3_out.splitlines() if p.startswith("tcp:")]
        stripped_portal = connection_info['data']['target_portal'].split(",")[0]
        if len(portals) == 0 or len([s for s in portals
                                     if stripped_portal ==
                                     s['portal'].split(",")[0]
                                     and
                                     s['iqn'] ==
                                     connection_info['data']['target_iqn']]
                                    ) == 0:
            cmd4 = "sudo iscsiadm -m node -T %s -p %s --login" % (target_iqn, target_portal)
            cmd4_status, cmd4_out, cmd4_err = ssh_client.execute(cmd4)
            LOG.debug("sudo cmd4 info status=%s ,out=%s, err=%s " % (cmd4_status, cmd4_out, cmd4_err))
            cmd5 = "sudo iscsiadm -m node -T %s -p %s --op update -n node.startup  -v automatic" % \
                   (target_iqn, target_portal)
            cmd5_status, cmd5_out, cmd5_err = ssh_client.execute(cmd5)
            LOG.debug("sudo cmd5 info status=%s ,out=%s, err=%s " % (cmd5_status, cmd5_out, cmd5_err))
        ssh_client.close()

    def attach_volume(self, context, connection_info, instance, mountpoint,
                      disk_bus=None, device_type=None, encryption=None):
        """Attach volume storage to VM instance."""
        instance_name = instance['display_name']
        LOG.debug("Attach_volume: %(connection_info)s to %(instance_name)s",
                  {'connection_info': connection_info,
                   'instance_name': instance_name})
        volume_id = connection_info['data']['volume_id']
        driver_type = connection_info['driver_volume_type']

        volume = self._cinder_api.get(context, volume_id)
        volume_name = volume['display_name']
        # use volume_name as vcloud disk name, remove prefix str `volume@`
        # if volume_name does not start with volume@, then use volume id instead
        vcloud_volume_name = self._get_vcloud_volume_name(volume_id,  volume_name)

        # find volume reference by it's name
        vapp_name = self._get_vcloud_vapp_name(instance)
        if driver_type == 'iscsi':
            self._attach_volume_iscsi(instance, connection_info)
            return

        try:
            result, disk_ref = self._vcloud_client.get_disk_ref(vcloud_volume_name)
            if result:
                LOG.debug("Find volume successful, disk name is: %(disk_name)s"
                        "disk ref's href is: %(disk_href)s.",
                        {'disk_name': vcloud_volume_name,
                        'disk_href': disk_ref.href})

                if instance.system_metadata.get('image_container_format') == constants.HYBRID_VM \
                        and self._instance_is_active(instance):
                    vapp_ip = self.get_vapp_ip(vapp_name)
                    client = Client(vapp_ip, port = CONF.vcloud.hybrid_service_port)
                    odevs = set(client.list_volume()['devices'])
                    LOG.debug('volume_name %s odevs: %s', vcloud_volume_name, odevs)

                if self._vcloud_client.attach_disk_to_vm(vapp_name, disk_ref):
                    LOG.debug("Volume %(volume_name)s attached to: %(instance_name)s",
                            {'volume_name': vcloud_volume_name,
                            'instance_name': instance_name})

                if instance.system_metadata.get('image_container_format') == constants.HYBRID_VM \
                        and self._instance_is_active(instance):
                    ndevs = set(client.list_volume()['devices'])
                    LOG.debug('volume_name %s ndevs: %s', vcloud_volume_name, ndevs)

                    devs = ndevs - odevs
                    for dev in devs:
                        client.attach_volume(volume_id, dev, mountpoint)

                    LOG.debug ('attach volume successful')
            else:
                msg = _('Unable to find volume %s to instance') % vcloud_volume_name
                LOG.error(msg)
                raise exception.NovaException(msg)
        except Exception:
            msg = _('Unable to attach volume %s to instance') % vcloud_volume_name
            LOG.error(msg)
            raise exception.NovaException(msg)

    def _detach_volume_iscsi(self, instance, connection_info):
        user = CONF.vcloud.image_user
        pwd = CONF.vcloud.image_password
        vapp_ip = self._get_vapp_ip(instance)
        if vapp_ip:
            host = vapp_ip
        else:
            LOG.debug("vapp_ip is None ,attach volume failed")
            raise

        ssh_client = sshclient.SSH(user, host, password=pwd)
        target_iqn = connection_info['data']['target_iqn']
        target_portal = connection_info['data']['target_portal']
        cmd1 = "ls -l /dev/disk/by-path/ | grep %s | awk -F '/' '{print $NF}'" % target_iqn
        cmd1_status, cmd1_out, cmd1_err = ssh_client.execute(cmd1)
        LOG.debug(" cmd1 info status=%s ,out=%s, err=%s " % (cmd1_status, cmd1_out, cmd1_err))
        device = "/dev/" + cmd1_out.split('\n')[0]
        path = "/sys/block/" + cmd1_out.split('\n')[0] + "/device/delete"
        cmd2 = "sudo blockdev --flushbufs %s" % device
        cmd2_status, cmd2_out, cmd2_err = ssh_client.execute(cmd2)
        LOG.debug(" cmd2 info status=%s ,out=%s, err=%s " % (cmd2_status, cmd2_out, cmd2_err))
        cmd3 = "echo 1 | sudo tee -a %s" % path
        cmd3_status, cmd3_out, cmd3_err = ssh_client.execute(cmd3)
        LOG.debug("sudo cmd3 info status=%s ,out=%s, err=%s " % (cmd3_status, cmd3_out, cmd3_err))
        cmd4 = "sudo iscsiadm -m node -T %s -p %s --op update -n node.startup  -v manual" % (target_iqn, target_portal)
        cmd4_status, cmd4_out, cmd4_err = ssh_client.execute(cmd4)
        LOG.debug("sudo cmd4 info status=%s ,out=%s, err=%s " % (cmd4_status, cmd4_out, cmd4_err))
        cmd5 = "sudo iscsiadm -m node -T %s -p %s --logout" % (target_iqn, target_portal)
        cmd5_status, cmd5_out, cmd5_err = ssh_client.execute(cmd5)
        LOG.debug("sudo cmd5 info status=%s ,out=%s, err=%s " % (cmd5_status, cmd5_out, cmd5_err))
        cmd6 = "sudo iscsiadm -m node -T %s -p %s --op delete" % (target_iqn, target_portal)
        cmd6_status, cmd6_out, cmd6_err = ssh_client.execute(cmd6)
        LOG.debug("sudo cmd6 info status=%s ,out=%s, err=%s " % (cmd6_status, cmd6_out, cmd6_err))

    def detach_volume(self, connection_info, instance, mountpoint,
                      encryption=None):
        """Detach the disk attached to the instance."""
        instance_name = instance['display_name']
        LOG.debug("Detach_volume: %(connection_info)s to %(instance_name)s",
                  {'connection_info': connection_info,
                   'instance_name': instance_name})
        volume_id = connection_info['data']['volume_id']
        driver_type = connection_info['driver_volume_type']

        if driver_type == 'iscsi':
            self._detach_volume_iscsi(instance, connection_info)
            return

        volume_name = connection_info['data']['display_name']

        # use volume_name as vcloud disk name, remove prefix str `volume@`
        # if volume_name does not start with volume@, then use volume id instead
        vcloud_volume_name = self._get_vcloud_volume_name(volume_id,
                                                          volume_name)
        # find volume reference by it's name
        vapp_name = self._get_vcloud_vapp_name(instance)

        #if driver_type == 'iscsi':
        #    self._detach_volume_iscsi(instance, connection_info)
        #    return

        try:
            result, disk_ref = self._vcloud_client.get_disk_ref(vcloud_volume_name)
            if result:
                LOG.debug("Find volume successful, disk name is: %(disk_name)s"
                          "disk ref's href is: %(disk_href)s.",
                          {'disk_name': vcloud_volume_name,
                           'disk_href': disk_ref.href})

                if instance.system_metadata.get('image_container_format') == constants.HYBRID_VM \
                        and self._instance_is_active(instance):
                    vapp_ip = self.get_vapp_ip(vapp_name)
                    client = Client(vapp_ip, port = CONF.vcloud.hybrid_service_port)
                    client.detach_volume(volume_id)

                if self._vcloud_client.detach_disk_from_vm(vapp_name, disk_ref):
                    LOG.info("Volume %(volume_name)s detached from: %(instance_name)s",
                            {'volume_name': vcloud_volume_name,
                            'instance_name': instance_name})
            else:
                msg = _('Unable to find volume  %s from instance') % vcloud_volume_name
                LOG.error(msg)
                raise exception.NovaException(msg)
        except Exception:
            msg = _('Unable to attach volume %s to instance') % vcloud_volume_name
            LOG.error(msg)
            raise exception.NovaException(msg)

    def swap_volume(self, old_connection_info, new_connection_info,
                    instance, mountpoint, resize_to):
        LOG.debug("swap_volume")

    def attach_interface(self, instance, image_meta, vif):
        LOG.debug("attach interface: %s, %s" % (instance, vif))
        self._binding_host_vif(vif, instance.uuid)

        try:
            if instance.system_metadata.get('image_container_format') == constants.HYBRID_VM \
                    and self._instance_is_active(instance):
                vapp_name = self._get_vcloud_vapp_name(instance)
                vapp_ip = self.get_vapp_ip(vapp_name)
                client = Client(vapp_ip, port = CONF.vcloud.hybrid_service_port)
                client.attach_interface(vif)
        except Exception:
            msg = _('Unable to attach interface %s to instance %s') % (vif, instance)
            LOG.error(msg)
            raise exception.NovaException(msg)

        self._binding_host_vif(vif, instance.uuid)

    def detach_interface(self, instance, vif):
        LOG.debug("detach interface: %s, %s" % (instance, vif))

        try:
            if instance.system_metadata.get('image_container_format') == constants.HYBRID_VM \
                    and self._instance_is_active(instance):
                vapp_name = self._get_vcloud_vapp_name(instance)
                vapp_ip = self.get_vapp_ip(vapp_name)    
                client = Client(vapp_ip, port = CONF.vcloud.hybrid_service_port)
                client.detach_interface(vif)
        except Exception:
            msg = _('Unable to detach interface %s to instance %s') % (vif, instance)
            LOG.error(msg)
            raise exception.NovaException(msg)

    def migrate_disk_and_power_off(self, context, instance, dest,
                                   flavor, network_info,
                                   block_device_info=None,
                                   timeout=0, retry_interval=0):
        LOG.debug("migrate_disk_and_power_off")

    def _create_snapshot_metadata(self, base, instance, img_fmt, snp_name):
        metadata = {'is_public': False,
                    'status': 'active',
                    'name': snp_name,
                    'properties': {
                                   'kernel_id': instance['kernel_id'],
                                   'image_location': 'snapshot',
                                   'image_state': 'available',
                                   'owner_id': instance['project_id'],
                                   'ramdisk_id': instance['ramdisk_id'],
                                   }
                    }
        if instance['os_type']:
            metadata['properties']['os_type'] = instance['os_type']

        # NOTE(vish): glance forces ami disk format to be ami
        if base.get('disk_format') == 'ami':
            metadata['disk_format'] = 'ami'
        else:
            metadata['disk_format'] = img_fmt

        metadata['container_format'] = base.get('container_format', 'bare')

        return metadata

    def snapshot(self, context, instance, image_id, update_task_state):
        LOG.debug("context:%s", vars(context))
        LOG.debug("instance:%s", vars(instance))
        LOG.debug("image_id:%s update_task_state:%s", image_id, update_task_state)

        try:
            if instance.system_metadata.get('image_container_format') == constants.HYBRID_VM:
                base_image_ref = instance['image_ref']
                base = compute_utils.get_image_metadata(context, self._image_api, base_image_ref, instance)

                snapshot = self._image_api.get(context, image_id)
                metadata = self._create_snapshot_metadata(base,
                                                          instance,
                                                          snapshot['disk_format'],
                                                          snapshot['name'])

                update_task_state(task_state=task_states.IMAGE_PENDING_UPLOAD)

                vapp_name = self._get_vcloud_vapp_name(instance)
                vapp_ip = self.get_vapp_ip(vapp_name)
                client = Client(vapp_ip, CONF.vcloud.hybrid_service_port)
                self._wait_hybrid_service_up(vapp_ip, CONF.vcloud.hybrid_service_port)
                LOG.debug("vapp %s(ip: %s) hybrid service has been up", vapp_name, vapp_ip)

                LOG.debug('begin time of create image is %s', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                task = client.create_image(snapshot['name'], image_id)
                while task['code'] == client_constants.TASK_DOING:
                    eventlet.greenthread.sleep(10)
                    task = client.query_task(task)

                if task['code'] != client_constants.TASK_SUCCESS:
                    LOG.error(task['message'])
                    raise exception.NovaException(task['message'])
                else:
                    LOG.debug('end time of create image is %s', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

                image_info = client.image_info(snapshot['name'], image_id)
                if not image_info:
                    LOG.error('cannot get image info')
                    raise exception.NovaException('cannot get image info')

                update_task_state(task_state=task_states.IMAGE_UPLOADING,
                                  expected_state=task_states.IMAGE_PENDING_UPLOAD)

                with image_utils.temporary_file() as tmp:
                    image_service, image_id = glance.get_remote_image_service(context, image_id)
                    with fileutils.file_open(tmp, 'wb+') as f:
                        f.truncate(image_info['size'])
                        image_service.update(context, image_id, metadata, f)
                        self._update_vm_task_state(instance, task_state=instance.task_state)
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error('snapshot failed,reason %s' % e)

    def post_interrupted_snapshot_cleanup(self, context, instance):
        """Cleans up any resources left after an interrupted snapshot.

        :param context: security context
        :param instance: nova.objects.instance.Instance
        """
        LOG.debug("post_interrupted_snapshot_cleanup")

    def finish_migration(self, context, migration, instance, disk_info,
                         network_info, image_meta, resize_instance,
                         block_device_info=None, power_on=True):
        LOG.debug("finish_migration")

    def confirm_migration(self, migration, instance, network_info):
        LOG.debug("confirm_migration")

    def finish_revert_migration(self, context, instance, network_info,
                                block_device_info=None, power_on=True):
        LOG.debug("finish_revert_migration")

    def pause(self, instance):
        try:
            vapp_name = self._get_vcloud_vapp_name(instance)
            if instance.system_metadata.get('image_container_format') == constants.HYBRID_VM:
                vapp_ip = self.get_vapp_ip(vapp_name)                
                client = Client(vapp_ip, CONF.vcloud.hybrid_service_port)
                self._wait_hybrid_service_up(vapp_ip, CONF.vcloud.hybrid_service_port)
                client.pause_container()
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error('pause failed,reason %s' % e)

    def unpause(self, instance):
        try:
            vapp_name = self._get_vcloud_vapp_name(instance)
            if instance.system_metadata.get('image_container_format') == constants.HYBRID_VM:
                vapp_ip = self.get_vapp_ip(vapp_name)                
                client = Client(vapp_ip, CONF.vcloud.hybrid_service_port)
                self._wait_hybrid_service_up(vapp_ip, CONF.vcloud.hybrid_service_port)
                client.unpause_container()
        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error('unpause failed,reason %s' % e)

    def suspend(self, instance):
        LOG.debug("suspend")

    def resume(self, context, instance, network_info, block_device_info=None):
        LOG.debug("resume")

    def resume_state_on_host_boot(self, context, instance, network_info,
                                  block_device_info=None):
        LOG.debug("resume_state_on_host_boot")

    def rescue(self, context, instance, network_info, image_meta,
               rescue_password):
        LOG.debug("rescue")

    def set_bootable(self, instance, is_bootable):
        """Set the ability to power on/off an instance.

        :param instance: nova.objects.instance.Instance
        """
        LOG.debug("set_bootable")

    def unrescue(self, instance, network_info):
        LOG.debug("unrescue")

    def power_off(self, instance, shutdown_timeout=0, shutdown_attempts=0):
        LOG.debug('begin power off instance: %s' % instance.uuid)

        try:
            vapp_name = self._get_vcloud_vapp_name(instance)
            if instance.system_metadata.get('image_container_format') == constants.HYBRID_VM:
                vapp_ip = self.get_vapp_ip(vapp_name)
                client = Client(vapp_ip, CONF.vcloud.hybrid_service_port)
                client.stop_container()
                LOG.debug("stop container sucessful for instance %s" % instance.uuid)

            self._vcloud_client.power_off_vapp(vapp_name)
        except Exception as e:
            msg = 'power off instanc %s failed, reason %s' % (instance.uuid, e)
            LOG.error(msg)
            raise exception.NovaException(msg)

    def power_on(self, context, instance, network_info, block_device_info):
        LOG.debug('begin power on instance: %s' % instance.uuid)

        try:
            vapp_name = self._get_vcloud_vapp_name(instance)
            self._vcloud_client.power_on_vapp(vapp_name)

            if instance.system_metadata.get('image_container_format') == constants.HYBRID_VM: 
                vapp_ip = self.get_vapp_ip(vapp_name)                
                client = Client(vapp_ip, CONF.vcloud.hybrid_service_port)
                self._wait_hybrid_service_up(vapp_ip, CONF.vcloud.hybrid_service_port)
                client.start_container(network_info = network_info, block_device_info = block_device_info)
        except Exception as e:
            msg = 'power on instance %s failed, reason %s' % (instance.uuid, e)
            LOG.error(msg)
            raise exception.NovaException(msg)

    def soft_delete(self, instance):
        LOG.debug("soft_delete")

    def restore(self, instance):
        LOG.debug("restore")

    def get_available_resource(self, nodename):
        LOG.debug("get_available_resource")
        return {'vcpus': 32,
                'memory_mb': 164403,
                'local_gb': 5585,
                'vcpus_used': 0,
                'memory_mb_used': 69005,
                'local_gb_used': 3479,
                'hypervisor_type': 'vcloud',
                'hypervisor_version': 5005000,
                'hypervisor_hostname': nodename,
                'cpu_info': '{"model": ["Intel(R) Xeon(R) CPU E5-2670 0 @ 2.60GHz"], \
                        "vendor": ["Huawei Technologies Co., Ltd."], \
                        "topology": {"cores": 16, "threads": 32}}',
                'supported_instances': jsonutils.dumps(
                    [["i686", "vmware", "hvm"], ["x86_64", "vmware", "hvm"]]),
                'numa_topology': None,
                }

    def pre_live_migration(self, context, instance_ref, block_device_info,
                           network_info, disk, migrate_data=None):
        LOG.debug("pre_live_migration")

    def live_migration(self, context, instance_ref, dest,
                       post_method, recover_method, block_migration=False,
                       migrate_data=None):
        LOG.debug("live_migration")

    def rollback_live_migration_at_destination(self, context, instance,
                                               network_info,
                                               block_device_info,
                                               destroy_disks=True,
                                               migrate_data=None):
        """Clean up destination node after a failed live migration.

        :param context: security context
        :param instance: instance object that was being migrated
        :param network_info: instance network information
        :param block_device_info: instance block device information
        :param destroy_disks:
            if true, destroy disks at destination during cleanup
        :param migrate_data: implementation specific params

        """
        LOG.debug("rollback_live_migration_at_destination")


    def post_live_migration(self, context, instance, block_device_info,
                            migrate_data=None):
        """Post operation of live migration at source host.

        :param context: security context
        :instance: instance object that was migrated
        :block_device_info: instance block device information
        :param migrate_data: if not None, it is a dict which has data
        """
        LOG.debug("post_live_migration")

    def post_live_migration_at_source(self, context, instance, network_info):
        """Unplug VIFs from networks at source.

        :param context: security context
        :param instance: instance object reference
        :param network_info: instance network information
        """
        LOG.debug("post_live_migration_at_source")

    def post_live_migration_at_destination(self, context, instance,
                                           network_info,
                                           block_migration=False,
                                           block_device_info=None):
        LOG.debug("post_live_migration_at_destination")

    def check_instance_shared_storage_local(self, context, instance):
        """Check if instance files located on shared storage.

        This runs check on the destination host, and then calls
        back to the source host to check the results.

        :param context: security context
        :param instance: nova.db.sqlalchemy.models.Instance
        """
        LOG.debug("check_instance_shared_storage_local")

    def check_instance_shared_storage_remote(self, context, data):
        """Check if instance files located on shared storage.

        :param context: security context
        :param data: result of check_instance_shared_storage_local
        """
        LOG.debug("check_instance_shared_storage_remote")

    def check_instance_shared_storage_cleanup(self, context, data):
        """Do cleanup on host after check_instance_shared_storage calls

        :param context: security context
        :param data: result of check_instance_shared_storage_local
        """
        LOG.debug("check_instance_shared_storage_cleanup")

    def check_can_live_migrate_destination(self, context, instance,
                                           src_compute_info, dst_compute_info,
                                           block_migration=False,
                                           disk_over_commit=False):
        LOG.debug("check_can_live_migrate_destination")
        return {}

    def check_can_live_migrate_destination_cleanup(self, context,
                                                   dest_check_data):
        LOG.debug("check_can_live_migrate_destination_cleanup")

    def check_can_live_migrate_source(self, context, instance,
                                      dest_check_data):
        LOG.debug("check_can_live_migrate_source")

    def get_instance_disk_info(self, instance_name,
                                        block_device_info=None):
        LOG.debug("get_instance_disk_info")

    def refresh_security_group_rules(self, security_group_id):
        LOG.debug("refresh_security_group_rules")
        return True

    def refresh_security_group_members(self, security_group_id):
        LOG.debug("refresh_security_group_members")
        return True

    def refresh_provider_fw_rules(self):
        LOG.debug("refresh_provider_fw_rules")
        
    def refresh_instance_security_rules(self, instance):
        LOG.debug("refresh_instance_security_rules")
        return True

    def reset_network(self, instance):
        """reset networking for specified instance."""
        # TODO(Vek): Need to pass context in for access to auth_token
        LOG.debug("reset_network")

    def ensure_filtering_rules_for_instance(self, instance, network_info):
        LOG.debug("ensure_filtering_rules_for_instance")

    def filter_defer_apply_on(self):
        """Defer application of IPTables rules."""
        LOG.debug("filter_defer_apply_on")

    def filter_defer_apply_off(self):
        """Turn off deferral of IPTables rules and apply the rules now."""
        LOG.debug("filter_defer_apply_off")

    def unfilter_instance(self, instance, network_info):
        LOG.debug("unfilter_instance")

    def set_admin_password(self, instance, new_pass):
        LOG.debug("set instance %s admin password %s", instance, new_pass)

    def inject_file(self, instance, b64_path, b64_contents):
        LOG.debug("inject_file")

    def change_instance_metadata(self, context, instance, diff):
        LOG.debug("change_instance_metadata")

    def inject_network_info(self, instance, nw_info):
        """inject network info for specified instance."""
        # TODO(Vek): Need to pass context in for access to auth_token
        LOG.debug("inject_network_info")

    def poll_rebooting_instances(self, timeout, instances):
        LOG.debug("poll_rebooting_instances")

    def host_power_action(self, host, action):
        LOG.debug("host_power_action")
        return action

    def host_maintenance_mode(self, host, mode):
        LOG.debug("host_maintenance_mode")
        if not mode:
            return 'off_maintenance'
        return 'on_maintenance'

    def set_host_enabled(self, host, enabled):
        LOG.debug("set_host_enabled")
        if enabled:
            return 'enabled'
        return 'disabled'

    def get_host_uptime(self, host):
        """Returns the result of calling "uptime" on the target host."""
        # TODO(Vek): Need to pass context in for access to auth_token
        LOG.debug("get_host_uptime")

    def plug_vifs(self, instance, network_info):
        LOG.debug("plug_vifs")
        # TODO: retrieve provider info ips/macs for vcloud
        for vif in network_info:
            self.hyper_agent_api.plug(instance.uuid, vif, None)

    def unplug_vifs(self, instance, network_info):
        LOG.debug("unplug_vifs")
        for vif in network_info:
            self.hyper_agent_api.unplug(instance.uuid, vif)

    def get_host_stats(self, refresh=False):
        LOG.debug("get_host_stats")

    def get_host_cpu_stats(self):
        LOG.debug("get_host_cpu_stats")

    def block_stats(self, instance_name, disk_id):
        LOG.debug("block_stats")

    def interface_stats(self, instance_name, iface_id):
        LOG.debug("interface_stats")

    def deallocate_networks_on_reschedule(self, instance):
        """Does the driver want networks deallocated on reschedule?"""
        return False

    def macs_for_instance(self, instance):
        return None

    def dhcp_options_for_instance(self, instance):
        LOG.debug("dhcp_options_for_instance")

    def manage_image_cache(self, context, all_instances):
        """Manage the driver's local image cache.

        Some drivers chose to cache images for instances on disk. This method
        is an opportunity to do management of that cache which isn't directly
        related to other calls into the driver. The prime example is to clean
        the cache and remove images which are no longer of interest.

        :param instances: nova.objects.instance.InstanceList
        """
        LOG.debug("manage_image_cache")

    def add_to_aggregate(self, context, aggregate, host, **kwargs):
        """Add a compute host to an aggregate."""
        # NOTE(jogo) Currently only used for XenAPI-Pool
        LOG.debug("add_to_aggregate")

    def remove_from_aggregate(self, context, aggregate, host, **kwargs):
        """Remove a compute host from an aggregate."""
        LOG.debug("remove_from_aggregate")

    def undo_aggregate_operation(self, context, op, aggregate,
                                  host, set_error=True):
        """Undo for Resource Pools."""
        LOG.debug("undo_aggregate_operation")

    def get_volume_connector(self, instance):
        LOG.debug("get_volume_connector")
        return {'ip': '127.0.0.1', 'initiator': 'fake', 'host': 'fakehost'}

    def get_available_nodes(self, refresh=False):
        return [self._node_name]

    def node_is_available(self, nodename):
        """Return whether this compute service manages a particular node."""
        if nodename in self.get_available_nodes():
            return True
        # Refresh and check again.
        return nodename in self.get_available_nodes(refresh=True)

    def get_per_instance_usage(self):
        """Get information about instance resource usage.

        :returns: dict of  nova uuid => dict of usage info
        """
        return {}

    def instance_on_disk(self, instance):
        LOG.debug("instance_on_disk")
        return False

    def register_event_listener(self, callback):
        """Register a callback to receive events.

        Register a callback to receive asynchronous event
        notifications from hypervisors. The callback will
        be invoked with a single parameter, which will be
        an instance of the nova.virt.event.Event class.
        """

        self._compute_event_callback = callback

    def emit_event(self, event):
        """Dispatches an event to the compute manager.

        Invokes the event callback registered by the
        compute manager to dispatch the event. This
        must only be invoked from a green thread.
        """

        if not self._compute_event_callback:
            LOG.debug("Discarding event %s", str(event))
            return

        if not isinstance(event, virtevent.Event):
            raise ValueError(
                _("Event must be an instance of nova.virt.event.Event"))

        try:
            LOG.debug("Emitting event %s", str(event))
            self._compute_event_callback(event)
        except Exception as ex:
            LOG.error(_("Exception dispatching event %(event)s: %(ex)s"),
                      {'event': event, 'ex': ex})

    def delete_instance_files(self, instance):
        """Delete any lingering instance files for an instance.

        :param instance: nova.objects.instance.Instance
        :returns: True if the instance was deleted from disk, False otherwise.
        """
        return True

    @property
    def need_legacy_block_device_info(self):
        return True

    def volume_snapshot_create(self, context, instance, volume_id,
                               create_info):
        LOG.debug("volume_snapshot_create")

    def volume_snapshot_delete(self, context, instance, volume_id,
                               snapshot_id, delete_info):

        LOG.debug("volume_snapshot_delete")

    def default_root_device_name(self, instance, image_meta, root_bdm):
        """Provide a default root device name for the driver."""
        LOG.debug("default_root_device_name")

    def default_device_names_for_instance(self, instance, root_device_name,
                                          *block_device_lists):
        """Default the missing device names in the block device mapping."""
        LOG.debug("default_device_names_for_instance")

    def is_supported_fs_format(self, fs_type):
        """Check whether the file format is supported by this driver

        :param fs_type: the file system type to be checked,
                        the validate values are defined at disk API module.
        """
        # NOTE(jichenjc): Return False here so that every hypervisor
        #                 need to define their supported file system
        #                 type and implement this function at their
        #                 virt layer.
        return False

    def _download_vmdk_from_vcloud(self, context, src_url, dst_file_name):

        # local_file_handle = open(dst_file_name, "wb")
        local_file_handle = fileutils.file_open(dst_file_name, "wb")

        remote_file_handle = urllib2.urlopen(src_url)
        file_size = remote_file_handle.headers['content-length']

        util.start_transfer(context, remote_file_handle, file_size,
                            write_file_handle=local_file_handle)

    def _get_vapp_ip(self, instance):
        instance_id = instance.uuid
        neutron_client = neutronv2.get_client(context=None, admin=True)
        agent = neutron_client.list_agents(host=instance_id)
        times=10
        while len(agent['agents']) == 0:
            if times==0:
                break
            eventlet.greenthread.sleep(10)
            agent = neutron_client.list_agents(host=instance_id)
            times = times - 1
        if times==0:
            return None
        else:
            return agent['agents'][0]['configurations']['tunneling_ip']

    @_retry_decorator(max_retry_count=60,exceptions = (errors.APIError,errors.NotFound))
    def get_vapp_ip(self, vapp_name):
        return self._vcloud_client.get_vapp_ip(vapp_name)

    def get_pci_slots_from_xml(self, instance):
        """    
        :param instance:
        :return:
        """
        return []

    def _instance_is_active(self, instance):
        """ 
        """ 

        vapp_name = self._get_vcloud_vapp_name(instance)
        the_vapp = self._vcloud_client._get_vcloud_vapp(vapp_name)
        vapp_status = self._vcloud_client._get_status_first_vm(the_vapp)

        expected_vapp_status = 4
        if vapp_status == expected_vapp_status:
            return True
        else:
            return False

    @_retry_decorator(max_retry_count=60,exceptions=(errors.APIError,errors.NotFound, errors.ConnectionError, errors.InternalError))
    def _wait_hybrid_service_up(self, server_ip, port = '7127'):
        client = Client(server_ip, port = port)
        return client.get_version()
