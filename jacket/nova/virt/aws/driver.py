__author__ = 'wangfeng'

import time
import os
import shutil
from functools import wraps
from oslo.config import cfg
from libcloud.compute.types import StorageVolumeState,NodeState
from libcloud.compute.base import NodeSize, NodeImage,NodeAuthSSHKey
from libcloud.storage.types import ObjectDoesNotExistError
import sshclient
import random
from nova import utils
from nova import exception as exception
from nova.i18n import _, _LW
from nova.openstack.common import jsonutils
from nova.openstack.common import imageutils
from nova.openstack.common import fileutils as fileutils
from nova.openstack.common import log as logging
from nova.compute import task_states
from nova.volume.cinder import API as cinder_api
from nova.image.api import API as glance_api
from nova.compute import power_state
from nova.virt import driver
from nova.network import neutronv2
from nova.context import RequestContext
from nova.compute import utils as compute_utils
from nova.image import glance

from wormholeclient.client import Client
from wormholeclient import errors
from wormholeclient import constants as wormhole_constants
import traceback
import adapter
import exception_ex
from nova.virt.aws import image_utils

from jacketstatuscache.awssynchronizer import HCAWSS
from jacketstatuscache.jacketcache import JacketStatusCache


hybrid_cloud_opts = [

    cfg.StrOpt('provide_cloud_type',
         default='aws',
         help='provider cloud type  ')
]

hypernode_api_opts = [
    cfg.StrOpt('my_ip', help='internal base ip of rabbit host, for injecting in to hyper_vm')
]

ec2_opts = [
    cfg.StrOpt('conversion_dir',
               default='/tmp',
               help='where conversion happens'),

    cfg.StrOpt('access_key_id',
               help='the access key id for connection to EC2  '),

    cfg.StrOpt('secret_key',
               help='the secret key  for connection to EC2  '),

    cfg.StrOpt('region',
               default='us-east-1',
               help='the region for connection to EC2  '),

    cfg.StrOpt('availability_zone',
               default='us-east-1a',
               help='the availability_zone for connection to EC2  '),

    cfg.StrOpt('base_linux_image',
               default='ami-68d8e93a',
               help='use for create a base ec2 instance'),

    cfg.StrOpt('storage_tmp_dir',
               default='wfbucketse',
               help='a cloud storage temp directory '),

    cfg.StrOpt('cascaded_node_id',
               help='az31 node id in provider cloud'),

    cfg.StrOpt('subnet_api',
               help='api subnet'),

    cfg.StrOpt('subnet_data',
               help='data subnet'),

    cfg.StrOpt('cgw_host_ip',
               help='compute gateway ip'),

    cfg.StrOpt('cgw_host_id',
               help='compute gateway id in provider cloud'),

    cfg.StrOpt('cgw_user_name',
               help='compute gateway user name'),

    cfg.StrOpt('cgw_certificate',
               help='full name of compute gateway public key'),

    cfg.StrOpt('security_group',
                help=''),

    cfg.StrOpt('rabbit_host_ip_public',
                help=''),
    
    cfg.StrOpt('rabbit_password_public',
               help=''),

    cfg.StrOpt('vpn_route_gateway',
               help=''),

    cfg.DictOpt('flavor_map',
                default={'m1.tiny': 't2.micro', 'm1.small': 't2.micro', 'm1.medium': 't2.micro3',
                         'm1.large': 't2.micro', 'm1.xlarge': 't2.micro'},
                help='map nova flavor name to aws ec2 instance specification id'),
    
    cfg.StrOpt('driver_type',
               default ='agent',
               help='the network soulution type of aws driver'),
            
    cfg.StrOpt('image_user',
               default='',
               help=''),

    cfg.StrOpt('image_password',
               default='',
               help=''),

    cfg.StrOpt('agent_network',
               default='False',
               help=''),

    cfg.StrOpt('iscsi_subnet',
               default='',
               help=''),

    cfg.StrOpt('iscsi_subnet_route_gateway',
               default='',
               help=''),

    cfg.StrOpt('iscsi_subnet_route_mask',
               default='',
               help=''),
    cfg.StrOpt('tunnel_cidr',
               help='The tunnel cidr of provider network.'),
    cfg.StrOpt('route_gw',
               help='The route gw of the provider network.'),
    cfg.StrOpt('dst_path',
               default='/home/neutron_agent_conf.txt',
               help='The config location for hybrid vm.'),
    cfg.StrOpt('hybrid_service_port',
               default='7127',
               help='The route gw of the provider network.'),
    cfg.StrOpt('base_ami_id', default='ami-a6d104c5')
    ]

instance_task_map={}


AWS_POWER_STATE={
    NodeState.RUNNING:power_state.RUNNING,
    NodeState.TERMINATED:power_state.CRASHED,
    NodeState.PENDING:power_state.NOSTATE,
    NodeState.UNKNOWN:power_state.NOSTATE,
    NodeState.STOPPED:power_state.SHUTDOWN,              
}

MAX_RETRY_COUNT=20

CONTAINER_FORMAT_HYBRID_VM = 'hybridvm'

class aws_task_states:
    IMPORTING_IMAGE = 'importing_image'
    CREATING_VOLUME = 'creating_volume'
    CREATING_VM = 'creating_vm'
    
MOUNTPOINT_LIST = []

LOG = logging.getLogger(__name__)

CONF = cfg.CONF
CONF.register_opts(hybrid_cloud_opts)
CONF.register_opts(ec2_opts, 'provider_opts')

CHUNK_SIZE = 1024*4

# EC2 = get_driver(CONF.ec2.driver_type)

class RetryDecorator(object):
    """Decorator for retrying a function upon suggested exceptions.

    The decorated function is retried for the given number of times, and the
    sleep time between the retries is incremented until max sleep time is
    reached. If the max retry count is set to -1, then the decorated function
    is invoked indefinitely until an exception is thrown, and the caught
    exception is not in the list of suggested exceptions.
    """

    def __init__(self, max_retry_count=-1, inc_sleep_time=5,
                 max_sleep_time=60, exceptions=()):
        """Configure the retry object using the input params.

        :param max_retry_count: maximum number of times the given function must
                                be retried when one of the input 'exceptions'
                                is caught. When set to -1, it will be retried
                                indefinitely until an exception is thrown
                                and the caught exception is not in param
                                exceptions.
        :param inc_sleep_time: incremental time in seconds for sleep time
                               between retries
        :param max_sleep_time: max sleep time in seconds beyond which the sleep
                               time will not be incremented using param
                               inc_sleep_time. On reaching this threshold,
                               max_sleep_time will be used as the sleep time.
        :param exceptions: suggested exceptions for which the function must be
                           retried
        """
        self._max_retry_count = max_retry_count
        self._inc_sleep_time = inc_sleep_time
        self._max_sleep_time = max_sleep_time
        self._exceptions = exceptions
        self._retry_count = 0
        self._sleep_time = 0

    def __call__(self, f):
            @wraps(f)
            def f_retry(*args, **kwargs):
                max_retries, mdelay = self._max_retry_count, self._inc_sleep_time
                while max_retries > 1:
                    try:
                        return f(*args, **kwargs)
                    except self._exceptions as e:
                        LOG.error('retry times: %s, exception: %s' %
                                  (str(self._max_retry_count - max_retries), traceback.format_exc(e)))
                        time.sleep(mdelay)
                        max_retries -= 1
                        if mdelay >= self._max_sleep_time:
                            mdelay=self._max_sleep_time
                if max_retries == 1:
                    msg = 'func: %s, retry times: %s, failed' % (f.__name__, str(self._max_retry_count))
                    LOG.error(msg)
                return f(*args, **kwargs)

            return f_retry  # true decorator


class AwsEc2Driver(driver.ComputeDriver):

    def __init__(self, virtapi):

        if CONF.provide_cloud_type == 'aws':
            if (CONF.provider_opts.access_key_id is None or
                    CONF.provider_opts.secret_key is None):
                raise Exception(_("Must specify access_key_id and "
                                  "secret_key to use aws ec2"))
            self.compute_adapter = adapter.Ec2Adapter(CONF.provider_opts.access_key_id,
                                                      secret=CONF.provider_opts.secret_key,
                                                      region=CONF.provider_opts.region, secure=False)
            self.storage_adapter = adapter.S3Adapter(CONF.provider_opts.access_key_id,
                                                     secret=CONF.provider_opts.secret_key,
                                                     region=CONF.provider_opts.region,
                                                     secure=False)
        self.location = CONF.provider_opts.availability_zone
        self.cinder_api = cinder_api()
        self.glance_api = glance_api()

        self.provider_security_group_id = None
        self.provider_interfaces = []
        self.base_ami_id = CONF.provider_opts.base_ami_id

        if CONF.provider_opts.driver_type == 'agent':
            self.provider_subnet_data = CONF.provider_opts.subnet_data
            self.provider_subnet_api = CONF.provider_opts.subnet_api

            # for agent solution by default
            self.provider_interfaces = []
            if CONF.provider_opts.subnet_data:
                provider_interface_data = adapter.NetworkInterface(name='eth_data',
                                                                   subnet_id=self.provider_subnet_data,
                                                                   # security_groups=self.provider_security_group,
                                                                   device_index=0)
                self.provider_interfaces.append(provider_interface_data)

            if CONF.provider_opts.subnet_api:
                provider_interface_api = adapter.NetworkInterface(name='eth_control',
                                                                  subnet_id=self.provider_subnet_api,
                                                                  # security_groups=self.provider_security_group,
                                                                  device_index=1)
                self.provider_interfaces.append(provider_interface_api)

        else:
            if not CONF.provider_opts.security_group:
                self.provider_security_group_id = None
            else:
                self.provider_security_group_id = CONF.provider_opts.security_group

        hcawss = HCAWSS(CONF.provider_opts.access_key_id,
                        secret=CONF.provider_opts.secret_key,
                        region=CONF.provider_opts.region, secure=False)
        self.cache = JacketStatusCache(hcawss)

    def _get_auth(self, key_data, key_name):
        return None

    def init_host(self, host):
        pass

    def list_instances(self):
        """List VM instances from all nodes."""

        instances = []
        try:
            nodes = self.compute_adapter.list_nodes()
        except Exception as e:
            LOG.error('list nodes failed')
            LOG.error(e.message)
            return instances
        if nodes is None:
            LOG.error('list nodes failed, Nodes are null!')
            return instances
        for node in nodes:
            if node.state != NodeState.TERMINATED:
                instance_uuid = node.extra.get('tags').get('hybrid_cloud_instance_id')
                instances.append(instance_uuid)

        return instances

    def volume_snapshot_create(self, context, instance, volume_id,
                               create_info):
        pass

    def snapshot(self, context, instance, image_id, update_task_state):
        LOG.debug('start to do snapshot')
        update_task_state(task_state=task_states.IMAGE_PENDING_UPLOAD)

        image_container_type = instance.system_metadata.get('image_container_format')
        LOG.debug('image container type: %s' % image_container_type)

        if image_container_type == CONTAINER_FORMAT_HYBRID_VM:
            self._do_snapshot_for_hybrid_vm(context, instance, image_id, update_task_state)
        else:
            self._do_snapshot_2(context, instance, image_id, update_task_state)

    def _do_snapshot_for_hybrid_vm(self, context, instance, image_id, update_task_state):
        image_object_of_hybrid_cloud = self.glance_api.get(context, image_id)
        LOG.debug('get image object: %s' % image_object_of_hybrid_cloud)
        clients = self._get_hybrid_service_clients_by_instance(instance)
        LOG.debug('get clients: %s' % clients)

        # create image in docker repository
        create_image_task = self._clients_create_image_task(clients, image_object_of_hybrid_cloud)
        self._wait_for_task_finish(clients, create_image_task)
        LOG.debug('create image in docker image repository success')

        docker_image_info = self._clients_get_image_info(clients, image_object_of_hybrid_cloud)
        size = docker_image_info['size']
        LOG.debug('docker image size: %s' % size)
        image_object_of_hybrid_cloud['size'] = size
        LOG.debug('image with size: %s' % image_object_of_hybrid_cloud)

        update_task_state(task_state=task_states.IMAGE_UPLOADING,
                     expected_state=task_states.IMAGE_PENDING_UPLOAD)

        self._put_image_info_to_glance(context, image_object_of_hybrid_cloud, update_task_state, instance)

        # TODO, here get the base AMI id from config file, later will replace by image-manager module.
        provider_image_of_base_vm = self._get_provider_image_by_provider_id(self.base_ami_id)
        self._set_tag_for_provider_image(provider_image_of_base_vm, image_id)
        LOG.debug('finish do snapshot for create image')

    def _put_image_info_to_glance(self, context, image_object, update_task_state, instance):
        LOG.debug('start to put image info to glance, image obj: %s' % image_object)

        image_id = image_object['id']
        LOG.debug('image id: %s' % image_id)
        image_metadata = self._create_image_metadata(context, instance, image_object)
        LOG.debug('image metadata: %s' % image_metadata)

        # self.glance_api.update(context, image_id, image_metadata)
        with image_utils.temporary_file() as tmp:
            image_service, image_id = glance.get_remote_image_service(context, image_id)
            with fileutils.file_open(tmp, 'wb+') as f:
                f.truncate(image_object['size'])
                image_service.update(context, image_id, image_metadata, f)
                self._update_vm_task_state(instance, task_state=instance.task_state)

        LOG.debug('success to put image to glance')

    def _create_image_metadata(self, context, instance, image_object):

        base_image_ref = instance['image_ref']
        base = compute_utils.get_image_metadata(context, self.glance_api, base_image_ref, instance)

        metadata = {'is_public': False,
                    'status': 'active',
                    'name': image_object['name'],
                    'properties': {
                                   'kernel_id': instance['kernel_id'],
                                   'image_location': 'snapshot',
                                   'image_state': 'available',
                                   'owner_id': instance['project_id'],
                                   'ramdisk_id': instance['ramdisk_id'],
                                   'provider_image_id': 'ami-a6d104c5'
                                   }
                    }
        if instance['os_type']:
            metadata['properties']['os_type'] = instance['os_type']

        # NOTE(vish): glance forces ami disk format to be ami
        if base.get('disk_format') == 'ami':
            metadata['disk_format'] = 'ami'
        else:
            metadata['disk_format'] = image_object['disk_format']

        metadata['container_format'] = CONTAINER_FORMAT_HYBRID_VM
        metadata['size'] = image_object['size']

        return metadata

    def _do_snapshot_1(self, context, instance, image_id, update_task_state):

        # 1) get  provider node
        provider_node_id = self._get_provider_node_id(instance)
        provider_nodes = self.compute_adapter.list_nodes(ex_node_ids=[provider_node_id])
        if not provider_nodes:
            LOG.error('instance %s is not found' % instance.uuid)
            raise exception.InstanceNotFound(instance_id=instance.uuid)
        if len(provider_nodes)>1:
            LOG.error('instance %s are more than one' % instance.uuid)
            raise exception_ex.MultiInstanceConfusion
        provider_node = provider_nodes[0]

        # 2) get root-volume id
        provider_volumes = self.compute_adapter.list_volumes(node=provider_node)
        if not provider_volumes:
            raise exception.VolumeNotFound

        provider_volume = provider_volumes[0]

        # 3) export
        self.compute_adapter.export_volume(provider_volume.id,
                                           CONF.provider_opts.conversion_dir,
                                           image_id,
                                           cgw_host_id=CONF.provider_opts.cgw_host_id,
                                           cgw_host_ip=CONF.provider_opts.cgw_host_ip,
                                           cgw_username=CONF.provider_opts.cgw_username,
                                           cgw_certificate=CONF.provider_opts.cgw_certificate,
                                           transfer_station=CONF.provider_opts.storage_tmp_dir)

        # 4) upload to glance
        src_file_name = '%s/%s' %(CONF.provider_opts.conversion_dir, image_id)
        file_size = os.path.getsize(src_file_name)
        metadata = self.glance_api.get(context, image_id)
        image_metadata = {"disk_format": "qcow2",
                          "is_public": "false",
                          "name": metadata['name'],
                          "status": "active",
                          "container_format": "bare",
                          "size": file_size,
                          "properties": {"owner_id": instance['project_id']}}

        src_file_handle = fileutils.file_open(src_file_name, "rb")
        self.glance_api.create(context,image_metadata,src_file_handle)
        src_file_handle.close()


    def _do_snapshot_2(self, context, instance, image_id, update_task_state):
        # a) get  provider node id
        provider_node_id = self._get_provider_node_id(instance)
        provider_nodes = self.compute_adapter.list_nodes(ex_node_ids=[provider_node_id])
        if not provider_nodes:
            LOG.error('instance %s is not found' % instance.uuid)
            raise exception.InstanceNotFound(instance_id=instance.uuid)
        if len(provider_nodes)>1:
            LOG.error('instance %s are more than one' % instance.uuid)
            raise exception_ex.MultiInstanceConfusion
        provider_node = provider_nodes[0]

        # b) export-instance to s3
        # self.compute_adapter.ex_stop_node(provider_node)
        try:
            task = self.compute_adapter.create_export_instance_task(provider_node_id,
                                                                    CONF.provider_opts.storage_tmp_dir)
        except:
            task = self.compute_adapter.create_export_instance_task(provider_node_id,
                                                                    CONF.provider_opts.storage_tmp_dir)
        while not task.is_completed():
            time.sleep(10)
            task = self.compute_adapter.get_task_info(task)

        obj_key = task.export_to_s3_info.s3_key
        obj_bucket = task.export_to_s3_info.s3_bucket

        # c) download from s3
        obj = self.storage_adapter.get_object(obj_bucket,obj_key)
        conv_dir = '%s/%s' % (CONF.provider_opts.conversion_dir,image_id)
        fileutils.ensure_tree(conv_dir)
        org_full_name = '%s/%s.vmdk' % (conv_dir,image_id)

        self.storage_adapter.download_object(obj,org_full_name)

        # d) convert to qcow2
        dest_full_name = '%s/%s.qcow2' % (conv_dir,image_id)
        convert_image(org_full_name,
                     dest_full_name,
                      'qcow2')

        # upload to glance
        update_task_state(task_state=task_states.IMAGE_UPLOADING,
                          expected_state=task_states.IMAGE_PENDING_UPLOAD)

        file_size = os.path.getsize(dest_full_name)
        metadata = self.glance_api.get(context, image_id)
        image_metadata = {"disk_format": "qcow2",
                          "is_public": "false",
                          "name": metadata['name'],
                          "status": "active",
                          "container_format": "bare",
                          "size": file_size,
                          "properties": {"owner_id": instance['project_id']}}

        src_file_handle = fileutils.file_open(dest_full_name, "rb")
        self.glance_api.create(context,image_metadata,src_file_handle)
        src_file_handle.close()


    def _generate_provider_node_name(self, instance):
        return instance.hostname

    def _get_provider_node_size(self, flavor):
        return NodeSize(id=CONF.provider_opts.flavor_map[flavor.name],
                        name=None, ram=None, disk=None, bandwidth=None,price=None, driver=self.compute_adapter)


    def _get_image_id_from_meta(self, image_meta):
        if 'id' in image_meta:
            # create from image
            return image_meta['id']
        elif 'image_id' in image_meta:
            # attach
            return image_meta['image_id']
        elif 'properties' in image_meta:
            # create from volume
            return image_meta['properties']['image_id']
        else:
            return None

    def _get_image_name_from_meta(self, image_meta):
        if 'name' in image_meta:
            return image_meta['name']
        elif 'image_name' in image_meta:
            return image_meta['image_name']
        else:
            return NodeState

    def _spawn_from_image(self, context, instance, image_meta, injected_files,
                                    admin_password, network_info, block_device_info):
        # 0.get provider_image,
        LOG.info('begin time of _spawn_from_image is %s' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        retry_time = 3
        container_format = image_meta.get('container_format')
        provider_image_id = None
        provider_image = None
        while (not provider_image) and retry_time>0:
            provider_image = self._get_provider_image(image_meta)
            retry_time = retry_time-1
        if provider_image is None:
            image_uuid = self._get_image_id_from_meta(image_meta)
            LOG.error('Get image %s error at provider cloud' % image_uuid)

        # 1. if provider_image do not exist,, import image first
        vm_task_state = instance.task_state
        try:
            if not provider_image:
                LOG.debug('begin import image')
                #save the original state

                self._update_vm_task_state(
                    instance,
                    task_state=aws_task_states.IMPORTING_IMAGE)
                image_uuid = self._get_image_id_from_meta(image_meta)
                container = self.storage_adapter.get_container(CONF.provider_opts.storage_tmp_dir)

                try:
                    self.storage_adapter.get_object(container.name, image_uuid)
                except ObjectDoesNotExistError:
                    # 1.1 download qcow2 file from glance


                    this_conversion_dir = '%s/%s' % (CONF.provider_opts.conversion_dir,image_uuid)
                    orig_file_full_name = '%s/%s.qcow2' % (this_conversion_dir,'orig_file')
                    fileutils.ensure_tree(this_conversion_dir)
                    self.glance_api.download(context, image_uuid, dest_path=orig_file_full_name)

                    # 1.2 convert to provider image format
                    converted_file_format = 'vmdk'
                    converted_file_name = '%s.%s' % ('converted_file', converted_file_format)
                    converted_file_full_name =  '%s/%s' % (this_conversion_dir,converted_file_name)

                    convert_image(orig_file_full_name,
                                  converted_file_full_name,
                                  converted_file_format,
                                  subformat='streamoptimized')

                # 1.3 upload to provider_image_id

                    object_name = image_uuid
                    extra = {'content_type': 'text/plain'}

                    with open(converted_file_full_name,'rb') as f:
                        obj = self.storage_adapter.upload_object_via_stream(container=container,
                                                                   object_name=object_name,
                                                                   iterator=f,
                                                                   extra=extra)

                task = self.compute_adapter.create_import_image_task(CONF.provider_opts.storage_tmp_dir,
                                                             image_uuid,
                                                             image_name=image_uuid)
                try:
                    task_list = instance_task_map[instance.uuid]
                    if not task_list:
                        task_list.append(task)
                        instance_task_map[instance.uuid]=task_list
                except KeyError:
                    task_list=[task]
                    instance_task_map[instance.uuid]=task_list

                while not task.is_completed():
                    time.sleep(5)
                    task = self.compute_adapter.get_task_info(task)

                provider_image = self.compute_adapter.get_image(task.image_id)
                set_tag_func = getattr(self.compute_adapter, 'ex_create_tags')
                if set_tag_func:
                    set_tag_func(provider_image, {'hybrid_cloud_image_id': image_uuid})
        except Exception, e:
            LOG.error('Error when import image, exception: %s' % traceback.format_exc(e))
            raise e

        # 2.1 map flovar to node size, from configuration
        provider_size = self._get_provider_node_size(instance.get_flavor())

        # 2.2 get a subnets and create network interfaces
        
        # provider_interface_data = adapter.NetworkInterface(name='eth_data',
        #                                                    subnet_id=CONF.provider_opts.subnet_data,
        #                                                    device_index=0)
        #
        # provider_interface_api = adapter.NetworkInterface(name='eth_control',
        #                                                    subnet_id=CONF.provider_opts.subnet_api,
        #                                                    device_index=1)
        # provider_interfaces = [provider_interface_data,provider_interface_api]

        # 2.3 generate provider node name, which useful for debugging
        provider_node_name = self._generate_provider_node_name(instance)

        # 2.4 generate user data, which use for network initialization
        user_data = self._generate_user_data(instance)

        # 2.5 create data volumes' block device mappings, skip boot volume
        provider_bdms = None
        data_bdm_list = []
        source_provider_volumes=[]
        bdm_list = block_device_info.get('block_device_mapping',[])
        if len(bdm_list)>0:
            
            self._update_vm_task_state(
                instance,
                task_state=aws_task_states.CREATING_VOLUME)
            
            root_volume_name = block_device_info.get('root_device_name',None)

            # if data volume exist: more than one block device mapping
            # 2.5.1 import volume to aws
            provider_volume_ids = []
           
            for bdm in bdm_list:
                # skip boot volume
                if bdm.get('mount_device') == root_volume_name:
                    continue
                data_bdm_list.append(bdm)

                if container_format != CONTAINER_FORMAT_HYBRID_VM:
                    connection_info = bdm.get('connection_info', None)
                    volume_id = connection_info['data']['volume_id']

                    provider_volume_id = self._get_provider_volume_id(context,volume_id)
                    # only if volume DO NOT exist in aws when import volume
                    if not provider_volume_id:
                        provider_volume_id = self._import_volume_from_glance(
                            context,
                            volume_id,
                            instance,
                            CONF.provider_opts.availability_zone)

                    provider_volume_ids.append(provider_volume_id)

            # 2.5.2 create snapshot
            # if container format is hybridvm, then need to attach volume after create node one by one
            if container_format != CONTAINER_FORMAT_HYBRID_VM:
                provider_snapshots = []

                if len(provider_volume_ids) > 0:
                    source_provider_volumes = self.compute_adapter.list_volumes(ex_volume_ids=provider_volume_ids)
                    for provider_volume in source_provider_volumes:
                        provider_snapshots.append(self.compute_adapter.create_volume_snapshot(provider_volume))

                provider_snapshot_ids = []
                for snap in provider_snapshots:
                    provider_snapshot_ids.append(snap.id)

                self._wait_for_snapshot_completed(provider_snapshot_ids)

                # 2.5.3 create provider bdm list from bdm_info and snapshot
                provider_bdms = []
                if len(provider_snapshots) > 0:
                    for ii in range(0, len(data_bdm_list)):
                        provider_bdm = {'DeviceName':
                                            self._trans_device_name(data_bdm_list[ii].get('mount_device')),
                                        'Ebs': {'SnapshotId':provider_snapshots[ii].id,
                                                'DeleteOnTermination': data_bdm_list[ii].get('delete_on_termination')}
                                        }
                        provider_bdms.append(provider_bdm)

        # 3. create node
        try:
            
            self._update_vm_task_state(
                instance,
                task_state=aws_task_states.CREATING_VM)

            if (len(self.provider_interfaces)>1):
                provider_node = self.compute_adapter.create_node(name=provider_node_name,
                                                                 image=provider_image,
                                                                 size=provider_size,
                                                                 location=CONF.provider_opts.availability_zone,
                                                                 # ex_subnet=provider_subnet_data,
                                                                 ex_blockdevicemappings=provider_bdms,
                                                                 ex_network_interfaces=self.provider_interfaces,
                                                                 ex_userdata=user_data,
                                                                 auth=self._get_auth(instance._key_data,
                                                                                     instance._key_name))
            elif(len(self.provider_interfaces)==1):

                provider_subnet_data_id = self.provider_interfaces[0].subnet_id
                provider_subnet_data = self.compute_adapter.ex_list_subnets(subnet_ids=[provider_subnet_data_id])[0]


                provider_node = self.compute_adapter.create_node(name=provider_node_name,
                                                                 image=provider_image,
                                                                 size=provider_size,
                                                                 location=CONF.provider_opts.availability_zone,
                                                                 ex_subnet=provider_subnet_data,
                                                                 ex_security_group_ids=self.provider_security_group_id,
                                                                 ex_blockdevicemappings=provider_bdms,
                                                                 # ex_network_interfaces=self.provider_interfaces,
                                                                 ex_userdata=user_data,
                                                                 auth=self._get_auth(instance._key_data,
                                                                                     instance._key_name))
            else:
                provider_node = self.compute_adapter.create_node(name=provider_node_name,
                                                                 image=provider_image,
                                                                 size=provider_size,
                                                                 location=CONF.provider_opts.availability_zone,
                                                                 # ex_subnet=provider_subnet_data,
                                                                 ex_security_group_ids=self.provider_security_group_id,
                                                                 ex_blockdevicemappings=provider_bdms,
                                                                 # ex_network_interfaces=self.provider_interfaces,
                                                                 ex_userdata=user_data,
                                                                 auth=self._get_auth(instance._key_data,
                                                                                     instance._key_name))

        except Exception as e:
            LOG.warning('Provider instance is booting error')
            LOG.error(e.message)
            provider_node = self.compute_adapter.list_nodes(ex_filters={'tag:name':provider_node_name})
            if not provider_node:
                raise e
            
        # 4. mapping instance id to provider node, using metadata
        instance.metadata['provider_node_id'] = provider_node.id
        instance.save()
        set_tag_func = getattr(self.compute_adapter, 'ex_create_tags')
        try:
            if set_tag_func:
                set_tag_func(provider_node, {'hybrid_cloud_instance_id': instance.uuid})
        except Exception as e:
            time.sleep(5)
            aws_node=self.compute_adapter.list_nodes(ex_filters={'tag:hybrid_cloud_instance_id':instance.uuid})
            if not aws_node:
                set_tag_func(provider_node, {'hybrid_cloud_instance_id': instance.uuid})
                
        # 5 wait for node avalaible
        while provider_node.state!=NodeState.RUNNING and provider_node.state!=NodeState.STOPPED:
            try:
                #modified by liuling
                #provider_node = self.compute_adapter.list_nodes(ex_node_ids=[provider_node.id])[0]
                provider_nodes = self.compute_adapter.list_nodes(ex_node_ids=[provider_node.id])
                if len(provider_nodes) ==0:
                    break
                else:
                    provider_node = provider_nodes[0]
            except:
                LOG.warning('Provider instance is booting but adapter is failed to get status. Try it later')
            time.sleep(10)

        if container_format == CONTAINER_FORMAT_HYBRID_VM:
            self._create_hyper_service_container(context,
                                             instance,
                                             provider_node,
                                             network_info,
                                             block_device_info,
                                             image_meta,
                                             injected_files,
                                             admin_password,image_meta)
        else:
            # 6 mapp data volume id to provider
            provider_bdm_list = provider_node.extra.get('block_device_mapping')
            for ii in range(0, len(data_bdm_list)):
                provider_volume_id = provider_bdm_list[ii+1].get('ebs').get('volume_id')
                provider_volumes = self.compute_adapter.list_volumes(ex_volume_ids=[provider_volume_id])

                connection_info = data_bdm_list[ii].get('connection_info',[])
                volume_id = connection_info['data']['volume_id']

                self._map_volume_to_provider(context, volume_id, provider_volumes[0])

            # delete the  tmp volume
            for provider_volume in source_provider_volumes:
                self.compute_adapter.destroy_volume(provider_volume)
          
        #reset the original state
        self._update_vm_task_state(
                instance,
                task_state=vm_task_state)

        LOG.info('end time of _spawn_from_image is %s' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        return provider_node

    def _wait_for_volume_is_attached(self, provider_hybrid_volume):
        LOG.debug('wait for volume is attached')
        not_in_status = [StorageVolumeState.ERROR, StorageVolumeState.DELETED, StorageVolumeState.DELETING]
        status = self._wait_for_volume_in_specified_status(provider_hybrid_volume, StorageVolumeState.INUSE,
                                                           not_in_status)
        LOG.debug('volume status: %s' % status)
        LOG.debug('volume is attached.')
        return

    def _wait_for_volume_is_available(self, provider_hybrid_volume):
        LOG.debug('wait for volume is available')
        not_in_status = [StorageVolumeState.ERROR, StorageVolumeState.DELETED, StorageVolumeState.DELETING]
        # import pdb; pdb.set_trace()
        status = self._wait_for_volume_in_specified_status(provider_hybrid_volume, StorageVolumeState.AVAILABLE,
                                                           not_in_status)
        LOG.debug('volume status: %s' % status)
        LOG.debug('volume is available')
        return status

    @RetryDecorator(max_retry_count=10,inc_sleep_time=5,max_sleep_time=60,exceptions=(exception_ex.RetryException))
    def _wait_for_volume_in_specified_status(self, provider_hybrid_volume, status, not_in_status_list):
        """

        :param provider_hybrid_volume:
        :param status: StorageVolumeState
        :return: specified_status
        """
        LOG.debug('wait for volume in specified status: %s' % status)
        LOG.debug('not_in_status_list: %s' % not_in_status_list)
        provider_volume_id = provider_hybrid_volume.id
        LOG.debug('wait for volume:%s in specified status: %s' % (provider_volume_id, status))
        created_volumes = self.compute_adapter.list_volumes(ex_volume_ids=[provider_volume_id])

        if not created_volumes:
            error_info = 'created docker app volume failed.'
            raise exception_ex.RetryException(error_info=error_info)

        created_volume = created_volumes[0]
        current_status = created_volume.state
        LOG.debug('current_status: %s' % current_status)
        error_info = 'volume: %s status is %s' % (provider_hybrid_volume.id, current_status)

        if status == current_status:
            LOG.debug('current status: %s is the same with specified status %s ' % (current_status, status))
        elif not_in_status_list:
            if status in not_in_status_list:
                raise Exception(error_info)
            else:
                raise exception_ex.RetryException(error_info=error_info)
        else:
            raise exception_ex.RetryException(error_info=error_info)

        return current_status

    def _get_provider_volumes_map_from_bdm(self, context, instance, block_device_info):
        """
        if there isn't any provider volume tag with hybrid cloud volume id, then import it from image of glance.
        if there is provider volume mapped with hybrid cloud volume id, return it directly.
        
        
        
        :param context:
        :param instance:
        :param block_device_info:
        {
            'block_device_mapping': [{
                'guest_format': None,
                'boot_index': None,
                'mount_device': u'/dev/sdb',
                'connection_info': {
                    u'driver_volume_type': u'provider_volume',
                    'serial': u'8ff7107a-74b9-4acb-8fab-46d8901f5bf2',
                    u'data': {
                        u'access_mode': u'rw',
                        u'qos_specs': None,
                        u'provider_location': u'vol-e4005a3e',
                        u'volume_id': u'8ff7107a-74b9-4acb-8fab-46d8901f5bf2'
                    }
                },
                'disk_bus': None,
                'device_type': None,
                'delete_on_termination': False
            }],
            'root_device_name': u'/dev/sda',
            'ephemerals': [],
            'swap': None
        }

        :return: dict, {hybrid_volume_id: provider_volume, ...}
        """
        LOG.debug('start to get provider volumes map.')
        provider_volume_map = {}
        bdm_map = {}
        bdm_list = block_device_info.get('block_device_mapping')

        if bdm_list and len(bdm_list) > 0:
            root_volume_name = block_device_info.get('root_device_name', None)
            LOG.debug('root_volume_name: %s' % root_volume_name)
            for bdm in bdm_list:
                # skip boot volume
                if bdm.get('mount_device') == root_volume_name:
                    continue
                else:
                    connection_info = bdm.get('connection_info', None)
                    volume_id = connection_info['data']['volume_id']

                    provider_volume = self._get_provider_volume(volume_id)
                    # only if volume DO NOT exist in aws when import volume
                    if not provider_volume:
                        LOG.debug('provider volume is not exist for volume: %s' % volume_id)
                        provider_volume_id = self._import_volume_from_glance(
                            context,
                            volume_id,
                            instance,
                            CONF.provider_opts.availability_zone)
                        created_provider_volume = self._get_provider_volume_by_provider_volume_id(provider_volume_id)
                        self._map_volume_to_provider(context, volume_id, created_provider_volume)
                        provider_volume = self._get_provider_volume(volume_id)

                    if provider_volume:
                        provider_volume_map[volume_id] = provider_volume
                        bdm_map[volume_id] = bdm

        LOG.debug('end to get provider volumes map.')

        return provider_volume_map, bdm_map

    def _deal_with_spawn_docker_app_failed(self, error_info, volume=None):
        LOG.error(error_info)
        if volume:
            self._delete_volume(volume)
        raise exception.NovaException(error_info)

    def _delete_volume(self, volume):
        """

        :param volume:
        :return: boolean
        """
        LOG.debug('start to delete container volume')
        destroy_result = self.compute_adapter.destroy_volume(volume)
        LOG.debug('end to delete container volume')
        return destroy_result

    def _trans_device_name(self, orig_name):
        if not orig_name:
            return orig_name
        else:
            return orig_name.replace('/dev/vd', '/dev/sd')

    def _wait_for_snapshot_completed(self, provider_id_list):

        is_all_completed = False

        while not is_all_completed:
            snapshot_list = self.compute_adapter.list_snapshots(snapshot_ids=provider_id_list)
            is_all_completed = True
            for snapshot in snapshot_list:
                if snapshot.extra.get('state') != 'completed':
                    is_all_completed = False
                    time.sleep(10)
                    break

    def _get_snapshot_by_tag_image_id(self, image_id):
        LOG.debug('start to get snapshot by tag image_id: %s' % image_id)
        provider_snapshot = None

        snapshots = self.compute_adapter.list_snapshots(ex_filters={'tag:hybrid_image_id':image_id})
        if len(snapshots) >= 1:
            provider_snapshot = snapshots[0]

        LOG.debug('end to get snapshot: %s' % provider_snapshot)
        return provider_snapshot

    def _create_data_volume_for_container(self, size, name, location, image_id):
        snapshot = self._get_snapshot_by_tag_image_id(image_id)
        if not snapshot:
            LOG.debug('create blank volume')
            volume = self.compute_adapter.create_volume(size, name, location=location)
        else:
            LOG.debug('create data volume by snapshot: %s' % snapshot)
            volume = self.compute_adapter.create_volume(size, name, location=location, snapshot=snapshot)

        LOG.debug('end to create data volume: %s' % volume)
        return volume


    def _generate_user_data(self, instance):
        return 'RABBIT_HOST_IP=%s;RABBIT_PASSWORD=%s;VPN_ROUTE_GATEWAY=%s' % (CONF.provider_opts.rabbit_host_ip_public,
                                                          CONF.provider_opts.rabbit_password_public,
                                                          CONF.provider_opts.vpn_route_gateway)

    def _spawn_from_volume(self, context, instance, image_meta, injected_files,
                                        admin_password, network_info, block_device_info):
        self._create_node_ec2(context, instance, image_meta, injected_files,
                              admin_password, network_info, block_device_info)

    def _spawn_from_volume_for_hybrid_vm(self, context, instance, image_meta, injected_files,
                                        admin_password, network_info, block_device_info):
        try:
            self._create_hypervm_from_volume(context, instance, image_meta, injected_files,
                                        admin_password, network_info, block_device_info)
        except Exception, e:
            LOG.error('spawn from volume failed!!,exception: %s' % traceback.format_exc(e))
            time.sleep(5)
            raise e

    def _get_root_bdm_from_bdms(self, bdms, root_device_name):
        root_bdm = None
        for bdm in bdms:
            if bdm['mount_device'] == root_device_name:
                root_bdm = bdm
                break
        return root_bdm

    def _get_volume_from_bdm(self, context, bdm):
        volume_id = bdm['connection_info']['data']['volume_id']
        volume = self.cinder_api.get(context, volume_id)
        if not volume:
            raise Exception('can not find volume for volume id: %s' % volume_id)

        return volume

    def _get_image_metadata_from_volume(self, volume):
        volume_image_metadata = volume.get('volume_image_metadata')
        return volume_image_metadata

    def _get_image_metadata_from_bdm(self, context, bdm):
        volume = self._get_volume_from_bdm(context, bdm)
        image_metadata = self._get_image_metadata_from_volume(volume)

        return image_metadata

    @RetryDecorator(max_retry_count=10, inc_sleep_time=5, max_sleep_time=60, exceptions=(Exception))
    def _set_tag_for_provider_instance(self, instance, provider_node):
        LOG.debug('start to set tag')

        aws_node = self.compute_adapter.list_nodes(ex_filters={'tag:hybrid_cloud_instance_id': instance.uuid})
        if aws_node:
            LOG.debug('Already exist tag for provider_node: %s' % provider_node)
            return
        else:
            set_tag_func = getattr(self.compute_adapter, 'ex_create_tags')
            LOG.debug('get function of set tag')
            if set_tag_func:
                    set_tag_func(provider_node, {'hybrid_cloud_instance_id': instance.uuid})
            else:
                aws_node = self.compute_adapter.list_nodes(ex_filters={'tag:hybrid_cloud_instance_id': instance.uuid})
                if not aws_node:
                    raise Exception('There is no node taged.')
            LOG.debug('end to set tag')

    @RetryDecorator(max_retry_count=10, inc_sleep_time=5, max_sleep_time=60, exceptions=(Exception))
    def _set_tag_for_provider_volume(self, provider_volume, volume_id):
        set_tag_func = getattr(self.compute_adapter, 'ex_create_tags')
        if set_tag_func:
            set_tag_func(provider_volume, {'hybrid_cloud_volume_id': volume_id})
        else:
            LOG.warning('No ex_create_tags function, '
                        'so did not set tag for provider_volume: %s with hybrid cloud volume id: %s') %\
            (provider_volume, volume_id)

    @RetryDecorator(max_retry_count=10, inc_sleep_time=5, max_sleep_time=60, exceptions=(Exception))
    def _set_tag_for_provider_image(self, provider_image, hybrid_cloud_image_id):
        LOG.debug('start to set tag for provider image')
        set_tag_func = getattr(self.compute_adapter, 'ex_create_tags')
        if set_tag_func:
            set_tag_func(provider_image, {'hybrid_cloud_image_id': hybrid_cloud_image_id})
        else:
            LOG.warning('No ex_create_tags function, '
                        'so did not set tag for provider_image: %s with hybrid cloud image id: %s') %\
            (provider_image, hybrid_cloud_image_id)
        LOG.debug('end to set tag for provider image, tag name is: %s, value is: %s' %
                  ('hybrid_cloud_image_id', hybrid_cloud_image_id))

    def _create_node(self, instance, provider_node_name, provider_image, provider_size, provider_bdms, user_data):
        try:

            self._update_vm_task_state(
                instance,
                task_state=aws_task_states.CREATING_VM)
            LOG.info('provider_interfaces: %s' % self.provider_interfaces)
            if len(self.provider_interfaces) > 1:
                LOG.debug('Create provider node, length: %s' % len(self.provider_interfaces))
                provider_node = self.compute_adapter.create_node(name=provider_node_name,
                                                                 image=provider_image,
                                                                 size=provider_size,
                                                                 location=CONF.provider_opts.availability_zone,
                                                                 # ex_subnet=provider_subnet_data,
                                                                 ex_blockdevicemappings=provider_bdms,
                                                                 ex_network_interfaces=self.provider_interfaces,
                                                                 ex_userdata=user_data,
                                                                 auth=self._get_auth(instance._key_data,
                                                                                     instance._key_name))
            elif len(self.provider_interfaces) == 1:
                LOG.debug('Create provider node, length: %s' % len(self.provider_interfaces))
                provider_subnet_data_id = self.provider_interfaces[0].subnet_id
                provider_subnet_data = self.compute_adapter.ex_list_subnets(subnet_ids=[provider_subnet_data_id])[0]
                provider_node = self.compute_adapter.create_node(name=provider_node_name,
                                                                 image=provider_image,
                                                                 size=provider_size,
                                                                 location=CONF.provider_opts.availability_zone,
                                                                 ex_subnet=provider_subnet_data,
                                                                 ex_security_group_ids=self.provider_security_group_id,
                                                                 ex_blockdevicemappings=provider_bdms,
                                                                 # ex_network_interfaces=self.provider_interfaces,
                                                                 ex_userdata=user_data,
                                                                 auth=self._get_auth(instance._key_data,
                                                                                     instance._key_name))
            else:
                LOG.debug('Create provider node, length: %s' % len(self.provider_interfaces))
                provider_node = self.compute_adapter.create_node(name=provider_node_name,
                                                                 image=provider_image,
                                                                 size=provider_size,
                                                                 location=CONF.provider_opts.availability_zone,
                                                                 # ex_subnet=provider_subnet_data,
                                                                 ex_security_group_ids=self.provider_security_group_id,
                                                                 ex_blockdevicemappings=provider_bdms,
                                                                 # ex_network_interfaces=self.provider_interfaces,
                                                                 ex_userdata=user_data,
                                                                 auth=self._get_auth(instance._key_data,
                                                                                     instance._key_name))

        except Exception as e:
            LOG.error('Provider instance is booting error')
            LOG.error(e.message)
            provider_node = self.compute_adapter.list_nodes(ex_filters={'tag:name':provider_node_name})
            if not provider_node:
                raise e
            raise e
        LOG.debug('create node success, provider_node: %s' % provider_node)
        #mapping instance id to provider node, using metadata
        instance.metadata['provider_node_id'] = provider_node.id
        instance.save()

        self._set_tag_for_provider_instance(instance, provider_node)

        node_is_ok = False
        while not node_is_ok:
            provider_nodes = self.compute_adapter.list_nodes(ex_node_ids=[provider_node.id])
            if not provider_nodes:
                error_info = 'There is no node created in provider. node id: %s' % provider_node.id
                LOG.error(error_info)
                time.sleep(10)
                continue
            else:
                provider_node = provider_nodes[0]
                if provider_node.state == NodeState.RUNNING or provider_node.state == NodeState.STOPPED:
                    LOG.debug('Node %s is created, and status is: %s' % (provider_node.name, provider_node.state))
                    node_is_ok = True
            time.sleep(10)

        return provider_node

    def _create_hypervm_from_image(self, context, instance, image_meta, injected_files,
                                        admin_password, network_info, block_device_info):
        LOG.debug('start to create hypervm_from_image')
        LOG.debug('image meta is: %s' % image_meta)
        vm_task_state = instance.task_state
        provider_image = None
        retry_time = 3
        while (not provider_image) and retry_time > 0:
            # if can found the taged provider image, then use it directly.
            provider_image = self._get_provider_image(image_meta)

            # if not found taged provider image, use base ami in stead.
            if not provider_image:
                provider_image = self._get_provider_image_by_provider_id(self.base_ami_id)

            retry_time -= 1
            time.sleep(1)
        if provider_image is None:
            image_uuid = self._get_image_id_from_meta(image_meta)
            LOG.error('Get image %s error at provider cloud' % image_uuid)
            raise Exception('Get image %s error at provider cloud' % image_uuid)
            #return

        LOG.debug('provider_image: %s' % provider_image)
        provider_size = self._get_provider_node_size(instance.get_flavor())
        LOG.debug('privoder size: %s' % provider_size)
        provider_node_name = self._generate_provider_node_name(instance)
        LOG.debug('provider_node_name: %s' % provider_node_name)

        user_data = self._generate_user_data(instance)

        LOG.debug('Start to create node.')
        provider_bdms = None
        provider_node = self._create_node(instance,
                                          provider_node_name,
                                          provider_image,
                                          provider_size,
                                          provider_bdms,
                                          user_data)
        LOG.debug('node: %s' % provider_node)
        LOG.debug('-------------Start to create hyper service container.-------------')
        self._create_hyper_service_container(context,
                                             instance,
                                             provider_node,
                                             network_info,
                                             block_device_info,
                                             image_meta,
                                             injected_files, admin_password,
                                             image_meta)
        LOG.debug('-------------SUCCESS to create hyper service container.---------------')

        # reset the original state
        self._update_vm_task_state(instance, task_state=vm_task_state)

    def _create_hypervm_from_volume(self, context, instance, image_meta, injected_files,
                                        admin_password, network_info, block_device_info):
        LOG.debug('Start to create hypervm from volume')
        LOG.debug('instance: %s' % instance)
        LOG.debug('image_meta: %s' % image_meta)
        LOG.debug('injected_files: %s' % injected_files)
        LOG.debug('admin_pasword: %s' % admin_password)
        LOG.debug('network_info: %s' % network_info)
        LOG.debug('block_device_info: %s' % block_device_info)

        vm_task_state = instance.task_state
        bdms = block_device_info.get('block_device_mapping',[])
        root_device_name = block_device_info.get('root_device_name', '')
        root_bdm = self._get_root_bdm_from_bdms(bdms, root_device_name)
        if root_bdm is None:
            error_info = 'boot bdm is None.'
            LOG.error(error_info)
            raise Exception(error_info)
        LOG.debug('root_bdm: %s' % root_bdm)
        image_metadata_of_root_volume = self._get_image_metadata_from_bdm(context, root_bdm)
        LOG.debug('get image metadata of root volume: %s' % image_metadata_of_root_volume)

        image_id = image_metadata_of_root_volume['image_id']
        LOG.debug('image id of boot volume is: %s' % image_id)
        image_name = image_metadata_of_root_volume['image_name']
        LOG.debug('image name of boot volume is: %s' % image_name)

        provider_image = self._get_provider_image_by_hybrid_id(image_id)
        if not provider_image:
            provider_image = self._get_provider_image_by_provider_id(self.base_ami_id)
            if provider_image is None:
                LOG.error('Can not get provider image for base ami: %s of docker' % self.base_ami_id)
                raise Exception('Can not get provider image for base ami: %s of docker' % self.base_ami_id)

        LOG.debug('provider_image: %s' % provider_image)
        provider_size = self._get_provider_node_size(instance.get_flavor())
        LOG.debug('provider size: %s' % provider_size)
        provider_node_name = self._generate_provider_node_name(instance)
        LOG.debug('provider_node_name: %s' % provider_node_name)

        user_data = self._generate_user_data(instance)

        LOG.debug('Start to create node.')
        provider_bdms = None
        provider_node = self._create_node(instance,
                                          provider_node_name,
                                          provider_image,
                                          provider_size,
                                          provider_bdms,
                                          user_data)
        LOG.debug('node: %s' % provider_node)
        LOG.debug('-------------Start to create hyper service container.-------------')
        self._create_hyper_service_container(context,
                                             instance,
                                             provider_node,
                                             network_info,
                                             block_device_info,
                                             image_metadata_of_root_volume,
                                             injected_files, admin_password, image_meta)
        LOG.debug('-------------SUCCESS to create hyper service container.---------------')

        #reset the original state
        self._update_vm_task_state(
                instance,
                task_state=vm_task_state)


    def _get_inject_file_data(self, instance):
        rabbit_host = CONF.hypernode_api.my_ip
        if not rabbit_host:
            raise ValueError('rabbit host is None' +
                             ' please config it in /etc/nova/nova-compute.conf, ' +
                             'hypernode_api section, my_ip option')

        LOG.info('rabbit_host: %s' % rabbit_host)
        LOG.info('host: %s' % instance.uuid)
        file_data = 'rabbit_userid=%s\nrabbit_password=%s\nrabbit_host=%s\n' % \
                    (CONF.rabbit_userid, CONF.rabbit_password, rabbit_host)
        file_data += 'host=%s\ntunnel_cidr=%s\nroute_gw=%s\n' % \
                     (instance.uuid, CONF.provider_opts.tunnel_cidr,CONF.provider_opts.vpn_route_gateway)
        LOG.info('end to composite user data: %s' % file_data)

        return file_data

    def _create_hyper_service_container(self, context, instance, provider_node,
                                        network_info, block_device_info,
                                        image_metadata, inject_file, admin_password, image_meta):
        LOG.debug('Start to create hyper service container')
        image_id = image_meta.get('id')
        LOG.debug('image_id: %s' % image_id)
        instance.metadata['is_hybrid_vm'] = True
        instance.save()
        image_name = self._get_image_name_from_meta(image_metadata)
        image_uuid = self._get_image_id_from_meta(image_metadata)
        # update port bind host
        self._binding_host(context, network_info, instance.uuid)

        size = instance.get_flavor().get('root_gb')
        provider_location = self._get_location()

        root_volume = self._get_root_volume(context, block_device_info)
        volume_name = provider_node.id
        if not root_volume:
            # if not exist hybrid root volume, then it is spawn from image.
            #provider_hybrid_volume = self._create_data_volume_for_container(provider_node, size, provider_location)
            provider_hybrid_volume = self._create_data_volume_for_container(size, volume_name, provider_location, image_id)
            try:
                self._wait_for_volume_is_available(provider_hybrid_volume)
            except Exception, e:
                LOG.error('exception: %s' % traceback.format_exc(e))
                time.sleep(2)
                self._deal_with_spawn_docker_app_failed(e.message, provider_hybrid_volume)
        else:
            # if exist hybrid root volume, it means spawn from volume, need to check if exist mapped root volume in aws.
            # if not exist mapped root volume of aws, means it is first time spawn from root volume, then need to create
            # mapped root volume in aws. if exist mapped root volume of aws, use it directly.
            provider_hybrid_volume = self._get_provider_volume(root_volume.get('id'))
            if not provider_hybrid_volume:
                #provider_hybrid_volume = self._create_data_volume_for_container(provider_node, size, provider_location)
                provider_hybrid_volume = self._create_data_volume_for_container(size, volume_name, provider_location, image_id)
                try:
                    self._wait_for_volume_is_available(provider_hybrid_volume)
                except Exception, e:
                    self._deal_with_spawn_docker_app_failed(e.message, provider_hybrid_volume)
                self._map_volume_to_provider(context, root_volume.get('id'), provider_hybrid_volume)

        device = '/dev/sdz'
        self._attache_volume_and_wait_for_attached(provider_node, provider_hybrid_volume, device)

        LOG.debug('Start to get clients.')
        clients = self._get_hybrid_service_clients_by_node(provider_node)

        try:
            LOG.debug('wait for docker service starting')
            is_docker_up = self._clients_wait_hybrid_service_up(clients)
        except Exception, e:
            error_info = 'docker server is not up, create docker app failed, exception: %s' %\
                         traceback.format_exc(e)
            self._deal_with_spawn_docker_app_failed(error_info, volume=provider_hybrid_volume)
        LOG.info('start to composite user data.')

        try:
            LOG.debug('Start to inject file')
            file_data = self._get_inject_file_data(instance)
            inject_result = self._hype_inject_file(clients, file_data)
            LOG.debug('inject_file result: %s' % inject_result)
        except Exception, e:
            LOG.error('inject file failed, exception: %s' % traceback.format_exc(e))
            self._deal_with_spawn_docker_app_failed(e.message, volume=provider_hybrid_volume)

        LOG.debug('old block_device_info: %s' % block_device_info)
        block_device_info = self._attache_volume_and_get_new_bdm(context, instance, block_device_info, provider_node)
        LOG.debug('new block_device_info: %s' % block_device_info)

        try:
            create_container_task = self._hyper_create_container_task(clients, image_name, image_uuid,
                                                                      inject_file, admin_password, network_info,
                                                                      block_device_info)
            self._wait_for_task_finish(clients, create_container_task)

        except Exception, e:
            LOG.error('create container failed, exception: %s' % traceback.format_exc(e))
            self._deal_with_spawn_docker_app_failed(e.message)
        # try:
        #     LOG.debug('Start to create container by using image: %s' % image_name)
        #     created_container = self._hype_create_container(clients, image_name)
        #     LOG.debug('created_container: %s' % created_container)
        # except Exception, e:
        #     LOG.error('create container failed, exception: %s' % traceback.format_exc(e))
        #     self._deal_with_spawn_docker_app_failed(e.message)

        try:
            LOG.info('network_info: %s' % network_info)
            LOG.info('block device info: %s' % block_device_info)
            LOG.debug('Star to start container.')
            started_container = self._hype_start_container(clients,
                                                      network_info=network_info,
                                                      block_device_info=block_device_info)
            LOG.debug('end to start container: %s' % started_container)
        except Exception, e:
            LOG.error('start container failed:%s' % traceback.format_exc(e))
            self._deal_with_spawn_docker_app_failed(e.message)

        # provider_volume_map, bdm_map = self._get_provider_volumes_map_from_bdm(context, instance, block_device_info)
        # LOG.debug('get provider volume map: %s' % provider_volume_map)
        # LOG.debug('get bdm_map: %s' % bdm_map)
        # if provider_volume_map:
        #     for hybrid_volume_id, provider_volume in provider_volume_map.items():
        #         if bdm_map.get(hybrid_volume_id):
        #             mount_point = bdm_map.get(hybrid_volume_id).get('mount_device')
        #             LOG.debug('mount_point: %s' % mount_point)
        #             self._attache_volume_for_docker_app(context, instance, hybrid_volume_id,
        #                                                 mount_point,
        #                                                 provider_node,
        #                                                 provider_volume)
        #         else:
        #             LOG.debug('can not get mount_device for hybrid_volume_id: %s' % hybrid_volume_id)
        #
        # if inject_file:
        #     try:
        #         # self._hype_inject_file_to_container(clients, inject_file)
        #         LOG.debug('inject file success.')
        #     except Exception, e:
        #         LOG.error('inject file to container failed. exception: %s' % exception)
        #         self._deal_with_spawn_docker_app_failed(e.message)

        self._binding_host(context, network_info, instance.uuid)

    def _attache_volume_and_wait_for_attached(self, provider_node, provider_hybrid_volume, device):
        LOG.debug('Start to attach volume')
        attache_result = self.compute_adapter.attach_volume(provider_node, provider_hybrid_volume, device)
        self._wait_for_volume_is_attached(provider_hybrid_volume)
        LOG.info('end to attache volume: %s' % attache_result)

    def _get_location(self):
        LOG.debug('Start to get location')
        provider_location = self.compute_adapter.get_location(self.location)
        LOG.debug('provider_location: %s' % provider_location)
        if not provider_location:
            error_info = 'No provider_location, release resource and return'
            raise ValueError(error_info)
        LOG.debug('get location: %s' % provider_location)

        return provider_location

    def _get_root_volume(self, context, block_device_info):
        LOG.debug('start to get root volume for block_device_info: %s' % block_device_info)
        bdms = block_device_info.get('block_device_mapping', [])
        root_device_name = block_device_info.get('root_device_name', '')
        if root_device_name:
            root_bdm = self._get_root_bdm_from_bdms(bdms, root_device_name)
            if root_bdm:
                root_volume = self._get_volume_from_bdm(context, root_bdm)
            else:
                root_volume = None
        else:
            root_volume = None

        LOG.debug('end to get root volume: %s' % root_volume)
        return root_volume

    def _get_root_volume_by_index_0(self, context, block_device_info):
        LOG.debug('start to get root volume by index 0 for block_device_info: %s' % block_device_info)
        bdms = block_device_info.get('block_device_mapping', [])
        root_bdm = self._get_root_bdm_from_bdms_by_index_0(bdms)
        if root_bdm:
            root_volume = self._get_volume_from_bdm(context, root_bdm)
        else:
            root_volume = None

        LOG.debug('end to get root volume: %s' % root_volume)
        return root_volume

    def _get_root_bdm_from_bdms_by_index_0(self, bdms):
        root_bdm = None
        for bdm in bdms:
            if bdm['boot_index'] == 0:
                root_bdm = bdm
                break
        return root_bdm


    def _create_node_ec2(self, context, instance, image_meta, injected_files,
                                        admin_password, network_info, block_device_info):

        # 1. create a common vm
        # 1.1 map flovar to node size, from configuration
        provider_size = self._get_provider_node_size(instance.get_flavor())
        # 1.2 get common image
        provder_image = self.compute_adapter.get_image(CONF.provider_opts.base_linux_image)
        # 1.3. create_node, and get_node_stat, waiting for node creation finish
        provider_node_name = self._generate_provider_node_name(instance)
        provider_node = self.compute_adapter.create_node(name=provider_node_name, image=provder_image,
                                                         size=provider_size,
                                                         auth=self._get_auth(instance._key_data,
                                                                                     instance._key_name))

        # 2. power off the vm
        self.compute_adapter.ex_stop_node(provider_node)

        # 3. detach origin root volume
        provider_volumes = self.compute_adapter.list_volumes(node=provider_node)
        provider_volume = provider_volumes[0]
        self.compute_adapter.detach_volume(provider_volume)

        # 4. attach this volume
        self.compute_adapter.attach_volume(provider_node,
                                           provider_volume,
                                           self._trans_device_name(provider_volume.extra.get('device')))

    def _get_volume_ids_from_bdms(self, bdms):
        volume_ids = []
        for bdm in bdms:
            volume_ids.append(bdm['connection_info']['data']['volume_id'])
        return volume_ids

    def spawn(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        """Create VM instance."""
        LOG.debug(_("image meta is:%s") % image_meta)
        LOG.debug(_("instance is:%s") % instance)
        LOG.debug(_("network_info is: %s") % network_info)
        LOG.debug(_("block_device_info is: %s") % block_device_info)
        bdms = block_device_info.get('block_device_mapping', [])
        image_container_type = instance.system_metadata.get('image_container_format')
        if not instance.image_ref and len(bdms) > 0:
            LOG.debug('image_container_type: %s' % image_container_type)
            if image_container_type == CONTAINER_FORMAT_HYBRID_VM:
                self._spawn_from_volume_for_hybrid_vm(context, instance, image_meta, injected_files,
                                        admin_password, network_info, block_device_info)
            else:
                volume_ids = self._get_volume_ids_from_bdms(bdms)
                root_volume_id = volume_ids[0]
                provider_root_volume_id = self._get_provider_volume_id(context, root_volume_id)
                if provider_root_volume_id is not None:
                    provider_volumes = self.compute_adapter.list_volumes(ex_volume_ids=[provider_root_volume_id])
                else:
                    provider_volumes = []

                if not provider_volumes:
                    # if has no provider volume, boot from image: (import image in provider cloud, then boot instance)
                    provider_node = self._spawn_from_image(context, instance, image_meta, injected_files,
                                                           admin_password, network_info, block_device_info)

                    provider_bdm_list = provider_node.extra.get('block_device_mapping')
                    provider_root_volume_id = provider_bdm_list[0].get('ebs').get('volume_id')
                    provider_root_volume = self.compute_adapter.list_volumes(ex_volume_ids=[provider_root_volume_id])[0]
                    self._map_volume_to_provider(context, root_volume_id, provider_root_volume)

                elif len(provider_volumes) == 0:
                    # if has provider volume, boot from volume:
                    self._spawn_from_volume(context, instance, image_meta, injected_files,
                                            admin_password, network_info, block_device_info)
                else:
                    LOG.error('create instance %s faild: multi volume confusion' % instance.uuid)
                    raise exception_ex.MultiVolumeConfusion
        else:
            # if boot from image: (import image in provider cloud, then boot instance)
            if image_container_type == CONTAINER_FORMAT_HYBRID_VM:
                self._create_hypervm_from_image(context, instance, image_meta, injected_files,
                                                           admin_password, network_info, block_device_info)
            else:
                self._spawn_from_image(context, instance, image_meta, injected_files,
                                    admin_password, network_info, block_device_info)
        LOG.debug("creating instance %s success!" % instance.uuid)

    def _map_volume_to_provider(self, context, volume_id, provider_volume):
        # mapping intance root-volume to cinder volume
        if not provider_volume:
            self.cinder_api.delete_volume_metadata(context,
                                                   volume_id,
                                                   ['provider_volume_id'])

        else:
            self.cinder_api.update_volume_metadata(context,
                                                   volume_id,
                                                   {'provider_volume_id': provider_volume.id})

            self._set_tag_for_provider_volume(provider_volume, volume_id)

    def _get_provider_image_id(self, image_obj):
        image_uuid = self._get_image_id_from_meta(image_obj)
        try:
            provider_image = self.compute_adapter.list_images(ex_filters={'tag:hybrid_cloud_image_id': image_uuid})
            if provider_image is None:
                raise exception_ex.ProviderRequestTimeOut

            if len(provider_image) == 0:
                # raise exception.ImageNotFound
                LOG.warning('Image %s NOT Found at provider cloud' % image_uuid)
                return None
            elif len(provider_image) > 1:
                raise exception_ex.MultiImageConfusion
            else:
                return provider_image[0].id
        except Exception as e:
            LOG.error('Can NOT get image %s from provider cloud tag' % image_uuid)
            LOG.error(e.message)
            return None

    def _get_provider_image(self, image_obj):

        try:
            image_uuid = self._get_image_id_from_meta(image_obj)
            provider_image = self.compute_adapter.list_images(
                ex_filters={'tag:hybrid_cloud_image_id':image_uuid})
            if provider_image is None:
                LOG.error('Can NOT get image %s from provider cloud tag' % image_uuid)
                return provider_image
            if len(provider_image)==0:
                LOG.debug('Image %s NOT exist at provider cloud' % image_uuid)
                return provider_image
            elif len(provider_image)>1:
                LOG.error('ore than one image are found through tag:hybrid_cloud_instance_id %s' % image_uuid)
                raise exception_ex.MultiImageConfusion
            else:
                return provider_image[0]
        except Exception as e:
            LOG.error('get provider image failed: %s' % e.message)
            return None

    def _get_provider_image_by_provider_id(self, image_id):
        LOG.debug('start to _get_provider_image_by_provider_id')
        provider_image = None
        provider_images = self.compute_adapter.list_images(ex_image_ids=[image_id])
        if provider_images:
            if len(provider_images) == 1:
                provider_image = provider_images[0]
            elif len(provider_image) > 1:
                error_info = 'More then one image are found for id: %s' % image_id
                LOG.error(error_info)
                raise exception_ex.MultiImageConfusion
            else:
                LOG.debug('len of image list result is 0, return None')
                provider_image = None
        else:
            LOG.debug('list result is None, can not found provider images. return None')
            provider_image = None

        LOG.debug('end to _get_provider_image_by_provider_id: %s' % provider_image)
        return provider_image

    @RetryDecorator(max_retry_count=3,inc_sleep_time=1,max_sleep_time=60,
                        exceptions=(Exception))
    def _get_provider_image_by_hybrid_id(self, image_uuid):
        provider_images = self.compute_adapter.list_images(
            ex_filters={'tag:hybrid_cloud_image_id': image_uuid})

        if provider_images is None:
            provider_image = None
        else:
            length_provider_images = len(provider_images)
            if length_provider_images == 0:
                provider_image = None
            elif length_provider_images > 1:
                error_info = 'More than one image are found through tag:hybrid_cloud_instance_id %s' % image_uuid
                LOG.error(error_info)
                raise Exception(error_info)
            elif length_provider_images == 1:
                provider_image = provider_images[0]
            else:
                raise Exception('Unknow issue, the length of images is less then 0')

        return provider_image

    def _update_vm_task_state(self, instance, task_state):
        instance.task_state = task_state
        instance.save()

    def resume_state_on_host_boot(self, context, instance, network_info,
                                  block_device_info=None):
        pass

    def _import_volume_from_glance(self, context, volume_id,instance, volume_loc):
        LOG.debug('start to import volume from glance')
        volume = self.cinder_api.get(context,volume_id)
        image_meta = volume.get('volume_image_metadata')
        if not image_meta:
            LOG.error('Provider Volume NOT Found!')
            exception_ex.VolumeNotFoundAtProvider
        else:
            # 1.1 download qcow2 file from glance
            image_uuid = self._get_image_id_from_meta(image_meta)

            orig_file_name = 'orig_file.qcow2'
            this_conversion_dir = '%s/%s' % (CONF.provider_opts.conversion_dir,volume_id)
            orig_file_full_name = '%s/%s' % (this_conversion_dir,orig_file_name)

            fileutils.ensure_tree(this_conversion_dir)
            self.glance_api.download(context, image_uuid,dest_path=orig_file_full_name)

            # 1.2 convert to provider image format
            converted_file_format = 'vmdk'
            converted_file_name = '%s.%s' % ('converted_file', converted_file_format)
            converted_file_path = '%s/%s' % (CONF.provider_opts.conversion_dir,volume_id)
            converted_file_full_name =  '%s/%s' % (converted_file_path,converted_file_name)
            convert_image(orig_file_full_name,
                          converted_file_full_name,
                          converted_file_format,
                          subformat='streamoptimized')

            # 1.3 upload volume file to provider storage (S3,eg)
            container = self.storage_adapter.get_container(CONF.provider_opts.storage_tmp_dir)
            # self.storage_adapter.upload_object(converted_file_full_name,container,volume_id)

            object_name = volume_id
            extra = {'content_type': 'text/plain'}

            with open(converted_file_full_name,'rb') as f:
                obj = self.storage_adapter.upload_object_via_stream(container=container,
                                                           object_name=object_name,
                                                           iterator=f,
                                                           extra=extra)

            # 1.4 import volume
            obj = self.storage_adapter.get_object(container.name,volume_id)

            task = self.compute_adapter.create_import_volume_task(CONF.provider_opts.storage_tmp_dir,
                                                                  volume_id,
                                                                  'VMDK',
                                                                  obj.size,
                                                                  str(volume.get('size')),
                                                                  volume_loc=volume_loc)

            try:
                task_list =instance_task_map[instance.uuid]
                if not task_list:
                    task_list.append(task)
                    instance_task_map[instance.uuid]=task_list
            except KeyError:
                task_list=[task]
                instance_task_map[instance.uuid]=task_list

            while not task.is_completed():
                time.sleep(10)
                if task.is_cancelled():
                    LOG.error('import volume fail!')
                    raise exception_ex.UploadVolumeFailure
                task = self.compute_adapter.get_task_info(task)

            task.clean_up()
            LOG.debug('finish to import volume, id: %s' % task.volume_id)

            return task.volume_id

    def _add_route_to_iscsi_subnet(self, ssh_client,
                                   iscsi_subnet,
                                   iscsi_subnet_route_gateway,
                                   iscsi_subnet_route_mask):
        while True:
            try:
                # list routes
                cmd1 = "ip route show"
                cmd1_status, cmd1_out, cmd1_err = ssh_client.execute(cmd1)
                LOG.debug("cmd1 info status=%s ,out=%s, err=%s " %
                          (cmd1_status, cmd1_out, cmd1_err))
                if cmd1_status != 0:
                    raise Exception("fail to show routes")

                routes = [{'dest': p.split(" via ")[0],
                           'gateway': p.split(" via ")[1].split(" ")[0]}
                       for p in cmd1_out.splitlines() if
                       p.startswith(iscsi_subnet + "/" + iscsi_subnet_route_mask)]
                # assume same dest only allows one route, lazy to test len(routes) > 1
                if len(routes) > 0:
                    if routes[0]['gateway'] == iscsi_subnet_route_gateway:
                        LOG.debug("already got the route:%s" % routes)
                        return
                    else:
                        cmd_del_route = "sudo ip route delete %s" % routes[0]['dest']
                        cmd_del_status, cmd_del_out, cmd_del_err = \
                            ssh_client.execute(cmd_del_route)
                        LOG.debug("cmd delete route info status=%s ,out=%s, err=%s " %
                                  (cmd_del_status, cmd_del_out, cmd_del_err))
                        if cmd_del_status != 0:
                            raise Exception("fail to delete existed route")

                # route got deleted or no route, add one to route table
                cmd_add_route = "sudo ip route add %s via %s" % \
                                (iscsi_subnet + "/" + iscsi_subnet_route_mask,
                                 iscsi_subnet_route_gateway)
                cmd_add_status, cmd_add_out, cmd_add_err = \
                            ssh_client.execute(cmd_add_route)
                LOG.debug("cmd add route info status=%s ,out=%s, err=%s " %
                          (cmd_add_status, cmd_add_out, cmd_add_err))
                if cmd_add_status != 0:
                    raise Exception("fail to add route")

                # write route into rc.local
                cmd_write_local = "sudo sed -i '/PATH=/a ip route add %s via %s' /etc/init.d/rc.local" \
                                  % (iscsi_subnet + "/" + iscsi_subnet_route_mask,
                                     iscsi_subnet_route_gateway)
                cmd_write_status, cmd_write_out, cmd_write_err = \
                            ssh_client.execute(cmd_write_local)
                LOG.debug("cmd write route info status=%s ,out=%s, err=%s " %
                          (cmd_write_status, cmd_write_out, cmd_write_err))
                if cmd_write_status != 0:
                    raise Exception("fail to write route into rc.local")
                LOG.info("added route succeeds!")
                break
            except sshclient.SSHError:
                    LOG.debug("wait for vm to initialize network")
                    time.sleep(5)

    def _attach_volume_iscsi(self, provider_node, connection_info):
        user = CONF.provider_opts.image_user
        pwd = CONF.provider_opts.image_password
        if provider_node.private_ips:
            host = provider_node.private_ips[0]
        else:
            LOG.error("provider_node.private_ips None ,attach volume failed")
            raise Exception(_("provider_node.private_ips None ,attach volume failed"))

        ssh_client = sshclient.SSH(user, host, password=pwd)

        # add route if config exists
        if CONF.provider_opts.agent_network == 'True' and \
                CONF.provider_opts.iscsi_subnet and \
                CONF.provider_opts.iscsi_subnet_route_gateway and \
                CONF.provider_opts.iscsi_subnet_route_mask:
            LOG.debug("add route to vm:%s, %s, %s" % (CONF.provider_opts.iscsi_subnet,
                                                      CONF.provider_opts.iscsi_subnet_route_gateway,
                                                      CONF.provider_opts.iscsi_subnet_route_mask))
            self._add_route_to_iscsi_subnet(ssh_client,
                                          CONF.provider_opts.iscsi_subnet,
                                          CONF.provider_opts.iscsi_subnet_route_gateway,
                                          CONF.provider_opts.iscsi_subnet_route_mask)

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
                time.sleep(5)

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

    def _get_provider_volume_by_provider_volume_id(self, provider_volume_id):
        provider_volumes = self.compute_adapter.list_volumes(ex_volume_ids=[provider_volume_id])

        if not provider_volumes:
            LOG.error('get volume %s error at provider cloud' % provider_volume_id)
            return

        if len(provider_volumes)>1:
            LOG.error('volume %s are more than one' % provider_volume_id)
            raise exception_ex.MultiVolumeConfusion
        provider_volume = provider_volumes[0]

        if provider_volume.state != StorageVolumeState.AVAILABLE:
            LOG.error('volume %s is not available' % provider_volume_id)
            raise exception.InvalidVolume

        return provider_volume

    def attach_volume(self, context, connection_info, instance, mountpoint,
                      disk_bus=None, device_type=None, encryption=None):
        """Attach volume storage to VM instance."""
        volume_id = connection_info['data']['volume_id']
        instance_id = instance.uuid
        driver_type = connection_info['driver_volume_type']
        LOG.info("attach volume")

        provider_node = self._get_provider_node(instance)
        if not provider_node:
            LOG.error('get instance %s error at provider cloud' % instance_id)
            return

        if driver_type == 'iscsi':
            self._attach_volume_iscsi(provider_node, connection_info)
            return

        # 2.get volume exist or import volume
        provider_volume_id = self._get_provider_volume_id(context, volume_id)
        if not provider_volume_id:
            provider_volume_id = self._import_volume_from_glance(context, volume_id, instance,
                                                                 provider_node.extra.get('availability'))

            provider_volume = self._get_provider_volume_by_provider_volume_id(provider_volume_id)
            LOG.debug('get provider_volume: %s' % provider_volume)

            # map imported provider_volume id with hybrid cloud volume id by tagging hybrid_cloud_volume_id
            LOG.debug('start to map volume')
            self._map_volume_to_provider(context, volume_id, provider_volume)
            LOG.debug('end to map volume')
        else:
            provider_volume = self._get_provider_volume_by_provider_volume_id(provider_volume_id)

        image_container_type = instance.system_metadata.get('image_container_format')
        LOG.debug('image_container_type: %s' % image_container_type)
        # if is hybrid_vm, need to attache volume for docker app(container).
        if image_container_type == CONTAINER_FORMAT_HYBRID_VM and provider_node.state == NodeState.RUNNING:
            self._attache_volume_for_docker_app(context,
                                                instance,
                                                volume_id,
                                                mountpoint,
                                                provider_node,
                                                provider_volume)
        else:
            self.compute_adapter.attach_volume(provider_node, provider_volume,
                                               self._trans_device_name(mountpoint))

    def _get_volume_devices_list_for_docker_app(self, instance, clients):
        """

        :param instance:
        :param clients:
        :return: type list, e.g. [u'/dev/xvdb', u'/dev/xvdz']
        """
        LOG.debug('Start to get volume list for docker app')
        volume_device_list = []

        image_container_type = instance.system_metadata.get('image_container_format')
        LOG.debug('image_container_type: %s' % image_container_type)
        # if is hybrid_vm, need to attache volume for docker app(container).
        if image_container_type == CONTAINER_FORMAT_HYBRID_VM:
                self._clients_wait_hybrid_service_up(clients)
                volume_devices = self._clients_list_volume_devices_for_docker_app(clients)
                volume_device_list = volume_devices.get('devices')
        LOG.debug('End to get volume list for docker app, volumes list: %s ' % volume_device_list)

        return volume_device_list

    def _attache_volume_for_docker_app(self, context, instance, volume_id, mountpoint, provider_node, provider_volume):
        LOG.debug('start attach volume for docker app')
        clients = self._get_hybrid_service_clients_by_node(provider_node)
        old_volumes_list = self._get_volume_devices_list_for_docker_app(instance, clients)
        LOG.debug('old_volumes_list: %s' % old_volumes_list)

        self._attache_volume_and_wait_for_attached(provider_node, provider_volume, self._trans_device_name(mountpoint))

        try:
            is_docker_service_up = self._clients_wait_hybrid_service_up(clients)
        except Exception, e:
            LOG.error('docker is not start, exception: %s' % traceback.format_exc(e))
            raise e
        LOG.debug('start to get added device')
        added_device = self._get_added_device(instance, clients, old_volumes_list)
        if is_docker_service_up:
            try:
                LOG.debug('start attach to docker app')
                self._clients_attach_volume_for_docker_app(clients, volume_id, added_device, mountpoint)
            except Exception, e:
                error_info = 'Start container failed, exception: %s' % traceback.format_exc(e)
                LOG.error(error_info)
                raise exception.NovaException(error_info)

    def _attache_volume_and_get_new_bdm(self, context, instance, block_device_info, provider_node):
        bdm_list = block_device_info.get('block_device_mapping')
        for bdm in bdm_list:
            if bdm['boot_index'] == 0:
                hybrid_volume = self._get_volume_from_bdm(context, bdm)
                bdm['size'] = hybrid_volume.get('size')
                continue
            hybrid_cloud_volume_id = bdm.get('connection_info').get('data').get('volume_id')
            provider_volume_id = self._get_provider_volume_id(context, hybrid_cloud_volume_id)
            # if volume doesn't exist in aws, it need to import volume from image
            if not provider_volume_id:
                LOG.debug('provider volume is not exist for volume: %s' % hybrid_cloud_volume_id)
                provider_volume_id = self._import_volume_from_glance(context,
                                                                     hybrid_cloud_volume_id,
                                                                     instance,
                                                                     CONF.provider_opts.availability_zone)
                created_provider_volume = self._get_provider_volume_by_provider_volume_id(provider_volume_id)
                self._map_volume_to_provider(context, hybrid_cloud_volume_id, created_provider_volume)
                provider_volume = self._get_provider_volume(hybrid_cloud_volume_id)
            else:
                provider_volume = self._get_provider_volume(hybrid_cloud_volume_id)
            mount_device = bdm.get('mount_device')
            clients = self._get_hybrid_service_clients_by_node(provider_node)
            old_volumes_list = self._get_volume_devices_list_for_docker_app(instance, clients)
            LOG.debug('old_volumes_list: %s' % old_volumes_list)

            self._attache_volume_and_wait_for_attached(provider_node, provider_volume, self._trans_device_name(mount_device))

            try:
                is_docker_service_up = self._clients_wait_hybrid_service_up(clients)
            except Exception, e:
                LOG.error('docker is not start, exception: %s' % traceback.format_exc(e))
                raise e
            added_device = self._get_added_device(instance, clients, old_volumes_list)
            bdm['real_device'] = added_device

            hybrid_volume = self._get_volume_from_bdm(context, bdm)
            bdm['size'] = hybrid_volume.get('size')

        return block_device_info


    @RetryDecorator(max_retry_count=60, inc_sleep_time=2, max_sleep_time=60, exceptions=(Exception))
    def _get_added_device(self, instance, clients, old_volumes_list):
        LOG.debug('start to get added device')
        added_device = None
        new_volumes_list = self._get_volume_devices_list_for_docker_app(instance, clients)
        LOG.debug('new_volumes_list: %s' % new_volumes_list)
        added_device_list = [device for device in new_volumes_list if device not in old_volumes_list]

        if not added_device_list:
            e_info = 'added device in docker is empty, can not do container attach operation'
            LOG.error(e_info)
            raise Exception(e_info)
        else:
            added_device = added_device_list[0]

        LOG.debug('end to get added device: %s' % added_device)
        return added_device


    def _detach_volume_for_docker_app(self, clients, volume_id):
            try:
                is_docker_service_up = self._clients_wait_hybrid_service_up(clients)
            except Exception, e:
                LOG.error('docker is not start, exception: %s' % traceback.format_exc(e))
                raise e

            if is_docker_service_up:
                try:
                    self._clients_detach_volume_for_docker_app(clients, volume_id)
                except Exception, e:
                    error_info = 'detach volume for docker app failed, exception: %s' % traceback.format_exc(e)
                    LOG.error(error_info)
                    raise exception.NovaException(error_info)

    def _get_provider_volume_id(self, context, volume_id):

        provider_volume_id = self.cinder_api.get_volume_metadata_value(context,volume_id,'provider_volume_id')

        if not provider_volume_id:
            try:
                provider_volumes = self.compute_adapter.list_volumes(ex_filters={'tag:hybrid_cloud_volume_id':volume_id})
                if len(provider_volumes) == 1:
                    provider_volume_id = provider_volumes[0].id
                    self.cinder_api.update_volume_metadata(context, volume_id, {'provider_volume_id':provider_volume_id})
                elif len(provider_volumes)>1:
                    LOG.warning('More than one instance are found through tag:hybrid_cloud_volume_id %s' % volume_id)

                else:
                    LOG.warning('Volume %s NOT Found at provider cloud' % volume_id)
                    # raise exception.ImageNotFound
            except Exception as e:
                LOG.error('Can NOT get volume %s from provider cloud tag' % volume_id)
                LOG.error(e.message)

        return provider_volume_id

    def _get_provider_volume(self, volume_id):

        provider_volume = None
        try:
            #if not provider_volume_id:
            provider_volumes = self.compute_adapter.list_volumes(ex_filters={'tag:hybrid_cloud_volume_id':volume_id})
            if provider_volumes is None:
                LOG.warning('Can not get volume through tag:hybrid_cloud_volume_id %s' % volume_id)
                return provider_volumes
            if len(provider_volumes) == 1:

                provider_volume = provider_volumes[0]
            elif len(provider_volumes) >1:
                LOG.warning('More than one volumes are found through tag:hybrid_cloud_volume_id %s' % volume_id)
            else:
                LOG.warning('Volume %s NOT Found at provider cloud' % volume_id)
        except Exception as e:
            LOG.error('Can NOT get volume %s from provider cloud tag' % volume_id)
            LOG.error(e.message)
        return provider_volume

    def _detach_volume_iscsi(self, provider_node, connection_info):
        user = CONF.provider_opts.image_user
        pwd = CONF.provider_opts.image_password
        if provider_node.private_ips:
            host = provider_node.private_ips[0]
        else:
            LOG.debug("provider_node.private_ips None ,attach volume failed")
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

    def detach_interface(self, instance, vif):
        LOG.debug("detach interface: %s, %s" % (instance, vif))
        node = self._get_provider_node(instance)

        if instance.system_metadata.get('image_container_format') == CONTAINER_FORMAT_HYBRID_VM \
                and self._node_is_active(node):
            clients = self._get_hybrid_service_clients_by_node(node)
            self._clients_detach_interface(clients, vif)

    def detach_volume(self, connection_info, instance, mountpoint,
                      encryption=None):
        """Detach the disk attached to the instance."""
        LOG.info("detach volume")

        volume_id = connection_info['data']['volume_id']
        instance_id = instance.uuid
        driver_type = connection_info['driver_volume_type']

        provider_node=self._get_provider_node(instance)
        if not provider_node:
            LOG.error('get instance %s error at provider cloud' % instance_id)
            return
        if driver_type == 'iscsi':
            self._detach_volume_iscsi(provider_node, connection_info)
            return

        provider_volume=self._get_provider_volume(volume_id)
        if not provider_volume:
            LOG.error('get volume %s error at provider cloud' % volume_id)
            return


        if provider_volume.state != StorageVolumeState.ATTACHING:
            LOG.error('volume %s is not attaching' % volume_id)

        image_container_type = instance.system_metadata.get('image_container_format')
        LOG.debug('image_container_type: %s' % image_container_type)
        if image_container_type == CONTAINER_FORMAT_HYBRID_VM:
            clients = self._get_hybrid_service_clients_by_node(provider_node)
            self._detach_volume_for_docker_app(clients, volume_id)

        # 2.dettach
        self.compute_adapter.detach_volume(provider_volume)
        time.sleep(3)
        retry_time = 60
        provider_volume=self._get_provider_volume(volume_id)
        while retry_time > 0:
            if provider_volume and \
               provider_volume.state == StorageVolumeState.AVAILABLE and \
               provider_volume.extra.get('attachment_status') is None:
                break
            else:
                time.sleep(2)
                provider_volume=self._get_provider_volume(volume_id)
                retry_time = retry_time-1

    def get_available_resource(self, nodename):
        """Retrieve resource info.

        This method is called when nova-compute launches, and
        as part of a periodic task.

        :returns: dictionary describing resources

        """
        # xxx(wangfeng):
        return {'vcpus': 10000,
                'memory_mb': 100000000,
                'local_gb': 100000000,
                'vcpus_used': 0,
                'memory_mb_used': 1000,
                'local_gb_used': 1000,
                'hypervisor_type': 'aws',
                'hypervisor_version': 5005000,
                'hypervisor_hostname': nodename,
                'cpu_info': '{"model": ["Intel(R) Xeon(R) CPU E5-2670 0 @ 2.60GHz"], \
                "vendor": ["Huawei Technologies Co., Ltd."], \
                "topology": {"cores": 16, "threads": 32}}',
                'supported_instances': jsonutils.dumps(
                    [["i686", "ec2", "hvm"], ["x86_64", "ec2", "hvm"]]),
                'numa_topology': None,
                }

    def get_available_nodes(self, refresh=False):
        """Returns nodenames of all nodes managed by the compute service.

        This method is for multi compute-nodes support. If a driver supports
        multi compute-nodes, this method returns a list of nodenames managed
        by the service. Otherwise, this method should return
        [hypervisor_hostname].
        """
        # return "aws-ec2-hypervisor"
        return "hybrid_%s" % CONF.provider_opts.region

    def attach_interface(self, instance, image_meta, vif):
        LOG.debug("attach interface: %s, %s" % (instance, vif))
        self._binding_host_vif(vif, instance.uuid)
        node = self._get_provider_node(instance)

        if instance.system_metadata.get('image_container_format') == CONTAINER_FORMAT_HYBRID_VM \
                and self._node_is_active(node):
            clients = self._get_hybrid_service_clients_by_node(node)
            self._clients_attach_interface(clients, vif)
        self._binding_host_vif(vif, instance.uuid)

    def get_pci_slots_from_xml(self, instance):
        """
        :param instance:
        :return:
        """
        return []

    def _node_is_active(self, node):
        is_active = False

        nova_state = node.state
        if nova_state == NodeState.RUNNING or nova_state == NodeState.STOPPED:
            is_active = True
        else:
            is_active = False

        return is_active

    def get_info(self, instance):
        LOG.debug('begin get the instance %s info ' % instance.uuid)
        state = power_state.NOSTATE
        provider_node_id = None
        # xxx(wangfeng): it is too slow to connect to aws to get info. so I delete it

        provider_node_id = self._get_provider_node_id(instance)

        if provider_node_id:
            state = self.cache.query_status(provider_node_id)
        if state:
            LOG.debug('end get the instance %s info ,provider node is %s ' % (instance.uuid, provider_node_id))

        '''
        node = self._get_provider_node(instance)
        if node:
            LOG.debug('end get the instance %s info ,provider node is %s ' % (instance.uuid,node.id))
            node_status = node.state
            try:
                state = AWS_POWER_STATE[node_status]
            except KeyError:
                state = power_state.NOSTATE
        '''

        return {'state': state,
                'max_mem': 0,
                'mem': 0,
                'num_cpu': 1,
                'cpu_time': 0}

    def destroy(self, context, instance, network_info, block_device_info=None,
                destroy_disks=True, migrate_data=None):
        """Destroy VM instance."""
        LOG.debug('begin destroy node %s',instance.uuid)
        LOG.debug('destroy_disks: %s' % destroy_disks)
        try:
            task_list = instance_task_map[instance.uuid]
            if task_list:
                for task in task_list:
                    LOG.debug('the task of instance %s is %s' %(instance.uuid, task.task_id))
                    task = self.compute_adapter.get_task_info(task)
                    if not task.is_completed():
                        task._cancel_task()
            instance_task_map.pop(instance.uuid)
        except KeyError:
            LOG.debug('the instance %s does not have task', instance.uuid)

        node = self._get_provider_node(instance)
        if node is None:
            LOG.error('get instance %s error at provider cloud' % instance.uuid)
            reason = "Error getting instance."
            raise exception.InstanceTerminationFailure(reason=reason)
        if not node:
            LOG.error('instance %s not exist at provider cloud' % instance.uuid)
            return

        # 0.1 get network interfaces
        provider_eth_list = node.extra.get('network_interfaces',None)

        # 0.2 get volume
        provider_vol_list = self.compute_adapter.list_volumes(node=node)
        provider_volume_ids = []
        local_volume_ids = []
        all_volume_ids = []
        if len(block_device_info) > 0:
            # get volume id
            bdms = block_device_info.get('block_device_mapping',[])
            for device in bdms:
                volume_id = device['connection_info']['data']['volume_id']
                all_volume_ids.append(volume_id)
                if device['connection_info']['driver_volume_type'] == 'iscsi':
                    local_volume_ids.append(volume_id)
                else:
                    provider_volume_ids.append(self._get_provider_volume_id(context, volume_id))

        # # 1. dettach volumes, if needed
        # if not destroy_disks:
        #         # get volume in provide cloud
        #         provider_volumes = self.compute_adapter.list_volumes(ex_volume_ids=provider_volume_ids)
        #
        #         # detach
        #         for provider_volume in provider_volumes:
        #             self.compute_adapter.detach_volume(provider_volume)
        #         for local_volume in local_volume_ids:
        #             volume = self.cinder_api.get(context, local_volume)
        #             attachment = self.cinder_api.get_volume_attachment(volume, instance['uuid'])
        #             if attachment:
        #                 self.cinder_api.detach(context, local_volume, attachment['attachment_id'])

        image_container_type = instance.system_metadata.get('image_container_format')
        LOG.debug('image_container_type: %s' % image_container_type)
        # if is hybrid_vm, need to stop docker app(container) first, then stop node.

        if image_container_type == CONTAINER_FORMAT_HYBRID_VM:
            root_volume = self._get_root_volume_by_index_0(context, block_device_info)
            # if not exist root volume, means it is boot from image, need to remote data volume of container.
            # if exist root volume, the data volume is a root volume. How to delete it will decided by manager.
            if not root_volume:
                LOG.debug('image type of instance is hybridvm, need to remove data volume of container.')

                provider_volume_name_for_hybrid_vm_container = node.id
                hybrid_container_volume = self._get_provider_container_data_volume(provider_vol_list,
                                                             provider_volume_name_for_hybrid_vm_container)
                if node.state != NodeState.STOPPED and node.state != NodeState.TERMINATED:
                    self._stop_node(node)
                if hybrid_container_volume:
                    self._detach_volume(hybrid_container_volume)
                    self._delete_volume(hybrid_container_volume)
                else:
                    LOG.warning('There is no container data volume, pass to'
                                ' detach volume and delete volume for node: %s' % node.id)
            # no matter it is boot from volume or image, both need to remove neutron agent.
            self._remove_neutron_agent(instance)

        # 2.destroy node
        if node.state != NodeState.TERMINATED:
            self.compute_adapter.destroy_node(node)

            while node.state != NodeState.TERMINATED:
                time.sleep(5)
                nodes = self.compute_adapter.list_nodes(ex_node_ids=[node.id])
                if not nodes:
                    break
                else:
                    node = nodes[0]

        # 3. clean up
        # 3.1 delete network interface anyway
        for eth in provider_eth_list:
            try:
                self.compute_adapter.ex_delete_network_interface(eth)
            except:
                LOG.warning('Failed to delete network interface %s', eth.id)

        # 3.2 delete volumes, if needed
        # if destroy_disks:
        #     for vol in provider_vol_list:
        #         try:
        #             self.compute_adapter.destroy_volume(vol)
        #         except:
        #             LOG.warning('Failed to delete provider vol %s', vol.id)

        # todo: unset volume mapping
        bdms = block_device_info.get('block_device_mapping',[])
        volume_ids = self._get_volume_ids_from_bdms(bdms)
        for volume_id in volume_ids:
            try:
                self._map_volume_to_provider(context, volume_id, None)
            except Exception as e:
                LOG.info("got exception:%s" % str(e))

    def _stop_node(self, node):
        LOG.debug('start to stop node: %s' % node.name)
        self.compute_adapter.ex_stop_node(node)
        self._wait_for_node_in_specified_state(node, NodeState.STOPPED)
        LOG.debug('end to stop node: %s' % node.name)

    def _wait_for_node_in_specified_state(self, node, state):
        LOG.debug('wait for node is in state: %s' % state)
        state_of_current_node = self._get_node_state(node)
        time.sleep(2)
        while state_of_current_node != state:
            state_of_current_node = self._get_node_state(node)
            time.sleep(2)

    def _if_node_in_specified_states_with_times(self, node, states, sleep_time, wait_times):
        LOG.debug('wait for node is in states: %s' % states)
        state_of_current_node = self._get_node_state(node)
        time.sleep(sleep_time)
        while state_of_current_node not in states:
            state_of_current_node = self._get_node_state(node)
            time.sleep(sleep_time)
            wait_times -= 1
            if wait_times == 0:
                break
        if state_of_current_node in states:
            is_node_state_the_same_with_specified_state = True
        else:
            is_node_state_the_same_with_specified_state = False

        return is_node_state_the_same_with_specified_state

    def _get_node_state(self, node):
        nodes = self.compute_adapter.list_nodes(ex_node_ids=[node.id])
        if nodes and len(nodes) == 1:
            current_node = nodes[0]
            state_of_current_node = current_node.state
        else:
            raise Exception('Node is not exist, node id: %s' % node.id)
        LOG.debug('state of current is: %s' % state_of_current_node)

        return state_of_current_node

    def _detach_volume(self, volume):
        LOG.debug('start to detach volume')
        self.compute_adapter.detach_volume(volume)
        LOG.debug('end to detach volume')
        self._wait_for_volume_in_specified_state(volume, StorageVolumeState.AVAILABLE)

    def _wait_for_volume_in_specified_state(self, volume, state):
        LOG.debug('wait for volume in state: %s' % state)
        state_of_volume = self._get_volume_state(volume)
        time.sleep(2)
        while state_of_volume != state:
            state_of_volume = self._get_volume_state(volume)

            time.sleep(2)

    def _get_volume_state(self, volume):
        volume_id = volume.id
        provider_volumes = self.compute_adapter.list_volumes(ex_volume_ids=[volume_id])
        if provider_volumes and len(provider_volumes) == 1:
            current_volume = provider_volumes[0]
            state_of_volume = current_volume.state
        else:
            raise Exception('There is not provider volume for id: %s' % volume_id)
        LOG.debug('current volume state is: %s' % state_of_volume)

        return state_of_volume

    def _get_provider_container_data_volume(self, provider_volume_list, provider_volume_name_for_hybrid_vm_container):
        """

        :param provider_volume_list: volume list of attchement of provider node
        :param provider_volume_name_for_hybrid_vm_container: the name of data volume used by docker.
        Currently the name is the same as provider vm id.
        :return:
        """
        hybrid_container_volume = None
        #TODO:delete
        for volume in provider_volume_list:
            if volume.name == provider_volume_name_for_hybrid_vm_container:
                hybrid_container_volume = volume
                break
            else:
                continue

        return hybrid_container_volume

    def _remove_neutron_agent(self, instance):
        LOG.debug('start to remove neutron agent for instance: %s' % instance.uuid)
        instance_id = instance.uuid
        neutron_client = neutronv2.get_client(context=None, admin=True)
        agent = neutron_client.list_agents(host=instance_id)
        if len(agent['agents']) == 1:
            neutron_client.delete_agent(agent['agents'][0]['id'])
        else:
            LOG.warning('can not find neutron agent for instance: %s, did not delete agent for it' % instance.uuid)
        LOG.debug('end to remove neutron agent for instance: %s' % instance.uuid)

    def _get_provider_node_id(self, instance_obj):

        """map openstack instance_uuid to ec2 instance id"""
        # if instance has metadata:provider_node_id, it's provider node id
        provider_node_id = instance_obj.metadata.get('provider_node_id')

        # if instance has NOT metadata:provider_node_id, search provider cloud instance's tag
        if not provider_node_id:
            try:
                provider_node = self.compute_adapter.list_nodes(ex_filters={'tag:hybrid_cloud_instance_id':instance_obj.uuid})
                if len(provider_node) == 1:
                    provider_node_id = provider_node[0].id
                    instance_obj.metadata.set('provider_node_id', provider_node_id)
                    instance_obj.save()
                elif len(provider_node)>1:
                    LOG.warning('More than one instance are found through tag:hybrid_cloud_instance_id %s' % instance_obj.uuid)
                else:
                    # raise exception.ImageNotFound
                    LOG.warning('Instance %s NOT Found at provider cloud' % instance_obj.uuid)
            except Exception as e:
                LOG.error('Can NOT get instance %s from provider cloud tag' % instance_obj.uuid)
                LOG.error(e.message)

        return provider_node_id

    def _get_provider_node(self, instance_obj):
        """map openstack instance to ec2 instance """

        provider_node_id = instance_obj.metadata.get('provider_node_id')
        provider_node = None
        if not provider_node_id:
            try:
                provider_nodes = self.compute_adapter.list_nodes(ex_filters={'tag:hybrid_cloud_instance_id':instance_obj.uuid})
                if provider_nodes is None:
                    LOG.error('Can NOT get node through tag:hybrid_cloud_instance_id %s' % instance_obj.uuid)
                    return provider_nodes

                if len(provider_nodes) == 1:
                    provider_node_id = provider_nodes[0].id
                    instance_obj.metadata['provider_node_id']= provider_node_id
                    instance_obj.save()
                    provider_node = provider_nodes[0]
                elif len(provider_nodes) >1:
                    LOG.debug('More than one instance are found through tag:hybrid_cloud_instance_id %s' % instance_obj.uuid)
                else:
                    LOG.debug('Instance %s NOT exist at provider cloud' % instance_obj.uuid)
                    return []
            except Exception as e:
                LOG.error('Can NOT get instance through tag:hybrid_cloud_instance_id %s' % instance_obj.uuid)
                LOG.error(e.message)
        else:
            try:
                nodes = self.compute_adapter.list_nodes(ex_node_ids=[provider_node_id])
                if nodes is None:
                    LOG.error('Can NOT get instance %s from provider cloud tag' % provider_node_id)
                    return nodes
                if len(nodes) == 0:
                    LOG.debug('Instance %s NOT exist at provider cloud' % instance_obj.uuid)
                    return []
                else:
                    provider_node=nodes[0]
            except Exception as e:
                LOG.error('Can NOT get instance %s from provider cloud tag' % provider_node_id)
                LOG.error(e.message)

        return  provider_node

    def get_volume_connector(self, instance):
        pass

    def power_off(self, instance, timeout=0, retry_interval=0):
        LOG.debug('Power off node %s',instance.uuid)
        node = self._get_provider_node(instance)

        if node:
            image_container_type = instance.system_metadata.get('image_container_format')
            LOG.debug('image_container_type: %s' % image_container_type)
            # if is hybrid_vm, need to stop docker app(container) first, then stop node.
            if image_container_type == CONTAINER_FORMAT_HYBRID_VM:
                self._stop_container_in_loop(node)

            if self._if_node_in_specified_states_with_times(node, [NodeState.RUNNING], 2, 60):
                self.compute_adapter.ex_stop_node(node)
                self._wait_for_node_in_specified_state(node, NodeState.STOPPED)
            else:
                node_current_state = self._get_node_state(node)
                error_info = 'aws node is in state: %s, can not be started.' % node_current_state
                LOG.error(error_info)
                raise Exception(error_info)
        else:
            raise exception.InstanceNotFound(instance_id=instance.uuid)

    def _stop_container_in_loop(self, node):
        is_stop = False
        clients = self._get_hybrid_service_clients_by_node(node)
        try:
            is_stop = self._clients_stop_container(clients)
        except Exception as e:
            LOG.error("power off container failed, exception:%s" % traceback.format_exc(e))

        return is_stop

    def power_on(self, context, instance, network_info,
                 block_device_info=None):
        LOG.debug('Power on node %s',instance.uuid)
        # start server of aws
        node = self._get_provider_node(instance)
        if node:
            if self._if_node_in_specified_states_with_times(node, [NodeState.STOPPED], 2, 60):
                self.compute_adapter.ex_start_node(node)
                self._wait_for_node_in_specified_state(node, NodeState.RUNNING)
            else:
                node_current_state = self._get_node_state(node)
                error_info = 'aws node is in state: %s, can not be started.' % node_current_state
                LOG.error(error_info)
                raise Exception(error_info)
        else:
            raise exception.InstanceNotFound(instance_id=instance.uuid)
        LOG.debug('is_hybrid_vm: %s' % instance.metadata.get('is_hybrid_vm', False))

        image_container_type = instance.system_metadata.get('image_container_format')
        LOG.debug('image_container_type: %s' % image_container_type)

        if image_container_type == CONTAINER_FORMAT_HYBRID_VM:
            LOG.debug('Start to start container.')
            self._start_container_in_loop_clients(node, network_info, block_device_info)
            LOG.debug('End to start container.')

    def _start_container_in_loop_clients(self, node, network_info, block_device_info):
        clients = self._get_hybrid_service_clients_by_node(node)
        is_docker_service_up = False
        try:
            is_docker_service_up = self._clients_wait_hybrid_service_up(clients)
        except Exception, e:
            LOG.error('docker is not start, exception: %s' % traceback.format_exc(e))

        if is_docker_service_up:
            try:
                self._hype_start_container(clients=clients,
                                           network_info=network_info,
                                           block_device_info=block_device_info)
            except Exception, e:
                error_info = 'Start container failed, exception: %s' % traceback.format_exc(e)
                LOG.error(error_info)
                raise exception.NovaException(error_info)

    def get_instance_macs(self, instance):
        LOG.debug('Start to get macs of instance %s', instance)
        filters = {'tag:hybrid_cloud_instance_id': instance['uuid']}
        nodes = self.compute_adapter.list_nodes(ex_filters=filters)
        instance_macs = dict()
        if nodes is not None and len(nodes) == 1:
            node = nodes[0]
            nw_interfaces = node.extra['network_interfaces']
            for nw_interface in nw_interfaces:
                subnet_id = nw_interface.extra['subnet_id']
                vpc_id = nw_interface.extra['vpc_id']
                mac_address = nw_interface.extra['mac_address']

                # NOTE(nkapotoxin): Now we make the subnet_id is the provider
                # network id
                instance_macs[subnet_id] = mac_address
            return instance_macs

    def reboot(self, context, instance, network_info, reboot_type,
               block_device_info=None, bad_volumes_callback=None):
        """Reboot the specified instance.
        """
        # 1.get node
        instance_id = instance.uuid
        provider_node_id = self._get_provider_node_id(instance)
        if not provider_node_id:
            LOG.error('instance %s is not found' % instance_id)
            raise exception.InstanceNotFound
        else:
            provider_nodes = self.compute_adapter.list_nodes(ex_node_ids=[provider_node_id])

        if not provider_nodes:
            LOG.error('instance %s is not found' % instance_id)
            raise exception.InstanceNotFound
        if len(provider_nodes)>1:
            LOG.error('instance %s are more than one' % instance_id)
            raise exception_ex.MultiInstanceConfusion
        provider_node = provider_nodes[0]

        image_container_type = instance.system_metadata.get('image_container_format')
        LOG.debug('image_container_type: %s' % image_container_type)

        if image_container_type == CONTAINER_FORMAT_HYBRID_VM:
            clients = self._get_hybrid_service_clients_by_node(provider_node)

            try:
                is_docker_service_up = self._clients_wait_hybrid_service_up(clients)
            except Exception, e:
                LOG.error('docker is not start, exception: %s' % traceback.format_exc(e))
                raise e

            if is_docker_service_up:
                try:
                    self._clients_reboot_app(clients,
                                             network_info=network_info,
                                             block_device_info=block_device_info)
                except Exception, e:
                    error_info = 'Start container failed, exception: %s' % traceback.format_exc(e)
                    LOG.error(error_info)
                    raise exception.NovaException(error_info)
        else:
            try:
                self.compute_adapter.reboot_node(provider_node)
            except Exception as e:
                raise e

    @RetryDecorator(max_retry_count= 50,inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError,errors.NotFound, errors.ConnectionError, errors.InternalError))
    def _wait_hybrid_service_up(self, client):
            return client.get_version()

    @RetryDecorator(max_retry_count=20,inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError,errors.NotFound,
                                    errors.ConnectionError, errors.InternalError, Exception))
    def _hypervm_inject_file(self, client, file_data):
        LOG.info('start to inject file.')
        inject_reslut = client.inject_file(CONF.provider_opts.dst_path, file_data=file_data)
        LOG.info('end to inject file....')
        return inject_reslut

    @RetryDecorator(max_retry_count= 100,inc_sleep_time=5,max_sleep_time=120,
                        exceptions=(errors.APIError,errors.NotFound,
                                    errors.ConnectionError, errors.InternalError, Exception))
    def _start_container(self, client, network_info, block_device_info):
        return client.start_container(network_info=network_info, block_device_info=block_device_info)

    @RetryDecorator(max_retry_count= MAX_RETRY_COUNT,inc_sleep_time=5,max_sleep_time=60,
                    exceptions=(errors.APIError,errors.NotFound,
                                errors.ConnectionError, errors.InternalError, Exception))
    def _hype_create_container(self, clients, name):
        LOG.info('start to create container')
        created_container = None
        tmp_except = Exception('client is None')
        for client in clients:
            try:
                created_container = client.create_container(name)
                break
            except Exception, e:
                tmp_except = e
                LOG.error('exception when create container, exception: %s' % traceback.format_exc(e))
                time.sleep(1)
                continue
        if not created_container:
            raise tmp_except

        LOG.info('end to create container, created_container: %s' % created_container)
        return created_container


    @RetryDecorator(max_retry_count=MAX_RETRY_COUNT, inc_sleep_time=5, max_sleep_time=60, exceptions=(
    errors.APIError, errors.NotFound, errors.ConnectionError, errors.InternalError, Exception))
    def _hyper_create_container_task(self, clients, image_name, image_uuid, injected_files, admin_password,
                                     network_info, block_device_info):
        LOG.info('start to submit task for creating container.')
        LOG.debug('admin_password: %s' % admin_password)
        LOG.debug('injected_files: %s' % injected_files)
        created_task = None
        tmp_exception = Exception('empty for creating container')
        for client in clients:
            try:
                created_task = client.create_container(image_name, image_uuid, inject_files=injected_files, admin_password=admin_password,
                                network_info=network_info, block_device_info=block_device_info)
                break
            except Exception, e:
                tmp_exception = e
                LOG.error('exception when create container, exception: %s' % traceback.format_exc(e))
                continue
        if not created_task:
            raise tmp_exception

        LOG.info('end to submit task for creating container, task: %s' % created_task)

        return created_task

    @RetryDecorator(max_retry_count=50, inc_sleep_time=5, max_sleep_time=60,
                    exceptions=(exception_ex.RetryException))
    def _wait_for_task_finish(self, clients, task):
        task_finish = False
        if task['code'] == wormhole_constants.TASK_SUCCESS:
            return True
        current_task = self._hyper_query_task(clients, task)
        task_code = current_task['code']

        if wormhole_constants.TASK_DOING == task_code:
            LOG.debug('task is DOING, status: %s' % task_code)
            raise exception_ex.RetryException(error_info='task status is: %s' % task_code)
        elif wormhole_constants.TASK_ERROR == task_code:
            LOG.debug('task is ERROR, status: %s' % task_code)
            raise Exception('task error, task status is: %s' % task_code)
        elif wormhole_constants.TASK_SUCCESS == task_code:
            LOG.debug('task is SUCCESS, status: %s' % task_code)
            task_finish = True
        else:
            raise Exception('UNKNOW ERROR, task status: %s' % task_code)

        LOG.debug('task: %s is finished' % task )

        return task_finish



    @RetryDecorator(max_retry_count=MAX_RETRY_COUNT, inc_sleep_time=5, max_sleep_time=60, exceptions=(
    errors.APIError, errors.NotFound, errors.ConnectionError, errors.InternalError, Exception))
    def _hyper_query_task(self, clients, task):
        LOG.debug('star to query task.')
        current_task = None
        tmp_exception = 'empty for query task'
        for client in clients:
            try:
                current_task = client.query_task(task)
                break
            except Exception, e:
                tmp_exception = e
                LOG.error('exception when query task. exception: %s' % traceback.format_exc(e))
                continue
        if not current_task:
            raise tmp_exception

        return current_task

    @RetryDecorator(max_retry_count= MAX_RETRY_COUNT,inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError,errors.NotFound,
                                    errors.ConnectionError, errors.InternalError, Exception))
    def _hype_start_container(self, clients, network_info, block_device_info):
        LOG.info('Start to start container')
        started_container = None
        tmp_except = None
        for client in clients:
            try:
                started_container = client.start_container(network_info=network_info, block_device_info=block_device_info)
                break
            except Exception, e:
                tmp_except = e
                continue
        if not started_container:
            raise tmp_except

        LOG.info('end to start container, started_container: %s' % started_container)
        return started_container

    @RetryDecorator(max_retry_count=20, inc_sleep_time=5, max_sleep_time=60,
                        exceptions=(errors.APIError, errors.NotFound,
                                    errors.ConnectionError, errors.InternalError, Exception))
    def _hype_inject_file_to_container(self, clients, inject_file):
        """

        :param clients:
        :param inject_file: (path, file_contents)
        :return:
        """
        LOG.debug('start to inject file to container, inject_file: %s' % inject_file)
        inject_result = None
        tmp_except = None
        for client in clients:
            try:
                inject_result = client.inject_files(inject_file)
                break
            except Exception, e:
                tmp_except = e
                continue
        if not inject_result:
            raise tmp_except

        LOG.info('end to inject file to container, inject_file: %s' % inject_file)

        return  inject_result

    @RetryDecorator(max_retry_count= 20,inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError,errors.NotFound,
                                    errors.ConnectionError, errors.InternalError, Exception))
    def _hype_inject_file(self, clients, file_data):

        inject_result = None
        tmp_except = None
        for client in clients:
            try:
                inject_result = client.inject_file(CONF.provider_opts.dst_path, file_data=file_data)
                break
            except Exception, e:
                tmp_except = e
                continue
        if not inject_result:
            raise tmp_except

        return inject_result

    def _get_node_private_ips(self, provider_node):
        """

        :param provider_node: type Node,
        :return: type list, return list of private ips of Node
        """
        LOG.debug('start to get node private ips for node:%s' % provider_node.name)
        private_ips = []
        interfaces = self.compute_adapter.ex_list_network_interfaces(node=provider_node)
        for interface in interfaces:
            if len(interface.extra.get('private_ips')) > 0:
                for private_ip_dic in interface.extra.get('private_ips'):
                    private_ip = private_ip_dic.get('private_ip')
                    if private_ip:
                        private_ips.append(private_ip)
                    else:
                        continue
            else:
                continue

        LOG.debug('end to get node private ips, private_ips: %s' % private_ips)

        return private_ips

    def _get_hybrid_service_clients_by_instance(self, instance):
        LOG.debug('start to get hybrid service clients.')
        provider_node = self._get_provider_node(instance)
        if not provider_node:
            error_info = 'get instance %s error at provider cloud' % instance.uuid
            LOG.error(error_info)
            raise Exception(error_info)
        clients = self._get_hybrid_service_clients_by_node(provider_node)

        LOG.debug('end to get hybrid service clients')
        return clients

    def _get_hybrid_service_clients_by_node(self, provider_node):
        port = CONF.provider_opts.hybrid_service_port
        private_ips = self._get_node_private_ips(provider_node)
        LOG.debug('port: %s' % port)
        LOG.debug('private ips: %s' % private_ips)

        clients = self._get_hybrid_service_client(private_ips, port)

        return clients

    def _get_hybrid_service_client(self, ips, port):
        clients = []
        for ip in ips:
            clients.append(Client(ip, port))

        return clients

    @RetryDecorator(max_retry_count=100, inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError, errors.NotFound, errors.ConnectionError, errors.InternalError))
    def _clients_wait_hybrid_service_up(self, clients):
        is_docker_up = False
        tmp_except = Exception('Can not get version of docker server ')
        for client in clients:
            try:
                docker_version = client.get_version()
                LOG.debug('docker version: %s, docker is up.' % docker_version)
                is_docker_up = True
                break
            except Exception, e:
                tmp_except = e
                continue

        if not is_docker_up:
            raise tmp_except

        return is_docker_up

    @RetryDecorator(max_retry_count=50,inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError,errors.NotFound, errors.ConnectionError, errors.InternalError))
    def _clients_reboot_app(self, clients, network_info, block_device_info):

        is_rebooted = False
        tmp_except = Exception('Reboot app failed.')
        for client in clients:
            try:
                client.restart_container(network_info=network_info, block_device_info=block_device_info)
                LOG.debug('Reboot app success.')
                is_rebooted = True
                break
            except Exception, e:
                tmp_except = e
                continue

        if not is_rebooted:
            raise tmp_except

        return is_rebooted

    @RetryDecorator(max_retry_count=50,inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError,errors.NotFound, errors.ConnectionError, errors.InternalError))
    def _clients_stop_container(self, clients):
        is_stop = False
        tmp_except = Exception('Reboot app failed.')
        for client in clients:
            try:
                client.stop_container()
                LOG.debug('Reboot app success.')
                is_stop = True
                break
            except Exception, e:
                tmp_except = e
                continue

        if not is_stop:
            raise tmp_except

        return is_stop

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

    @RetryDecorator(max_retry_count=50,inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError,errors.NotFound, errors.ConnectionError, errors.InternalError))
    def _clients_attach_volume_for_docker_app(self, clients, volume_id, device, mount_device):
        attached = False
        tmp_except = Exception('attach volume for app failed.')
        for client in clients:
            try:
                client.attach_volume(volume_id, device, mount_device)
                LOG.debug('attach volume for app success.')
                attached = True
                break
            except Exception, e:
                tmp_except = e
                continue

        if not attached:
            raise tmp_except

        return attached

    @RetryDecorator(max_retry_count=50,inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError,errors.NotFound, errors.ConnectionError, errors.InternalError))
    def _clients_create_image_task(self, clients, image):
        image_name = image['name']
        LOG.debug('image name : %s' % image_name)
        image_id = image['id']
        LOG.debug('image id: %s' % image_id)

        create_image_task = None
        tmp_exception = Exception('tmp exception in create image task')
        for client in clients:
            try:
                create_image_task = client.create_image(image_name, image_id)
                LOG.debug('create image task: %s' % create_image_task)
                break
            except Exception, e:
                tmp_exception = e
                continue
        if not create_image_task:
            raise tmp_exception

        return create_image_task

    @RetryDecorator(max_retry_count=50, inc_sleep_time=5, max_sleep_time=60,
                    exceptions=(errors.APIError, errors.NotFound, errors.ConnectionError, errors.InternalError))
    def _clients_get_image_info(self, clients, image):
        image_name = image['name']
        image_id = image['id']

        image_info = None
        tmp_exception = Exception('tmp exception in get image_info')
        for client in clients:
            try:
                image_info = client.image_info(image_name, image_id)
                LOG.debug('get image_info: %s' % image_info)
                break
            except Exception, e:
                tmp_exception = e
                continue
        if not image_info:
            raise tmp_exception

        return image_info


    @RetryDecorator(max_retry_count=50,inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError,errors.NotFound, errors.ConnectionError, errors.InternalError))
    def _clients_list_volume_devices_for_docker_app(self, clients):
        volume_devices = None
        tmp_except = Exception('list volumes devices failed.')
        for client in clients:
            try:
                volume_devices = client.list_volume()
                LOG.debug('list volume devices success, volume list: %s' % volume_devices)
                break
            except Exception, e:
                tmp_except = e
                continue

        if not volume_devices:
            raise tmp_except

        return volume_devices

    @RetryDecorator(max_retry_count=50,inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError,errors.NotFound, errors.ConnectionError, errors.InternalError))
    def _clients_detach_volume_for_docker_app(self, clients, volume_id):
        detached = False
        tmp_except = Exception('detach volume for app failed.')
        for client in clients:
            try:
                client.detach_volume(volume_id)
                LOG.debug('detach volume for app success.')
                detached = True
                break
            except Exception, e:
                tmp_except = e
                continue

        if not detached:
            raise tmp_except

        return detached


    @RetryDecorator(max_retry_count=50,inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError,errors.NotFound, errors.ConnectionError, errors.InternalError))
    def _clients_detach_interface(self, clients, vif):
        detached = False
        tmp_except = Exception('detach interface for app failed.')
        for client in clients:
            try:
                client.detach_interface(vif)
                LOG.debug('detach interface for app success.')
                detached = True
                break
            except Exception, e:
                tmp_except = e
                continue

        if not detached:
            raise tmp_except

        return detached

    @RetryDecorator(max_retry_count=50,inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError,errors.NotFound, errors.ConnectionError, errors.InternalError))
    def _clients_attach_interface(self, clients, vif):
        attached = False
        tmp_except = Exception('attach interface for app failed.')
        for client in clients:
            try:
                client.attach_interface(vif)
                LOG.debug('attach interface for app success.')
                attached = True
                break
            except Exception, e:
                tmp_except = e
                continue

        if not attached:
            raise tmp_except

        return attached

    @RetryDecorator(max_retry_count=50,inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError,errors.NotFound, errors.ConnectionError, errors.InternalError))
    def _clients_pause_container(self, clients):
        paused = False
        tmp_except = Exception('pause container failed.')
        for client in clients:
            try:
                client.pause_container()
                LOG.debug('pause container success.')
                paused = True
                break
            except Exception, e:
                tmp_except = e
                continue

        if not paused:
            raise tmp_except

        return paused

    @RetryDecorator(max_retry_count=50,inc_sleep_time=5,max_sleep_time=60,
                        exceptions=(errors.APIError,errors.NotFound, errors.ConnectionError, errors.InternalError))
    def _clients_unpause_container(self, clients):
        unpaused = False
        tmp_except = Exception('unpause container failed.')
        for client in clients:
            try:
                client.unpause_container()
                LOG.debug('unpause container success.')
                unpaused = True
                break
            except Exception, e:
                tmp_except = e
                continue

        if not unpaused:
            raise tmp_except

        return unpaused

    def pause(self, instance):
        """Pause the specified instance.

        :param instance: nova.objects.instance.Instance
        """
        LOG.debug('start to pause instance: %s' % instance)

        node = self._get_provider_node(instance)
        LOG.debug("Node is: %s" % node)

        if instance.system_metadata.get('image_container_format') == CONTAINER_FORMAT_HYBRID_VM \
                and self._node_is_active(node):
            clients = self._get_hybrid_service_clients_by_node(node)

            is_docker_service_up = False
            try:
                is_docker_service_up = self._clients_wait_hybrid_service_up(clients)
            except Exception, e:
                LOG.error('docker is not start, exception: %s' % traceback.format_exc(e))
            if is_docker_service_up:
                self._clients_pause_container(clients)
        LOG.debug('end to pause instance success.')

    def unpause(self, instance):
        """Unpause paused VM instance.

        :param instance: nova.objects.instance.Instance
        """
        LOG.debug('start to unpause instance: %s' % instance)
        node = self._get_provider_node(instance)

        if instance.system_metadata.get('image_container_format') == CONTAINER_FORMAT_HYBRID_VM \
                and self._node_is_active(node):
            clients = self._get_hybrid_service_clients_by_node(node)

            is_docker_service_up = False
            try:
                is_docker_service_up = self._clients_wait_hybrid_service_up(clients)
            except Exception, e:
                LOG.error('docker is not start, exception: %s' % traceback.format_exc(e))
            if is_docker_service_up:
                self._clients_unpause_container(clients)
        LOG.debug('end to unpause instance success.')

def qemu_img_info(path):
    """Return an object containing the parsed output from qemu-img info."""
    # flag.
    if not os.path.exists(path):
        msg = (_("Path does not exist %(path)s") % {'path': path})
        raise exception.InvalidDiskInfo(reason=msg)

    out, err = utils.execute('env', 'LC_ALL=C', 'LANG=C',
                             'qemu-img', 'info', path)
    if not out:
        msg = (_("Failed to run qemu-img info on %(path)s : %(error)s") %
               {'path': path, 'error': err})
        raise exception.InvalidDiskInfo(reason=msg)

    return imageutils.QemuImgInfo(out)


def convert_image(source, dest, out_format, run_as_root=False, **kwargs):
    """Convert image to other format."""
    cmd = ('qemu-img', 'convert', '-O', out_format, source, dest)
    utils.execute(*cmd, run_as_root=run_as_root)

    if kwargs.has_key('subformat'):
        if kwargs.get('subformat') == 'streamoptimized':
            dir_name = os.path.dirname(dest)
            base_name = os.path.basename(dest)

            ovf_name = '%s/%s.ovf' % (dir_name,base_name)
            vmx_name_temp = '%s/vmx/template.vmx' % CONF.provider_opts.conversion_dir
            vmx_name = '%s/template.vmx' % dir_name
            shutil.copy2(vmx_name_temp,vmx_name)

            mk_ovf_cmd = ('ovftool', '-o',vmx_name, ovf_name)
            convert_file = '%s/converted-file.vmdk' % dir_name
            os.rename(dest, convert_file)
            utils.execute(*mk_ovf_cmd, run_as_root=run_as_root)
            vmdk_file_name = '%s/%s-disk1.vmdk' % (dir_name,base_name)

            fileutils.delete_if_exists(dest)
            os.rename(vmdk_file_name, dest)

            fileutils.delete_if_exists(ovf_name)
            fileutils.delete_if_exists('%s/%s.mf' % (dir_name,base_name))
            fileutils.delete_if_exists(convert_file)
