import cinder.compute.nova as nova
from cinder.image import image_utils
from oslo.config import cfg
from functools import wraps
from cinder import exception
from cinder.i18n import _
from cinder.openstack.common import excutils
from cinder.openstack.common import fileutils
from cinder.openstack.common import log as logging
from cinder.openstack.common import units
from cinder.openstack.common import uuidutils
from cinder.volume import driver
from cinder.volume import volume_types

from oslo.config import cfg
# from libcloud.compute.types import Provider
#from libcloud.compute.providers import get_driver
#from libcloud.compute.base import Node
from adapter import Ec2Adapter as Ec2Adapter
import adapter
from libcloud.compute.types import StorageVolumeState,NodeState
import exception_ex
import os
import cinder.context
import pdb
import requests
from keystoneclient.v2_0 import client as kc
from libcloud.compute.base import NodeSize
from wormhole_business import WormHoleBusinessAWS
from wormholeclient import constants as wormhole_constants

import time
import string
import rpyc
import traceback

HYPER_SERVICE_PORT = 7127
ec2api_opts = [
    cfg.StrOpt('access_key_id',
               default='',
               help='the access key id for connection to EC2  '),

    cfg.StrOpt('secret_key',
               default='',
               help='the secret key  for connection to EC2  '),

    cfg.StrOpt('region',
               default='ap-southeast-1',
               help='the region for connection to EC2  '),

    cfg.StrOpt('driver_type',
               # default='ec2_ap_southeast',
               default='agent',
               help='the type for driver  '),

    cfg.StrOpt('provider_image_conversion_dir',
               default='/tmp/ec2/',
               help='volume convert to image dir'),

    cfg.StrOpt('provider_instance_id',
               default='',
               help='aws instance id'),

    cfg.StrOpt('cgw_host_id',
               default='',
               help='compute gateway id in provider cloud'),

    cfg.StrOpt('cgw_host_ip',
               default='',
               help='compute gateway ip'),

    cfg.StrOpt('cgw_username',
               default='',
               help='compute gateway user name'),

    cfg.StrOpt('cgw_certificate',
               default='',
               help='full name of compute gateway public key'),

    cfg.StrOpt('storage_tmp_dir',
               default='wfbucketse',
               help='a cloud storage temp directory'),

    cfg.StrOpt('availability_zone',
               default='ap-southeast-1a',
               help='the availability_zone for connection to EC2  '),

    cfg.StrOpt('hybrid_service_port',
               default='7127',
               help='port of hybrid hyper service'),
    cfg.StrOpt('subnet_data',
               default='subnet-804178e5',
               help='provider subnet id of tunnel bearing net'),
    cfg.StrOpt('subnet_api',
               default='subnet-864178e3',
               help='provider subnet id of external api net'),
    cfg.StrOpt('base_ami_id',
               default='ami-a6d104c5',
               help='id of aim of base vm'),
    cfg.StrOpt('security_group',
               default=None,
               help='security group'),
]

vgw_opts = [
   cfg.DictOpt('vgw_url',
                default={
                    'fs_vgw_url': 'http://162.3.114.107:8090/',
                    'vcloud_vgw_url': 'http://162.3.114.108:8090/',
                    'aws_vgw_url': 'http://172.27.12.245:8090/'
                },
                help="These values will be used for upload/download image "
                     "from vgw host."),
    cfg.StrOpt('store_file_dir',
               default='/home/upload',
               help='Directory used for temporary storage '
                    'during migrate volume'),
    cfg.StrOpt('rpc_service_port',
               default='1111',
               help='port of rpc service')      
]

keystone_opts =[
    cfg.StrOpt('tenant_name',
               default='admin',
               help='tenant name for connecting to keystone in admin context'),
    cfg.StrOpt('user_name',
               default='cloud_admin',
               help='username for connecting to cinder in admin context'),
    cfg.StrOpt('keystone_auth_url',
               default='https://identity.cascading.hybrid.huawei.com:443/identity-admin/v2.0',
               help='value of keystone url'),
]

keystone_auth_group = cfg.OptGroup(name='keystone_authtoken',
                               title='keystone_auth_group')

LOG = logging.getLogger(__name__)

CONF = cfg.CONF
CONF.register_opts(ec2api_opts)
CONF.register_opts(vgw_opts,'vgw')
CONF.register_group(keystone_auth_group)
CONF.register_opts(keystone_opts,'keystone_authtoken')

CONTAINER_FORMAT_HYBRID_VM = 'hybridvm'

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
                        LOG.error('retry times: %s' % str(self._max_retry_count - max_retries))
                        LOG.error('exception: %s' % traceback.format_exc(e))
                        time.sleep(mdelay)
                        max_retries -= 1
                        if mdelay >= self._max_sleep_time:
                            mdelay=self._max_sleep_time
                if max_retries == 1:
                    msg = 'func: %s, retry times: %s, failed' % (f.__name__, str(self._max_retry_count))
                    LOG.error(msg)
                return f(*args, **kwargs)

            return f_retry

class SnapshotStatus(object):
    PENDING = 'pending'
    COMPLETED = 'completed'
    ERROR = 'error'


class AwsEc2VolumeDriver(driver.VolumeDriver):
    VERSION = "1.0"

    def __init__(self, *args, **kwargs):
        super(AwsEc2VolumeDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(ec2api_opts)
        #self.configuration.append_config_values(vgw_opts)
        LOG.info("access_key_id = %s,secret_key = %s" % (self.configuration.access_key_id,
                                                         self.configuration.secret_key))
        if (self.configuration.access_key_id is None or
                    self.configuration.secret_key is None):
            raise Exception(_("Must specify access_key_id and "
                              "secret_key to use aws ec2"))
        self.adpter = Ec2Adapter(self.configuration.access_key_id, secret=self.configuration.secret_key,
                                 region=self.configuration.region, secure=False)

        self.provider_subnet_data = self.configuration.subnet_data
        LOG.debug('provider_subnet_data: %s' % self.provider_subnet_data)
        self.provider_subnet_api = self.configuration.subnet_api
        LOG.debug('provider_subnet_api: %s' % self.provider_subnet_api)
        self.base_ami_id = self.configuration.base_ami_id
        LOG.debug('base_ami_id: %s' % self.base_ami_id)
        LOG.debug('driver_type: %s' % self.configuration.driver_type)
        if self.configuration.driver_type == 'agent':
            # for agent solution by default
            self.provider_interfaces = []
            if self.configuration.subnet_data:
                provider_interface_data = adapter.NetworkInterface(name='eth_data',
                                                                   subnet_id=self.provider_subnet_data,
                                                                   device_index=0)
                self.provider_interfaces.append(provider_interface_data)

            if self.configuration.subnet_api:
                provider_interface_api = adapter.NetworkInterface(name='eth_control',
                                                                  subnet_id=self.provider_subnet_api,
                                                                  device_index=1)
                self.provider_interfaces.append(provider_interface_api)
        else:
            if not self.configuration.security_group:
                self.provider_security_group_id = None
            else:
                self.provider_security_group_id = self.configuration.security_group

    def do_setup(self, context):
        """Instantiate common class and log in storage system."""
        pass

    def check_for_setup_error(self):
        """Check configuration file."""
        pass

    def create_volume(self, volume):
        """Create a volume."""
        LOG.debug('start to create volume')
        size = volume['size']
        name = volume['display_name']
        location = self.adpter.get_location(self.configuration.availability_zone)
        if not location:
            raise exception_ex.ProviderLocationError
        provider_volume = self.adpter.create_volume(size, name, location)
        if not provider_volume:
            raise exception_ex.ProviderCreateVolumeError(volume_id=volume['id'])
        LOG.info("create volume: %s; provider_volume: %s " % (volume['id'], provider_volume.id))

        self._tag_provider_volume_with_hybrid_cloud_volume_id(provider_volume, volume)
        self._add_metadata_for_hybrid_volume(volume, provider_volume)

        model_update = {'provider_location': provider_volume.id}
        LOG.debug('end to create volume')
        return model_update

    def create_volume_from_snapshot(self, volume, snapshot):
        """Create a volume from a snapshot."""
        LOG.debug('start to create volume form snapshot')
        size = volume['size']
        name = volume['display_name']
        location = self._get_location()
        provider_snapshot = self._get_provider_snapshot_by_hybrid_cloud_snapshot_id(snapshot.id)

        if provider_snapshot:
            provider_volume = self.adpter.create_volume_from_snapshot(size, name, location, provider_snapshot)
            if not provider_volume:
                raise exception_ex.ProviderCreateVolumeError(volume_id=volume['id'])
            LOG.info("create volume: %s; provider_volume: %s " % (volume['id'], provider_volume.id))

            self._tag_provider_volume_with_hybrid_cloud_volume_id(provider_volume, volume)
            self._add_metadata_for_hybrid_volume(volume, provider_volume)
        else:
            error_info = 'Can not find provider snapshot for hybrid cloud snapshot: %s' % snapshot.id
            LOG.error(error_info)
            raise exception.CinderException(error_info)
        LOG.debug('end to create volume form snapshot')

        model_update = {'provider_location': provider_volume.id}
        return model_update

    def _get_location(self):
        location = self.adpter.get_location(self.configuration.availability_zone)
        LOG.debug('location is: %s' % location)
        if not location:
            raise exception_ex.ProviderLocationError
        return location

    def _add_metadata_for_hybrid_volume(self, hybrid_cloud_volume, provider_volume):

        ctx = cinder.context.get_admin_context()
        if ctx:
            self.db.volume_metadata_update(ctx, hybrid_cloud_volume['id'], {'provider_volume_id': provider_volume.id}, False)
        LOG.debug('end to add metadata for hybrid volume: %s' % hybrid_cloud_volume)

    def _tag_provider_volume_with_hybrid_cloud_volume_id(self, provider_volume, hybrid_cloud_volume):
        LOG.debug('start to tag provider volume: %s' % provider_volume.id)
        create_tags_func = getattr(self.adpter, 'ex_create_tags')
        if create_tags_func:
            create_tags_func(provider_volume, {'hybrid_cloud_volume_id': hybrid_cloud_volume['id']})
        LOG.debug('end to tag provider volume: %s' % provider_volume.id)

    def _tag_provider_snapshot_with_hybrid_cloud_backup_id(self, provider_snapshot, backup):
        LOG.debug('start to tag provider volume: %s with backup: %s' % (provider_snapshot.id, backup['id']))
        create_tags_func = getattr(self.adpter, 'ex_create_tags')
        if create_tags_func:
            create_tags_func(provider_snapshot, {'hybrid_cloud_backup_id': backup['id']})
        LOG.debug('end to tag provider volume: %s with backup: %s' % (provider_snapshot.id, backup['id']))

    def _get_provider_snapshot_by_hybrid_cloud_snapshot_id(self, hybrid_cloud_snapshot_id):
        LOG.debug('start to get provider snapshot for hybrid cloud snapshot id:%s' % hybrid_cloud_snapshot_id)
        provider_snapshots =\
            self.adpter.list_snapshots(ex_filters={'tag:hybrid_cloud_snapshot_id': hybrid_cloud_snapshot_id})
        if provider_snapshots and len(provider_snapshots) == 1:
            provider_snapshot = provider_snapshots[0]
            LOG.debug('get provider snapshot: %s' % provider_snapshot.id)
        else:
            provider_snapshot = None
        LOG.debug('End to get provider snapshot for hybrid cloud snapshot id:%s' % hybrid_cloud_snapshot_id)

        return provider_snapshot

    def create_cloned_volume(self, volume, src_vref):
        """Create a clone of the specified volume."""
        LOG.debug('start to create volume form volume, volume is: %s, src_vref: %s' % (volume, src_vref))
        src_hybrid_volume_id = src_vref.id
        new_hybrid_volume_id = volume.id
        LOG.debug('src_hybrid_volume_id: %s ' % src_hybrid_volume_id)
        LOG.debug('new_hybrid_volume_id: %s' % new_hybrid_volume_id)


        provider_src_volume = self._get_provider_volume_by_tag_hybrid_cloud_volume_id(src_hybrid_volume_id)
        tmp_provider_snapshot = self._create_tmp_snapshot(provider_src_volume)
        self._wait_for_provider_snapshot_completed(tmp_provider_snapshot)
        location = self._get_location()

        provider_new_volume = \
            self.adpter.create_volume_from_snapshot(volume.size, volume.name, location, tmp_provider_snapshot)

        if not provider_new_volume:
            raise exception_ex.ProviderCreateVolumeError(volume_id=volume['id'])
        LOG.info("created new provider volume: %s for new hybrid volume: %s " % (provider_new_volume.id, volume['id']))

        self._tag_provider_volume_with_hybrid_cloud_volume_id(provider_new_volume, volume)
        self._add_metadata_for_hybrid_volume(volume, provider_new_volume)

        self._delete_provider_snapshot(tmp_provider_snapshot)

        model_update = {'provider_location': provider_new_volume.id}
        return model_update

    def _wait_for_provider_snapshot_completed(self, snapshot):

        snapshot_status = self._get_provider_snapshot_status(snapshot)
        time.sleep(2)

        while snapshot_status != SnapshotStatus.COMPLETED:
            snapshot_status = self._get_provider_snapshot_status(snapshot)
            time.sleep(2)

        return snapshot_status

    @RetryDecorator(max_retry_count=5, inc_sleep_time=2, max_sleep_time=60, exceptions=(Exception))
    def _get_snapshot_by_provider_snapshot(self, provider_snapshot):
        snapshots = self.adpter.list_snapshots(provider_snapshot)
        if snapshots and len(snapshots) == 1:
            snapshot = snapshots[0]
        else:
            raise Exception('Can not get snapshot')

        return snapshot

    def _get_provider_snapshot_status(self, provider_snapshot):
        """
        snapshot.extra.get('state')
        :param snapshot:
        :return:
        """
        snapshot = self._get_snapshot_by_provider_snapshot(provider_snapshot)
        LOG.debug('snapshot extra: %s' % snapshot.extra)
        snapshot_status = snapshot.extra.get('state')
        LOG.debug('snapshot status: %s' % snapshot_status)
        return snapshot_status

    def _delete_provider_snapshot(self, snapshot):
        destroy_result = self.adpter.destroy_volume_snapshot(snapshot)
        if not destroy_result:
            LOG.warning('snapshot: %s is not delete in provider pool.' % snapshot.id)
        else:
            LOG.debug('snapshot: %s is deleted.' % snapshot.id)


    def _create_tmp_snapshot(self, provider_origin_volume):
        LOG.debug('start to create tmp snapshot for provider volume: %s' % provider_origin_volume.id)
        tmp_provider_snapshot_name = provider_origin_volume.id
        tmp_provider_snapshot = self.adpter.create_volume_snapshot(provider_origin_volume, tmp_provider_snapshot_name)
        if not tmp_provider_snapshot:
            e_info = 'Can not create tmp provider snapshot for provider volume: %s' % provider_origin_volume.id
            LOG.error(e_info)
            raise Exception(e_info)
        LOG.debug('end to create tmp snapshot: %s' % tmp_provider_snapshot.id)

        return tmp_provider_snapshot

    def extend_volume(self, volume, new_size):
        """Extend a volume."""
        pass

    def _get_provider_volumeid_from_volume(self, volume):
        if not volume.get('provider_location',None):
            ctx = cinder.context.get_admin_context()
            metadata = self.db.volume_metadata_get(ctx, volume['id'])
            return metadata.get('provider_volume_id',None)
        else:
            return volume.get('provider_location',None)

    def _get_provider_volume_id_by_hybrid_cloud_volume_id(self, hybrid_cloud_volume_id):
        provider_volume = self._get_provider_volume_by_tag_hybrid_cloud_volume_id(hybrid_cloud_volume_id)
        provider_volume_id = provider_volume.id

        return provider_volume_id

    def delete_volume(self, volume):
        """Delete a volume."""
        provider_volume_id = self._get_provider_volumeid_from_volume(volume)
        if not provider_volume_id:
            LOG.error('NO Mapping between cinder volume and provider volume')
            return

        provider_volumes = self.adpter.list_volumes(ex_volume_ids=[provider_volume_id])
        if not provider_volumes:
            LOG.error('provider_volume  is not found')
            return
            #raise exception.VolumeNotFound(volume_id=volume['id'])
        elif len(provider_volumes) > 1:
            LOG.error('volume %s has more than one provider_volume' % volume['id'])
            raise exception_ex.ProviderMultiVolumeError(volume_id=volume['id'])
        delete_ret = self.adpter.destroy_volume(provider_volumes[0])
        LOG.info("deleted volume return%d" % delete_ret)

    def _get_provider_volumeID_from_snapshot(self, snapshot):
        provider_volume_id = self._get_provider_volumeid_from_volume(snapshot['volume'])
        return provider_volume_id

    def _get_provider_volume(self, volume_id):
        """
        get provider volume by provider volume id.
        :param volume_id:  provider volume id
        :return:
        """

        provider_volume = None
        try:
            #if not provider_volume_id:
            provider_volumes = self.adpter.list_volumes(ex_volume_ids=[volume_id])
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
            LOG.error(traceback.format_exc(exception))
        LOG.debug('provider volume: %s' % provider_volume)

        return provider_volume

    def _get_provider_volume_by_tag_hybrid_cloud_volume_id(self, hybrid_cloud_volume_id):
        LOG.debug('start to get provider volume')
        provider_volumes = \
            self.adpter.list_volumes(ex_filters={'tag:hybrid_cloud_volume_id': hybrid_cloud_volume_id})
        if not provider_volumes:
            error_info = 'Can not get volume through tag:hybrid_cloud_volume_id%s' % hybrid_cloud_volume_id
            LOG.error(error_info)
            raise Exception(error_info)
        if len(provider_volumes) == 1:
            provider_volume = provider_volumes[0]
        elif len(provider_volumes) >1:
            error_info = 'More than one volumes are found through tag:hybrid_cloud_volume_id %s' \
                         % hybrid_cloud_volume_id
            LOG.error(error_info)
            raise Exception(error_info)
        else:
            error_info = 'Volume %s NOT Found at provider cloud' % hybrid_cloud_volume_id
            LOG.error(error_info)
            raise Exception(error_info)
        LOG.debug('end to get provider volume, provider_volume: %s' % provider_volume)

        return provider_volume

    def _get_provider_node(self, provider_node_id):
        provider_node = None
        try:
            nodes = self.adpter.list_nodes(ex_node_ids=[provider_node_id])
            if nodes is None:
                LOG.error('Can NOT get node %s from provider cloud tag' % provider_node_id)
                return nodes
            if len(nodes) == 0:
                LOG.debug('node %s NOT exist at provider cloud' % provider_node_id)
                return []
            else:
                provider_node=nodes[0]
        except Exception as e:
            LOG.error('Can NOT get node %s from provider cloud tag' % provider_node_id)
            LOG.error(e.message)

        return provider_node

    def _get_provider_node_by_hybrid_cloud_server_id(self, hybrid_server_id):
        LOG.debug('start to _get_provider_node_by_hybrid_cloud_server_id for: %s' % hybrid_server_id)
        provider_node = None
        provider_nodes = self.adpter.list_nodes(ex_filters={'tag:hybrid_cloud_instance_id': hybrid_server_id})
        if provider_nodes is None:
            LOG.error('Can NOT get node through tag:hybrid_cloud_instance_id %s' % hybrid_server_id)
            provider_node = None
        else:
            if len(provider_nodes) == 1:
                provider_node = provider_nodes[0]
            elif len(provider_nodes) > 1:
                LOG.debug('More than one instance are found through tag:hybrid_cloud_instance_id %s' % hybrid_server_id)
                provider_node = None
            else:
                LOG.debug('Instance %s NOT exist at provider cloud' % hybrid_server_id)
                provider_node = None
        LOG.debug('End to _get_provider_node_by_hybrid_cloud_server_id for: %s, provider node: %s' %
                  (hybrid_server_id, provider_node))

        return provider_node

    def create_snapshot(self, snapshot):
        """Create a snapshot."""
        LOG.debug('start to create_snapshot, hybrid cloud snapshot: %s' % dir(snapshot))
        hybrid_cloud_snapshot_id = snapshot.id
        LOG.debug('hybrid_cloud_snapshot_id: %s' % hybrid_cloud_snapshot_id)
        hybrid_volume_id = snapshot.volume_id
        LOG.debug('hybrid_volume_id: %s' % hybrid_volume_id)
        provider_volume = self._get_provider_volume_by_tag_hybrid_cloud_volume_id(hybrid_volume_id)

        LOG.debug('Start to create snapshot')
        provider_snapshot = self.adpter.create_volume_snapshot(provider_volume, snapshot.name)
        LOG.debug('Created provider_snapshot id is: %s' % provider_snapshot.id)
        if not provider_snapshot:
            raise exception_ex.ProviderCreateSnapshotError(snapshot_id=hybrid_cloud_snapshot_id)

        self._tag_snapshot_with_hybrid_cloud_snapshot_id(provider_snapshot, hybrid_cloud_snapshot_id)

        self._add_snapshot_metadata_with_provider_snapshot_id(snapshot, provider_snapshot)

        model_update = {'provider_location': provider_snapshot.id}
        return model_update

    def _tag_snapshot_with_hybrid_cloud_snapshot_id(self, provider_snapshot, hybrid_cloud_snapshot_id):
        LOG.debug('start to add tag for provider snapshot: %s' % provider_snapshot.id)
        create_tags_func = getattr(self.adpter, 'ex_create_tags')
        if create_tags_func:
            create_tags_func(provider_snapshot, {'hybrid_cloud_snapshot_id': hybrid_cloud_snapshot_id})
        LOG.debug('end to add tag for provider snapshot: %s' % provider_snapshot.id)

    def _add_snapshot_metadata_with_provider_snapshot_id(self, hybrid_cloud_snapshot, provider_snapshot):
        LOG.debug('start to add metadata for hybrid cloud snapshot: %s' % hybrid_cloud_snapshot.id)
        ctx = cinder.context.get_admin_context()
        if ctx:
            self.db.snapshot_metadata_update(ctx,
                                             hybrid_cloud_snapshot.id,
                                             {'provider_snapshot_id': provider_snapshot.id},
                                             False)
        LOG.debug('end to add metadata for hybrid cloud snapshot: %s' % hybrid_cloud_snapshot.id)

    def delete_snapshot(self, snapshot):
        """Delete a snapshot."""
        LOG.debug('start to delete snapshot: %s' % snapshot.id)
        hybrid_cloud_snapshot_id = snapshot.id
        if not hybrid_cloud_snapshot_id:
            LOG.error('snapshot has not id.')
            raise ValueError('snapshot has not id.')

        provider_snapshot = self._get_provider_snapshot_by_hybrid_cloud_snapshot_id(hybrid_cloud_snapshot_id)
        if provider_snapshot:
            delete_ret = self.adpter.destroy_volume_snapshot(provider_snapshot)
        else:
            LOG.warning('there is not provider snapshot tag with hybrid snapshot id: %s,'
                        ' no need to delete provider snapshot id' % snapshot.id)
        LOG.debug('end to delete snapshot: %s' % snapshot.id)

    def get_volume_stats(self, refresh=False):
        """Get volume stats."""
        #volume_backend_name = self.adpter.get_volume_backend_name()
        backend_name = self.configuration.safe_get('volume_backend_name')
        LOG.info('******************************backend_name is %s'%backend_name)
        if not backend_name:
            backend_name = 'AMAZONEC2'
        data = {'volume_backend_name': backend_name,
                'storage_protocol': 'LSI Logic SCSI',
                'driver_version': self.VERSION,
                'vendor_name': 'Huawei',
                'total_capacity_gb': 1024,
                'free_capacity_gb': 1024,
                'reserved_percentage': 0}
        return data

    def create_export(self, context, volume):
        """Export the volume."""
        pass

    def ensure_export(self, context, volume):
        """Synchronously recreate an export for a volume."""
        pass

    def remove_export(self, context, volume):
        """Remove an export for a volume."""
        pass

    def initialize_connection(self, volume, connector):
        """Map a volume to a host."""
        LOG.info("attach volume: %s; provider_location: %s " % (volume['id'],
                                                                volume['provider_location']))
        properties = {'volume_id': volume['id'],
                      'provider_location': volume['provider_location']}
        LOG.info("initialize_connection success. Return data: %s."
                 % properties)
        return {'driver_volume_type': 'provider_volume', 'data': properties}

    def terminate_connection(self, volume, connector, **kwargs):
        pass

    def _get_next_device_name(self,node):
        provider_bdm_list = node.extra.get('block_device_mapping')
        used_device_letter=set()
        all_letters=set(string.ascii_lowercase)
        for bdm in provider_bdm_list:
            used_device_letter.add(bdm.get('device_name')[-1])
        unused_device_letter=list(all_letters - used_device_letter)
        device_name='/dev/xvd'+unused_device_letter[0]
        return device_name

    def _get_management_url(self, kc,image_name, **kwargs):
        endpoint_info= kc.service_catalog.get_endpoints(**kwargs)
        endpoint_list = endpoint_info.get(kwargs.get('service_type'),None)
        region_name = image_name.split('_')[-1]
        if endpoint_list:
            for endpoint in endpoint_list:
                if region_name == endpoint.get('region'):
                    return endpoint.get('publicURL')

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        LOG.error('begin time of copy_volume_to_image is %s' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        container_format = image_meta.get('container_format')
        LOG.debug('container_format: %s' % container_format)
        LOG.debug('image_meta: %s' % container_format)
        image_name = image_meta.get('name')
        file_name = image_meta.get('id')
        if container_format == 'vgw_url':
            LOG.debug('get the vgw url')
            #vgw_url = CONF.vgw.vgw_url.get(container_format)
            kwargs = {
                    'auth_url': CONF.keystone_authtoken.keystone_auth_url,
                    'tenant_name': CONF.keystone_authtoken.tenant_name,
                    'username': CONF.keystone_authtoken.user_name,
                    'password': CONF.keystone_authtoken.admin_password,
                    'insecure': True
                }
            keystoneclient = kc.Client(**kwargs)


            vgw_url = self._get_management_url(keystoneclient,image_name, service_type='v2v')

            #vgw_url = 'http://162.3.125.52:9999/'
            volume_id = volume['id']

            #1.get the provider_volume at provider cloud  
            provider_volume_id = self._get_provider_volumeid_from_volume(volume)
            if not provider_volume_id:
                LOG.error('get provider_volume_id of volume %s error' % volume_id)
                raise exception_ex.ProviderVolumeNotFound(volume_id=volume_id)
            provider_volume=self._get_provider_volume(provider_volume_id)
            if not provider_volume:
                LOG.error('get provider_volume of volume %s at provider cloud error' % volume_id)
                raise exception_ex.ProviderVolumeNotFound(volume_id=volume_id)

            origin_provider_volume_state = provider_volume.extra.get('attachment_status')

            LOG.error('the origin_provider_volume_info is %s' % str(provider_volume.__dict__))
            origin_attach_node_id = None
            origin_device_name=None
            #2.judge if the volume is available
            if origin_provider_volume_state is not None:
                origin_attach_node_id = provider_volume.extra['instance_id']
                origin_device_name = provider_volume.extra['device']
                self.adpter.detach_volume(provider_volume)
                time.sleep(1)
                retry_time = 90
                provider_volume=self._get_provider_volume(provider_volume_id)
                LOG.error('the after detach _volume_info is %s' % str(provider_volume.__dict__))
                while retry_time > 0:
                    if provider_volume and provider_volume.extra.get('attachment_status') is None:
                        break
                    else:
                        time.sleep(2)
                        provider_volume=self._get_provider_volume(provider_volume_id)
                        LOG.error('the after detach _volume_info is %s,the retry_time is %s' % (str(provider_volume.__dict__),str(retry_time)))
                        retry_time = retry_time-1
            #3.attach the volume to vgw host
            try:
                #3.1 get the vgw host
                vgw_host= self._get_provider_node(self.configuration.cgw_host_id)
                if not vgw_host:
                    raise exception_ex.VgwHostNotFound(Vgw_id=self.configuration.cgw_host_id)
                device_name=self._get_next_device_name(vgw_host)
                LOG.error('**********************************************')
                LOG.error('the volume status %s' %provider_volume.state)
                self.adpter.attach_volume(vgw_host, provider_volume,
                                       device_name)
                #query volume status
                time.sleep(1)
                retry_time = 120
                provider_volume=self._get_provider_volume(provider_volume_id)
                while retry_time > 0:
                    if provider_volume and provider_volume.extra.get('attachment_status') =='attached':
                        break
                    else:
                        time.sleep(2)
                        provider_volume=self._get_provider_volume(provider_volume_id)
                        retry_time = retry_time-1

            except Exception as e:
                raise e
            time.sleep(5)
            conn=rpyc.connect(self.configuration.cgw_host_ip,int(CONF.vgw.rpc_service_port))
            LOG.error('begin time of copy_volume_to_file is %s' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            full_file_path = conn.root.copy_volume_to_file(device_name,file_name,CONF.vgw.store_file_dir)
            LOG.error('end time of copy_volume_to_image is %s' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            #todo exception occured clean env
            if not full_file_path:
                self.adpter.detach_volume(provider_volume)
                conn.close()
                raise exception_ex.ProviderExportVolumeError(volume_id=volume_id)
            LOG.error('begin time of push_file_to_vgw is %s' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            push_file_result =conn.root.exposed_push_file_to_vgw(full_file_path,vgw_url)
            LOG.error('end time of push_file_to_vgw is %s' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            if not push_file_result:
                LOG.error('post file file %s  to %s failed' %(push_file_result,vgw_url))
                self.adpter.detach_volume(provider_volume)
                conn.close()
                raise exception_ex.ProviderExportVolumeError(volume_id=volume_id)
            conn.close()
            #create a empty file to glance
            with image_utils.temporary_file() as tmp:
                image_utils.upload_volume(context,
                                          image_service,
                                          image_meta,
                                          tmp)
            fileutils.delete_if_exists(tmp)
            #4.detach form vgw
            self.adpter.detach_volume(provider_volume)
            time.sleep(1)
            retry_time = 120
            provider_volume=self._get_provider_volume(provider_volume_id)
            while retry_time > 0:
                if provider_volume and provider_volume.extra.get('attachment_status') is None:
                    break
                else:
                    time.sleep(2)
                    provider_volume=self._get_provider_volume(provider_volume_id)
                    retry_time = retry_time-1
            LOG.error('**********************************************')
            LOG.error('the volume status %s' %provider_volume.state)
            #attach the volume back         
            if origin_provider_volume_state is not None:
                origin_attach_node = self._get_provider_node(origin_attach_node_id)

                self.adpter.attach_volume(origin_attach_node, provider_volume,
                                           origin_device_name)
        elif container_format == CONTAINER_FORMAT_HYBRID_VM:
            self._copy_volume_to_image_for_hyper_vm(context, volume, image_service, image_meta)
        else:
            if not os.path.exists(self.configuration.provider_image_conversion_dir):
                fileutils.ensure_tree(self.configuration.provider_image_conversion_dir)
            provider_volume_id = self._get_provider_volumeid_from_volume(volume)
            task_ret = self.adpter.export_volume(provider_volume_id,
                                                 self.configuration.provider_image_conversion_dir,
                                                 str(image_meta['id']),
                                                 cgw_host_id=self.configuration.cgw_host_id,
                                                 cgw_host_ip=self.configuration.cgw_host_ip,
                                                 cgw_username=self.configuration.cgw_username,
                                                 cgw_certificate=self.configuration.cgw_certificate,
                                                 transfer_station=self.configuration.storage_tmp_dir)
            if not task_ret:
                raise exception_ex.ProviderExportVolumeError
            temp_path = os.path.join(self.configuration.provider_image_conversion_dir, str(image_meta['id']))
            upload_image = temp_path

            try:
                image_utils.upload_volume(context, image_service, image_meta,
                                          upload_image)
            finally:
                fileutils.delete_if_exists(upload_image)
        LOG.error('end time of copy_volume_to_image is %s' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    def _copy_volume_to_image_for_hyper_vm(self, context, volume, image_service, image_meta):
        LOG.debug('volume: %s' % volume)
        LOG.debug('image_meta: %s' % image_meta)
        hybrid_cloud_volume_id = volume['id']
        provider_volume = self._get_provider_volume_by_tag_hybrid_cloud_volume_id(hybrid_cloud_volume_id)
        LOG.debug('provider volume state: %s' % provider_volume.state)
        if provider_volume.state == StorageVolumeState.AVAILABLE:
            # create base-vm node in aws first.
            provider_node = self._create_node_for_copy_volume_to_image(hybrid_cloud_volume_id)

            # attache data volume for user docker container to base-vm.
            self._attache_volume_and_wait_for_attached(provider_node, provider_volume, '/dev/sdz')
            port = self.configuration.hybrid_service_port

            # create image in docker repository
            wormhole_business = WormHoleBusinessAWS(provider_node, self.adpter, port)
            self._wait_for_hyper_service_up(wormhole_business)
            self._create_image_into_docker_repository(wormhole_business, image_meta)

            # update image info in glance and upload image data to glance.
            docker_image_info = wormhole_business.image_info(image_meta['name'], image_meta['id'])
            docker_image_size = docker_image_info['size']
            image_meta['container_format'] = CONTAINER_FORMAT_HYBRID_VM
            image_meta['size'] = docker_image_size
            self._put_image_info_to_glance(context, image_meta, image_service)

            # detach data volume and clear base-vm node.
            provider_node = self._get_provider_node(provider_node.id)
            LOG.debug('provider_node: %s, status is: %s' % (provider_node.id, provider_node.state))
            if provider_node.state != NodeState.STOPPED and provider_node.state != NodeState.TERMINATED:
                    self._stop_node(provider_node)
            self._detach_provider_volume(provider_volume)
            self._destroy_node(provider_node)

        elif provider_volume.state == StorageVolumeState.INUSE:
            LOG.debug('Volume is inuse')
            # get provider node which attaching the provider volume.
            provider_node_id = provider_volume.extra['instance_id']
            provider_node = self._get_provider_node(provider_node_id)
            LOG.debug('get provider_node: %s' % provider_node.id)
            if not provider_node:
                raise Exception('provider node is None')

            port = self.configuration.hybrid_service_port
            LOG.debug('wormhole port: %s' % port)
            LOG.debug('Start to get clients')

            # create image in docker repository
            wormhole_business = WormHoleBusinessAWS(provider_node, self.adpter, port)
            self._wait_for_hyper_service_up(wormhole_business)
            self._create_image_into_docker_repository(wormhole_business, image_meta)

            # update image info in glance and upload image data to glance.
            docker_image_info = wormhole_business.image_info(image_meta['name'], image_meta['id'])
            docker_image_size = docker_image_info['size']
            image_meta['container_format'] = CONTAINER_FORMAT_HYBRID_VM
            image_meta['size'] = docker_image_size
            self._put_image_info_to_glance(context, image_meta, image_service)

    def _stop_node(self, node):
        LOG.debug('start to stop node: %s' % node.id)
        self.adpter.ex_stop_node(node)
        self._wait_for_node_in_specified_state(node, NodeState.STOPPED)
        LOG.debug('end to stop node: %s' % node.id)

    def _wait_for_node_in_specified_state(self, node, state):
        LOG.debug('wait for node is in state: %s' % state)
        state_of_current_node = self._get_node_state(node)
        time.sleep(2)
        while state_of_current_node != state:
            state_of_current_node = self._get_node_state(node)
            LOG.debug('status of node currently is: %s' % state_of_current_node)
            time.sleep(2)

    def _get_node_state(self, node):
        nodes = self.adpter.list_nodes(ex_node_ids=[node.id])
        if nodes and len(nodes) == 1:
            current_node = nodes[0]
            state_of_current_node = current_node.state
        else:
            raise Exception('Node is not exist, node id: %s' % node.id)
        LOG.debug('state of current is: %s' % state_of_current_node)

        return state_of_current_node

    def _destroy_node(self, node):
        LOG.debug('start to destroy node: %s' % node.id)
        if node.state != NodeState.TERMINATED:
            self.adpter.destroy_node(node)

            while node.state != NodeState.TERMINATED:
                time.sleep(5)
                nodes = self.adpter.list_nodes(ex_node_ids=[node.id])
                if not nodes:
                    break
                else:
                    node = nodes[0]
        LOG.debug('end to destory node: %s' % node.id)

    def _detach_provider_volume(self, volume):
        LOG.debug('start to detach volume')
        self.adpter.detach_volume(volume)
        self._wait_for_volume_in_specified_state(volume, StorageVolumeState.AVAILABLE)
        LOG.debug('end to detach volume')

    def _wait_for_volume_in_specified_state(self, volume, state):
        LOG.debug('wait for volume in state: %s' % state)
        state_of_volume = self._get_volume_state(volume)
        time.sleep(2)
        while state_of_volume != state:
            state_of_volume = self._get_volume_state(volume)

            time.sleep(2)

    def _get_volume_state(self, volume):
        volume_id = volume.id
        provider_volumes = self.adpter.list_volumes(ex_volume_ids=[volume_id])
        if provider_volumes and len(provider_volumes) == 1:
            current_volume = provider_volumes[0]
            state_of_volume = current_volume.state
        else:
            raise Exception('There is not provider volume for id: %s' % volume_id)
        LOG.debug('current volume state is: %s' % state_of_volume)

        return state_of_volume

    def _put_image_info_to_glance(self, context, image_metadata, image_service):
        LOG.debug('start to put image info to glance')
        LOG.debug('image metadata: %s' % image_metadata)

        with image_utils.temporary_file() as tmp:
            try:
                image_utils.upload_volume(context, image_service, image_metadata, tmp)
            finally:
                fileutils.delete_if_exists(tmp)

        LOG.debug('success to put image to glance')

    def _create_image_into_docker_repository(self, wormhole_business, image_meta):
        LOG.debug('start to create image into docker repository')
        image_name = image_meta['name']
        LOG.debug('image_name: %s' % image_name)
        image_id = image_meta['id']
        LOG.debug('image_id: %s' % image_id)
        create_image_task = wormhole_business.create_image(image_name, image_id)
        self._wait_for_task_finish(wormhole_business, create_image_task)
        LOG.debug('success to create image into docker repository.')

    @RetryDecorator(max_retry_count=50, inc_sleep_time=5, max_sleep_time=60,
                    exceptions=(exception_ex.RetryException))
    def _wait_for_task_finish(self, wormhole_business, task):
        task_finish = False
        if task['code'] == wormhole_constants.TASK_SUCCESS:
            return True
        current_task = wormhole_business.query_task(task)
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

    def _trans_device_name(self, orig_name):
        if not orig_name:
            return orig_name
        else:
            return orig_name.replace('/dev/vd', '/dev/sd')

    def _get_volume_device(self, wormhole_business):
        volume_devices = wormhole_business.list_volume()
        volume_device_list = volume_devices.get('devices')

        return volume_device_list

    def _wait_for_hyper_service_up(self, wormhole_business):
        """
        call get version
        :param wormhole_business:
        :return:
        """
        docker_version = wormhole_business.get_version()
        LOG.debug('docker version is: %s' % docker_version)

        return docker_version


    def _create_node_for_copy_volume_to_image(self, node_name):
        LOG.debug('start to create node')
        provider_image = self._get_provider_image_by_provider_id(self.base_ami_id)
        provider_node_size = self._get_provider_node_size('t2.micro')

        provider_node = self._create_node(node_name, provider_image, provider_node_size)

        LOG.debug('success to create node: %s' % provider_node.id)
        return provider_node

    def _get_provider_node_size(self, flavor_id):
        return NodeSize(id=flavor_id,
                        name="", ram="", disk="", bandwidth="", price="", driver=self.adpter)

    def _attache_volume_and_wait_for_attached(self, provider_node, provider_hybrid_volume, device):
        LOG.debug('Start to attach volume')
        attache_result = self.adpter.attach_volume(provider_node, provider_hybrid_volume, device)
        self._wait_for_volume_is_attached(provider_hybrid_volume)
        LOG.info('end to attache volume: %s' % attache_result)

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
        created_volumes = self.adpter.list_volumes(ex_volume_ids=[provider_volume_id])

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

    def _get_provider_image_by_provider_id(self, image_id):
        provider_image = None
        provider_images = self.adpter.list_images(ex_image_ids=[image_id])
        if provider_images:
            if len(provider_images) == 1:
                provider_image = provider_images[0]
            elif len(provider_image) > 1:
                error_info = 'More then one image are found for id: %s' % image_id
                LOG.error(error_info)
                raise Exception(error_info)
            else:
                provider_image = None
        else:
            provider_image = None

        return provider_image

    def _create_node(self, provider_node_name, provider_image, provider_size):
        try:
            LOG.info('provider_interfaces: %s' % self.provider_interfaces)
            if len(self.provider_interfaces) > 1:
                LOG.debug('Create provider node, length: %s' % len(self.provider_interfaces))
                provider_node = self.adpter.create_node(name=provider_node_name,
                                                        image=provider_image,
                                                        size=provider_size,
                                                        location=self.configuration.availability_zone,
                                                        ex_network_interfaces=self.provider_interfaces)
            elif len(self.provider_interfaces) == 1:
                LOG.debug('Create provider node, length: %s' % len(self.provider_interfaces))
                provider_subnet_data_id = self.provider_interfaces[0].subnet_id
                provider_subnet_data = self.adpter.ex_list_subnets(subnet_ids=[provider_subnet_data_id])[0]
                provider_node = self.adpter.create_node(name=provider_node_name,
                                                                 image=provider_image,
                                                                 size=provider_size,
                                                                 location=self.configuration.availability_zone,
                                                                 ex_subnet=provider_subnet_data,
                                                                 ex_security_group_ids=self.provider_security_group_id)
            else:
                LOG.debug('Create provider node, length: %s' % len(self.provider_interfaces))
                provider_node = self.adpter.create_node(name=provider_node_name,
                                                                 image=provider_image,
                                                                 size=provider_size,
                                                                 location=self.configuration.availability_zone,
                                                                 ex_security_group_ids=self.provider_security_group_id)

        except Exception as e:
            LOG.error('Provider instance is booting error, exception: %s' % traceback.format_exc(e))
            provider_node = self.adpter.list_nodes(ex_filters={'tag:name':provider_node_name})
            if not provider_node:
                raise e
            raise e
        LOG.debug('create node success, provider_node: %s' % provider_node)

        node_is_ok = False
        while not node_is_ok:
            provider_nodes = self.adpter.list_nodes(ex_node_ids=[provider_node.id])
            if not provider_nodes:
                error_info = 'There is no node created in provider. node id: %s' % provider_node.id
                LOG.error(error_info)
                continue
            else:
                provider_node = provider_nodes[0]
                if provider_node.state == NodeState.RUNNING or provider_node.state == NodeState.STOPPED:
                    LOG.debug('Node %s is created, and status is: %s' % (provider_node.name, provider_node.state))
                    node_is_ok = True
            time.sleep(10)

        return provider_node

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        LOG.error('begin time of copy_image_to_volume is %s' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        image_meta = image_service.show(context, image_id)
        container_format=image_meta.get('container_format')
        if container_format == 'vgw_url':
            #1.get the provider_volume at provider cloud  
            provider_volume_id = self._get_provider_volumeid_from_volume(volume)
            retry_time = 10
            provider_volume=self._get_provider_volume(provider_volume_id)
            while retry_time > 0:
                if provider_volume and \
                   provider_volume.state == StorageVolumeState.AVAILABLE and \
                   provider_volume.extra.get('attachment_status') is None:
                    break
                else:
                    time.sleep(1)
                    provider_volume=self._get_provider_volume(provider_volume_id)
                    retry_time = retry_time-1
            try:
                #3.1 get the vgw host
                vgw_host= self._get_provider_node(self.configuration.cgw_host_id)
                if not vgw_host:
                    raise exception_ex.VgwHostNotFound(Vgw_id=self.configuration.cgw_host_id)
                device_name=self._get_next_device_name(vgw_host)
                self.adpter.attach_volume(vgw_host, provider_volume,
                                       device_name)
                #query volume status
                time.sleep(1)
                retry_time = 10
                provider_volume=self._get_provider_volume(provider_volume_id)
                while retry_time > 0:
                    if provider_volume and provider_volume.extra.get('attachment_status') =='attached':
                        break
                    else:
                        time.sleep(1)
                        provider_volume=self._get_provider_volume(provider_volume_id)
                        retry_time = retry_time-1
                LOG.error('**********************************************')
                LOG.error('the volume status %s' %provider_volume.state)
                conn=rpyc.connect(self.configuration.cgw_host_ip,int(CONF.vgw.rpc_service_port))

                copy_file_to_device_result = conn.root.copy_file_to_volume(image_id,CONF.vgw.store_file_dir,device_name)
                if not copy_file_to_device_result:
                    LOG.error("qemu-img convert %s %s failed" %(image_id,device_name))
                    self.adpter.detach_volume(provider_volume)
                    conn.close()
                    raise exception.ImageUnacceptable(
                        reason= ("copy image %s file to volume %s failed " %(image_id,volume['id'])))
                conn.close()
                self.adpter.detach_volume(provider_volume)
                retry_time = 10
                while retry_time > 0:
                    if provider_volume and provider_volume.extra.get('attachment_status') is None:
                        break
                    else:
                        time.sleep(1)
                        provider_volume=self._get_provider_volume(provider_volume_id)
                        retry_time = retry_time-1

                LOG.error('**********************************************')
                LOG.error('the volume status %s' %provider_volume.state)

            except Exception as e:
                raise e
        elif container_format == 'hybridvm':
            info = 'Create volume from image, image_id: %s' % image_id
            LOG.debug(info)
            pass

        LOG.error('end time of copy_image_to_volume is %s' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))


    def validate_connector(self, connector):
        """Fail if connector doesn't contain all the data needed by driver."""
        pass

    def clone_image(self, volume, image_location, image_id, image_meta):
        """Create a volume efficiently from an existing image.

        image_location is a string whose format depends on the
        image service backend in use. The driver should use it
        to determine whether cloning is possible.

        image_id is a string which represents id of the image.
        It can be used by the driver to introspect internal
        stores or registry to do an efficient image clone.

        image_meta is a dictionary that includes 'disk_format' (e.g.
        raw, qcow2) and other image attributes that allow drivers to
        decide whether they can clone the image without first requiring
        conversion.

        Returns a dict of volume properties eg. provider_location,
        boolean indicating whether cloning occurred
        """
        container_format=image_meta.get('container_format')
        if container_format == 'vgw_url':
            return {'provider_location': None}, False
        else:
            return {'provider_location': None}, True

    def backup_volume(self, context, backup, backup_service):
        """
        This function replace the same name function in farther class.
        so when use this volume driver, function "backup" in backup driver will be no usage.

        :param context:
        :param backup:
        :param backup_service:
        :return:
        """
        LOG.debug('context: %s' % context)
        LOG.debug('backup: %s' % backup)
        LOG.debug('backup_service: %s' % backup_service)
        LOG.debug('type of backup_service: %s' % type(backup_service))
        hybrid_volume_id = backup['volume_id']
        provider_volume = self._get_provider_volume_by_tag_hybrid_cloud_volume_id(hybrid_volume_id)

        backup_snapshot = self.adpter.create_volume_snapshot(provider_volume)
        if not backup_snapshot:
            raise Exception('can not create snapshot for backup: %s' % backup['id'])
        LOG.info("create backup_snapshot: %s for hybrid cloud volume: %s " % (backup_snapshot, hybrid_volume_id))

        self._tag_provider_snapshot_with_hybrid_cloud_backup_id(backup_snapshot, backup)

    def restore_backup(self, context, backup, volume, backup_service):
        """

        :param context:
        :param backup:
        :param volume:
        :param backup_service:
        :return:
        """
        LOG.debug('context: %s' % context)
        LOG.debug('backup: %s' % _(backup))
        LOG.debug('backup: %s ' % backup)
        LOG.debug('backup id: %s' % backup['id'])
        LOG.debug('volume: %s' % volume)
        LOG.debug('volume id: %s' % volume['id'])
        LOG.debug('volume: %s' % _(backup_service))

        hybrid_cloud_backup_id = backup['id']
        size = volume.size
        name = volume.name
        location = self._get_location()
        provider_snapshot = self._get_provider_snapshot_by_tag_hybrid_backup_id(hybrid_cloud_backup_id)

        original_provider_volume = self._get_provider_volume_by_tag_hybrid_cloud_volume_id(volume['id'])

        if provider_snapshot:
            provider_volume = self.adpter.create_volume_from_snapshot(size, name, location, provider_snapshot)
            if not provider_volume:
                raise exception_ex.ProviderCreateVolumeError(volume_id=volume['id'])
            LOG.info("create volume: %s; provider_volume: %s " % (volume['id'], provider_volume.id))

            # delete old mapped provider volume
            self.adpter.destroy_volume(original_provider_volume)
            # map new provider restored volume with hybrid cloud volume id
            self._tag_provider_volume_with_hybrid_cloud_volume_id(provider_volume, volume)
            self._add_metadata_for_hybrid_volume(volume, provider_volume)
        else:
            error_info = 'Can not find provider snapshot for hybrid cloud backup: %s' % hybrid_cloud_backup_id
            LOG.error(error_info)
            raise exception.CinderException(error_info)
        LOG.debug('end to create volume form snapshot')

    def _get_provider_snapshot_by_tag_hybrid_backup_id(self, hybrid_cloud_backup_id):
        LOG.debug('start to get provider snapshot by hybrid cloud backup id:%s' % hybrid_cloud_backup_id)
        provider_snapshots =\
            self.adpter.list_snapshots(ex_filters={'tag:hybrid_cloud_backup_id': hybrid_cloud_backup_id})
        if provider_snapshots and len(provider_snapshots) == 1:
            provider_snapshot = provider_snapshots[0]
            LOG.debug('get provider snapshot: %s' % provider_snapshot.id)
        else:
            provider_snapshot = None
        LOG.debug('End to get provider snapshot for hybrid cloud backup id:%s' % hybrid_cloud_backup_id)

        return provider_snapshot
