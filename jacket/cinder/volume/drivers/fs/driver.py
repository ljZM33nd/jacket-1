__author__ = 'Administrator'

from cinder.volume import driver
from cinder.openstack.common import log as logging
from oslo.config import cfg

from cinder.volume.drivers.fs.fs_service import OpenstackService
from cinder.volume.api import API as cinder_api


LOG = logging.getLogger(__name__)
provider_opts = [
    cfg.StrOpt('availability_zone',  help='the availability_zone for connection to fs')]
CONF = cfg.CONF
CONF.register_opts(provider_opts, 'provider_opts')


class FSVolumeDriver(driver.VolumeDriver):
    VERSION = "1.0"

    def __init__(self, *args, **kwargs):
        super(FSVolumeDriver, self).__init__( *args, **kwargs)
        self.PROVIDER_AVAILABILITY_ZONE = CONF.provider_opts.availability_zone

    def check_for_setup_error(self):
        """Check configuration file."""
        pass

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        LOG.debug('dir volume: %s' % dir(volume))
        LOG.debug('volume: %s' % volume)
        openstack_service = OpenstackService()
        sub_fs_project_id = openstack_service.keystone_service.get_tenant_id_by_tenant_name()
        LOG.debug('sub_fs_project_id: %s' % sub_fs_project_id)
        size = volume.size
        LOG.debug('size: %s' % size)
        sub_image = openstack_service.glance_service.get_sub_image_by_image_name(self._get_sub_fs_image_name(image_id))
        LOG.debug('sub_image: %s' % sub_image)
        sub_volume_name = self._get_sub_fs_volume_name(volume.display_name, volume.id)
        LOG.debug('sub_volume_name: %s' % sub_volume_name)
        volume_type_id = volume.volume_type_id
        LOG.debug('volume type id %s ' % volume_type_id)
        if volume_type_id:
            volume_type_obj = openstack_service.cinder_service.get_volume_type_by_id(volume_type_id)
            LOG.debug('dir volume_type: %s, volume_type: %s' % (volume_type_obj, volume_type_obj))
            volume_type_name = volume_type_obj.name
        else:
            volume_type_name = None
        LOG.debug('start to su create-volume task')

        description = volume.display_description
        LOG.debug('description: %s' % description)

        shareable = volume.shareable

        snapshot_id = volume.snapshot_id
        LOG.debug('snapshot id: %s' % snapshot_id)
        source_volid = volume.source_volid
        LOG.debug('source volid: %s' % source_volid)
        user_id = volume.user_id
        LOG.debug('user_id: %s' % user_id)
        metadata = volume.metadata
        LOG.debug('metadata: %s' % metadata)
        sub_volume = openstack_service.cinder_service.volume_create(size=size,
                                                                    imageRef=sub_image.id,
                                                                    display_name=sub_volume_name,
                                                                    display_description=description,
                                                                    volume_type=volume_type_name,
                                                                    availability_zone=self.PROVIDER_AVAILABILITY_ZONE,
                                                                    project_id=sub_fs_project_id,
                                                                    shareable=shareable,
                                                                    snapshot_id=snapshot_id,
                                                                    source_volid=source_volid
                                                                    )
        LOG.debug('submit create-volume task to sub fs. sub volume id: %s' % sub_volume.id)

        LOG.debug('start to wait for volume %s in status available' % sub_volume.id)
        openstack_service.cinder_service.wait_for_volume_in_specified_status(sub_volume.id, 'available', 600)

        LOG.debug('create volume %s success.' % volume.id)

    def _get_sub_fs_volume_name(self, volume_name, volume_id):
        return '@'.join([volume_name, volume_id])

    def _get_sub_fs_image_name(self, image_id):
        return '@'.join(['image', image_id])

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        pass

    def create_cloned_volume(self, volume, src_vref):
        """Create a clone of the specified volume."""
        pass

    def create_export(self, context, volume):
        """Export the volume."""
        pass

    def create_snapshot(self, snapshot):
        pass

    def create_volume(self, volume):
        LOG.debug('start to create volume')
        LOG.debug('volume glance image metadata: %s' % volume.volume_glance_metadata)
        if volume.get('image_id') is None:
            openstack = OpenstackService()
            sub_volume_name = self._get_sub_fs_volume_name(volume.display_name, volume.id)
            LOG.debug('sub_volume_name: %s' % sub_volume_name)
            description = volume.display_description
            LOG.debug('description: %s' % description)
            size = volume.size
            LOG.debug('volume size: %s' % size)
            shareable = volume.shareable
            volume_type_id = volume.volume_type_id
            LOG.debug('volume type id %s ' % volume_type_id)
            if volume_type_id:
                volume_type_obj = openstack_service.cinder_service.get_volume_type_by_id(volume_type_id)
                LOG.debug('dir volume_type: %s, volume_type: %s' % (volume_type_obj, volume_type_obj))
                volume_type_name = volume_type_obj.name
            else:
                volume_type_name = None
            sub_fs_project_id = openstack.keystone_service.get_tenant_id_by_tenant_name()
            LOG.debug('sub_fs_project_id: %s' % sub_fs_project_id)
            sub_volume = openstack.cinder_service.volume_create(size=size,
                                                                display_name=sub_volume_name,
                                                                display_description=description,
                                                                volume_type=volume_type_name,
                                                                availability_zone=self.PROVIDER_AVAILABILITY_ZONE,
                                                                project_id=sub_fs_project_id,
                                                                shareable=shareable)
            LOG.debug('submit create-volume task to sub fs. sub volume id: %s' % sub_volume.id)
            LOG.debug('start to wait for volume %s in status available' % sub_volume.id)
            openstack.cinder_service.wait_for_volume_in_specified_status(sub_volume.id, 'available', 600)
            LOG.debug('create volume %s success.' % volume.id)

        LOG.debug('end to create volume')
        return {'provider_location': 'SUB-FusionSphere'}

    def create_volume_from_snapshot(self, volume, snapshot):
        """Create a volume from a snapshot."""
        pass

    def delete_snapshot(self, snapshot):
        """Delete a snapshot."""
        pass

    def delete_volume(self, volume):
        openstack = OpenstackService()
        sub_volume_name = self._get_sub_fs_volume_name(volume.display_name, volume.id)
        LOG.debug('sub_volume_name: %s' % sub_volume_name)
        sub_volume = openstack.cinder_service.get_sub_volume_by_name(sub_volume_name)
        if sub_volume:
            LOG.debug('submit delete-volume task')
            openstack.cinder_service.volume_delete(sub_volume)
            LOG.debug('wait for volume delete')
            openstack.cinder_service.wait_for_volume_deleted(sub_volume, 600)
        else:
            LOG.debug('no sub-volume exist, no need to delete sub volume: %s' % sub_volume_name)

    def do_setup(self, context):
        """Instantiate common class and log in storage system."""
        pass

    def ensure_export(self, context, volume):
        """Synchronously recreate an export for a volume."""
        pass

    def extend_volume(self, volume, new_size):
        """Extend a volume."""
        pass

    def get_volume_stats(self, refresh=False):
        """Get volume stats."""
        # pdb.set_trace()
        if not self._stats:
            backend_name = self.configuration.safe_get('volume_backend_name')
            LOG.debug('*******backend_name is %s' %backend_name)
            if not backend_name:
                backend_name = 'FS'
            data = {'volume_backend_name': backend_name,
                    'vendor_name': 'Huawei',
                    'driver_version': self.VERSION,
                    'storage_protocol': 'LSI Logic SCSI',
                    'reserved_percentage': 0,
                    'total_capacity_gb': 1000,
                    'free_capacity_gb': 1000}
            self._stats = data
        return self._stats

    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info."""
        LOG.debug('vCloud Driver: initialize_connection')

        driver_volume_type = 'fs_clouds_volume'
        data = {}
        data['backend'] = 'fsclouds'
        data['volume_id'] = volume['id']
        data['display_name'] = volume['display_name']

        return {'driver_volume_type': driver_volume_type,
                 'data': data}

    def remove_export(self, context, volume):
        """Remove an export for a volume."""
        pass

    def terminate_connection(self, volume, connector, **kwargs):
        """Disallow connection from connector"""
        LOG.debug('vCloud Driver: terminate_connection')
        pass

    def validate_connector(self, connector):
        """Fail if connector doesn't contain all the data needed by driver."""
        LOG.debug('vCloud Driver: validate_connector')
        pass

    def attach_volume(self, context, volume, instance_uuid, host_name,
                      mountpoint):
        """Callback for volume attached to instance or host."""
        pass

    def detach_volume(self, context, volume):
        """Callback for volume detached."""
        pass
