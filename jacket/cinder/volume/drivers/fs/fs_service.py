__author__ = 'Administrator'

import time
import traceback

from cinder.openstack.common import log as logging
from cinder.volume.drivers.fs import exception_ex
from cinder.volume.drivers.fs import Clients

LOG = logging.getLogger(__name__)

class OpenstackService(object):

    def __init__(self, user=None, pwd=None, auth_url=None, tenant=None, region=None):
        openstack_clients = Clients(user, pwd, auth_url, tenant, region)
        self.nova_service = NovaService(openstack_clients.novaclient)
        self.glance_service = GlanceService(openstack_clients.glanceclient)
        self.cinder_service = CinderService(openstack_clients.cinderclient)
        self.keystone_service = KeystoneService(openstack_clients.keystone_client)

class NovaService(object):

    def __init__(self, nova_client):
        self.nova_client = nova_client

    def create_server(self, name, image, flavor, meta=None, files=None,
               reservation_id=None, min_count=None,
               max_count=None, security_groups=None, userdata=None,
               key_name=None, availability_zone=None,
               block_device_mapping=None, block_device_mapping_v2=None,
               nics=None, scheduler_hints=None,
               config_drive=None, disk_config=None, **kwargs):
        """
        Create (boot) a new server.

        :param name: Something to name the server.
        :param image: The :class:`Image` to boot with.
        :param flavor: The :class:`Flavor` to boot onto.
        :param meta: A dict of arbitrary key/value metadata to store for this
                     server. A maximum of five entries is allowed, and both
                     keys and values must be 255 characters or less.
        :param files: A dict of files to overrwrite on the server upon boot.
                      Keys are file names (i.e. ``/etc/passwd``) and values
                      are the file contents (either as a string or as a
                      file-like object). A maximum of five entries is allowed,
                      and each file must be 10k or less.
        :param userdata: user data to pass to be exposed by the metadata
                      server this can be a file type object as well or a
                      string.
        :param reservation_id: a UUID for the set of servers being requested.
        :param key_name: (optional extension) name of previously created
                      keypair to inject into the instance.
        :param availability_zone: Name of the availability zone for instance
                                  placement.
        :param block_device_mapping: (optional extension) A dict of block
                      device mappings for this server.
        :param block_device_mapping_v2: (optional extension) A dict of block
                      device mappings for this server.
        :param nics:  (optional extension) an ordered list of nics to be
                      added to this server, with information about
                      connected networks, fixed ips, port etc.
        :param scheduler_hints: (optional extension) arbitrary key-value pairs
                            specified by the client to help boot an instance
        :param config_drive: (optional extension) value for config drive
                            either boolean, or volume-id
        :param disk_config: (optional extension) control how the disk is
                            partitioned when the server is created.  possible
                            values are 'AUTO' or 'MANUAL'.
        """
        return self.nova_client.servers.create(name, image, flavor, meta, files,
               reservation_id, min_count,
               max_count, security_groups, userdata,
               key_name, availability_zone,
               block_device_mapping, block_device_mapping_v2,
               nics, scheduler_hints,
               config_drive, disk_config)

    def get(self, server):
        return self.nova_client.servers.get(server)

    def wait_for_server_in_specified_status(self, server, status, timeout):
        """
        :param server, type Server
        :param status, string, nova.compute.vm_states, e.g. nova.compute.vm_states.ACTIVE
        :param timeout, int, e.g. 30
        """
        LOG.debug('start to wait for server %s in status: %s' % (server.id, status))
        server = self.get(server)
        status_of_server = server.status
        start = int(time.time())
        while status_of_server != status:
            time.sleep(2)
            server = self.get(server)
            status_of_server = server.status
            cost_time = int(time.time()) - start
            LOG.debug('server: %s status is: %s, cost time: %s' % (server.id, status_of_server, str(cost_time)))
            if status_of_server == 'ERROR':
                raise exception_ex.ServerStatusException(status=status_of_server)
            if int(time.time()) - start >= timeout:
                raise exception_ex.ServerStatusTimeoutException(server_id=server.id,
                                                                status=status_of_server,
                                                                timeout=timeout)
        cost_time = int(time.time()) - start
        LOG.debug('end to wait for server in specified state, state is: %s, cost time: %s' %
                  (status_of_server, str(cost_time)))

    def list(self, detailed=True, search_opts=None, marker=None, limit=None):
        """

        :param detailed: type boolean
        :param search_opts: type dict
        :param marker:
        :param limit:
        :return:
        """
        return self.nova_client.servers.list(detailed, search_opts, marker, limit)

    def delete(self, server):
        """

        :param server: type Server
        :return:
        """
        return self.nova_client.servers.delete(server)

    def wait_for_delete_server_complete(self, server, timeout):
        start = int(time.time())
        while True:
            time.sleep(2)
            server_list = self.list(search_opts={'name':server.name})
            if server_list and len(server_list) > 0:
                cost_time = int(time.time()) - start
                if cost_time >= timeout:
                    LOG.warning('Time out for delete server: %s over %s seconds' % (server.name, timeout))
                    raise exception_ex.ServerDeleteException(server_id=server.id, timeout=timeout)
                else:
                    LOG.debug('server %s is exist, still not be deleted. cost time: %s' % (server.name, str(cost_time)))
                    continue
            else:
                cost_time = int(time.time()) - start
                LOG.debug('server %s is delete success. cost time: %s' % (server.name, str(cost_time)))
                break

    def stop(self, server):
        return self.nova_client.servers.stop(server)

    def start(self, server):
        return self.nova_client.servers.start(server)

    def get_server_by_name(self, server_name):
        server_list = self.list(search_opts={'name': server_name})
        if server_list and len(server_list) > 0:
            server = server_list[0]
        else:
            server = None

        return server


class GlanceService(object):
    def __init__(self, glance_client):
        self.glance_client = glance_client

    def image_list(self, **kwargs):
        """Retrieve a listing of Image objects

        :param **kwargs
            :param filters, type dict, support key - name, container_format ,
                disk_format , status, size_min, size_max , changes-since
            :param page_size, type int
            :param limit, typ int
        :returns generator over list of Images
            To use list(generator) to get Images list.
        Image:
            {
                u'status': u'active',
                u'tags': [],
                u'container_format': u'bare',
                u'min_ram': 0,
                u'updated_at': u'2016-05-26T02: 27: 53Z',
                u'visibility': u'public',
                u'owner': u'f56a08604f754e4d86ada827a512e65d',
                u'file': u'/v2/images/dd2a5a9f-5c44-4849-91b7-e1861c309ee0/file',
                u'min_disk': 0,
                u'id': u'dd2a5a9f-5c44-4849-91b7-e1861c309ee0',
                u'size': 13147648,
                u'name': u'cirros',
                u'checksum': u'd972013792949d0d3ba628fbe8685bce',
                u'created_at': u'2016-05-26T02: 27: 51Z',
                u'disk_format': u'qcow2',
                u'protected': False,
                u'direct_url': u'swift+https: //service%3Aswift: RAL6gWVnI3C6KM0mdHFqyUBf3icxWSxkGXtRUco9iQY%3D@identity.az43.hws--jacket.huawei.com: 443/identity-admin/v2.0/glance/dd2a5a9f-5c44-4849-91b7-e1861c309ee0',
                u'schema': u'/v2/schemas/image'
            }
        """
        return self.glance_client.images.list(**kwargs)

    def image_get(self, image_id):
        return self.glance_client.images.get(image_id)

    def image_delete(self, image_id):
        return self.glance_client.images.delete(image_id)

    def image_create(self, **kwargs):
        """

        :param name, type string, Name for the image. Note that the name of an image is not unique to an Image service
            node. The API cannot expect users to know the names of images that other users own.
        :param disk_format,The disk format of a VM image is the format of the underlying disk image.
            Virtual appliance vendors have different formats for laying out the information contained in a VM disk image.
        :param container_format, A container format defines the file format of the file
            that contains the image and metadata about the actual VM
        :param createImage , Local file path where the image is stored.

        :return:
        """
        return self.glance_client.images.create(**kwargs)

    def get_sub_image_by_image_name(self, sub_image_name):
        """

        :param sub_image_name: type string
        :return:
        {
            u'status': u'active',
            u'tags': [],
            u'container_format': u'bare',
            u'min_ram': 0,
            u'updated_at': u'2016-05-27T08: 33: 41Z',
            u'visibility': u'public',
            u'owner': u'accf4038aa564fff918fd4ed6cbcc967',
            u'file': u'/v2/images/5c6dc2df-ac84-4787-bada-6032519a6cb5/file',
            u'min_disk': 0,
            u'id': u'5c6dc2df-ac84-4787-bada-6032519a6cb5',
            u'size': 13147648,
            u'name': u'image@dd2a5a9f-5c44-4849-91b7-e1861c309ee0',
            u'checksum': u'd972013792949d0d3ba628fbe8685bce',
            u'created_at': u'2016-05-27T08: 30: 33Z',
            u'disk_format': u'qcow2',
            u'protected': False,
            u'direct_url': u'swift+https: //service%3Aswift: CzbJmCklDpVqvDV5%2BhHPt1%2Bx4wjFHD4FnHdH3cfliTE%3D@identity.az01.sz--fusionsphere.huawei.com: 443/identity-admin/v2.0/glance/5c6dc2df-ac84-4787-bada-6032519a6cb5',
            u'schema': u'/v2/schemas/image'
        }
        """
        sub_image = None
        image_list = list(self.image_list(filters={'name':sub_image_name}))
        if image_list and len(image_list) > 0:
            sub_image = image_list[0]
        else:
            sub_image = None

        LOG.debug('sub-image: %s' % sub_image)
        return sub_image


class CinderService(object):
    def __init__(self, cinder_client):
        self.cinder_client = cinder_client

    def volume_create(self, size, snapshot_id=None, source_volid=None,
               display_name=None, display_description=None,
               volume_type=None, user_id=None,
               project_id=None, availability_zone=None,
               metadata=None, imageRef=None, shareable=False):
        return self.cinder_client.volumes.create(size, snapshot_id, source_volid,
               display_name, display_description,
               volume_type, user_id,
               project_id, availability_zone,
               metadata, imageRef, shareable)

    def volume_get(self, volume_id):
        """Get a volume.

        :param volume_id: The ID of the volume to get.
        :rtype: :class:`Volume`
        """
        return self.cinder_client.volumes.get(volume_id)

    def volumes_list(self, detailed=True, search_opts=None):
        """Lists all volumes.

        :param detailed: Whether to return detailed volume info.
        :param search_opts: Search options to filter out volumes.
        :param marker: Begin returning volumes that appear later in the volume
                       list than that represented by this volume id.
        :param limit: Maximum number of volumes to return.
        :param sort_key: Key to be sorted.
        :param sort_dir: Sort direction, should be 'desc' or 'asc'.
        :rtype: list of :class:`Volume`
        """
        return self.cinder_client.volumes.list(detailed, search_opts)

    def volume_delete(self, volume):
        """Delete a volume.

        :param volume: The :class:`Volume` to delete.
        """
        return self.cinder_client.volumes.delete( volume)

    def get_sub_volume_by_name(self, volume_name):
        volume_list = self.volumes_list(search_opts={'name': volume_name})
        if volume_list and len(volume_list) > 0:
            sub_volume = volume_list[0]
        else:
            sub_volume = None

        return sub_volume

    def wait_for_volume_in_specified_status(self, volume_id, status, timeout):
        volume = self.volume_get(volume_id)
        status_of_volume = volume.status
        start = int(time.time())
        while status_of_volume != status:
            time.sleep(2)
            volume = self.volume_get(volume_id)
            status_of_volume = volume.status
            cost_time = int(time.time()) - start
            LOG.debug('volume: %s status is: %s, cost time: %s' % (volume.id, status_of_volume, str(cost_time)))
            if status_of_volume == 'ERROR':
                raise exception_ex.VolumeCreateException(volume_id=volume.id)
            if int(time.time()) - start >= timeout:
                raise exception_ex.VolumeStatusTimeoutException(volume_id=volume.id,
                                                                status=status_of_volume,
                                                                timeout=timeout)
        cost_time = int(time.time()) - start
        LOG.debug('end to wait for volume in specified state, state is: %s, cost time: %s' %
                  (status_of_volume, str(cost_time)))

    def get_volume_type_by_id(self, volume_type_id):
        volume_type = self.cinder_client.volume_types.get(volume_type_id)
        return volume_type

    def wait_for_volume_deleted(self, volume, timeout):
        start = int(time.time())
        while True:
            time.sleep(2)
            try:
                volume = self.volume_get(volume.id)
                status_of_volume = volume.status
                cost_time = int(time.time()) - start
                LOG.debug('volume: %s status is: %s, cost time: %s' % (volume.id, status_of_volume, str(cost_time)))
            except Exception, e:
                LOG.debug('volume: %s is deleted' % volume.id)
                LOG.debug('exception: %s' % traceback.format_exc(e))
                break
            if int(time.time()) - start >= timeout:
                raise exception_ex.VolumeDeleteTimeoutException(volume_id=volume.id)


class KeystoneService(object):

    def __init__(self, keystone_client):
        self.keystoneclient = keystone_client

    def get_tenant_by_tenant_name(self, tenant_name):
        tenant = None
        tenants = self.keystoneclient.tenants.list()

        if tenants is None:
            LOG.debug('No any tenant in keystone.')
        else:
            for tmp_tenant in tenants:
                if tmp_tenant.name == tenant_name:
                    tenant = tmp_tenant
                    break
                else:
                    continue

        return tenant

    def get_tenant_id_by_tenant_name(self, tenant_name=None):
        LOG.debug('start to get_tenant_id_by_tenant_name, tenant_name: %s' % tenant_name)
        if not tenant_name:
            tenant_name = self.keystoneclient.tenant_name
        tenant = self.get_tenant_by_tenant_name(tenant_name)

        if tenant:
            tenant_id = tenant.id
        else:
            tenant_id = None

        LOG.debug('end to get tenant id: %s by tenant_name: %s' % (tenant.id, tenant_name))
        return tenant.id

if __name__ == '__main__':
    openstack_service = OpenstackService()
    server_list = openstack_service.nova_service.list(search_opts={'name': ''})
    import pdb;pdb.set_trace()
    if server_list:
        server = server_list[0]
        print server
        print dir(server)