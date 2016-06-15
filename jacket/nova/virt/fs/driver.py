__author__ = 'Administrator'

import socket
import traceback
import base64

from nova.virt import driver
from nova.openstack.common import jsonutils
from nova.compute import power_state
from nova import exception
from nova.openstack.common.context import RequestContext
from nova.virt.fs import ClientsManager
from nova.virt.fs.fs_service import OpenstackService
from nova.openstack.common import log as logging
from nova.network import neutronv2
from nova.virt.fs import hyper_agent_api
from nova.compute import vm_states
from nova.virt.fs import exception_ex
from oslo.config import cfg


LOG = logging.getLogger(__name__)

provider_opts = [
    cfg.StrOpt('availability_zone', default='us-east-1a', help='the availability_zone for connection to fs'),
    cfg.StrOpt('base_linux_image', default='ami-68d8e93a', help='use for create a base ec2 instance'),
    cfg.StrOpt('net_api', help='api net'), cfg.StrOpt('net_data', help='data subnet'),
    cfg.StrOpt('flavor_map', help=''), cfg.StrOpt('security_groups', help=''),
    cfg.StrOpt('user', help=''), cfg.StrOpt('tenant', help=''), cfg.StrOpt('pwd', help=''),
    cfg.StrOpt('auth_url', help=''), cfg.StrOpt('region', help=''), ]

hybrid_cloud_agent_opts = [
    cfg.StrOpt('tunnel_cidr', help='tunnel_cidr', default='172.28.48.0/24'),
    cfg.StrOpt('route_gw', help='route_gw', default='172.28.48.254'),
    cfg.StrOpt('personality_path', help='config file path for hybrid cloud agent',
               default='/media/metadata/userdata.txt'),
    cfg.StrOpt('rabbit_host_ip', help='rabbit host ip for hybrid cloud agent to connect with', default='172.28.0.12'),
    cfg.StrOpt('rabbit_host_user_id', help='rabbit_host_user_id'),
    cfg.StrOpt('rabbit_host_user_password', help='password of rabbit user of the rabbit host which for hybrid '
                                                 'cloud agent to connect with')]

CONF = cfg.CONF
CONF.register_opts(provider_opts, 'provider_opts')
CONF.register_opts(hybrid_cloud_agent_opts, 'hybrid_cloud_agent_opts')

FS_DOMAIN_NOSTATE = 0
FS_DOMAIN_RUNNING = 1
FS_DOMAIN_BLOCKED = 2
FS_DOMAIN_PAUSED = 3
FS_DOMAIN_SHUTDOWN = 4
FS_DOMAIN_SHUTOFF = 5
FS_DOMAIN_CRASHED = 6
FS_DOMAIN_PMSUSPENDED = 7

FS_POWER_STATE = {
    FS_DOMAIN_NOSTATE: power_state.NOSTATE,
    FS_DOMAIN_RUNNING: power_state.RUNNING,
    FS_DOMAIN_BLOCKED: power_state.RUNNING,
    FS_DOMAIN_PAUSED: power_state.PAUSED,
    FS_DOMAIN_SHUTDOWN: power_state.SHUTDOWN,
    FS_DOMAIN_SHUTOFF: power_state.SHUTDOWN,
    FS_DOMAIN_CRASHED: power_state.CRASHED,
    FS_DOMAIN_PMSUSPENDED: power_state.SUSPENDED,
}

enable_logger_help = True
logger_header = 'start to %s, args: %s, kwargs: %s'
logger_end = 'end to %s, return: %s'
def logger_helper():
    def _wrapper(func):
        def __wrapper(self, *args, **kwargs):
            try:
                if enable_logger_help:
                    LOG.debug(logger_header % (func.func_name, args, kwargs))
                result = func(self, *args, **kwargs)
                if enable_logger_help:
                    LOG.debug(logger_end % (func.func_name, result))
                return result
            except Exception, e:
                LOG.error('exception occur when execute %s, exception: %s' %
                          (func.func_name, traceback.format_exc(e)))
                raise e
        return __wrapper

    return _wrapper

class FsComputeDriver(driver.ComputeDriver):

    def __init__(self, virtapi):
        super(FsComputeDriver, self).__init__(virtapi)
        self.USER = CONF.provider_opts.user
        self.TENANT = CONF.provider_opts.tenant
        self.OS_AUTH_URL = CONF.provider_opts.auth_url
        self.PWD = CONF.provider_opts.pwd
        self.REGION = CONF.provider_opts.region
        self.PROVIDER_AVAILABILITY_ZONE = CONF.provider_opts.availability_zone
        self.PROVIDER_SECURITY_GROUPS = self._get_provider_security_groups_list()
        self.PROVIDER_NICS = self._get_provider_nics()

        self.hyper_agent_api = hyper_agent_api.HyperAgentAPI()

    def _get_sub_service_of_project_by_context(self, context):
        user, pwd, auth_url, tenant, region = self._get_auth_info_from_context(context)

        return OpenstackService(user=user, pwd=pwd, auth_url=auth_url, tenant=tenant, region=region)

    def _get_auth_info_from_context(self, context):
        #TODO
        user = self.USER
        pwd = self.PWD
        auth_url = self.OS_AUTH_URL
        tenant = self.TENANT
        region = self.REGION
        return user, pwd, auth_url, tenant, region

    def _get_sub_service_of_project(self, project_id):
        user, pwd, auth_url, tenant, region = self._get_auth_info_from_project(project_id)

        return OpenstackService(user=user, pwd=pwd, auth_url=auth_url, tenant=tenant, region=region)

    def _get_auth_info_from_project(self, project_id):
        #TODO
        user = self.USER
        pwd = self.PWD
        auth_url = self.OS_AUTH_URL
        tenant = self.TENANT
        region = self.REGION
        return user, pwd, auth_url, tenant, region

    def _get_sub_service_of_admin(self):
        return OpenstackService()

    def after_detach_volume_fail(self, job_detail_info, **kwargs):
        pass

    def after_detach_volume_success(self, job_detail_info, **kwargs):
        pass

    def attach_volume(self, context, connection_info, instance, mountpoint, disk_bus=None, device_type=None,
                      encryption=None):
        """


        :param context:
            ['auth_token',
            'elevated',
            'from_dict',
            'instance_lock_checked',
            'is_admin',
            'project_id',
            'project_name',
            'quota_class',
            'read_deleted',
            'remote_address',
            'request_id',
            'roles',
            'service_catalog',
            'tenant',
            'timestamp',
            'to_dict',
            'update_store',
            'user',
            'user_id',
            'user_name']
        :param connection_info:
            {
                u'driver_volume_type': u'vcloud_volume',
                'serial': u'824d397e-4138-48e4-b00b-064cf9ef4ed8',
                u'data': {
                    u'access_mode': u'rw',
                    u'qos_specs': None,
                    u'display_name': u'volume_02',
                    u'volume_id': u'824d397e-4138-48e4-b00b-064cf9ef4ed8',
                    u'backend': u'vcloud'
                }
            }
        :param instance:
        Instance(
            access_ip_v4=None,
            access_ip_v6=None,
            architecture=None,
            auto_disk_config=False,
            availability_zone='az01.hws--fusionsphere',
            cell_name=None,
            cleaned=False,
            config_drive='',
            created_at=2016-01-14T07: 17: 40Z,
            default_ephemeral_device=None,
            default_swap_device=None,
            deleted=False,
            deleted_at=None,
            disable_terminate=False,
            display_description='volume_backend_01',
            display_name='volume_backend_01',
            ephemeral_gb=0,
            ephemeral_key_uuid=None,
            fault=<?>,
            host='420824B8-AC4B-7A64-6B8D-D5ACB90E136A',
            hostname='volume-backend-01',
            id=57,
            image_ref='',
            info_cache=InstanceInfoCache,
            instance_type_id=2,
            kernel_id='',
            key_data=None,
            key_name=None,
            launch_index=0,
            launched_at=2016-01-14T07: 17: 43Z,
            launched_on='420824B8-AC4B-7A64-6B8D-D5ACB90E136A',
            locked=False,
            locked_by=None,
            memory_mb=512,
            metadata={

            },
            node='420824B8-AC4B-7A64-6B8D-D5ACB90E136A',
            numa_topology=<?>,
            os_type=None,
            pci_devices=<?>,
            power_state=0,
            progress=0,
            project_id='e178f1b9539b4a02a9c849dd7ea3ea9e',
            ramdisk_id='',
            reservation_id='r-marvoq8g',
            root_device_name='/dev/sda',
            root_gb=1,
            scheduled_at=None,
            security_groups=SecurityGroupList,
            shutdown_terminate=False,
            system_metadata={
                image_base_image_ref='',
                image_checksum='d972013792949d0d3ba628fbe8685bce',
                image_container_format='bare',
                image_disk_format='qcow2',
                image_image_id='617e72df-41ba-4e0d-ac88-cfff935a7dc3',
                image_image_name='cirros',
                image_min_disk='0',
                image_min_ram='0',
                image_size='13147648',
                instance_type_ephemeral_gb='0',
                instance_type_flavorid='1',
                instance_type_id='2',
                instance_type_memory_mb='512',
                instance_type_name='m1.tiny',
                instance_type_root_gb='1',
                instance_type_rxtx_factor='1.0',
                instance_type_swap='0',
                instance_type_vcpu_weight=None,
                instance_type_vcpus='1'
            },
            task_state=None,
            terminated_at=None,
            updated_at=2016-01-14T07: 17: 43Z,
            user_data=u'<SANITIZED>,
            user_id='d38732b0a8ff451eb044015e80bbaa65',
            uuid=9eef20f0-5ebf-4793-b4a2-5a667b0acad0,
            vcpus=1,
            vm_mode=None,
            vm_state='active')

        Volume object:
        {
            'status': u'attaching',
            'volume_type_id': u'type01',
            'volume_image_metadata': {
                u'container_format': u'bare',
                u'min_ram': u'0',
                u'disk_format': u'qcow2',
                u'image_name': u'cirros',
                u'image_id': u'617e72df-41ba-4e0d-ac88-cfff935a7dc3',
                u'checksum': u'd972013792949d0d3ba628fbe8685bce',
                u'min_disk': u'0',
                u'size': u'13147648'
            },
            'display_name': u'volume_02',
            'attachments': [],
            'attach_time': '',
            'availability_zone': u'az01.hws--fusionsphere',
            'bootable': True,
            'created_at': u'2016-01-18T07: 03: 57.822386',
            'attach_status': 'detached',
            'display_description': None,
            'volume_metadata': {
                u'readonly': u'False'
            },
            'shareable': u'false',
            'snapshot_id': None,
            'mountpoint': '',
            'id': u'824d397e-4138-48e4-b00b-064cf9ef4ed8',
            'size': 120
        }
        :param mountpoint: string. e.g. "/dev/sdb"
        :param disk_bus:
        :param device_type:
        :param encryption:
        :return:
        """
        LOG.debug('start to attach volume.')
        cascading_volume_id = connection_info['data']['volume_id']
        cascading_volume_name = connection_info['data']['display_name']
        su_volume_name = self._get_sub_fs_volume_name(cascading_volume_name, cascading_volume_id)
        openstack = self._get_sub_service_of_project_by_context(context)
        sub_volume = openstack.cinder_service.get_sub_volume_by_name(su_volume_name)
        if not sub_volume:
            LOG.error('Can not find volume in provider fs, volume: %s ' % cascading_volume_id)
            raise exception_ex.VolumeNotFoundAtProvider()
        sub_server = self._get_sub_fs_instance(instance)
        if not sub_server:
            LOG.error('Can not find server in provider fs, server: %s' % instance.uuid)
            raise exception_ex.ServerNotExistException(server_name=instance.display_name)
        if sub_volume.status == 'available':
            openstack.cinder_service.attach(sub_volume, sub_server.id, mountpoint)
            openstack.cinder_service.wait_for_volume_in_specified_status(sub_volume.id, 'in-use', 600)
        else:
            raise Exception('sub volume %s of volume: %s is not available, status is %s' %
                            (sub_volume.id, cascading_volume_id, sub_volume.status))
        LOG.debug('attach volume : %s success.' % cascading_volume_id)

    def _get_sub_fs_volume_name(self, volume_name, volume_id):
        return '@'.join([volume_name, volume_id])

    def destroy(self, context, instance, network_info, block_device_info=None, destroy_disks=True, migrate_data=None):
        """
        :param instance:
        :param network_info:
        :param block_device_info:
        :param destroy_disks:
        :param migrate_data:
        :return:
        """
        openstack_service = self._get_sub_service_of_project_by_context(context)
        nova_client = openstack_service.nova_service
        sub_fs_server = self._get_sub_fs_instance(instance)
        if sub_fs_server:
            nova_client.delete(sub_fs_server)
            nova_client.wait_for_delete_server_complete(sub_fs_server, 600)
        else:
            LOG.error('Can not found server to delete.')
            # raise exception_ex.ServerNotExistException(server_name=instance.display_name)

        LOG.debug('success to delete instance: %s' % instance.uuid)

    def detach_volume(self, connection_info, instance, mountpoint, encryption=None):
        LOG.debug('start to detach volume.')
        LOG.debug('instance: %s' % instance)
        LOG.debug('connection_info: %s' % connection_info)
        cascading_volume_id = connection_info['data']['volume_id']
        cascading_volume_name = connection_info['data']['display_name']
        sub_volume_name = self._get_sub_fs_volume_name(cascading_volume_name, cascading_volume_id)
        openstack = self._get_sub_service_of_project(instance.project_id)
        sub_volume = openstack.cinder_service.get_sub_volume_by_name(sub_volume_name)
        if not sub_volume:
            LOG.error('Can not find volume in provider fs, volume: %s ' % cascading_volume_id)
            raise exception_ex.VolumeNotFoundAtProvider()
        LOG.debug('sub_volume: %s' % sub_volume)
        LOG.debug('sub_volume.attachments: %s' % sub_volume.attachments)
        attachment_uuid = self._get_attachment_id_for_volume(sub_volume)
        LOG.debug('attachment_uuid: %s' % attachment_uuid)
        LOG.debug('submit detach task')
        openstack.cinder_service.detach(sub_volume, attachment_uuid)
        LOG.debug('wait for volume in available status.')
        openstack.cinder_service.wait_for_volume_in_specified_status(sub_volume.id, 'available', 600)

    def _get_attachment_id_for_volume(self, sub_volume):
        LOG.debug('start to _get_attachment_id_for_volume: %s' % sub_volume)
        attachment_id = None
        attachments = sub_volume.attachments
        LOG.debug('attachments: %s' % attachments)
        for attachment in attachments:
            volume_id = attachment.get('volume_id')
            tmp_attachment_id = attachment.get('attachment_id')
            if volume_id == sub_volume.id:
                attachment_id = tmp_attachment_id
                break
            else:
                continue

        LOG.debug('get attachment id: %s' % attachment_id)
        return attachment_id



    def get_available_nodes(self, refresh=False):
        """Returns nodenames of all nodes managed by the compute service.

        This method is for multi compute-nodes support. If a driver supports
        multi compute-nodes, this method returns a list of nodenames managed
        by the service. Otherwise, this method should return
        [hypervisor_hostname].
        """
        hostname = socket.gethostname()
        return [hostname]

    def _get_host_stats(self, hostname):
        return {'vcpus': 999999, 'vcpus_used': 0, 'memory_mb': 999999, 'memory_mb_used': 0, 'local_gb': 99999999,
                'local_gb_used': 0, 'host_memory_total': 99999999, 'disk_total': 99999999, 'host_memory_free': 99999999,
                'disk_used': 0, 'hypervisor_type': 'fusionsphere', 'hypervisor_version': '5005000',
                'hypervisor_hostname': hostname, 'cpu_info': '{"model": ["Intel(R) Xeon(R) CPU E5-2670 0 @ 2.60GHz"],'
                                                             '"vendor": ["Huawei Technologies Co., Ltd."], '
                                                             '"topology": {"cores": 16, "threads": 32}}',
                'supported_instances': jsonutils.dumps(
                    [["i686", "fs", "fusionsphere"], ["x86_64", "fs", "fusionsphere"]]), 'numa_topology': None, }

    def get_available_resource(self, nodename):
        host_stats = self._get_host_stats(nodename)

        return {'vcpus': host_stats['vcpus'], 'memory_mb': host_stats['host_memory_total'],
                'local_gb': host_stats['disk_total'], 'vcpus_used': 0,
                'memory_mb_used': host_stats['host_memory_total'] - host_stats['host_memory_free'],
                'local_gb_used': host_stats['disk_used'], 'hypervisor_type': host_stats['hypervisor_type'],
                'hypervisor_version': host_stats['hypervisor_version'],
                'hypervisor_hostname': host_stats['hypervisor_hostname'],
                'cpu_info': jsonutils.dumps(host_stats['cpu_info']),
                'supported_instances': jsonutils.dumps(host_stats['supported_instances']), 'numa_topology': None, }

    def get_info(self, instance):
        LOG.debug('get_info: %s' % instance)
        STATUS = power_state.NOSTATE
        server = self._get_sub_fs_instance(instance)
        LOG.debug('server: %s' % server)
        if server:
            instance_power_state = getattr(server, 'OS-EXT-STS:power_state')
            STATUS = FS_POWER_STATE[instance_power_state]
        LOG.debug('end to get_info: %s' % STATUS)
        return {'state': STATUS,
                'max_mem': 0,
                'mem': 0,
                'num_cpu': 1,
                'cpu_time': 0}

    def get_instance_macs(self, instance):
        """
        No need to implement.
        :param instance:
        :return:
        """
        pass

    def get_volume_connector(self, instance):
        pass

    def init_host(self, host):
        pass

    def list_instances(self):
        """List VM instances from all nodes.
        :return: list of instance id. e.g.['id_001', 'id_002', ...]
        """

        instances = []
        openstack_service = self._get_sub_service_of_admin()
        servers = openstack_service.nova_service.list()
        LOG.debug('servers: %s' % servers)
        for server in servers:
            server_id = server.id
            instances.append(server_id)

        LOG.debug('list_instance: %s' % instances)
        return instances

    def power_off(self, instance, timeout=0, retry_interval=0):
        LOG.debug('start to stop server: %s' % instance.uuid)
        server = self._get_sub_fs_instance(instance)
        if not server:
            LOG.debug('can not find sub fs server for instance: %s' % instance.uuid)
            raise exception_ex.ServerNotExistException(server_name=instance.display_name)
        LOG.debug('server: %s status is: %s' % (server.id, server.status))
        if server.status == vm_states.ACTIVE.upper():
            openstack_service = self._get_sub_service_of_project(instance.project_id)
            LOG.debug('start to add stop task')
            openstack_service.nova_service.stop(server)
            LOG.debug('submit stop task')
            openstack_service.nova_service.wait_for_server_in_specified_status(server, 'SHUTOFF', 300)
            LOG.debug('stop server: %s success' % instance.uuid)
        elif server.status == 'SHUTOFF':
            LOG.debug('sub instance status is already STOPPED.')
            LOG.debug('stop server: %s success' % instance.uuid)
            return
        else:
            LOG.warning('server status is not in ACTIVE OR STOPPED, can not do POWER_OFF operation')
            raise exception_ex.ServerStatusException(status=server.status)

    def power_on(self, context, instance, network_info,
                 block_device_info=None):
        LOG.debug('start to start server: %s' % instance.uuid)
        server = self._get_sub_fs_instance(instance)
        if not server:
            LOG.debug('can not find sub fs server for instance: %s' % instance.uuid)
            raise exception_ex.ServerNotExistException(instance.display_name)
        LOG.debug('server: %s status is: %s' % (server.id, server.status))
        if server.status == 'SHUTOFF':
            openstack_service = self._get_sub_service_of_project(instance.project_id)
            LOG.debug('start to add start task')
            openstack_service.nova_service.start(server)
            LOG.debug('submit start task')
            openstack_service.nova_service.wait_for_server_in_specified_status(server, vm_states.ACTIVE.upper(), 300)
            LOG.debug('stop server: %s success' % instance.uuid)
        elif server.status == vm_states.ACTIVE.upper():
            LOG.debug('sub instance status is already ACTIVE.')
            return
        else:
            LOG.warning('server status is not in ACTIVE OR STOPPED, can not do POWER_ON operation')
            raise exception_ex.ServerStatusException(status=server.status)

    def reboot(self, context, instance, network_info, reboot_type,
               block_device_info=None, bad_volumes_callback=None):
        LOG.debug('start to reboot server: %s' % instance.uuid)
        server = self._get_sub_fs_instance(instance)
        if not server:
            LOG.debug('can not find sub fs server for instance: %s' % instance.uuid)
            raise exception_ex.ServerNotExistException(server_name=instance.display_name)
        LOG.debug('server: %s status is: %s' % (server.id, server.status))
        if server.status == vm_states.ACTIVE.upper():
            openstack_service = self._get_sub_service_of_project(instance.project_id)
            LOG.debug('start to add reboot task')
            openstack_service.nova_service.reboot(server)
            LOG.debug('submit reboot task')
            openstack_service.nova_service.wait_for_server_in_specified_status(server, vm_states.ACTIVE.upper(), 300)
            LOG.debug('reboot server: %s success' % instance.uuid)
        elif server.status == 'SHUTOFF':
            openstack_service = self._get_sub_service_of_project(instance.project_id)
            LOG.debug('start to add reboot task')
            openstack_service.nova_service.start(server)
            LOG.debug('submit reboot task')
            openstack_service.nova_service.wait_for_server_in_specified_status(server, vm_states.ACTIVE.upper(), 300)
            LOG.debug('reboot server: %s success' % instance.uuid)
        else:
            LOG.warning('server status is not in ACTIVE OR STOPPED, can not do POWER_OFF operation')
            raise exception_ex.ServerStatusException(status=server.status)

    def resume_state_on_host_boot(self, context, instance, network_info,
                                  block_device_info=None):
        pass

    def snapshot(self, context, instance, image_id, update_task_state):
        pass

    @logger_helper()
    def spawn(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        """Create a new instance/VM/domain on the virtualization platform.

        Once this successfully completes, the instance should be
        running (power_state.RUNNING).

        If this fails, any partial instance should be completely
        cleaned up, and the virtualization platform should be in the state
        that it was before this call began.

        :param context: security context
        :param instance: nova.objects.instance.Instance
                         This function should use the data there to guide
                         the creation of the new instance.
                         Instance(
                             access_ip_v4=None,
                             access_ip_v6=None,
                             architecture=None,
                             auto_disk_config=False,
                             availability_zone='az31.shenzhen--aws',
                             cell_name=None,
                             cleaned=False,
                             config_drive='',
                             created_at=2015-08-31T02:44:36Z,
                             default_ephemeral_device=None,
                             default_swap_device=None,
                             deleted=False,
                             deleted_at=None,
                             disable_terminate=False,
                             display_description='server@daa5e17c-cb2c-4014-9726-b77109380ca6',
                             display_name='server@daa5e17c-cb2c-4014-9726-b77109380ca6',
                             ephemeral_gb=0,
                             ephemeral_key_uuid=None,
                             fault=<?>,
                             host='42085B38-683D-7455-A6A3-52F35DF929E3',
                             hostname='serverdaa5e17c-cb2c-4014-9726-b77109380ca6',
                             id=49,
                             image_ref='6004b47b-d453-4695-81be-cd127e23f59e',
                             info_cache=InstanceInfoCache,
                             instance_type_id=2,
                             kernel_id='',
                             key_data=None,
                             key_name=None,
                             launch_index=0,
                             launched_at=None,
                             launched_on='42085B38-683D-7455-A6A3-52F35DF929E3',
                             locked=False,
                             locked_by=None,
                             memory_mb=512,
                             metadata={},
                             node='h',
                             numa_topology=None,
                             os_type=None,
                             pci_devices=<?>,
                             power_state=0,
                             progress=0,
                             project_id='52957ad92b2146a0a2e2b3279cdc2c5a',
                             ramdisk_id='',
                             reservation_id='r-d1dkde4x',
                             root_device_name='/dev/sda',
                             root_gb=1,
                             scheduled_at=None,
                             security_groups=SecurityGroupList,
                             shutdown_terminate=False,
                             system_metadata={
                                 image_base_image_ref='6004b47b-d453-4695-81be-cd127e23f59e',
                                 image_container_format='bare',
                                 image_disk_format='qcow2',
                                 image_min_disk='1',
                                 image_min_ram='0',
                                 instance_type_ephemeral_gb='0',
                                 instance_type_flavorid='1',
                                 instance_type_id='2',
                                 instance_type_memory_mb='512',
                                 instance_type_name='m1.tiny',
                                 instance_type_root_gb='1',
                                 instance_type_rxtx_factor='1.0',
                                 instance_type_swap='0',
                                 instance_type_vcpu_weight=None,
                                 instance_type_vcpus='1'
                                 },
                             task_state='spawning',
                             terminated_at=None,
                             updated_at=2015-08-31T02:44:38Z,
                             user_data=u'<SANITIZED>,
                             user_id='ea4393b196684c8ba907129181290e8d',
                             uuid=92d22a62-c364-4169-9795-e5a34b5f5968,
                             vcpus=1,
                             vm_mode=None,
                             vm_state='building')
        :param image_meta: image object returned by nova.image.glance that
                           defines the image from which to boot this instance
                           e.g.
                           {
                               u'status': u'active',
                               u'deleted': False,
                               u'container_format': u'bare',
                               u'min_ram': 0,
                               u'updated_at': u'2015-08-17T07:46:48.708903',
                               u'min_disk': 0,
                               u'owner': u'52957ad92b2146a0a2e2b3279cdc2c5a',
                               u'is_public': True,
                               u'deleted_at': None,
                               u'properties': {},
                               u'size': 338735104,
                               u'name': u'emall-backend',
                               u'checksum': u'0f2294c98c7d113f0eb26ad3e76c86fa',
                               u'created_at': u'2015-08-17T07:46:20.581706',
                               u'disk_format': u'qcow2',
                               u'id': u'6004b47b-d453-4695-81be-cd127e23f59e'
                            }

        :param injected_files: User files to inject into instance.
        :param admin_password: Administrator password to set in instance.
        :param network_info:
           :py:meth:`~nova.network.manager.NetworkManager.get_instance_nw_info`
        :param block_device_info: Information about block devices to be
                                  attached to the instance.
        """

        self._binding_host(context, network_info, instance.uuid)
        self._spawn(context, instance, image_meta, injected_files,
              admin_password, network_info, block_device_info)
        self._binding_host(context, network_info, instance.uuid)

    def _spawn(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        try:
            LOG.debug('instance: %s' % instance)
            LOG.debug('block device info: %s' % block_device_info)
            openstack_service = self._get_sub_service_of_project_by_context(context)
            server_client = openstack_service.nova_service
            flavor = instance.get_flavor()
            LOG.debug('flavor: %s' % flavor)
            name = self._generate_sub_fs_instance_name(instance.display_name, instance.uuid)
            LOG.debug('name: %s' % name)
            if instance.image_ref:
                sub_image_name = self._get_sub_image_name(instance.image_ref)
                try:
                    sub_image = openstack_service.glance_service.get_sub_image_by_image_name(sub_image_name)
                    if sub_image:
                        image_ref = sub_image
                        LOG.debug('sub image is: %s' % image_ref)
                    else:
                        image_id = CONF.provider_opts.base_linux_image
                        image_ref = openstack_service.glance_service.image_get(image_id)
                        LOG.debug('No sub image exit mapping for image: %s, so use default: %s instead' %
                                  (instance.image_ref, image_ref))
                except Exception, e:
                    LOG.warning('exception occur when get image for %s, use default base image instead.' %
                                instance.image_ref)
                    image_id = CONF.provider_opts.base_linux_image
                    image_ref = openstack_service.glance_service.image_get(image_id)
                    LOG.debug('No sub image exit mapping for image: %s, so use default: %s instead' %
                              (instance.image_ref, image_ref))
                LOG.debug('image_ref: %s' % image_ref)
            else:
                image_ref = None

            metadata = self._add_agent_conf_to_metadata(instance)
            LOG.debug('metadata: %s' % metadata)
            app_security_groups = instance.security_groups
            LOG.debug('app_security_groups: %s' % app_security_groups)
            LOG.debug('injected files: %s' % injected_files)

            agent_inject_files = self._get_agent_inject_file(instance, injected_files)

            sub_bdm = self._transfer_to_sub_block_device_mapping_v2(block_device_info, openstack_service)
            LOG.debug('sub_bdm: %s' % sub_bdm)

            provider_server = server_client.create_server(name, image_ref, flavor.flavorid, meta=metadata,
                                        files=agent_inject_files, reservation_id=instance.reservation_id,
                                        security_groups=self.PROVIDER_SECURITY_GROUPS, nics=self.PROVIDER_NICS,
                                        availability_zone=self.PROVIDER_AVAILABILITY_ZONE,
                                        block_device_mapping_v2=sub_bdm)
            LOG.debug('create server job created.')
            LOG.debug('wait for server active')
            server_client.wait_for_server_in_specified_status(provider_server, vm_states.ACTIVE.upper(), timeout=600)
            LOG.debug('create server success.............!!!')

        except Exception, e:
            LOG.error('Exception when spawn, exception: %s' % traceback.format_exc(e))
            raise Exception('Exception when spawn, exception: %s' % traceback.format_exc(e))

    def _add_agent_conf_to_metadata(self, instance):
        metadata = instance.metadata
        added_meta = None
        personality_path = CONF.hybrid_cloud_agent_opts.personality_path
        tunnel_cidr = CONF.hybrid_cloud_agent_opts.tunnel_cidr
        route_gw = CONF.hybrid_cloud_agent_opts.route_gw
        if personality_path and tunnel_cidr and route_gw:
            neutron_agent_conf = {
                    "rabbit_userid": CONF.hybrid_cloud_agent_opts.rabbit_host_user_id,
                    "rabbit_password": CONF.hybrid_cloud_agent_opts.rabbit_host_user_password,
                    "rabbit_host": CONF.hybrid_cloud_agent_opts.rabbit_host_ip,
                    "host": instance.uuid,
                    "tunnel_cidr": tunnel_cidr,
                    "route_gw": route_gw
                    }
        else:
            neutron_agent_conf = {}

        if metadata:
            if neutron_agent_conf:
                added_meta = dict(metadata, **neutron_agent_conf)
            else:
                added_meta = metadata
        else:
            if neutron_agent_conf:
                added_meta = neutron_agent_conf
            else:
                added_meta = None

        return added_meta

    def _transfer_to_sub_block_device_mapping_v2(self, block_device_mapping, openstack=OpenstackService()):
        """

        :param block_device_mapping:
        {
            'block_device_mapping': [{
                'guest_format': None,
                'boot_index': 0,
                'mount_device': u'/dev/sda',
                'connection_info': {
                    u'driver_volume_type': u'fs_clouds_volume',
                    'serial': u'817492df-3e7f-439a-bfb3-6c2f6488a6e5',
                    u'data': {
                        u'access_mode': u'rw',
                        u'qos_specs': None,
                        u'display_name': u'image-v-02',
                        u'volume_id': u'817492df-3e7f-439a-bfb3-6c2f6488a6e5',
                        u'backend': u'fsclouds'
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
        :return: type list, [{
                            "boot_index": 0,
                            "uuid": "5e9ba941-7fad-4515-872a-0b2f1a05d577",
                            "volume_size": "1",
                            "device_name": "/dev/sda",
                            "source_type": "volume",
                            "volume_id": "5e9ba941-7fad-4515-872a-0b2f1a05d577",
                            "delete_on_termination": "False"}]
        """
        sub_bdms = []
        bdm_list = block_device_mapping.get('block_device_mapping')
        if bdm_list:
            for bdm in bdm_list:
                bdm_info_dict = {}
                device_name = bdm.get('mount_device')
                delete_on_termination = bdm.get('delete_on_termination')
                boot_index = bdm.get('boot_index')
                volume_id = bdm.get('connection_info').get('data').get('volume_id')
                if volume_id:
                    volume_display_name = bdm.get('connection_info').get('data').get('display_name')
                    sub_volume_name = self._get_sub_fs_volume_name(volume_display_name, volume_id)
                    sub_volume = openstack.cinder_service.get_sub_volume_by_name(sub_volume_name)
                    bdm_info_dict['boot_index'] = boot_index
                    bdm_info_dict['uuid'] = sub_volume.id
                    bdm_info_dict['volume_size'] = str(sub_volume.size)
                    bdm_info_dict['device_name'] = device_name
                    bdm_info_dict['source_type'] = 'volume'
                    bdm_info_dict['destination_type'] = 'volume'
                    bdm_info_dict['delete_on_termination'] = str(delete_on_termination)
                else:
                    #TODO: need to support snapshot id
                    continue
                sub_bdms.append(bdm_info_dict)
        else:
            sub_bdms = []

        if not sub_bdms:
            sub_bdms = None

        return sub_bdms

    def _get_provider_security_groups_list(self):
        provider_sg = CONF.provider_opts.security_groups
        return [item.strip() for item in provider_sg.split(',')]

    def _get_provider_nics(self):
        provider_net_data = CONF.provider_opts.net_data
        provider_net_api = CONF.provider_opts.net_api
        nics = [{
            'net-id': provider_net_data
        },{
            'net-id': provider_net_api
        }]

        return nics

    def _get_personality_data(self, instance):
        """

        :param instance:
        :return: string, string.  string personality_path, string personality_contents
        """
        personality_path = CONF.hybrid_cloud_agent_opts.personality_path
        tunnel_cidr = CONF.hybrid_cloud_agent_opts.tunnel_cidr
        route_gw = CONF.hybrid_cloud_agent_opts.route_gw

        if personality_path and tunnel_cidr and route_gw:
            user_data = {
                "rabbit_userid": CONF.hybrid_cloud_agent_opts.rabbit_host_user_id,
                "rabbit_password": CONF.hybrid_cloud_agent_opts.rabbit_host_user_password,
                "rabbit_host": CONF.hybrid_cloud_agent_opts.rabbit_host_ip,
                "host": instance.uuid,
                "tunnel_cidr": tunnel_cidr,
                "route_gw": route_gw
                }
            file_content = self._make_personality_content(user_data)
            #file_content = self._add_base64(file_content)
        else:
            LOG.info('personality setting incorrect, path: %s, tunnel_cidr: %s, route_gw: %s' %
                     (personality_path, tunnel_cidr, route_gw))
            personality_path = None
            file_content = None

        LOG.info('success to get personality setting, path: %s, tunnel_cidr: %s, route_gw: %s' %
                     (personality_path, tunnel_cidr, route_gw))

        return personality_path, file_content

    def _add_base64(self, contents):
        return base64.b64encode(contents)

    def _make_personality_content(self, user_data):
        file_content = ""
        for key, value in user_data.items():
            line_content = "".join(["=".join([key, value]), "\n"])
            file_content = "".join([file_content, line_content])

        return file_content

    @staticmethod
    def _binding_host(context, network_info, host_id):
        neutron = neutronv2.get_client(context, admin=True)
        port_req_body = {'port': {'binding:host_id': host_id}}
        for vif in network_info:
            neutron.update_port(vif.get('id'), port_req_body)

    def _transfer_inject_files(self, driver_param_inject_files):
        return dict(driver_param_inject_files)

    def _add_agent_conf_file_to_inject_files(self, instance, inject_files):
        """

        :param instance:
        :param inject_files: {'file_path': 'file_contents'}
        :return:
        """
        config_path, contents = self._get_personality_data(instance)
        inject_files[config_path] = contents

        return inject_files

    @logger_helper()
    def _get_agent_inject_file(self, instance,  driver_param_inject_files):
        """
        1. transfer format of inject file from [('file_path', 'file_contents')] to {'file_path': 'file_contents'}
        2. add hybrid agent config file to inject file.
           default path of hybrid agent config file is: '/home/neutron_agent_conf.txt'
           default content of hybrid agent config file is:
               {
                "rabbit_userid": CONF.hybrid_cloud_agent_opts.rabbit_host_user_id,
                "rabbit_password": CONF.hybrid_cloud_agent_opts.rabbit_host_user_password,
                "rabbit_host": CONF.hybrid_cloud_agent_opts.rabbit_host_ip,
                "host": instance.uuid,
                "tunnel_cidr": tunnel_cidr,
                "route_gw": route_gw
                }

        :param instance:
        :param driver_param_inject_files: [('file_path', 'file_contents')]
        :return: {'file_path': 'file_contents'}
        """
        inject_files = self._transfer_inject_files(driver_param_inject_files)
        # return self._add_agent_conf_file_to_inject_files(instance, inject_files)
        return inject_files

    def _generate_sub_fs_instance_name(self, instance_name, instance_id):
        """

        :param instance_name: type string
        :param instance_id: type string
        :return: type string, e.g. 'my_vm@97988012-4f48-4463-a150-d7e6b0a321d9'
        """
        return '@'.join([instance_name, instance_id])

    def _get_hybrid_instance_id(self, sub_fs_instance_name):
        """

        :param sub_fs_instance_name: type string, e.g. 'my_vm@97988012-4f48-4463-a150-d7e6b0a321d9'
        :return: type string, e.g. '97988012-4f48-4463-a150-d7e6b0a321d9'
        """
        hybrid_instance_id = None
        if '@' in sub_fs_instance_name and len(sub_fs_instance_name) > 36 and sub_fs_instance_name[-37] == '@':
            hybrid_instance_id = sub_fs_instance_name[-36:]

        return hybrid_instance_id

    @logger_helper()
    def _get_sub_fs_instance(self, hybrid_instance):
        server = None
        openstack_service = self._get_sub_service_of_project(hybrid_instance.project_id)
        server_client = openstack_service.nova_service
        sub_instance_name = self._generate_sub_fs_instance_name(hybrid_instance.display_name, hybrid_instance.uuid)
        server = server_client.get_server_by_name(sub_instance_name)

        return server

    def _get_sub_image_name(self, image_id):
        return '@'.join(['image', image_id])