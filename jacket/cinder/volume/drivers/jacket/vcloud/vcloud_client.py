import time
import eventlet
import subprocess
from oslo.config import cfg
from oslo.utils import units

from cinder import exception
from cinder.openstack.common import log as logging
from cinder.volume.drivers.jacket.vcloud import constants
from cinder.volume.drivers.jacket.vcloud import exceptions
from cinder.volume.drivers.jacket.vcloud.vcloud import RetryDecorator
from cinder.volume.drivers.jacket.vcloud.vcloud import VCloudAPISession

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class VCloudClient(object):

    def __init__(self, scheme):
        #self._metadata_iso_catalog = CONF.vcloud.metadata_iso_catalog
        self._session = VCloudAPISession(
            host_ip=CONF.vcloud.vcloud_host_ip,
            host_port=CONF.vcloud.vcloud_host_port,
            server_username=CONF.vcloud.vcloud_host_username,
            server_password=CONF.vcloud.vcloud_host_password,
            org=CONF.vcloud.vcloud_org,
            vdc=CONF.vcloud.vcloud_vdc,
            version=CONF.vcloud.vcloud_version,
            service=CONF.vcloud.vcloud_service,
            verify=CONF.vcloud.vcloud_verify,
            service_type=CONF.vcloud.vcloud_service_type,
            retry_count=CONF.vcloud.vcloud_api_retry_count,
            create_session=True,
            scheme=scheme)

    @property
    def org(self): 
        return self._session.org

    @property
    def username(self): 
        return self._session.username

    @property
    def password(self): 
        return self._session.password

    @property
    def vdc(self): 
        return self._session.vdc

    @property
    def host_ip(self): 
        return self._session.host_ip

    def _get_vcloud_vdc(self):
        return self._invoke_api("get_vdc", self._session.vdc)

    def _get_vcloud_vapp(self, vapp_name):
        the_vapp = self._invoke_api("get_vapp",
                                    self._get_vcloud_vdc(),
                                    vapp_name)

        if not the_vapp:
            LOG.info("can't find the vapp %s" % vapp_name)
            return None
        else:
            return the_vapp

    def _invoke_api(self, method_name, *args, **kwargs):
        res = self._session.invoke_api(self._session.vca,
                                       method_name,
                                       *args, **kwargs)
        LOG.info("_invoke_api (%s, %s, %s) = %s" %
                 (method_name, args, kwargs, res))
        return res

    def _invoke_vapp_api(self, the_vapp, method_name, *args, **kwargs):
        res = self._session.invoke_api(the_vapp, method_name, *args, **kwargs)
        LOG.info("_invoke_vapp_api (%s, %s, %s) = %s" %
                 (method_name, args, kwargs, res))
        return res

    def get_disk_ref(self, disk_name):
        disk_refs = self._invoke_api('get_diskRefs',
                                     self._get_vcloud_vdc())
        link = filter(lambda link: link.get_name() == disk_name, disk_refs)
        if len(link) == 1:
            return True, link[0]
        elif len(link) == 0:
            return False, 'disk not found'
        elif len(link) > 1:
            return False, 'more than one disks found with that name.'

    def power_off_vapp(self, vapp_name):
        @RetryDecorator(max_retry_count=60,
                        exceptions=exception.CinderException)
        def _power_off(vapp_name):
            expected_vapp_status = constants.VM_POWER_OFF_STATUS
            the_vapp = self._get_vcloud_vapp(vapp_name)
            vapp_status = self._get_status_first_vm(the_vapp)
            if vapp_status == expected_vapp_status:
                return the_vapp

            task_stop = self._invoke_vapp_api(the_vapp, "undeploy")
            if not task_stop:
                raise exception.CinderException(
                    "power off vapp failed, task")
            self._session.wait_for_task(task_stop)

            retry_times = 60
            while vapp_status != expected_vapp_status and retry_times > 0:
                eventlet.greenthread.sleep(3)
                the_vapp = self._get_vcloud_vapp(vapp_name)
                vapp_status = self._get_status_first_vm(the_vapp)
                LOG.debug('During power off vapp_name: %s, %s' % (vapp_name, vapp_status))
                retry_times -= 1
            return the_vapp

        return _power_off(vapp_name)

    def _get_status_first_vm(self, the_vapp):
        children = the_vapp.me.get_Children()
        if children:
            vms = children.get_Vm()
            for vm in vms:
                return vm.get_status()
        return None

    def power_on_vapp(self, vapp_name):
        @RetryDecorator(max_retry_count=60,
                        exceptions=exception.CinderException)
        def _power_on(vapp_name):
            the_vapp = self._get_vcloud_vapp(vapp_name)

            vapp_status = self._get_status_first_vm(the_vapp)
            expected_vapp_status = constants.VM_POWER_ON_STATUS
            if vapp_status == expected_vapp_status:
                return the_vapp

            task = self._invoke_vapp_api(the_vapp, "poweron")
            if not task:
                raise exception.CinderException("power on vapp failed, task")
            self._session.wait_for_task(task)

            retry_times = 60
            while vapp_status != expected_vapp_status and retry_times > 0:
                eventlet.greenthread.sleep(3)
                the_vapp = self._get_vcloud_vapp(vapp_name)
                vapp_status = self._get_status_first_vm(the_vapp)
                LOG.debug('During power on vapp_name: %s, %s' %
                        (vapp_name, vapp_status))
                retry_times -= 1
            return the_vapp

        return _power_on(vapp_name)

    def create_vapp(self, vapp_name, template_name, network_configs, root_gb=None):
        result, task = self._session.invoke_api(self._session.vca,
                                                "create_vapp",
                                                self.vdc, vapp_name,
                                                template_name, network_configs=network_configs, root_gb=root_gb)

        # check the task is success or not
        if not result:
            raise exceptions.VCloudDriverException(
                "Create_vapp error, task:" +
                task)
        self._session.wait_for_task(task)
        the_vdc = self._session.invoke_api(self._session.vca, "get_vdc", self.vdc)

        return self._session.invoke_api(self._session.vca, "get_vapp", the_vdc, vapp_name)

    def delete_vapp(self, vapp_name):
        the_vapp = self._get_vcloud_vapp(vapp_name)
        task = self._invoke_vapp_api(the_vapp, "delete")
        if not task:
            raise exception.CinderException(
                "delete vapp failed, task: %s" % task)
        self._session.wait_for_task(task)

    def reboot_vapp(self, vapp_name):
        the_vapp = self._get_vcloud_vapp(vapp_name)
        task = self._invoke_vapp_api(the_vapp, "reboot")
        if not task:
            raise exception.CinderException(
                "reboot vapp failed, task: %s" % task)
        self._session.wait_for_task(task)

    def get_vapp_ip(self, vapp_name):
        the_vapp = self._get_vcloud_vapp(vapp_name)
        vms_network_infos = self._invoke_vapp_api(the_vapp, "get_vms_network_info")

        for vm_network_infos in vms_network_infos:
            for vm_network_info in vm_network_infos:
                if vm_network_info['ip']:
                    return vm_network_info['ip']

        return None

    def create_volume(self, disk_name, disk_size):
        result, resp = self._session.invoke_api(self._session.vca, "add_disk", self.vdc, disk_name, int(disk_size) * units.Gi)
        if result:
            self._session.wait_for_task(resp.get_Tasks().get_Task()[0])
            LOG.info('Created volume : %s sucess', disk_name)
        else:
            err_msg = 'Unable to create volume, reason: %s' % resp
            LOG.error(err_msg)
            raise exception.VolumeBackendAPIException(err_msg)

    def delete_volume(self, disk_name):
        result, resp = self._session.invoke_api(self._session.vca, "delete_disk", self.vdc, disk_name)
        if result:
            self._session.wait_for_task(resp)
            LOG.info('delete volume : %s success', disk_name)
        else:
            if resp == 'disk not found':
                LOG.warning('delete volume: unable to find volume %(name)s', {'name': disk_name})
            else:
                raise exception.VolumeBackendAPIException("Unable to delete volume %s" % disk_name)

    def attach_disk_to_vm(self, vapp_name, disk_ref):
        @RetryDecorator(max_retry_count=60,
                        exceptions=exception.CinderException)
        def _attach_disk(vapp_name, disk_ref):
            the_vapp = self._get_vcloud_vapp(vapp_name)
            task = the_vapp.attach_disk_to_vm(disk_ref)
            if not task:
                raise exception.CinderException(
                    "Unable to attach disk to vm %s" % vapp_name)
            else:
                self._session.wait_for_task(task)
                return True

        return _attach_disk(vapp_name, disk_ref)

    def detach_disk_from_vm(self, vapp_name, disk_ref):
        @RetryDecorator(max_retry_count=60,
                        exceptions=exception.CinderException)
        def _detach_disk(vapp_name, disk_ref):
            the_vapp = self._get_vcloud_vapp(vapp_name)
            task = the_vapp.detach_disk_from_vm(disk_ref)
            if not task:
                raise exception.CinderException(
                    "Unable to detach disk from vm %s" % vapp_name)
            else:
                self._session.wait_for_task(task)
                return True

        return _detach_disk(vapp_name, disk_ref)

    def get_network_configs(self, network_names):
        return self._session.invoke_api(self._session.vca, "get_network_configs", self.vdc, network_names)

    def get_network_connections(self, vapp, network_names):
        return self._session.invoke_api(vapp, "get_network_connections", network_names)

    def update_vms_connections(self, vapp, network_connections):
        result, task = self._session.invoke_api(vapp, "update_vms_connections", network_connections)

        # check the task is success or not
        if not result:
            raise exceptions.VCloudDriverException(
                "Update_vms_connections error, task:" +
                task)

        self._session.wait_for_task(task)

    def get_disk_attached_vapp(self, disk_name):
        vapps = self._invoke_api('get_disk_attached_vapp', self.vdc, disk_name)

        if len(vapps) == 1:
            return vapps[0]
        else:
            return None
