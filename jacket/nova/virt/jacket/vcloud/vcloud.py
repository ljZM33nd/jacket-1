# VMware vCloud Python helper
# Copyright (c) 2014 Huawei, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import requests
from threading import Lock
from StringIO import StringIO
from oslo.utils import excutils
from oslo.vmware.common import loopingcall
from requests import exceptions as requests_excep

from nova.i18n import _, _LI, _LW, _LE
from nova.openstack.common import log as logging
from nova.virt.jacket.vcloud import exceptions

from pyvcloud.helper import CommonUtils
from pyvcloud.vapp import VAPP as sdk_vapp
from pyvcloud.vcloudair import VCA as sdk_vca
from pyvcloud.schema.vcd.v1_5.schemas.vcloud import vAppType, \
    organizationListType, vdcType, catalogType, queryRecordViewType, \
    networkType, vcloudType, taskType, vAppTemplateType, vmsType
from pyvcloud.schema.vcd.v1_5.schemas.vcloud.networkType import IpScopeType,\
    OrgVdcNetworkType, ReferenceType, NetworkConfigurationType, \
    IpScopesType, IpRangesType, IpRangeType, DhcpPoolServiceType


LOG = logging.getLogger(__name__)


class VCLOUD_STATUS:
    """
     status Attribute Values for VAppTemplate, VApp, Vm, and Media Objects
    """

    FAILED_CREATION = -1
    UNRESOLVED = 0
    RESOLVED = 1
    DEPLOYED = 2
    SUSPENDED = 3
    POWERED_ON = 4
    WAITING_FOR_INPUT = 5
    UNKNOWN = 6
    UNRECOGNIZED = 7
    POWERED_OFF = 8
    INCONSISTENT_STATE = 9
    MIXED = 10
    DESCRIPTOR_PENDING = 11
    COPYING_CONTENTS = 12
    DISK_CONTENTS_PENDING = 13
    QUARANTINED = 14
    QUARANTINE_EXPIRED = 15
    REJECTED = 16
    TRANSFER_TIMEOUT = 17
    VAPP_UNDEPLOYED = 18
    VAPP_PARTIALLY_DEPLOYED = 19



def synchronized(method):
    """
    A decorator object that used to synchronized method.
    """

    def new_synchronized_method(self, *args, **kwargs):
        if hasattr(self, "_auto_lock"):
            with self._auto_lock:
                return method(self, *args, **kwargs)
        else:
            raise AttributeError("Object is missing _auto_lock")

    return new_synchronized_method

class RetryDecorator(object):
    # TODO(nkapotoxin) Use oslo_utils.excutils.py instead.

    """Decorator for retrying a function upon suggested exceptions.

    The decorated function is retried for the given number of times, and the
    sleep time between the retries is incremented until max sleep time is
    reached. If the max retry count is set to -1, then the decorated function
    is invoked indefinitely until an exception is thrown, and the caught
    exception is not in the list of suggested exceptions.
    """

    def __init__(self, max_retry_count=-1, inc_sleep_time=10,
                 max_sleep_time=10, exceptions=()):
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

        def _func(*args, **kwargs):
            func_name = f.__name__
            result = None
            try:
                if self._retry_count:
                    LOG.debug("Invoking %(func_name)s; retry count is "
                              "%(retry_count)d.",
                              {'func_name': func_name,
                               'retry_count': self._retry_count})
                result = f(*args, **kwargs)
            except self._exceptions:
                with excutils.save_and_reraise_exception() as ctxt:
                    LOG.warn(_LW("Exception which is in the suggested list of "
                                 "exceptions occurred while invoking function:"
                                 " %s."),
                             func_name,
                             exc_info=True)
                    if (self._max_retry_count != -1 and
                            self._retry_count >= self._max_retry_count):
                        LOG.error(_LE("Cannot retry upon suggested exception "
                                      "since retry count (%(retry_count)d) "
                                      "reached max retry count "
                                      "(%(max_retry_count)d)."),
                                  {'retry_count': self._retry_count,
                                   'max_retry_count': self._max_retry_count})
                    else:
                        ctxt.reraise = False
                        self._retry_count += 1
                        self._sleep_time += self._inc_sleep_time
                        return self._sleep_time
            raise loopingcall.LoopingCallDone(result)

        def func(*args, **kwargs):
            loop = loopingcall.DynamicLoopingCall(_func, *args, **kwargs)
            evt = loop.start(periodic_interval_max=self._max_sleep_time)
            LOG.debug("Waiting for function %s to return.", f.__name__)
            return evt.wait()

        return func

class NetworkConfig(object):
    def __init__(self, network_name=None, fence_mode=None, href=None):
        self._network_name = network_name
        self._fence_mode = fence_mode
        self._href = href

    @property
    def network_name(self):
        return self._network_name

    @property
    def fence_mode(self):
        return self._fence_mode

    @property
    def href(self):
        return self._href

class NetworkConnection(object):
    def __init__(self, network_name=None, ip_allocation_mode=None, ip_address=None, mac_address=None):
        self._network_name = network_name
        self._ip_allocation_mode = ip_allocation_mode
        self._ip_address = ip_address
        self._mac_address = mac_address

    @property
    def network_name(self):
        return self._network_name

    @property
    def ip_allocation_mode(self):
        return self._ip_allocation_mode

    @property
    def ip_address(self):
        return self._ip_address

    @property
    def mac_address(self):
        return self._mac_address

class VCA(sdk_vca):

    """
    Packaged Vmware vcloud python sdk vca.
    Vclouddriver just use func here.
    """

    def __init__(self, host, username, service_type='ondemand',
                 version='5.5', verify=True):

        super(VCA, self).__init__(
            host,
            username,
            service_type=service_type,
            version=version,
            verify=verify
        )

    def create_isolated_vdc_network(self, vdc_name, network_name, gateway_name,
                                    start_address, end_address, gateway_ip,
                                    netmask, dns1=None, dns2=None,
                                    dns_suffix=None):
        vdc = self.get_vdc(vdc_name)
        if vdc is None:
            LOG.error("Create isolated vdc network error, cannot get vdc:"
                      "%s info", vdc_name)
            raise exceptions.VCloudDriverException("Create isolated vdc"
                                                   " network error, cannot"
                                                   "find the vdc_name:" +
                                                   vdc_name)
        iprange = IpRangeType(StartAddress=start_address,
                              EndAddress=end_address)
        ipranges = IpRangesType(IpRange=[iprange])
        ipscope = IpScopeType(IsInherited=False,
                              Gateway=gateway_ip,
                              Netmask=netmask,
                              Dns1=dns1,
                              Dns2=dns2,
                              DnsSuffix=dns_suffix,
                              IpRanges=ipranges)
        ipscopes = IpScopesType(IpScope=[ipscope])

        configuration = NetworkConfigurationType(IpScopes=ipscopes,
                                                 FenceMode="isolated")
        net = OrgVdcNetworkType(name=network_name,
                                Description="Network created name is " +
                                network_name,
                                EdgeGateway=None, Configuration=configuration,
                                IsShared=False)
        namespacedef = 'xmlns="http://www.vmware.com/vcloud/v1.5"'
        content_type = "application/vnd.vmware.vcloud.orgVdcNetwork+xml"
        body = '<?xml version="1.0" encoding="UTF-8"?>{0}'.format(
            CommonUtils.convertPythonObjToStr(net, name='OrgVdcNetwork',
                                              namespacedef=namespacedef))
        postlink = filter(lambda link: link.get_type() == content_type,
                          vdc.get_Link())[0].href
        headers = self.vcloud_session.get_vcloud_headers()
        headers["Content-Type"] = content_type
        response = self._invoke_api(requests, 'post',
                                    postlink,
                                    data=body,
                                    headers=headers,
                                    verify=self.verify)

        if response.status_code == requests.codes.forbidden:
            raise exceptions.ForbiddenException("Create_isolated_vdc_network"
                                                "error, network_name:" +
                                                network_name)
        if response.status_code == requests.codes.created:
            network = networkType.parseString(response.content, True)
            task = network.get_Tasks().get_Task()[0]
            return (True, task)
        else:
            return (False, response.content)

    def delete_isolated_vdc_network(self, vdc_name, network_name):
        netref = self.get_isolated_network_href(vdc_name, network_name)
        if netref is None:
            return (True, None)
        vcloud_headers = self.vcloud_session.get_vcloud_headers()
        response = self._invoke_api(requests, 'delete',
                                    netref,
                                    headers=vcloud_headers,
                                    verify=self.verify)

        if response.status_code == requests.codes.forbidden:
            excep_msg = "Delete_isolated_vdc_network error, network_name:%s"\
                % (network_name)
            raise exceptions.ForbiddenException(excep_msg)
        if response.status_code == requests.codes.bad_request:
            excep_msg = "Delete_isolated_vdc_network bad request, network_name:%s"\
                % (network_name)
            raise exceptions.DeleteException(excep_msg)
        if response.status_code == requests.codes.accepted:
            task = taskType.parseString(response.content, True)
            return (True, task)
        else:
            return (False, response.content)

    def get_task_result(self, task):
        """
        Wait the task finished, and return the result
        """
        headers = self.vcloud_session.get_vcloud_headers()
        headers["Content-Type"] = "application/vnd.vmware.vcloud.task+xml"
        response = self._invoke_api(requests, 'get',
                                    task.href, headers=headers,
                                    verify=self.verify)
        if response.status_code == requests.codes.forbidden:
            excep_msg = "Get task result error, task :%s" % task
            raise exceptions.ForbiddenException(excep_msg)
        if response.status_code == requests.codes.ok:
            doc = self.parsexml_(response.content)
            return doc.attrib
        return {'status': "Error"}

    def get_isolated_network_href(self, vdc_name, network_name):
        vdc = self.get_vdc(vdc_name)
        if vdc is None:
            LOG.error("Get_isolated_network_href error, cannot get vdc:"
                      "%s info", vdc_name)
            excep_msg = "Get_isolated_network_href error, cannot find the "\
                "vdc:%s" % (vdc_name)
            raise exceptions.VCloudDriverException(excep_msg)

        link = filter(lambda link: link.get_rel() == "orgVdcNetworks",
                      vdc.get_Link())
        vcloud_headers = self.vcloud_session.get_vcloud_headers()
        response = self._invoke_api(requests, 'get',
                                    link[0].get_href(),
                                    headers=vcloud_headers,
                                    verify=self.verify)
        queryResultRecords = queryRecordViewType.parseString(response.content,
                                                             True)
        if response.status_code == requests.codes.forbidden:
            excep_msg = "Get_isolated_network_href error, network_name:%s"\
                % (network_name)
            raise exceptions.ForbiddenException(excep_msg)
        if response.status_code == requests.codes.ok:
            for record in queryResultRecords.get_Record():
                if record.name == network_name:
                    return record.href
        elif response.status_code == requests.codes.forbidden:
            excep_msg = "Get_isolated_network_href forbidden, network_name:%s"\
                % (network_name)
            raise exceptions.ForbiddenException(excep_msg)
        else:
            excep_msg = "Get_isolated_network_href failed response:%s"\
                % (response)
            raise exceptions.VCloudDriverException(excep_msg)

    def get_vdc(self, vdc_name):
        if self.vcloud_session and self.vcloud_session.organization:
            refs = filter(lambda ref: ref.name == vdc_name and ref.type_ ==
                          'application/vnd.vmware.vcloud.vdc+xml',
                          self.vcloud_session.organization.Link)
            if len(refs) == 1:
                headers = self.vcloud_session.get_vcloud_headers()
                response = self._invoke_api(requests, 'get',
                                            refs[0].href,
                                            headers=headers,
                                            verify=self.verify)
                if response.status_code == requests.codes.ok:
                    return vdcType.parseString(response.content, True)
                elif response.status_code == requests.codes.forbidden:
                    excep_msg = "Get_vdc forbidden, vdc_name:%s" % (vdc_name)
                    raise exceptions.ForbiddenException(excep_msg)
                else:
                    excep_msg = "Get_vdc failed, response:%s" % (response)
                    raise exceptions.VCloudDriverException(excep_msg)

    def get_vapp_template(self, vdc, vapp_template_name):
        resource_entities = vdc.get_ResourceEntities().get_ResourceEntity()
        return [resource_entity for resource_entity in resource_entities
                    if resource_entity.get_type() == "application/vnd.vmware.vcloud.vAppTemplate+xml" and resource_entity.get_name() == vapp_template_name ]

    def get_vapp_first_vm_href(self, vapp_template_href):
        headers = self.vcloud_session.get_vcloud_headers()
        response = self._invoke_api(requests, 'get', vapp_template_href, headers=headers, verify=self.verify)
        if response.status_code == requests.codes.ok:
            vapptemplate = vAppTemplateType.parseString(response.content, True)
            children = vapptemplate.get_Children()
            if children and children.get_Vm():
                return children.get_Vm()[0].href
            raise exceptions.VCloudDriverException("Cannot find the vm in vapptemplate %s" % (vapp_template_href))
        elif response.status_code == requests.codes.forbidden:
            excep_msg = "Get_vm from vapptemplate forbidden, vdc_name:%s" % (vdc_name)
            raise exceptions.ForbiddenException(excep_msg)
        else:
            excep_msg = "Get_vm from vapptemplate failed, response:%s" % (response)
            raise exceptions.VCloudDriverException(excep_msg)


    def get_diskRefs(self, vdc_ref):
        return super(VCA, self).get_diskRefs(vdc_ref)

    def _generate_network_config(self, network_configs):
        generated_configs = []
        for network_config in network_configs:
            generated_config = vcloudType.VAppNetworkConfigurationType(networkName = network_config.network_name)
            config = vcloudType.NetworkConfigurationType(FenceMode=network_config.fence_mode,
                                                         ParentNetwork=\
                                                             vcloudType.ReferenceType(href=network_config.href))
            generated_config.set_Configuration(config)
            generated_configs.append(generated_config)

        return generated_configs

    def _generate_instantiate_vapp_params(self, name, template_href, deploy=False, poweron=False,
                                          network_configs=[], root_gb=None):
        template_params = vcloudType.InstantiateVAppTemplateParamsType(name=name, powerOn=poweron, deploy=deploy)
        source = vcloudType.ReferenceType(href=template_href)
        template_params.set_Source(source)
        generated_configs = self._generate_network_config(network_configs)
        if len(generated_configs) >= 1:
            instantiation_params = vcloudType.InstantiationParamsType()
            network_config_section = vcloudType.NetworkConfigSectionType()
            network_config_section.original_tagname_="NetworkConfigSection"
            for generated_config in generated_configs:
                network_config_section.add_NetworkConfig(generated_config)

            instantiation_params.add_Section(network_config_section)
            template_params.set_InstantiationParams(instantiation_params)

        # modify disk
        if root_gb:
            source_vm_params = vcloudType.SourcedVmInstantiationParamsType()
            hardware_cus = vcloudType.InstantiateVmHardwareCustomizationParamsType()
            vm_href = self.get_vapp_first_vm_href(template_href)
            vm_source = vcloudType.ReferenceType(href=vm_href)
            disk_conf = vcloudType.DiskType4(instanceId="2000", Size=root_gb * 1024)
            hardware_cus.add_Disk(disk_conf)
            source_vm_params.set_HardwareCustomization(hardware_cus)
            source_vm_params.set_Source(vm_source)
            template_params.add_SourcedVmInstantiationParams(source_vm_params)

        template_params.set_AllEULAsAccepted("true")
        return template_params

    def create_vapp(self, vdc_name, vapp_name, template_name, catalog_name=None, network_configs=[], deploy=False,
                    poweron=False, root_gb=None):
        vdc = self.get_vdc(vdc_name)
        vapp_template_entity = self.get_vapp_template(vdc, template_name)
        if len(vapp_template_entity) < 1:
            raise exceptions.ForbiddenException("Create_vapp error, cannot find the template %s" % template_name)
        template_params = self._generate_instantiate_vapp_params(vapp_name, vapp_template_entity[0].href,
                                                                       deploy=deploy, poweron=poweron,
                                                                       network_configs=network_configs,
                                                                       root_gb=root_gb)
        output = StringIO()
        template_params.export(output, 0, name_ = 'InstantiateVAppTemplateParams',
                               namespacedef_ = '''xmlns="http://www.vmware.com/vcloud/v1.5" xmlns:ovf="http://schemas.dmtf.org/ovf/envelope/1"''',
                                               pretty_print = False)
        body = '<?xml version="1.0" encoding="UTF-8"?>' + \
                output.getvalue().replace('class:', 'rasd:')\
                                 .replace(' xmlns:vmw="http://www.vmware.com/vcloud/v1.5"', '')\
                                 .replace('vmw:', 'rasd:')\
                                 .replace('ovf:NetworkConfigSection', 'NetworkConfigSection')\
                                 .replace('Info>', "ovf:Info>")\
                                 .replace('<NetworkConfigSection>', '<NetworkConfigSection><ovf:Info>networks</ovf:Info>')
        content_type = "application/vnd.vmware.vcloud.instantiateVAppTemplateParams+xml"
        link = filter(lambda link: link.get_type() == content_type, vdc.get_Link())
        headers = self.vcloud_session.get_vcloud_headers()
        response = self._invoke_api(requests, 'post',
                                            link[0].href,
                                            headers=headers,
                                            data=body,
                                            verify=self.verify)
        if response.status_code == requests.codes.forbidden:
            raise exceptions.ForbiddenException("Create_vapp error, vapp_name:%s" % vapp_name)
        if response.status_code == requests.codes.created:
            vapp = vAppType.parseString(response.content, True)
            task = vapp.get_Tasks().get_Task()[0]
            return (True, task)
        else:
            return (False, response.content)

    def get_vapp(self, vdc, vapp_name):
        vapp = super(VCA, self).get_vapp(vdc, vapp_name)
    
        if not vapp:
            LOG.error("cannot get vapp %s info" % vapp_name)
            raise exceptions.ForbiddenException("cannot get vapp %s info" % vapp_name)

        return VAPP(vapp.me, vapp.headers, vapp.verify)

    def get_media(self, catalog_name, media_name):
        refs = filter(lambda ref: ref.name == catalog_name and ref.type_ == 'application/vnd.vmware.vcloud.catalog+xml', self.vcloud_session.organization.Link)
        if len(refs) == 1:
            response = self._invoke_api(requests, 'get',
                                        refs[0].get_href(), 
                                        headers=self.vcloud_session.get_vcloud_headers(),
                                        verify=self.verify)
            if response.status_code == requests.codes.ok:
                catalog = catalogType.parseString(response.content, True)
                catalog_items = filter(lambda catalogItemRef: catalogItemRef.get_name() == media_name, catalog.get_CatalogItems().get_CatalogItem())
                if len(catalog_items) == 1:
                    response = self._invoke_api(requests, 'get',
                                                catalog_items[0].get_href(),
                                                headers=self.vcloud_session.get_vcloud_headers(),
                                                verify=self.verify)
                    if response.status_code == requests.codes.ok:
                        doc = self.parsexml_(response.content)
                        for element in doc._children:
                            if element.tag == '{http://www.vmware.com/vcloud/v1.5}Entity':
                                return element.attrib
                    elif response.status_code == requests.codes.forbidden:
                        excep_msg = "Get_media forbidden,get media_ref error, media_name:%s" % (media_name)
                        raise exceptions.ForbiddenException(excep_msg)
                    else:
                        excep_msg = "Get_media failed, get media_ref error, response:%s" % (response)
                        raise exceptions.VCloudDriverException(excep_msg)
                else:
                    excep_msg = "Get_media error, catalog_items size is not one, media_name:%s" % (media_name) 
                    raise exceptions.VCloudDriverException(excep_msg)
            elif response.status_code == requests.codes.forbidden:
                excep_msg = "Get_media forbidden, media_name:%s" % (media_name)
                raise exceptions.ForbiddenException(excep_msg)
            else:
                excep_msg = "Get_media failed, response:%s" % (response)
                raise exceptions.VCloudDriverException(excep_msg)

    def get_network_ref(self, vdc_name, network_name):
        vdc = self.get_vdc(vdc_name)
        if not vdc:
            return None
        networks = vdc.get_AvailableNetworks().get_Network()
        for n in networks:
            if n.get_name() == network_name:
                return n.get_href()

    def get_network_configs(self, vdc_name, network_names):
        network_configs = []
        for n in network_names:
            # find the network ref in vcloud
            href = self.get_network_ref(vdc_name, n)
            if not href:
                raise exceptions.VCloudDriverException("Cannot find the network %s" % n)
            network_config = NetworkConfig(network_name="VM Network", fence_mode="bridged", href=href)
            network_configs.append(network_config)

        return network_configs
    
    def get_disk_attached_vapp(self, vdc_name, disk_name):
        '''
        '''

        found = False
        disks = self.get_disks(vdc_name)
        for disk, vms in disks:
            if disk.get_name() == disk_name:
                found = True
                break

        if not found:
            return []
        
        vapps = []
        for vm in vms:
            response = self._invoke_api(requests, 'get',
                            vm.get_href(), 
                            headers=self.vcloud_session.get_vcloud_headers(),
                            verify=self.verify)
            if response.status_code == requests.codes.ok:
                vm = vmsType.parseString(response.content, True)

                vapp_links = filter(lambda link: link.get_type() == "application/vnd.vmware.vcloud.vApp+xml", vm.get_Link())
                if len(vapp_links) == 1:
                    response = self._invoke_api(requests, 'get',
                                vapp_links[0].get_href(), 
                                headers=self.vcloud_session.get_vcloud_headers(),
                                verify=self.verify)
                    if response.status_code == requests.codes.ok:
                        vapp = vAppType.parseString(response.content, True)
                        vapps.append(vapp.get_name())
                    else:
                        raise exceptions.VCloudDriverException('Can not find vapp information')
            else:
                raise exceptions.VCloudDriverException('Can not find vm information')

        return vapps

    def session_is_active(self):
        """If session is not active, this will return false, else
        will return true"""
        is_active = False
        try:
            if self.vcloud_session is not None:
                is_active = self.login(
                    token=self.vcloud_session.token,
                    org=self.vcloud_session.org,
                    org_url=self.vcloud_session.org_url)
        except Exception:
            LOG.error("Session_is_active error", exc_info=True)
            return False

        return is_active


    def _invoke_api(self, module, method, *args, **kwargs):
        try:
            api_method = getattr(module, method)
            return api_method(*args, **kwargs)
        except requests_excep.SSLError as excep:
            excep_msg = ("Invoking method SSLError %(module)s.%(method)s" %
                         {'module': module, 'method': method})
            LOG.error(excep_msg, exc_info=True)
            raise exceptions.SSLError(excep_msg)
        except requests_excep.RequestException as re:
            excep_msg = ("Invoking method request error"
                         "%(module)s.%(method)s" %
                         {'module': module, 'method': method})
            LOG.error(excep_msg, exc_info=True)
            raise exceptions.VCloudDriverException(re)

    def __str__(self):
        return "VCA Object"

    def __repr__(self):
        return "VCA Object"

class VAPP(sdk_vapp):

    def __init__(self, vApp, headers, verify):
        super(VAPP, self).__init__(vApp, headers, verify)

    def get_network_connections(self, network_names):
        network_connections = []
        for n in network_names:
            network_connection = NetworkConnection(network_name=n, ip_allocation_mode="DHCP")
            network_connections.append(network_connection)

        return network_connections

    def enableDownload(self):
        headers = self.headers
        url = '%s/action/enableDownload' % self.me.get_href()
        self.response = requests.post(
            url,
            data={},
            headers=headers,
            verify=self.verify)
        if self.response.status_code == requests.codes.accepted:
            return taskType.parseString(self.response.content, True)
        elif self.response.status_code == requests.codes.forbidden:
            LOG.error("enableDownloadfailed: forbidden vapp %s" % self.name)
            raise exceptions.ForbiddenException("enableDownloadfailed: forbidden vapp %s" % self.name)
        else:
            return False

    def get_ovf_descriptor(self):
        link = filter(
            lambda link: link.get_rel() == 'download:default',
            self.me.get_Link())
        if not link:
            print "Can Not get download url"
            return

        headers = self.headers
        url = link[0].get_href()
        self.response = requests.get(url, headers=headers, verify=self.verify)
        if self.response.status_code == requests.codes.ok:
            return vAppType.parseString(self.response.content, True)
        elif self.response.status_code == requests.codes.forbidden:
            LOG.error("get_ovf_descriptor failed: forbidden. vapp %s " % self.name)
            raise exceptions.ForbiddenException("get_ovf_descriptor failed: forbidden. vapp %s" % self.name)
        else:
            return False

    def get_referenced_file_url(self, ovf):

        link = filter(
            lambda link: link.get_rel() == 'download:default',
            self.me.get_Link())
        if not link:
            print "Can Not get download url"
            return

        ref_file_id = ovf.References.File[0].anyAttributes_[
            '{http://schemas.dmtf.org/ovf/envelope/1}href']
        url = link[0].get_href()
        url = url.replace('descriptor.ovf', ref_file_id)
        return url

    def _create_network_connection(self, network_connections):
        if len(network_connections) > 1:
            network_connection_section = vcloudType.NetworkConnectionSectionType(PrimaryNetworkConnectionIndex=0)
            index = 0
            for network_connection in network_connections:
                generated_network_connection = vcloudType.NetworkConnectionType()
                generated_network_connection.set_network(network_connection.network_name)
                generated_network_connection.set_NetworkConnectionIndex(index)
                generated_network_connection.set_IpAddressAllocationMode(network_connection.ip_allocation_mode)
                generated_network_connection.set_IsConnected(True)
                if network_connection.ip_address and network_connection.ip_allocation_mode == 'MANUAL':
                    generated_network_connection.set_IpAddress(network_connection.ip_address)
                if network_connection.mac_address:
                    generated_network_connection.set_MACAddress(network_connection.mac_address)
                network_connection_section.add_NetworkConnection(generated_network_connection)
                index += 1
            return network_connection_section

    def update_vms_connections(self, network_connections):
        children = self.me.get_Children()
        if children:
            vms = children.get_Vm()
            for vm in vms:
                network_connection = self._create_network_connection(network_connections)
                output = StringIO()
                network_connection.export(output,
                    0,
                    name_ = 'NetworkConnectionSection',
                    namespacedef_ = 'xmlns="http://www.vmware.com/vcloud/v1.5" xmlns:vmw="http://www.vmware.com/vcloud/v1.5" xmlns:ovf="http://schemas.dmtf.org/ovf/envelope/1"',
                    pretty_print = True)
                body=output.getvalue().replace("vmw:Info", "ovf:Info")\
                                      .replace("<PrimaryNetworkConnectionIndex>", "<ovf:Info>Change network.</ovf:Info><PrimaryNetworkConnectionIndex>")
                response = self._invoke_api(requests, 'put',
                                            vm.get_href() + "/networkConnectionSection/",
                                            headers=self.headers,
                                            data=body,
                                            verify=self.verify)
                if response.status_code == requests.codes.forbidden:
                    raise exceptions.ForbiddenException("Update_vms_connections error")
                if response.status_code == requests.codes.accepted:
                    task = taskType.parseString(response.content, True)
                    return (True, task)
                else:
                    return (False, response.content)

    def  modify_vm_memory(self, new_size):
        children = self.me.get_Children()
        if children:
            vms = children.get_Vm()
            for vm in vms:
                sections = vm.get_Section()
                virtualHardwareSection = filter(lambda section: section.__class__.__name__== "VirtualHardwareSection_Type", sections)[0]
                items = virtualHardwareSection.get_Item()
                memory = filter(lambda item: item.get_Description().get_valueOf_() == "Memory Size", items)[0]
                href = memory.get_anyAttributes_().get('{http://www.vmware.com/vcloud/v1.5}href')
                en = memory.get_ElementName()
                en.set_valueOf_('%s MB of memory' % new_size)
                memory.set_ElementName(en)
                vq = memory.get_VirtualQuantity()
                vq.set_valueOf_(new_size)
                memory.set_VirtualQuantity(vq)
                weight = memory.get_Weight()
                weight.set_valueOf_(str(int(new_size)*10))
                memory.set_Weight(weight)
                memory_string = CommonUtils.convertPythonObjToStr(memory, 'Memory')
                output = StringIO()
                memory.export(output,
                    0,
                    name_ = 'Item',
                    namespacedef_ = 'xmlns="http://www.vmware.com/vcloud/v1.5" xmlns:ovf="http://schemas.dmtf.org/ovf/envelope/1" xmlns:rasd="http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_ResourceAllocationSettingData"',
                    pretty_print = True)
                body = output.getvalue().\
                    replace('Info msgid=""', "ovf:Info").replace("/Info", "/ovf:Info").\
                    replace("vmw:", "").replace("class:", "rasd:").replace("ResourceType", "rasd:ResourceType")
                response = self._invoke_api(requests, 'put',
                                            href,
                                            headers=self.headers,
                                            data=body,
                                            verify=self.verify)
                if response.status_code == requests.codes.accepted:
                    task = taskType.parseString(response.content, True)
                    return (True, task)
                else:
                    return (False, response.content)
        raise Exception('can\'t find vm')

    def modify_vm_cpu(self, cpus):
        children = self.me.get_Children()
        if children:
            vms = children.get_Vm()
            for vm in vms:
                sections = vm.get_Section()
                virtualHardwareSection = filter(lambda section: section.__class__.__name__== "VirtualHardwareSection_Type", sections)[0]
                items = virtualHardwareSection.get_Item()
                cpu = filter(lambda item: (item.get_anyAttributes_().get('{http://www.vmware.com/vcloud/v1.5}href') != None and item.get_anyAttributes_().get('{http://www.vmware.com/vcloud/v1.5}href').endswith('/virtualHardwareSection/cpu')), items)[0]
                href = cpu.get_anyAttributes_().get('{http://www.vmware.com/vcloud/v1.5}href')
                en = cpu.get_ElementName()
                en.set_valueOf_('%s virtual CPU(s)' % cpus)
                cpu.set_ElementName(en)
                vq = cpu.get_VirtualQuantity()
                vq.set_valueOf_(cpus)
                cpu.set_VirtualQuantity(vq)
                cpu_string = CommonUtils.convertPythonObjToStr(cpu, 'CPU')
                output = StringIO()
                cpu.export(output,
                    0,
                    name_ = 'Item',
                    namespacedef_ = 'xmlns="http://www.vmware.com/vcloud/v1.5" xmlns:ovf="http://schemas.dmtf.org/ovf/envelope/1" xmlns:rasd="http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_ResourceAllocationSettingData"',
                    pretty_print = True)
                body = output.getvalue().\
                    replace('Info msgid=""', "ovf:Info").replace("/Info", "/ovf:Info").\
                    replace("vmw:", "").replace("class:", "rasd:").replace("ResourceType", "rasd:ResourceType")
                response = self._invoke_api(requests, 'put',
                                            href,
                                            headers=self.headers,
                                            data=body,
                                            verify=self.verify)
                if response.status_code == requests.codes.accepted:
                    task = taskType.parseString(response.content, True)
                    return (True, task)
                else:
                    return (False, response.content)
        raise Exception('can\'t find vm')

    def vm_media(self, media, operation):
        """
        Return a list of details for a media device attached to the VM.
        :param media_name: (str): The name of the attached media.

        :return: (dict) a dictionary containing media details. \n
         Dictionary keys 'name','type','href'
        """
        children = self.me.get_Children()
        if children:
            vms = children.get_Vm()
            if len(vms) ==1:
                body = """
                 <MediaInsertOrEjectParams xmlns="http://www.vmware.com/vcloud/v1.5">
                     <Media
                       type="%s"
                       name="%s"
                       href="%s" />
                 </MediaInsertOrEjectParams>
                """ % (media.get('name'), media.get('id'), media.get('href'))
                return self.execute("media:%sMedia" % operation, "post", body=body, targetVM=vms[0])

    def attach_disk_to_vm(self, disk_ref):
        """
        Attach a disk volume to a VM.

        The volume must have been previously added to the VDC.

        :param disk_ref: (str): The url of a disk resource.
        :return: (TaskType) a :class:`pyvcloud.schema.vcd.v1_5.schemas.admin.vCloudEntities.TaskType` object that can be used to monitor the request.

        *Note:* A list of disk references for the vdc can be obtained using the VCA get_diskRefs() method
        """
        children = self.me.get_Children()
        if children:
            vms = children.get_Vm()
            if len(vms) ==1:
                body = """
                 <DiskAttachOrDetachParams xmlns="http://www.vmware.com/vcloud/v1.5">
                     <Disk type="application/vnd.vmware.vcloud.disk+xml"
                         href="%s" />
                 </DiskAttachOrDetachParams>
                """ % disk_ref.href
                return self.execute("disk:attach", "post", body=body, targetVM=vms[0])

    def detach_disk_from_vm(self, disk_ref):
        """
        Detach a disk volume from a VM.

        The volume must have been previously attached to the VM.

        :param disk_ref: (str): The url of a disk resource.
        :return: (TaskType) a :class:`pyvcloud.schema.vcd.v1_5.schemas.admin.vCloudEntities.TaskType` object that can be used to monitor the request.

        *Note:* A list of disk references for the vdc can be obtained using the VCA get_diskRefs() method
        """
        children = self.me.get_Children()
        if children:
            vms = children.get_Vm()
            if len(vms) ==1:
                body = """
                 <DiskAttachOrDetachParams xmlns="http://www.vmware.com/vcloud/v1.5">
                     <Disk type="application/vnd.vmware.vcloud.disk+xml"
                         href="%s" />
                 </DiskAttachOrDetachParams>
                """ % disk_ref.href
                return self.execute("disk:detach", "post", body=body, targetVM=vms[0])


    def _invoke_api(self, module, method, *args, **kwargs):
        try:
            api_method = getattr(module, method)
            return api_method(*args, **kwargs)
        except requests_excep.SSLError as excep:
            excep_msg = ("Invoking method SSLError %(module)s.%(method)s" %
                         {'module': module, 'method': method})
            LOG.error(excep_msg, exc_info=True)
            raise exceptions.SSLError(excep_msg)
        except requests_excep.RequestException as re:
            excep_msg = ("Invoking method request error"
                         "%(module)s.%(method)s" %
                         {'module': module, 'method': method})
            LOG.error(excep_msg, exc_info=True)
            raise exceptions.VCloudDriverException(re)


class VCloudAPISession(object):

    """Sets up a session with the vcloud and handles all
    the calls made to the vcloud.
    """

    def __init__(self, host_ip, host_port, server_username, server_password,
                 org, vdc, version, service, verify, service_type,
                 retry_count, create_session=True, scheme="https",
                 task_poll_interval=1):
        self._host_ip = host_ip
        self._server_username = server_username
        self._server_password = server_password
        self._org = org
        self._vdc = vdc
        self._version = version
        self._verify = verify
        self._service_type = service_type
        self._retry_count = retry_count
        self._scheme = scheme
        self._host_port = host_port
        self._session_username = None
        self._session_id = None
        self._vca = None
        self._task_poll_interval = task_poll_interval
        self._auto_lock = Lock()
        if create_session:
            self._create_session()

    @synchronized
    def _create_session(self):
        """Establish session with the server."""

        if self._session_id and self.is_current_session_active():
            LOG.debug("Current session: %s is active.",
                      self._session_id)
            return

        # Login and create new session with the server for making API calls.
        LOG.debug("Logging in with username = %s.", self._server_username)
        result = self.vca.login(password=self._server_password, org=self._org)
        if not result:
            raise exceptions.VCloudDriverException(
                "Logging error with username:%s " % self._server_username)
        result = self.vca.login(
            token=self.vca.token,
            org=self._org,
            org_url=self.vca.vcloud_session.org_url)
        if not result:
            raise exceptions.VCloudDriverException(
                "Logging error with username:%s with token " %
                self._server_username)

        self._session_id = self.vca.token

        # We need to save the username in the session since we may need it
        # later to check active session. The SessionIsActive method requires
        # the username parameter to be exactly same as that in the session
        # object. We can't use the username used for login since the Login
        # method ignores the case.
        self._session_username = self.vca.username
        LOG.info("Successfully established new session; session ID is %s.",
                 self._session_id)

    def is_current_session_active(self):
        """Check if current session is active.

        :returns: True if the session is active; False otherwise
        """
        LOG.debug("Checking if the current session: %s is active.",
                  self._session_id)

        is_active = False
        try:
            is_active = self.vca.session_is_active()
        except Exception:
            LOG.error("Check session is active error %s." % self._session_id,
                      exc_info=True)

        return is_active

    def invoke_api(self, module, method, *args, **kwargs):
        """Wrapper method for invoking APIs.

        The API call is retried in the event of exceptions due to session
        overload or connection problems.

        :param module: module corresponding to the VCA API call
        :param method: method in the module which corresponds to the
                       VCA API call
        :param args: arguments to the method
        :param kwargs: keyword arguments to the method
        :returns: response from the API call
        :raises: VCloudDriverException
        """
        @RetryDecorator(max_retry_count=self._retry_count,
                        exceptions=(exceptions.ForbiddenException,
                                    exceptions.SSLError,
                                    exceptions.DeleteException))
        def _invoke_api(module, method, *args, **kwargs):
            try:
                api_method = getattr(module, method)
                return api_method(*args, **kwargs)
            except exceptions.ForbiddenException as excep:
                # If this is due to an inactive session, we should re-create
                # the session and retry.
                if self.is_current_session_active():
                    excep_msg = "VCloud connect error while invoking method "\
                        "%s.%s." % (module, method)
                    LOG.error(excep_msg, exc_info=True)
                    raise exceptions.NoPermissionException(excep_msg)
                else:
                    LOG.warn(_LW("Re-creating session due to connection "
                                 "problems while invoking method "
                                 "%(module)s.%(method)s."),
                             {'module': module,
                              'method': method},
                             exc_info=True)
                    self._create_session()
                    raise excep

        return _invoke_api(module, method, *args, **kwargs)

    @property
    def vca(self):
        if not self._vca:
            self._vca = VCA(host=self._host_ip, username=self._server_username,
                            service_type=self._service_type,
                            version=self._version,
                            verify=self._verify)
        return self._vca

    @property
    def vdc(self):
        return self._vdc

    @property
    def username(self):
        return self._server_username

    @property
    def password(self):
        return self._server_password

    @property
    def host_ip(self):
        return self._host_ip

    @property
    def host_port(self):
        return self._host_port

    @property
    def org(self):
        return self._org

    def wait_for_task(self, task):
        """Waits for the given task to complete and returns the result.

        The task is polled until it is done. The method returns the task
        information upon successful completion. In case of any error,
        appropriate exception is raised.

        :param task: managed object reference of the task
        :returns: task info upon successful completion of the task
        :raises: VCloudDriverException
        """
        loop = loopingcall.FixedIntervalLoopingCall(self._poll_task, task)
        evt = loop.start(self._task_poll_interval)
        LOG.debug("Waiting for the task: %s to complete.", task)
        return evt.wait()

    def _poll_task(self, task):
        """Poll the given task until completion.

        If the task completes successfully, the method returns the task info
        using the input event (param done). In case of any error, appropriate
        exception is set in the event.

        :param task: managed object reference of the task
        """
        LOG.debug("Invoking VCA API to read info of task: %s.", task)
        try:
            task_info = self.invoke_api(self.vca, "get_task_result", task)
        except exceptions.VCloudDriverException:
            with excutils.save_and_reraise_exception():
                LOG.exception("Error occurred while reading info of "
                              "task: %s.", task)
        else:
            if task_info['status'] in ['queued', 'running']:
                if hasattr(task_info, 'progress'):
                    LOG.debug("Task: %(task)s progress is %(progress)s%%.",
                              {'task': task,
                               'progress': task_info.progress})
            elif task_info['status'] == 'success':
                LOG.debug("Task: %s status is success.", task)
                raise loopingcall.LoopingCallDone(task_info)
            else:
                raise exceptions.VCloudDriverException(
                    "Task execute failed, task:%s" % task)

    def wait_for_lease_ready(self, lease):
        """Waits for the given lease to be ready.

        This method return when the lease is ready. In case of any error,
        appropriate exception is raised.

        :param lease: lease to be checked for
        :raises: VCloudDriverException
        """
        pass

    def _get_error_message(self, lease):
        """Get error message associated with the given lease."""
        return "Unknown"
