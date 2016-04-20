# -*- coding:utf-8 -*-
import base64
import requests
from urllib import *
import log as logger

from nova.compute import power_state
from pyvcloud.schema.vcd.v1_5.schemas.vcloud import sessionType, queryRecordViewType

status_dict_vapp_to_instance = {
    "FAILED_CREATION": power_state.CRASHED,
    "UNRESOLVED": power_state.NOSTATE,
    "RESOLVED": power_state.NOSTATE,
    "DEPLOYED": power_state.NOSTATE,
    "SUSPENDED": power_state.SUSPENDED,
    "POWERED_ON": power_state.RUNNING,
    "WAITING_FOR_INPUT": power_state.NOSTATE,
    "UNKNOWN": power_state.NOSTATE,
    "UNRECOGNIZED": power_state.NOSTATE,
    "POWERED_OFF": power_state.SHUTDOWN,
    "INCONSISTENT_STATE": power_state.NOSTATE,
    "MIXED": power_state.NOSTATE,
    "DESCRIPTOR_PENDING": power_state.NOSTATE,
    "COPYING_CONTENTS": power_state.NOSTATE,
    "DISK_CONTENTS_PENDING": power_state.NOSTATE,
    "QUARANTINED": power_state.NOSTATE,
    "QUARANTINE_EXPIRED": power_state.NOSTATE,
    "REJECTED": power_state.NOSTATE,
    "TRANSFER_TIMEOUT": power_state.NOSTATE,
    "VAPP_UNDEPLOYED": power_state.NOSTATE,
    "VAPP_PARTIALLY_DEPLOYED": power_state.NOSTATE
}
class HCVCS(object):
    def __init__(self, host, username, org, password,
                 version="5.5", scheme="https", verify=False):
        if not (scheme == "http" or scheme == "https"):
            scheme = "https"

        self.host = host
        self.base_url = "%s://%s" % (scheme, host)
        self.username = username
        self.org = org
        self.password = password
        self.version = version
        self.verify = verify

        self.token = None
        self.query_url = None
        self._login()

    def _login(self):
        logger.info("try to login vcloud.")

        url = self.base_url + '/api/sessions'
        encode = "Basic " + base64.standard_b64encode(
            self.username + "@" + self.org + ":" + self.password)
        headers = {"Authorization": encode.rstrip(),
                   "Accept": "application/*+xml;version=" + self.version}
        response = rest_execute(method="post", url=url,
                                headers=headers, verify=self.verify)

        if not response:
            logger.error("vcloud login failed, "
                         "host: %s, username: %s, org: %s"
                         % (self.base_url, self.username, self.org))
            return False

        if response.status_code == requests.codes.ok:
            logger.info("login vcloud success.")
            self.token = response.headers["x-vcloud-authorization"]
            session = sessionType.parseString(response.content, True)
            self.query_url = filter(lambda link: link.type_ == 'application/vnd.vmware.vcloud.query.queryList+xml', session.Link)[0].href
            return True

        elif response.status_code == requests.codes.unauthorized:
            logger.error("vcloud authorize failed, "
                         "host: %s, username: %s, org: %s"
                         % (self.base_url, self.username, self.org))
            return False

        else:
            logger.error("vcloud authorize failed, "
                         "host: %s, username: %s, org: %s, response_code: %s"
                         % (self.base_url, self.username, self.org,
                            response.status_code))
            return False

    def synchronize_status(self):
        logger.debug("synchronize status.")
        if not self.query_url and not self._login():
            logger.error("can not get query_url, stop synchronize status")
            return {}

        headers = {"x-vcloud-authorization": self.token,
                   "Accept": "application/vnd.vmware.vcloud.query.records+xml;version=" + self.version}
        params = {
            'type': 'vApp',
            'page': 1,
            'pageSize': 100,
            'format': 'records'
        }

        first_page_url = self.query_url + "?" + urlencode(params)
        return self._synchronize_status_step_by_step(url=first_page_url,
                                                     headers=headers,
                                                     verify=self.verify)

    def _synchronize_status_step_by_step(self, url, headers, verify):
        this_page = {}
        response = rest_execute(method="get", url=url,
                                headers=headers, verify=verify)

        if not response or response.status_code != requests.codes.ok:
            logger.error("synchronize status step by step failed, response = %s" % response)
            if not self._login():
                logger.error("synchronize status step by step failed.")
                return this_page

            response = rest_execute(method="get", url=url,
                                    headers=headers, verify=verify)

        if not response or response.status_code != requests.codes.ok:
            logger.error("synchronize status step by step failed, response = %s" % response)
            return this_page
        else:
            content = queryRecordViewType.parseString(response.content, True)
            for vapp in content.get_Record():
                this_page[vapp.get_name()] = unify_power_state(vapp.get_status())

            next_page_link = filter(lambda link: link.rel == 'nextPage',
                                    content.Link)
            if not next_page_link:
                return this_page

            next_page_url = next_page_link[0].href
            next_page = self._synchronize_status_step_by_step(url=next_page_url,
                                                              headers=headers,
                                                              verify=verify)
            return dict(this_page, **next_page)


def rest_execute(method, url, headers, verify):
    if "get" == method:
        fun = requests.get
    elif "post" == method:
        fun = requests.post

    for i in range(3):
        try:
            response = fun(url=url, headers=headers, verify=verify)
            return response
        except Exception as e:
            logger.error("execute rest failed, "
                         "method: %s, url: %s, headers: %s, "
                         "error: %s"
                         % (method, url, headers, e.message))
    return None


def unify_power_state(vapp_status):
    if vapp_status in status_dict_vapp_to_instance:
        return status_dict_vapp_to_instance.get(vapp_status)

    return power_state.NOSTATE
