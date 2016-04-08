__author__ = 'Administrator'


from functools import wraps
from wormholeclient.client import Client
from wormholeclient import errors
from wormholeclient import constants as wormhole_constants
from exception_ex import RetryException

import logging
import time
import traceback

LOG = logging.getLogger(__name__)

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
                            mdelay = self._max_sleep_time
                if max_retries == 1:
                    msg = 'func: %s, retry times: %s, failed' % (f.__name__, str(self._max_retry_count))
                    LOG.error(msg)
                return f(*args, **kwargs)

            return f_retry

class WormHoleBusiness(object):

    def __init__(self, wormhole_service_port):
        self.wormhole_service_port = wormhole_service_port
        self.clients = self.get_clients()

    def get_clients(self):
        raise NotImplementedError()

    def get_version(self):
        docker_version = self._run_function_of_clients('get_version')
        return docker_version

    def restart_container(self, network_info, block_device_info):
        return self._run_function_of_clients('restart_container', network_info=network_info,
                                      block_device_info=block_device_info)

    def stop_container(self):
        return self._run_function_of_clients('stop_container')

    def inject_file(self, dst_path, src_path=None, file_data=None, timeout=10):
        return self._run_function_of_clients('inject_file', dst_path=dst_path, src_path=src_path,
                                             file_data=file_data, timeout=timeout)

    def list_volume(self):
        return self._run_function_of_clients('list_volume')

    def attach_volume(self, volume_id, device, mount_device, timeout=10):
        return self._run_function_of_clients('attach_volume', volume_id=volume_id,
                                             device=device, mount_device=mount_device, timeout=timeout)

    def create_image(self, image_name, image_id, timeout=10):
        return self._run_function_of_clients('create_image', image_name=image_name,
                                             image_id=image_id, timeout=timeout)

    def image_info(self, image_name, image_id):
        return self._run_function_of_clients('image_info', image_name=image_name, image_id=image_id)

    def query_task(self, task, timeout=10):
        return self._run_function_of_clients('query_task', task=task, timeout=timeout)

    @RetryDecorator(max_retry_count=50, inc_sleep_time=5, max_sleep_time=60, exceptions=(RetryException))
    def _run_function_of_clients(self, function_name, *args, **kwargs):
        result = None
        tmp_except = Exception('tmp exception when doing function: %s' % function_name)

        for client in self.clients:
            func = getattr(client, function_name)
            if func:
                try:
                    result = func(*args, **kwargs)
                    LOG.debug('Finish to execute %s' % function_name)
                    break
                except Exception, e:
                    tmp_except = e
                    continue
            else:
                raise Exception('There is not such function >%s< in wormhole client.' % function_name)

        if not result:
            LOG.debug('exception is: %s' % traceback.format_exc(tmp_except))
            raise RetryException(tmp_except.message)

        return result

class WormHoleBusinessAWS(WormHoleBusiness):
    def __init__(self, provider_node, aws_adapter, wormhole_service_port):
        self.provider_node = provider_node
        self.adapter = aws_adapter
        super(WormHoleBusinessAWS, self).__init__(wormhole_service_port)

    def get_clients(self):
        ips = self._get_node_private_ips(self.provider_node)
        return self._get_hybrid_service_client(ips, self.wormhole_service_port)

    def _get_node_private_ips(self, provider_node):
        """

        :param provider_node: type Node,
        :return: type list, return list of private ips of Node
        """
        LOG.debug('start to get node private ips for node:%s' % provider_node.name)
        private_ips = []
        interfaces = self.adapter.ex_list_network_interfaces(node=provider_node)
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

    def _get_hybrid_service_clients_by_node(self, provider_node):
        port = self.wormhole_service_port
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