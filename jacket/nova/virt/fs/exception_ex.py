__author__ = 'wangfeng'

from nova.exception import *
from nova.i18n import _

class MultiInstanceConfusion(NovaException):
    msg_fmt = _("More than one instance are found")


class MultiVolumeConfusion(NovaException):
    msg_fmt = _("More than one volume are found")

class MultiImageConfusion(NovaException):
    msg_fmt = _("More than one Image are found")

class UploadVolumeFailure(NovaException):
    msg_fmt = _("upload volume to provider cloud failure")

class VolumeNotFoundAtProvider(NovaException):
    msg_fmt = _("can not find this volume at provider cloud")

class ProviderRequestTimeOut(NovaException):
    msg_fmt = _("Time out when connect to provider cloud")

class RetryException(NovaException):
    msg_fmt = _('Need to retry, error info: %(error_info)s')

class ServerStatusException(NovaException):
    msg_fmt = _('Server status is error, status: %(status)s')

class ServerStatusTimeoutException(NovaException):
    msg_fmt = _('Server %(server_id)s status is in %(status)s over %(timeout)s seconds')

class ServerNotExistException(NovaException):
    msg_fmt = _('server named  %(server_name)s is not exist')

class ServerDeleteException(NovaException):
    msg_fmt = _('delete server %(server_id)s timeout over %(timeout)s seconds')

class VolumeCreateException(NovaException):
    msg_fmt = _('create volume %(volume_id)s error')

class VolumeStatusTimeoutException(NovaException):
    msg_fmt = _('Volume %(volume_id)s status is in %(status)s over %(timeout)s seconds')

class VolumeDeleteTimeoutException(NovaException):
    msg_fmt = _('delete volume %(volume_id)s timeout')