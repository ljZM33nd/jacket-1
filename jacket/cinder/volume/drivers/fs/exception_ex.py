__author__ = 'wangfeng'

from cinder.exception import *
from cinder.i18n import _

class MultiInstanceConfusion(CinderException):
    msg_fmt = _("More than one instance are found")


class MultiVolumeConfusion(CinderException):
    msg_fmt = _("More than one volume are found")


class MultiImageConfusion(CinderException):
    msg_fmt = _("More than one Image are found")


class UploadVolumeFailure(CinderException):
    msg_fmt = _("upload volume to provider cloud failure")


class VolumeNotFoundAtProvider(CinderException):
    msg_fmt = _("can not find this volume at provider cloud")


class ProviderRequestTimeOut(CinderException):
    msg_fmt = _("Time out when connect to provider cloud")

class RetryException(CinderException):
    msg_fmt = _('Need to retry, error info: %(error_info)s')


class ServerStatusException(CinderException):
    msg_fmt = _('Server status is error, status: %(status)s')


class ServerStatusTimeoutException(CinderException):
    msg_fmt = _('Server %(server_id)s status is in %(status)s over %(timeout)s seconds')

class ServerNotExistException(CinderException):
    msg_fmt = _('server named  %(server_name)s is not exist')


class ServerDeleteException(CinderException):
    msg_fmt = _('delete server %(server_id)s timeout over %(timeout)s seconds')

class VolumeCreateException(CinderException):
    msg_fmt = _('create volume %(volume_id)s error')

class VolumeStatusTimeoutException(CinderException):
    msg_fmt = _('Volume %(volume_id)s status is in %(status)s over %(timeout)s seconds')

class VolumeDeleteTimeoutException(CinderException):
    msg_fmt = _('delete volume %(volume_id)s timeout')