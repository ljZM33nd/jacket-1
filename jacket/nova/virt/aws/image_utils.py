# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
# Copyright (c) 2010 Citrix Systems, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Helper methods to deal with images.

This is essentially a copy from nova.virt.images.py
Some slight modifications, but at some point
we should look at maybe pushing this up to Oslo
"""
#  this file  added by liuling 

import contextlib
import math
import os
import tempfile


from oslo.config import cfg
from oslo.utils import timeutils
from oslo.utils import units

 
from nova.openstack.common import fileutils
from nova.openstack.common import imageutils
from nova.openstack.common import log as logging
from nova import utils
from nova import exception
from nova.i18n import _
 
import time
import socket
import urllib
import subprocess

LOG = logging.getLogger(__name__)

image_helper_opt = [cfg.StrOpt('image_conversion_dir',
                               default='$state_path/conversion',
                               help='Directory used for temporary storage '
                                    'during image conversion')
                    
                   ]

 
CONF = cfg.CONF
CONF.register_opts(image_helper_opt)


def qemu_img_info(path, run_as_root=True):
    """Return a object containing the parsed output from qemu-img info."""
    cmd = ('env', 'LC_ALL=C', 'qemu-img', 'info', path)
    if os.name == 'nt':
        cmd = cmd[2:]
    out, _err = utils.execute(*cmd, run_as_root=run_as_root)
    return imageutils.QemuImgInfo(out)


def convert_image(source, dest, out_format, run_as_root=True):
    """Convert image to other format."""

    cmd = ('qemu-img', 'convert',
                    '-O', out_format, source, dest)

    start_time = timeutils.utcnow()
    utils.execute(*cmd, run_as_root=run_as_root)
    duration = timeutils.delta_seconds(start_time, timeutils.utcnow())

    # NOTE(jdg): use a default of 1, mostly for unit test, but in
    # some incredible event this is 0 (cirros image?) don't barf
    if duration < 1:
        duration = 1
    fsz_mb = os.stat(source).st_size / units.Mi
    mbps = (fsz_mb / duration)
    msg = ("Image conversion details: src %(src)s, size %(sz).2f MB, "
           "duration %(duration).2f sec, destination %(dest)s")
    LOG.debug(msg % {"src": source,
                     "sz": fsz_mb,
                     "duration": duration,
                     "dest": dest})

    msg = _("Converted %(sz).2f MB image at %(mbps).2f MB/s")
    LOG.info(msg % {"sz": fsz_mb, "mbps": mbps})



def create_temporary_file(*args, **kwargs):
    if (CONF.image_conversion_dir and not
            os.path.exists(CONF.image_conversion_dir)):
        os.makedirs(CONF.image_conversion_dir)

    fd, tmp = tempfile.mkstemp(dir=CONF.image_conversion_dir, *args, **kwargs)
    os.close(fd)
    return tmp


@contextlib.contextmanager
def temporary_file(*args, **kwargs):
    tmp = None
    try:
        tmp = create_temporary_file(*args, **kwargs)
        yield tmp
    finally:
        if tmp:
            fileutils.delete_if_exists(tmp)


def temporary_dir():
    if (CONF.image_conversion_dir and not
            os.path.exists(CONF.image_conversion_dir)):
        os.makedirs(CONF.image_conversion_dir)

    return utils.tempdir(dir=CONF.image_conversion_dir)


 