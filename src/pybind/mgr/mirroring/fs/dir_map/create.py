import errno
import logging

import rados

from ..exception import MirrorException
from ..utils import MIRROR_OBJECT_NAME

log = logging.getLogger(__name__)

def create_mirror_object(ioctx):
    log.info(f'creating mirror object: {MIRROR_OBJECT_NAME}')
    try:
        with rados.WriteOpCtx() as write_op:
            write_op.new(rados.LIBRADOS_CREATE_EXCLUSIVE)
            ioctx.operate_write_op(write_op, MIRROR_OBJECT_NAME)
    except rados.Error as e:
        if e.errno == errno.EEXIST:
            # be graceful
            return -e.errno
        raise MirrorException(-e.args[0], e.args[1])
