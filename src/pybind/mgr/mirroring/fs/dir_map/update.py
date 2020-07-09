import errno
import pickle
import logging

import rados

from ..utils import MIRROR_OBJECT_NAME, DIRECTORY_MAP_PREFIX, \
    INSTANCE_ID_PREFIX

log = logging.getLogger(__name__)

MAX_UPDATE = 256

class UpdateDirMapRequest(object):
    def __init__(self, ioctx, update_mapping, removals, on_finish_callback):
        self.ioctx = ioctx
        self.update_mapping = update_mapping
        self.removals = removals
        self.on_finish_callback = on_finish_callback

    @staticmethod
    def omap_key(dir_name):
        return f'{DIRECTORY_MAP_PREFIX}{dir_name}'

    def send(self):
        log.info('updating image map')
        self.send_update()

    def send_update(self):
        log.debug(f'pending updates: {len(self.update_mapping)}+{len(self.removals)}')
        try:
            with rados.WriteOpCtx() as write_op:
                keys = []
                vals = []
                dir_keys = list(self.update_mapping.keys())[0:MAX_UPDATE]
                # gather updates
                for dir_name in dir_keys:
                    mapping = self.update_mapping.pop(dir_name)
                    keys.append(UpdateDirMapRequest.omap_key(dir_name))
                    vals.append(pickle.dumps(mapping))
                    self.ioctx.set_omap(write_op, tuple(keys), tuple(vals))
                # gather deletes
                slicept = MAX_UPDATE - len(dir_keys)
                removals = [UpdateDirMapRequest.omap_key(dir_name) for dir_name in self.removals[0:slicept]]
                self.removals = self.removals[slicept:]
                self.ioctx.remove_omap_keys(write_op, tuple(removals))
                log.debug(f'applying {len(keys)} updates, {len(removals)} deletes')
                self.ioctx.operate_aio_write_op(write_op, MIRROR_OBJECT_NAME, oncomplete=self.handle_update)
        except rados.Error as e:
            log.error(f'UpdateDirMapRequest.send_update exception: {e}')
            self.finish(-e.args[0])

    def handle_update(self, completion):
        r = completion.get_return_value()
        log.debug(f'handle_update: r={r}')
        if not r == 0:
            self.finish(r)
        elif self.update_mapping or self.removals:
            self.send_update()
        else:
            self.finish(0)

    def finish(self, r):
        log.info(f'finish: r={r}')
        self.on_finish_callback(r)

class UpdateInstanceRequest(object):
    def __init__(self, ioctx, instances_added, instances_removed, on_finish_callback):
        self.ioctx = ioctx
        self.instances_added = instances_added
        self.instances_removed = instances_removed
        self.on_finish_callback = on_finish_callback

    @staticmethod
    def omap_key(instance_id):
        return f'{INSTANCE_ID_PREFIX}{instance_id}'

    def send(self):
        log.info('updating instances')
        self.send_update()

    def send_update(self):
        log.debug(f'pending updates: {len(self.instances_added)}+{len(self.instances_removed)}')
        try:
            with rados.WriteOpCtx() as write_op:
                keys = []
                vals = []
                instance_ids = list(self.instances_added.keys())[0:MAX_UPDATE]
                # gather updates
                for instance_id in instance_ids:
                    data = self.instances_added.pop(instance_id)
                    keys.append(UpdateInstanceRequest.omap_key(instance_id))
                    vals.append(pickle.dumps(data))
                    self.ioctx.set_omap(write_op, tuple(keys), tuple(vals))
                # gather deletes
                slicept = MAX_UPDATE - len(instance_ids)
                removals = [UpdateInstanceRequest.omap_key(instance_id) \
                            for instance_id in self.instances_removed[0:slicept]]
                removals = []
                self.instances_removed = self.instances_removed[slicept:]
                self.ioctx.remove_omap_keys(write_op, tuple(removals))
                log.debug(f'applying {len(keys)} updates, {len(removals)} deletes')
                self.ioctx.operate_aio_write_op(write_op, MIRROR_OBJECT_NAME, oncomplete=self.handle_update)
        except rados.Error as e:
            log.error(f'UpdateInstanceRequest.send_update exception: {e}')
            self.finish(-e.args[0])

    def handle_update(self, completion):
        r = completion.get_return_value()
        log.debug(f'handle_update: r={r}')
        if not r == 0:
            self.finish(r)
        elif self.instances_added or self.instances_removed:
            self.send_update()
        else:
            self.finish(0)

    def finish(self, r):
        log.info(f'finish: r={r}')
        self.on_finish_callback(r)
