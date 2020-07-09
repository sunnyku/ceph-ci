import errno
import json
import logging
import os
import pickle
import threading
import uuid

import cephfs
import rados

from mgr_util import RTimer
from .notify import Notifier, InstanceWatcher
from .utils import INSTANCE_ID_PREFIX, MIRROR_OBJECT_NAME, Finisher, \
    AsyncOpTracker, connect_to_filesystem, disconnect_from_filesystem
from .exception import MirrorException
from .dir_map.create import create_mirror_object
from .dir_map.load import load_dir_map, load_instances
from .dir_map.update import UpdateDirMapRequest, UpdateInstanceRequest
from .dir_map.policy import Policy
from .dir_map.state_transition import ActionType

log = logging.getLogger(__name__)

CEPHFS_IMAGE_POLICY_UPDATE_THROTTLE_INTERVAL = 1

class FSPolicy(object):
    class InstanceListener(InstanceWatcher.Listener):
        def __init__(self, fspolicy):
            self.fspolicy = fspolicy

        def handle_instances(self, added, removed):
            self.fspolicy.update_instances(added, removed)

    def __init__(self, ioctx):
        self.ioctx = ioctx
        self.pending = []
        self.policy = Policy()
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.dir_names = []
        self.async_requests = {}
        self.finisher = Finisher()
        self.op_tracker = AsyncOpTracker()
        self.notifier = Notifier(ioctx)
        self.instance_listener = FSPolicy.InstanceListener(self)
        self.instance_watcher = None
        self.stopping = threading.Event()
        self.timer_task = RTimer(CEPHFS_IMAGE_POLICY_UPDATE_THROTTLE_INTERVAL,
                                 self.process_updates)
        self.timer_task.start()

    def schedule_action(self, dir_names):
        self.dir_names.extend(dir_names)

    def init(self, dir_mapping, instances):
        with self.lock:
            self.policy.init(dir_mapping)
            # we'll schedule action for all directories, so don't bother capturing
            # directory names here.
            self.policy.add_instances(list(instances.keys()), initial_update=True)
            self.instance_watcher = InstanceWatcher(self.ioctx, instances,
                                                    self.instance_listener)
            self.schedule_action(list(dir_mapping.keys()))

    def shutdown(self):
        with self.lock:
            log.debug('FSPolicy.shutdown')
            self.stopping.set()
            log.debug('canceling update timer task')
            self.timer_task.cancel()
            log.debug('update timer task canceled')
            if self.instance_watcher:
                log.debug('stopping instance watcher')
                self.instance_watcher.stop()
        self.op_tracker.wait_for_ops()
        log.debug('FSPolicy.shutdown done')

    def handle_update_mapping(self, updates, removals, request_id, callback, r):
        log.info(f'handle_update_mapping: {updates} {removals} {request_id} {callback} {r}')
        with self.lock:
            try:
                self.async_requests.pop(request_id)
                if callback:
                    callback(updates, removals, r)
            finally:
                self.op_tracker.finish_async_op()

    def handle_update_instances(self, instances_added, instances_removed, request_id, r):
        log.info(f'handle_update_instances: {instances_added} {instances_removed} {request_id} {r}')
        with self.lock:
            try:
                self.async_requests.pop(request_id)
                if self.stopping.is_set():
                    log.debug(f'handle_update_instances: policy shutting down')
                    return
                schedules = []
                if instances_removed:
                    schedules.extend(self.policy.remove_instances(instances_removed))
                if instances_added:
                    schedules.extend(self.policy.add_instances(instances_added))
                self.schedule_action(schedules)
            finally:
                self.op_tracker.finish_async_op()

    def update_mapping(self, update_map, removals, callback=None):
        log.info(f'updating directory map: {len(update_map)}+{len(removals)} updates')
        request_id = str(uuid.uuid4())
        def async_callback(r):
            self.finisher.queue(self.handle_update_mapping,
                                [list(update_map.keys()), removals, request_id, callback, r])
        request = UpdateDirMapRequest(self.ioctx, update_map.copy(), removals.copy(), async_callback)
        self.async_requests[request_id] = request
        self.op_tracker.start_async_op()
        log.debug(f'async request_id: {request_id}')
        request.send()

    def update_instances(self, added, removed):
        logging.debug(f'update_instances: added={added}, removed={removed}')
        with self.lock:
            instances_added = {}
            instances_removed = []
            for instance_id, data in added.items():
                instances_added[instance_id] = {'version': 1, 'addr': data}
            instances_removed = list(removed.keys())
            request_id = str(uuid.uuid4())
            def async_callback(r):
                self.finisher.queue(self.handle_update_instances,
                                    [list(instances_added.keys()), instances_removed, request_id, r])
            # blacklisted instances can be removed at this point. remapping directories
            # mapped to blacklisted instances on module startup is handled in policy
            # add_instances().
            request = UpdateInstanceRequest(self.ioctx, instances_added.copy(),
                                            instances_removed.copy(), async_callback)
            self.async_requests[request_id] = request
            log.debug(f'async request_id: {request_id}')
            self.op_tracker.start_async_op()
            request.send()

    def continue_action(self, updates, removals, r):
        log.debug(f'continuing action: {updates}+{removals} r={r}')
        if self.stopping.is_set():
            log.debug('continue_action: policy shutting down')
            return
        schedules = []
        for dir_name in updates:
            schedule = self.policy.finish_action(dir_name, r)
            if schedule:
                schedules.append(dir_name)
        for dir_name in removals:
            schedule = self.policy.finish_action(dir_name, r)
            if schedule:
                schedules.append(dir_name)
        self.schedule_action(schedules)

    def handle_peer_ack(self, dir_name, r):
        log.info(f'handle_peer_ack: {dir_name} r={r}')
        with self.lock:
            try:
                if self.stopping.is_set():
                    log.debug(f'handle_peer_ack: policy shutting down')
                    return
                self.continue_action([dir_name], [], r)
            finally:
                self.op_tracker.finish_async_op()

    def process_updates(self):
        def acquire_message(dir_name):
            return json.dumps({'dir_name': dir_name,
                               'mode': 'acquire'
                               })
        def release_message(dir_name):
            return json.dumps({'dir_name': dir_name,
                               'mode': 'release'
                               })
        with self.lock:
            if not self.dir_names or self.stopping.is_set():
                return
            update_map = {}
            removals = []
            notifies = {}
            instance_purges = []
            for dir_name in self.dir_names:
                action_type = self.policy.start_action(dir_name)
                lookup_info = self.policy.lookup(dir_name)
                log.debug(f'processing action: dir_name: {dir_name}, lookup_info: {lookup_info}, action_type: {action_type}')
                if action_type == ActionType.ACTION_TYPE_NONE:
                    continue
                elif action_type == ActionType.ACTION_TYPE_MAP_UPDATE:
                    # take care to not overwrite purge status
                    update_map[dir_name] = {'version': 1,
                                            'instance_id': lookup_info['instance_id'],
                                            'last_shuffled': lookup_info['mapped_time']
                    }
                    if lookup_info['purging']:
                        update_map[dir_name]['purging'] = 1
                elif action_type == ActionType.ACTION_TYPE_MAP_REMOVE:
                    removals.append(dir_name)
                elif action_type == ActionType.ACTION_TYPE_ACQUIRE:
                    notifies[dir_name] = (lookup_info['instance_id'], acquire_message(dir_name))
                elif action_type == ActionType.ACTION_TYPE_RELEASE:
                    notifies[dir_name] = (lookup_info['instance_id'], release_message(dir_name))
            if update_map or removals:
                self.update_mapping(update_map, removals, callback=self.continue_action)
            for dir_name, message in notifies.items():
                self.op_tracker.start_async_op()
                self.notifier.notify(dir_name, message, self.handle_peer_ack)
            self.dir_names.clear()

    def add_directory(self, dir_name):
        with self.lock:
            rd = self.policy.add_directory(dir_name)
            if rd['exists']:
                raise Exception(-errno.EEXIST, f'directory {dir_name} is already tracked')
            update_map = {dir_name: {'version': 1, 'instance_id': '', 'last_shuffled': 0.0}}
            updated = False
            def update_safe(updates, removals, r):
                nonlocal updated
                updated = True
                self.cond.notifyAll()
            self.update_mapping(update_map, [], callback=update_safe)
            self.cond.wait_for(lambda: updated)
            if rd['schedule']:
                self.schedule_action([dir_name])

    def remove_directory(self, dir_name):
        with self.lock:
            lookup_info = self.policy.lookup(dir_name)
            if not lookup_info:
                raise Exception(-errno.EEXIST, f'directory {dir_name} id not tracked')
            update_map = {dir_name: {'version': 1,
                                     'instance_id': lookup_info['instance_id'],
                                     'last_shuffled': lookup_info['mapped_time'],
                                     'purging': 1}}
            updated = False
            sync_lock = threading.Lock()
            sync_cond = threading.Condition(sync_lock)
            def update_safe(r):
                with sync_lock:
                    nonlocal updated
                    updated = True
                    sync_cond.notifyAll()
            request = UpdateDirMapRequest(self.ioctx, update_map.copy(), [], update_safe)
            request.send()
            with sync_lock:
                sync_cond.wait_for(lambda: updated)
            schedule = self.policy.remove_directory(dir_name)
            if schedule:
                self.schedule_action([dir_name])

    def status(self, dir_name):
        with self.lock:
            res = self.policy.dir_status(dir_name)
            return 0, json.dumps(res, indent=4, sort_keys=True), ''

    def summary(self):
        with self.lock:
            res = self.policy.instance_summary()
            return 0, json.dumps(res, indent=4, sort_keys=True), ''

class FSSnapshotMirror(object):
    def __init__(self, mgr):
        self.mgr = mgr
        self.rados = mgr.rados
        self.pool_policy = {}
        self.fs_map = self.mgr.get('fs_map')
        self.lock = threading.Lock()
        self.refresh_pool_policy()

    def notify(self, notify_type):
        if notify_type == 'fs_map':
            self.fs_map = self.mgr.get('fs_map')

    @staticmethod
    def get_metadata_pool(filesystem, fs_map):
        for fs in fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == filesystem:
                return fs['mdsmap']['metadata_pool']
        return None

    @staticmethod
    def get_filesystem_id(filesystem, fs_map):
        for fs in fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == filesystem:
                return fs['id']
        return None

    def filesystem_exist(self, filesystem):
        for fs in self.fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == filesystem:
                return True
        return False

    def filesystem_has_peers(self, filesystem):
        """To be used only when mirroring is enabled on a filesystem"""
        for fs in self.fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == filesystem:
                return not len(fs['mirror_info']['peers']) == 0

    def get_mirrored_filesystems(self):
        return [fs['mdsmap']['fs_name'] for fs in self.fs_map['filesystems'] if fs.get('mirror_info', None)]

    def set_mirror_info(self, local_fs_name, remote_cluster_spec, remote_fs_name):
        cluster_id = self.rados.get_fsid()
        filesystem_id = FSSnapshotMirror.get_filesystem_id(local_fs_name, self.fs_map)
        log.info(f'setting {cluster_id}:{filesystem_id} to remote: {remote_cluster_spec}/{remote_fs_name}')
        try:
            client_id, cluster_name = remote_cluster_spec.split('@')
            _, client_name = client_id.split('.')
        except ValueError:
            raise Exception(-errno.EINVAL, f'invalid remote cluster spec {remote_cluster_spec}')

        remote_cluster, remote_fs = connect_to_filesystem(client_name, cluster_name,
                                                          remote_fs_name, 'remote')
        try:
            remote_fs.setxattr('/', 'ceph.mirror.info',
                               json.dumps({'cluster_id': cluster_id,
                                           'filesystem_id': filesystem_id}).encode('utf-8'), os.XATTR_CREATE)
        except cephfs.Error as e:
            if e.errno == errno.EEXIST:
                raise Exception(-errno.EEXIST, f'peer {remote_cluster_spec} mirrored by another cluster')
            else:
                log.error(f'error setting mirrored fsid: {e}')
                raise MirrorException(-e.errno, e.args[0])
        finally:
            disconnect_from_filesystem(remote_cluster, remote_fs)    

    def init_pool_policy(self, filesystem):
        metadata_pool_id = FSSnapshotMirror.get_metadata_pool(filesystem, self.fs_map)
        if not metadata_pool_id:
            raise MirrorException(
                -errno.EINVAL, f'cannot find metadata pool-id for filesystem {filesystem}')
        try:
            ioctx = self.rados.open_ioctx2(metadata_pool_id)
            self.pool_policy[filesystem] = FSPolicy(ioctx)
            log.debug(f'init policy for filesystem {filesystem}: pool-id {metadata_pool_id}')
            return self.pool_policy[filesystem]
        except rados.Error as e:
            log.error(f'failed to access pool-id {metadata_pool_id} for filesystem {filesystem}: {e}')
            raise MirrorException(-e.errno, e.args[0])

    def init_mapping(self, fspolicy):
        try:
            # TODO: make async if required -- its just bootup so...
            dir_mapping = load_dir_map(fspolicy.ioctx)
            instances = load_instances(fspolicy.ioctx)
            fspolicy.init(dir_mapping, instances)
        except rados.Error as e:
            raise MirrorException(-e.errno, e.args[0])

    def refresh_pool_policy(self):
        try:
            with self.lock:
                filesystems = self.get_mirrored_filesystems()
                log.debug(f'refreshing policy for {filesystems}')
                for filesystem in filesystems:
                    fspolicy = self.init_pool_policy(filesystem)
                    create_mirror_object(fspolicy.ioctx)
                    self.init_mapping(fspolicy)
        except rados.Error as e:
            raise MirrorException(-e.args[0], e.args[1])

    def enable_mirror(self, filesystem):
        log.info(f'enabling mirror for filesystem {filesystem}')
        with self.lock:
            fspolicy = None
            try:
                if not self.filesystem_exist(filesystem):
                    raise Exception(-errno.ENOENT, f'filesystem {filesystem} does not exist')
                if filesystem in self.pool_policy:
                    raise Exception(-errno.EEXIST, f'filesystem {filesystem} is already mirrored')
                fspolicy = self.init_pool_policy(filesystem)
                create_mirror_object(fspolicy.ioctx)
                cmd = {'prefix': 'fs mirror enable', 'fs_name': filesystem}
                r, outs, err = self.mgr.mon_command(cmd)
                if r < 0:
                    log.error(f'mon command to enable mirror failed: {err}')
                    raise Exception(-errno.EINVAL, 'failed to enable mirroring')
                self.init_mapping(fspolicy)
                return 0, json.dumps({}), ''
            except MirrorException as me:
                if fspolicy:
                    fspolicy.shutdown()
                log.error(f'mirror exception: {me}')
                return me.args[0], '', 'failed to enable mirroring'
            except Exception as e:
                if fspolicy:
                    fspolicy.shutdown()
                log.error(f'failed to enable mirror: {e}')
                return e.args[0], '', e.args[1]

    def disable_mirror(self, filesystem):
        try:
            with self.lock:
                if not self.filesystem_exist(filesystem):
                    raise Exception(-errno.ENOENT, f'filesystem {filesystem} does not exist')
                if not filesystem in self.pool_policy:
                    raise Exception(-errno.EEXIST, f'filesystem {filesystem} is not mirrored')
                if self.filesystem_has_peers(filesystem):
                    raise Exception(-errno.EEXIST, f'filesystem {filesystem} has peers')
                fspolicy = self.pool_policy.pop(filesystem)
                fspolicy.shutdown()
                cmd = {'prefix': 'fs mirror disable', 'fs_name': filesystem}
                r, outs, err = self.mgr.mon_command(cmd)
                if r < 0:
                    log.error(f'mon command to disable mirror failed: {err}')
                    raise Exception(-errno.EINVAL, 'failed to disable mirroring')
                return 0, json.dumps({}), ''
        except MirrorException as me:
            log.error(f'mirror exception: {me}')
            return me.args[0], '', 'failed to enable mirroring'
        except Exception as e:
            log.error(f'failed to enable mirror: {e}')
            return e.args[0], '', e.args[1]

    def peer_add(self, filesystem, remote_cluster_spec, remote_fs_name):
        try:
            if remote_fs_name == None:
                remote_fs_name = filesystem
            with self.lock:
                if not self.filesystem_exist(filesystem):
                    raise Exception(-errno.ENOENT, f'filesystem {filesystem} does not exist')
                fspolicy = self.pool_policy.get(filesystem, None)
                if not fspolicy:
                    raise Exception(-errno.EEXIST, f'filesystem {filesystem} is not mirrored')
                self.set_mirror_info(filesystem, remote_cluster_spec, remote_fs_name)
                cmd = {'prefix': 'fs mirror peer_add',
                       'fs_name': filesystem,
                       'remote_cluster_spec': remote_cluster_spec,
                       'remote_fs_name': remote_fs_name}
                r, outs, err = self.mgr.mon_command(cmd)
                if r < 0:
                    log.error(f'mon command to add peer failed: {err}')
                    raise Exception(-errno.EINVAL, 'failed to add peer')
                return 0, json.dumps({}), ''
        except MirrorException as me:
            log.error(f'mirror exception: {me}')
            return me.args[0], '', 'failed to add peer'
        except Exception as e:
            log.error(f'failed to add peer: {e}')
            return e.args[0], '', e.args[1]

    def peer_remove(self, filesystem, peer_uuid):
        try:
            with self.lock:
                if not self.filesystem_exist(filesystem):
                    raise Exception(-errno.ENOENT, f'filesystem {filesystem} does not exist')
                fspolicy = self.pool_policy.get(filesystem, None)
                if not fspolicy:
                    raise Exception(-errno.EEXIST, f'filesystem {filesystem} is not mirrored')
                cmd = {'prefix': 'fs mirror peer_remove',
                       'fs_name': filesystem,
                       'uuid': peer_uuid}
                r, outs, err = self.mgr.mon_command(cmd)
                if r < 0:
                    log.error(f'mon command to remove peer failed: {err}')
                    raise Exception(-errno.EINVAL, 'failed to remove peer')
                return 0, json.dumps({}), ''
        except MirrorException as me:
            log.error(f'mirror exception: {me}')
            return me.args[0], '', 'failed to remove peer'
        except Exception as e:
            log.error(f'failed to remove peer: {e}')
            return e.args[0], '', e.args[1]

    def add_directory(self, filesystem, dir_name):
        try:
            with self.lock:
                if not self.filesystem_exist(filesystem):
                    raise Exception(-errno.ENOENT, f'filesystem {filesystem} does not exist')
                if not filesystem in self.pool_policy:
                    raise Exception(-errno.EINVAL, f'filesystem {filesystem} is not mirrored')
                fspolicy = self.pool_policy[filesystem]
                fspolicy.add_directory(dir_name)
                return 0, json.dumps({}), ''
        except Exception as e:
            return e.args[0], '', e.args[1]

    def remove_directory(self, filesystem, dir_name):
        try:
            with self.lock:
                if not self.filesystem_exist(filesystem):
                    raise Exception(-errno.ENOENT, f'filesystem {filesystem} does not exist')
                if not filesystem in self.pool_policy:
                    raise Exception(-errno.EINVAL, f'filesystem {filesystem} is not mirrored')
                fspolicy = self.pool_policy[filesystem]
                fspolicy.remove_directory(dir_name)
                return 0, json.dumps({}), ''
        except Exception as e:
            return e.args[0], '', e.args[1]

    def status(self,filesystem, dir_name):
        try:
            with self.lock:
                if not self.filesystem_exist(filesystem):
                    raise Exception(-errno.ENOENT, f'filesystem {filesystem} does not exist')
                if not filesystem in self.pool_policy:
                    raise Exception(-errno.EINVAL, f'filesystem {filesystem} is not mirrored')
                fspolicy = self.pool_policy[filesystem]
                return fspolicy.status(dir_name)
        except Exception as e:
            return e.args[0], '', e.args[1]

    def show_distribution(self, filesystem):
        try:
            with self.lock:
                if not self.filesystem_exist(filesystem):
                    raise Exception(-errno.ENOENT, f'filesystem {filesystem} does not exist')
                if not filesystem in self.pool_policy:
                    raise Exception(-errno.EINVAL, f'filesystem {filesystem} is not mirrored')
                fspolicy = self.pool_policy[filesystem]
                return fspolicy.summary()
        except Exception as e:
            return e.args[0], '', e.args[1]
