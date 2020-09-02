"""
Copyright (C) 2020 SUSE

LGPL2.1.  See file COPYING.
"""
import cephfs
import errno
import rados
from contextlib import contextmanager
from mgr_util import CephfsClient, CephfsConnectionException, \
        open_filesystem
from collections import OrderedDict
from datetime import datetime, timezone
import logging
from threading import Timer
import sqlite3
from .schedule import Schedule, parse_retention
import traceback


MAX_SNAPS_PER_PATH = 50
SNAP_SCHEDULE_NAMESPACE = 'cephfs-snap-schedule'
SNAP_DB_PREFIX = 'snap_db'
# increment this every time the db schema changes and provide upgrade code
SNAP_DB_VERSION = '0'
SNAP_DB_OBJECT_NAME = f'{SNAP_DB_PREFIX}_v{SNAP_DB_VERSION}'
SNAPSHOT_TS_FORMAT = '%Y-%m-%d-%H_%M_%S'
SNAPSHOT_PREFIX = 'scheduled'

log = logging.getLogger(__name__)


@contextmanager
def open_ioctx(self, pool):
    try:
        if type(pool) is int:
            with self.mgr.rados.open_ioctx2(pool) as ioctx:
                ioctx.set_namespace(SNAP_SCHEDULE_NAMESPACE)
                yield ioctx
        else:
            with self.mgr.rados.open_ioctx(pool) as ioctx:
                ioctx.set_namespace(SNAP_SCHEDULE_NAMESPACE)
                yield ioctx
    except rados.ObjectNotFound:
        log.error("Failed to locate pool {}".format(pool))
        raise


def updates_schedule_db(func):
    def f(self, fs, schedule_or_path, *args):
        func(self, fs, schedule_or_path, *args)
        path = schedule_or_path
        if isinstance(schedule_or_path, Schedule):
            path = schedule_or_path.path
        self.refresh_snap_timers(fs, path)
    return f


class SnapSchedClient(CephfsClient):

    def __init__(self, mgr):
        super(SnapSchedClient, self).__init__(mgr)
        # TODO maybe iterate over all fs instance in fsmap and load snap dbs?
        self.sqlite_connections = {}
        self.active_timers = {}

    def get_schedule_db(self, fs):
        if fs not in self.sqlite_connections:
            self.sqlite_connections[fs] = sqlite3.connect(
                ':memory:',
                check_same_thread=False)
            with self.sqlite_connections[fs] as con:
                con.row_factory = sqlite3.Row
                con.execute("PRAGMA FOREIGN_KEYS = 1")
                pool = self.get_metadata_pool(fs)
                with open_ioctx(self, pool) as ioctx:
                    try:
                        size, _mtime = ioctx.stat(SNAP_DB_OBJECT_NAME)
                        db = ioctx.read(SNAP_DB_OBJECT_NAME,
                                        size).decode('utf-8')
                        con.executescript(db)
                    except rados.ObjectNotFound:
                        log.debug(f'No schedule DB found in {fs}, creating one.')
                        con.executescript(Schedule.CREATE_TABLES)
        return self.sqlite_connections[fs]

    def store_schedule_db(self, fs):
        # only store db is it exists, otherwise nothing to do
        metadata_pool = self.get_metadata_pool(fs)
        if not metadata_pool:
            raise CephfsConnectionException(
                -errno.ENOENT, "Filesystem {} does not exist".format(fs))
        if fs in self.sqlite_connections:
            db_content = []
            db = self.sqlite_connections[fs]
            with db:
                for row in db.iterdump():
                    db_content.append(row)
        with open_ioctx(self, metadata_pool) as ioctx:
            ioctx.write_full(SNAP_DB_OBJECT_NAME,
                             '\n'.join(db_content).encode('utf-8'))

    def refresh_snap_timers(self, fs, path):
        try:
            log.debug(f'SnapDB on {fs} changed for {path}, updating next Timer')
            db = self.get_schedule_db(fs)
            rows = []
            with db:
                cur = db.execute(Schedule.EXEC_QUERY, (path,))
                rows = cur.fetchmany(1)
            timers = self.active_timers.get((fs, path), [])
            for timer in timers:
                timer.cancel()
            timers = []
            for row in rows:
                log.debug(f'Creating new snapshot timer for {path}')
                t = Timer(row[1],
                          self.create_scheduled_snapshot,
                          args=[fs, path, row[0], row[2], row[3]])
                t.start()
                timers.append(t)
                log.debug(f'Will snapshot {path} in fs {fs} in {row[1]}s')
            self.active_timers[(fs, path)] = timers
        except Exception:
            self._log_exception('refresh_snap_timers')

    def _log_exception(self, fct):
        log.error(f'{fct} raised an exception:')
        log.error(traceback.format_exc())

    def create_scheduled_snapshot(self, fs_name, path, retention, start, repeat):
        log.debug(f'Scheduled snapshot of {path} triggered')
        try:
            db = self.get_schedule_db(fs_name)
            sched = Schedule.get_db_schedules(path,
                                              db,
                                              fs_name,
                                              repeat,
                                              start)[0]
            time = datetime.now(timezone.utc)
            with open_filesystem(self, fs_name) as fs_handle:
                snap_ts = time.strftime(SNAPSHOT_TS_FORMAT)
                snap_name = f'{path}/.snap/{SNAPSHOT_PREFIX}-{snap_ts}'
                fs_handle.mkdir(snap_name, 0o755)
            log.info(f'created scheduled snapshot {snap_name}')
            sched.update_last(time, db)
        except cephfs.Error:
            self._log_exception('create_scheduled_snapshot')
            sched.set_inactive(db)
        except Exception:
            # catch all exceptions cause otherwise we'll never know since this
            # is running in a thread
            self._log_exception('create_scheduled_snapshot')
        finally:
            self.refresh_snap_timers(fs_name, path)
            self.prune_snapshots(sched)

    def prune_snapshots(self, sched):
        try:
            log.debug('Pruning snapshots')
            ret = sched.retention
            path = sched.path
            prune_candidates = set()
            time = datetime.now(timezone.utc)
            with open_filesystem(self, sched.fs) as fs_handle:
                with fs_handle.opendir(f'{path}/.snap') as d_handle:
                    dir_ = fs_handle.readdir(d_handle)
                    while dir_:
                        if dir_.d_name.decode('utf-8').startswith(f'{SNAPSHOT_PREFIX}-'):
                            log.debug(f'add {dir_.d_name} to pruning')
                            ts = datetime.strptime(
                                dir_.d_name.decode('utf-8').lstrip(f'{SNAPSHOT_PREFIX}-'),
                                SNAPSHOT_TS_FORMAT)
                            prune_candidates.add((dir_, ts))
                        else:
                            log.debug(f'skipping dir entry {dir_.d_name}')
                        dir_ = fs_handle.readdir(d_handle)
                to_prune = self.get_prune_set(prune_candidates, ret)
                for k in to_prune:
                    dirname = k[0].d_name.decode('utf-8')
                    log.debug(f'rmdir on {dirname}')
                    fs_handle.rmdir(f'{path}/.snap/{dirname}')
                if to_prune:
                    sched.update_pruned(time, self.get_schedule_db(sched.fs),
                                        len(to_prune))
        except Exception:
            self._log_exception('prune_snapshots')

    def get_prune_set(self, candidates, retention):
        PRUNING_PATTERNS = OrderedDict([
            # n is for keep last n snapshots, uses the snapshot name timestamp
            # format for lowest granularity
            ("n", SNAPSHOT_TS_FORMAT),
            # TODO remove M for release
            ("M", '%Y-%m-%d-%H_%M'),
            ("h", '%Y-%m-%d-%H'),
            ("d", '%Y-%m-%d'),
            ("w", '%G-%V'),
            ("m", '%Y-%m'),
            ("y", '%Y'),
        ])
        keep = []
        for period, date_pattern in PRUNING_PATTERNS.items():
            log.debug(f'compiling keep set for period {period}')
            period_count = retention.get(period, 0)
            if not period_count:
                continue
            last = None
            for snap in sorted(candidates, key=lambda x: x[0].d_name,
                               reverse=True):
                snap_ts = snap[1].strftime(date_pattern)
                if snap_ts != last:
                    last = snap_ts
                    if snap not in keep:
                        log.debug(f'keeping {snap[0].d_name} due to {period_count}{period}')
                        keep.append(snap)
                        if len(keep) == period_count:
                            log.debug(f'found enough snapshots for {period_count}{period}')
                            break
        if len(keep) > MAX_SNAPS_PER_PATH:
            log.info(f'Would keep more then {MAX_SNAPS_PER_PATH}, pruning keep set')
            keep = keep[:MAX_SNAPS_PER_PATH]
        return candidates - set(keep)

    def get_snap_schedules(self, fs, path):
        db = self.get_schedule_db(fs)
        return Schedule.get_db_schedules(path, db, fs)

    def list_snap_schedules(self, fs, path, recursive):
        db = self.get_schedule_db(fs)
        return Schedule.list_schedules(path, db, fs, recursive)

    @updates_schedule_db
    # TODO improve interface
    def store_snap_schedule(self, fs, path_, args):
        sched = Schedule(*args)
        log.debug(f'attempting to add schedule {sched}')
        db = self.get_schedule_db(fs)
        sched.store_schedule(db)
        self.store_schedule_db(sched.fs)

    @updates_schedule_db
    def rm_snap_schedule(self, fs, path, repeat, start):
        db = self.get_schedule_db(fs)
        Schedule.rm_schedule(db, path, repeat, start)

    @updates_schedule_db
    def add_retention_spec(self,
                           fs,
                           path,
                           retention_spec_or_period,
                           retention_count):
        retention_spec = retention_spec_or_period
        if retention_count:
            retention_spec = retention_count + retention_spec
        db = self.get_schedule_db(fs)
        Schedule.add_retention(db, path, retention_spec)

    @updates_schedule_db
    def rm_retention_spec(self,
                          fs,
                          path,
                          retention_spec_or_period,
                          retention_count):
        retention_spec = retention_spec_or_period
        if retention_count:
            retention_spec = retention_count + retention_spec
        db = self.get_schedule_db(fs)
        Schedule.rm_retention(db, path, retention_spec)

    @updates_schedule_db
    def activate_snap_schedule(self, fs, path, repeat, start):
        db = self.get_schedule_db(fs)
        schedules = Schedule.get_db_schedules(path, db, fs, repeat, start)
        [s.set_active(db) for s in schedules]

    @updates_schedule_db
    def deactivate_snap_schedule(self, fs, path, repeat, start):
        db = self.get_schedule_db(fs)
        schedules = Schedule.get_db_schedules(path, db, fs, repeat, start)
        [s.set_inactive(db) for s in schedules]
