import logging
import threading
from contextlib import contextmanager
from typing import Dict

MIRROR_OBJECT_PREFIX = 'cephfs_mirror'
MIRROR_OBJECT_NAME = MIRROR_OBJECT_PREFIX

INSTANCE_ID_PREFIX = "instance_"
DIRECTORY_MAP_PREFIX = "dir_map_"

log = logging.getLogger(__name__)

@contextmanager
def open_ioctx(cluster, pool_id):
    """
    """
    with cluster.open_ioctx2(pool_id) as ioctx:
        yield ioctx

class _ThreadWrapper(threading.Thread):
    def __init__(self, name):
        self.q = []
        self.stopping = threading.Event()
        self.terminated = threading.Event()
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        super(_ThreadWrapper, self).__init__(name=name)
        super(_ThreadWrapper, self).start()

    def run(self):
        try:
            with self.lock:
                while True:
                    self.cond.wait_for(lambda: self.q or self.stopping.is_set())
                    if self.stopping.is_set():
                        log.debug('thread exiting')
                        self.terminated.set()
                        self.cond.notifyAll()
                        return
                    q = self.q.copy()
                    self.q.clear()
                    self.lock.release()
                    try:
                        for item in q:
                            log.debug(f'calling {item[0]} params {item[1]}')
                            item[0](*item[1])
                    except Exception as e:
                        log.warn(f'callback exception: {e}')
                    self.lock.acquire()
        except Exception as e:
            log.info(f'threading exception: {e}')

    def queue(self, cbk, args):
        with self.lock:
            self.q.append((cbk, args))
            self.cond.notifyAll()

    def stop(self):
        with self.lock:
            self.stopping.set()
            self.cond.notifyAll()
            self.cond.wait_for(lambda: self.terminated.is_set())

class Finisher(object):
    """
    singleton design pattern taken from http://www.aleax.it/5ep.html
    """
    lock = threading.Lock()
    _shared_state = {
        'lock' : lock,
        'thread' : _ThreadWrapper(name='finisher'),
        'init' : False
    } # type: Dict

    def __init__(self):
        with self._shared_state['lock']:
            if not self._shared_state['init']:
                self._shared_state['init'] = True
        # share this state among all instances
        self.__dict__ = self._shared_state

    def queue(self, cbk, args=[]):
        with self._shared_state['lock']:
            self._shared_state['thread'].queue(cbk, args)

class AsyncOpTracker(object):
    def __init__(self):
        self.ops_in_progress = 0
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)

    def start_async_op(self):
        with self.lock:
            self.ops_in_progress += 1
            log.debug(f'start_async_op: {self.ops_in_progress}')

    def finish_async_op(self):
        with self.lock:
            self.ops_in_progress -= 1
            log.debug(f'finish_async_op: {self.ops_in_progress}')
            assert(self.ops_in_progress >= 0)
            self.cond.notifyAll()

    def wait_for_ops(self):
        with self.lock:
            log.debug(f'wait_for_ops: {self.ops_in_progress}')
            self.cond.wait_for(lambda: self.ops_in_progress == 0)
            log.debug(f'done')
