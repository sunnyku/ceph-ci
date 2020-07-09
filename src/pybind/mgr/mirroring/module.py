import logging

from mgr_module import MgrModule

from .fs.snapshot_mirror import FSSnapshotMirror

log = logging.getLogger(__name__)

class Module(MgrModule):
    COMMANDS = [
        {
            'cmd':  'fs snapshot mirror enable '
                    'name=fs,type=CephString ',
            'desc': 'enable snapshot mirroring for a filesystem',
            'perm': 'rw'
        },
        {
            'cmd':  'fs snapshot mirror disable '
                    'name=fs,type=CephString ',
            'desc': 'disable snapshot mirroring for a filesystem',
            'perm': 'rw'
        },
        {
            'cmd':  'fs snapshot mirror add '
                    'name=fs,type=CephString '
                    'name=path,type=CephString ',
            'desc': 'add a directory for snapshot mirroring',
            'perm': 'rw'
        },
        {
            'cmd':  'fs snapshot mirror remove '
                    'name=fs,type=CephString '
                    'name=path,type=CephString ',
            'desc': 'remove a directory for snapshot mirroring',
            'perm': 'rw'
            },
        {
            'cmd':  'fs snapshot mirror dirmap '
                    'name=fs,type=CephString '
                    'name=path,type=CephString ',
            'desc': 'retrieve status for mirrored directory',
            'perm': 'r'
            },
        {
            'cmd':  'fs snapshot mirror show distribution '
                    'name=fs,type=CephString ',
            'desc': 'retrieve status for mirrored directory',
            'perm': 'r'
            },
        ]
    MODULE_OPTIONS = []

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.fs_snapshot_mirror = FSSnapshotMirror(self)

    def notify(self, notify_type, notify_id):
        self.fs_snapshot_mirror.notify(notify_type)

    def handle_command(self, inbuf, cmd):
        prefix = cmd['prefix']
        if prefix.startswith('fs snapshot mirror enable'):
            return self.fs_snapshot_mirror.enable_mirror(cmd['fs'])
        elif prefix.startswith('fs snapshot mirror disable'):
            return self.fs_snapshot_mirror.disable_mirror(cmd['fs'])
        elif prefix.startswith('fs snapshot mirror add'):
            return self.fs_snapshot_mirror.add_directory(cmd['fs'], cmd['path'])
        elif prefix.startswith('fs snapshot mirror remove'):
            return self.fs_snapshot_mirror.remove_directory(cmd['fs'], cmd['path'])
        elif prefix.startswith('fs snapshot mirror dirmap'):
            return self.fs_snapshot_mirror.status(cmd['fs'], cmd['path'])
        elif prefix.startswith('fs snapshot mirror show distribution'):
            return self.fs_snapshot_mirror.show_distribution(cmd['fs'])
        else:
            raise NotImplementedError()
