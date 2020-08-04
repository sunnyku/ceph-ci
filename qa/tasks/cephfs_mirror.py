"""
Task for running cephfs mirror daemons
"""

import logging

from teuthology.orchestra import run
from teuthology import misc
from teuthology.exceptions import ConfigError
from teuthology.task import Task
from tasks.util import get_remote_for_role

class CephFSMirror(Task):
    def __init__(self, ctx, config):
        super(CephFSMirror, self).__init__(ctx, config)
        self.log = log

    def setup(self):
        super(CephFSMirror, self).setup()
        try:
            self.client = self.config['client']
        except KeyError:
            raise ConfigError('cephfs-mirror requires a client to connect')

        self.cluster_name, type_, self.client_id = misc.split_role(self.client)
        if not type_ == 'client':
            raise ConfigError(f'client role {self.client} must be a client')
        self.remote = get_remote_for_role(self.ctx, self.client)

    def begin(self):
        super(CephFSMirror, self).begin()
        testdir = misc.get_testdir(self.ctx)

        args = [
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'daemon-helper',
            'term',
            ]

        args.extend([
            'cephfs-mirror', '--foreground',
            '--id',
            self.client_id,
            ])

        self.ctx.daemons.add_daemon(
            self.remote, 'cephfs-mirror', self.client,
            args=args,
            logger=self.log.getChild(self.client),
            stdin=run.PIPE,
            wait=False,
        )

    def end(self):
        mirror_daemon = self.ctx.daemons.get_daemon('cephfs-mirror', self.client)
        mirror_daemon.stop()
        super(CephFSMirror, self).end()

task = CephFSMirror
