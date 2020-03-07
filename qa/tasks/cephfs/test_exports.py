import logging
import time
import unittest
from StringIO import StringIO
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

class TestExports(CephFSTestCase):
    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 2

    def test_export_pin(self):
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()

        status = self.fs.status()

        self.mount_a.run_shell(["mkdir", "-p", "1/2/3"])
        self._wait_subtrees(status, 0, [])

        # NOP
        self.mount_a.setfattr("1", "ceph.dir.pin", "-1")
        self._wait_subtrees(status, 0, [])

        # NOP (rank < -1)
        self.mount_a.setfattr("1", "ceph.dir.pin", "-2341")
        self._wait_subtrees(status, 0, [])

        # pin /1 to rank 1
        self.mount_a.setfattr("1", "ceph.dir.pin", "1")
        self._wait_subtrees(status, 1, [('/1', 1)])

        # Check export_targets is set properly
        status = self.fs.status()
        log.info(status)
        r0 = status.get_rank(self.fs.id, 0)
        self.assertTrue(sorted(r0['export_targets']) == [1])

        # redundant pin /1/2 to rank 1
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "1")
        self._wait_subtrees(status, 1, [('/1', 1), ('/1/2', 1)])

        # change pin /1/2 to rank 0
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "0")
        self._wait_subtrees(status, 1, [('/1', 1), ('/1/2', 0)])
        self._wait_subtrees(status, 0, [('/1', 1), ('/1/2', 0)])

        # change pin /1/2/3 to (presently) non-existent rank 2
        self.mount_a.setfattr("1/2/3", "ceph.dir.pin", "2")
        self._wait_subtrees(status, 0, [('/1', 1), ('/1/2', 0)])
        self._wait_subtrees(status, 1, [('/1', 1), ('/1/2', 0)])

        # change pin /1/2 back to rank 1
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "1")
        self._wait_subtrees(status, 1, [('/1', 1), ('/1/2', 1)])

        # add another directory pinned to 1
        self.mount_a.run_shell(["mkdir", "-p", "1/4/5"])
        self.mount_a.setfattr("1/4/5", "ceph.dir.pin", "1")
        self._wait_subtrees(status, 1, [('/1', 1), ('/1/2', 1), ('/1/4/5', 1)])

        # change pin /1 to 0
        self.mount_a.setfattr("1", "ceph.dir.pin", "0")
        self._wait_subtrees(status, 0, [('/1', 0), ('/1/2', 1), ('/1/4/5', 1)])

        # change pin /1/2 to default (-1); does the subtree root properly respect it's parent pin?
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "-1")
        self._wait_subtrees(status, 0, [('/1', 0), ('/1/4/5', 1)])

        if len(list(status.get_standbys())):
            self.fs.set_max_mds(3)
            self.fs.wait_for_state('up:active', rank=2)
            self._wait_subtrees(status, 0, [('/1', 0), ('/1/4/5', 1), ('/1/2/3', 2)])

            # Check export_targets is set properly
            status = self.fs.status()
            log.info(status)
            r0 = status.get_rank(self.fs.id, 0)
            self.assertTrue(sorted(r0['export_targets']) == [1,2])
            r1 = status.get_rank(self.fs.id, 1)
            self.assertTrue(sorted(r1['export_targets']) == [0])
            r2 = status.get_rank(self.fs.id, 2)
            self.assertTrue(sorted(r2['export_targets']) == [])

        # Test rename
        self.mount_a.run_shell(["mkdir", "-p", "a/b", "aa/bb"])
        self.mount_a.setfattr("a", "ceph.dir.pin", "1")
        self.mount_a.setfattr("aa/bb", "ceph.dir.pin", "0")
        if (len(self.fs.get_active_names()) > 2):
            self._wait_subtrees(status, 0, [('/1', 0), ('/1/4/5', 1), ('/1/2/3', 2), ('/a', 1), ('/aa/bb', 0)])
        else:
            self._wait_subtrees(status, 0, [('/1', 0), ('/1/4/5', 1), ('/a', 1), ('/aa/bb', 0)])
        self.mount_a.run_shell(["mv", "aa", "a/b/"])
        if (len(self.fs.get_active_names()) > 2):
            self._wait_subtrees(status, 0, [('/1', 0), ('/1/4/5', 1), ('/1/2/3', 2), ('/a', 1), ('/a/b/aa/bb', 0)])
        else:
            self._wait_subtrees(status, 0, [('/1', 0), ('/1/4/5', 1), ('/a', 1), ('/a/b/aa/bb', 0)])

    def test_export_pin_getfattr(self):
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()

        status = self.fs.status()

        self.mount_a.run_shell(["mkdir", "-p", "1/2/3"])
        self._wait_subtrees(status, 0, [])

        # pin /1 to rank 0
        self.mount_a.setfattr("1", "ceph.dir.pin", "1")
        self._wait_subtrees(status, 1, [('/1', 1)])

        # pin /1/2 to rank 1
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "1")
        self._wait_subtrees(status, 1, [('/1', 1), ('/1/2', 1)])

        # change pin /1/2 to rank 0
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "0")
        self._wait_subtrees(status, 1, [('/1', 1), ('/1/2', 0)])
        self._wait_subtrees(status, 0, [('/1', 1), ('/1/2', 0)])

         # change pin /1/2/3 to (presently) non-existent rank 2
        self.mount_a.setfattr("1/2/3", "ceph.dir.pin", "2")
        self._wait_subtrees(status, 0, [('/1', 1), ('/1/2', 0)])

        if len(list(status.get_standbys())):
            self.fs.set_max_mds(3)
            self.fs.wait_for_state('up:active', rank=2)
            self._wait_subtrees(status, 0, [('/1', 1), ('/1/2', 0), ('/1/2/3', 2)])

        if not isinstance(self.mount_a, FuseMount):
            p = self.mount_a.client_remote.run(args=['uname', '-r'], stdout=StringIO(), wait=True)
            dir_pin = self.mount_a.getfattr("1", "ceph.dir.pin")
            log.debug("mount.getfattr('1','ceph.dir.pin'): %s " % dir_pin)
            if str(p.stdout.getvalue()) < "5" and not(dir_pin):
                self.skipTest("Kernel does not support getting the extended attribute ceph.dir.pin")
        self.assertTrue(self.mount_a.getfattr("1", "ceph.dir.pin") == "1")
        self.assertTrue(self.mount_a.getfattr("1/2", "ceph.dir.pin") == "0")
        if (len(self.fs.get_active_names()) > 2):
            self.assertTrue(self.mount_a.getfattr("1/2/3", "ceph.dir.pin") == "2")

    def test_session_race(self):
        """
        Test session creation race.

        See: https://tracker.ceph.com/issues/24072#change-113056
        """

        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        rank1 = self.fs.get_rank(rank=1, status=status)

        # Create a directory that is pre-exported to rank 1
        self.mount_a.run_shell(["mkdir", "-p", "a/aa"])
        self.mount_a.setfattr("a", "ceph.dir.pin", "1")
        self._wait_subtrees(status, 1, [('/a', 1)])

        # Now set the mds config to allow the race
        self.fs.rank_asok(["config", "set", "mds_inject_migrator_session_race", "true"], rank=1)

        # Now create another directory and try to export it
        self.mount_b.run_shell(["mkdir", "-p", "b/bb"])
        self.mount_b.setfattr("b", "ceph.dir.pin", "1")

        time.sleep(5)

        # Now turn off the race so that it doesn't wait again
        self.fs.rank_asok(["config", "set", "mds_inject_migrator_session_race", "false"], rank=1)

        # Now try to create a session with rank 1 by accessing a dir known to
        # be there, if buggy, this should cause the rank 1 to crash:
        self.mount_b.run_shell(["ls", "a"])

        # Check if rank1 changed (standby tookover?)
        new_rank1 = self.fs.get_rank(rank=1)
        self.assertEqual(rank1['gid'], new_rank1['gid'])

class TestEphemeralPins(CephFSTestCase):
    MDSS_REQUIRED = 4
    CLIENTS_REQUIRED = 1

    def _setup_tree(self):
        self.mount_a.run_shell(["mkdir", "-p", "tree"])
        for i in range(0,100):
            name = "tree/%02d/subdir" % i
            self.mount_a.run_shell(["mkdir", "-p", name])

    def _get_subtrees(self, status=None):
        # only gets what rank 0 sees!
        subtrees = self.fs.rank_asok(["get", "subtrees"], status=status)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/'), subtrees)
        #subtrees = sorted([{'path': s['dir']['path'], 'auth': s['auth_first']} for s in subtrees])
        return subtrees

    def test_ephemeral_pin_dist_override(self):
        """
        That an ephemeral distributed pin overrides a normal export pin.
        """

        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        self._setup_tree()
        self.mount_a.setfattr("tree", "ceph.dir.pin", "0")
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed", "1")
        self._wait_distributed_subtrees(status, 0, 100)
        subtrees = self._get_subtrees(status=status)
        for s in subtrees:
            path = s['dir']['path']
            if path == '/tree':
                self.assertEqual(s['export_pin'], 0)
                self.assertEqual(s['auth_first'], 0)
            elif path.startswith('/tree/'):
                self.assertEqual(s['export_pin'], -1)
                self.assertTrue(s['distributed_ephemeral_pin'])

    def test_ephemeral_pin_dist_override_pin(self):
        """
        That an export pin overrides an ephemerally pinned directory.
        """

        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        self._setup_tree()
        self.mount_a.setfattr("tree", "ceph.dir.pin", "0")
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed", "1")
        self._wait_distributed_subtrees(status, 0, 100)
        subtrees = self._get_subtrees(status=status)
        which = None
        for s in subtrees:
            path = s['dir']['path']
            if path.startswith('/tree/') and s['auth_first'] == 1:
                self.mount_a.setfattr(path[1:], "ceph.dir.pin", "0")
                which = path
                break
        self.assertIsNotNone(which)
        time.sleep(10)
        subtrees = self._get_subtrees(status=status)
        for s in subtrees:
            path = s['dir']['path']
            if path == which:
                self.assertEqual(s['auth_first'], 0)
                self.assertFalse(s['distributed_ephemeral_pin'])
                return
        # it has been merged into /tree

    def test_ephemeral_pin_dist_off(self):
        """
        That turning off ephemeral distributed pin merges subtrees.
        """

        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        self._setup_tree()
        self.mount_a.setfattr("tree", "ceph.dir.pin", "0")
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed", "1")
        self._wait_distributed_subtrees(status, 0, 100)
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed", "0")
        self._wait_subtrees(status, 0, [('/tree', 0)])

    def test_ephemeral_pin_dist_conf_off(self):
        """
        That turning off ephemeral distributed pin config prevents distribution.
        """

        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        self._setup_tree()
        self.mount_a.setfattr("tree", "ceph.dir.pin", "0")
        self.config_set('mds', 'mds_export_ephemeral_distributed', False)
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed", "1")
        time.sleep(30)
        self._wait_subtrees(status, 0, [('/tree', 0)])

    def test_ephemeral_pin_dist_conf_off_merge(self):
        """
        That turning off ephemeral distributed pin config merges subtrees.
        """

        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        self._setup_tree()
        self.mount_a.setfattr("tree", "ceph.dir.pin", "0")
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed", "1")
        self._wait_distributed_subtrees(status, 0, 100)
        self.config_set('mds', 'mds_export_ephemeral_distributed', False)
        self._wait_subtrees(status, 0, [('/tree', 0)], timeout=60)

    def test_ephemeral_pin_distribution(self):
        """
        That ephemerally pinned subtrees are somewhat evenly distributed.
        """

        self.fs.set_max_mds(3)
        status = self.fs.wait_for_daemons()

        self.mount_a.run_shell(["mkdir", "-p", "a"])
        self._wait_subtrees(status, 0, [])

        for i in range(0,100):
          self.mount_a.run_shell(["mkdir", "-p", "a/" + str(i) + "/d"])
          
        self._wait_subtrees(status, 0, [])

        self.mount_a.setfattr("a", "ceph.dir.pin.distributed", "1")

        self._wait_distributed_subtrees(status, 0, 100)

        # Check if distribution is uniform
        rank0_distributed_subtree_ratio = len(self.get_distributed_auth_subtrees(status, 0))/len(self.get_auth_subtrees(status, 0))
        self.assertGreaterEqual(rank0_distributed_subtree_ratio, 0.2)

        rank1_distributed_subtree_ratio = len(self.get_distributed_auth_subtrees(status, 1))/len(self.get_auth_subtrees(status, 1))
        self.assertGreaterEqual(rank1_distributed_subtree_ratio, 0.2)

        rank2_distributed_subtree_ratio = len(self.get_distributed_auth_subtrees(status, 2))/len(self.get_auth_subtrees(status, 2))
        self.assertGreaterEqual(rank2_distributed_subtree_ratio, 0.2)

    @unittest.skip("random pinning needs fixed for subtree migration")
    def test_ephemeral_random(self):
        
        # Check if export ephemeral random is applied hierarchically
        
        self.fs.set_max_mds(3)
        self.fs.wait_for_daemons()

        status = self.fs.status()

        tmp_dir = ""
        for i in range(0, 100):
          tmp_dir = tmp_dir + str(i) + "/"
          self.mount_a.run_shell(["mkdir", "-p", tmp_dir])
          self.mount_a.setfattr(temp_dir, "ceph.dir.pin.random", "1")

        count = len(get_random_auth_subtrees(status,0))
        self.assertEqual(count, 100)

    def test_ephemeral_pin_grow_mds(self):
        
        # Increase the no of MDSs and verify that the no of subtrees that migrate are less than 1/3 of the total no of subtrees that are ephemerally pinned

        self.fs.set_max_mds(3)
        self.fs.wait_for_daemons()

        status = self.fs.status()

        for i in range(0,100):
          self.mount_a.run_shell(["mkdir", "-p", "a/" + str(i) + "/d"])
        self._wait_subtrees(status, 0, [])
        self.mount_a.setfattr("a", "ceph.dir.pin.distributed", "1")
        self._wait_distributed_subtrees(status, 0, 100)

        subtrees_old = self.get_ephemerally_pinned_auth_subtrees(status, 0) + self.get_ephemerally_pinned_auth_subtrees(status, 1) + self.get_ephemerally_pinned_auth_subtrees(status, 2)
        self.fs.set_max_mds(4)
        self.fs.wait_for_daemons()
        
        # Sleeping for a while to allow the ephemeral pin migrations to complete
        time.sleep(30)
        
        status = self.fs.status()
        subtrees_new = self.get_ephemerally_pinned_auth_subtrees(status, 0) + self.get_ephemerally_pinned_auth_subtrees(status, 1) + self.get_ephemerally_pinned_auth_subtrees(status, 2) + self.get_ephemerally_pinned_auth_subtrees(status, 3)
        count = 0
        for old_subtree in subtrees_old:
            for new_subtree in subtrees_new:
                if (old_subtree['dir']['path'] == new_subtree['dir']['path']) and (old_subtree['auth_first'] != new_subtree['auth_first']):
                    count = count + 1
                    break

        log.info("{0} migrations have occured due to the cluster resizing".format(count))
        self.assertLessEqual((count/len(subtrees_old)), 0.33)

    def test_ephemeral_pin_shrink_mds(self):

        # Shrink the no of MDSs

        self.fs.set_max_mds(3)
        self.fs.wait_for_daemons()

        status = self.fs.status()

        for i in range(0,100):
          self.mount_a.run_shell(["mkdir", "-p", "a/" + str(i) + "/d"])
        self._wait_subtrees(status, 0, [])
        self.mount_a.setfattr("a", "ceph.dir.pin.distributed", "1")
        self._wait_distributed_subtrees(status, 0, 100)

        subtrees_old = self.get_ephemerally_pinned_auth_subtrees(status, 0) + self.get_ephemerally_pinned_auth_subtrees(status, 1) + self.get_ephemerally_pinned_auth_subtrees(status, 2)
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()
        time.sleep(30)

        status = self.fs.status()
        subtrees_new = self.get_ephemerally_pinned_auth_subtrees(status, 0) + self.get_ephemerally_pinned_auth_subtrees(status, 1)
        count = 0
        for old_subtree in subtrees_old:
            for new_subtree in subtrees_new:
                if (old_subtree['dir']['path'] == new_subtree['dir']['path']) and (old_subtree['auth_first'] != new_subtree['auth_first']):
                    count = count + 1
                    break

        log.info("{0} migrations have occured due to the cluster resizing".format(count))
        self.assertLessEqual((count/len(subtrees_old)), 0.33)

    @unittest.skip("unset needs fixed")
    def test_ephemeral_pin_unset_config(self):

        # Check if unsetting the distributed pin config results in every distributed pin being unset

        self.fs.set_max_mds(3)
        self.fs.wait_for_daemons()

        status = self.fs.status()

        for i in range(0, 10):
            self.mount_a.run_shell(["mkdir", "-p", i +"/dummy_dir"])
            self.mount_a.setfattr(i, "ceph.dir.pin.distributed", "1")

        self._wait_distributed_subtrees(status, 0, 10)

        self.fs.mds_asok(["config", "set", "mds_export_ephemeral_distributed_config", "false"])
        # Sleep for a while to facilitate unsetting of the pins
        time.sleep(15)
        
        for i in range(0, 10):
            self.assertTrue(self.mount_a.getfattr(i, "ceph.dir.pin.distributed") == "0")

    @unittest.skip("unset needs fixed")
    def test_ephemeral_distributed_pin_unset(self):

        # Test if unsetting the distributed ephemeral pin on a parent directory then the children directory should not be ephemerally pinned anymore

        self.fs.set_max_mds(3)
        self.fs.wait_for_daemons()

        status = self.fs.status()

        for i in range(0, 10):
            self.mount_a.run_shell(["mkdir", "-p", i +"/a/b"])
            self.mount_a.setfattr(i, "ceph.dir.pin.distributed", "1")

        self._wait_distributed_subtrees(status, 0, 10)

        for i in range(0, 10):
            self.mount_a.run_shell(["mkdir", "-p", i +"/a/b"])
            self.mount_a.setfattr(i, "ceph.dir.pin.distributed", "0")

        time.sleep(15)

        subtree_count = len(get_distributed_auth_subtrees(status, 0))
        assertEqual(subtree_count, 0)

    def test_ephemeral_standby(self):

        # Test if the distribution is unaltered when a Standby MDS takes up a failed rank
        
        self.fs.set_max_mds(3)
        self.fs.wait_for_daemons()
        status = self.fs.status()

        for i in range(0,100):
          self.mount_a.run_shell(["mkdir", "-p", "a/" + str(i) + "/d"])

        self.mount_a.setfattr("a", "ceph.dir.pin.distributed", "1")
        
        self._wait_distributed_subtrees(status, 0, 100)

        original_subtrees = self.get_ephemerally_pinned_auth_subtrees(status, 2)

        # Flush the journal for rank 0
        self.fs.rank_asok(["flush", "journal"], rank=2, status=status)

        # Kill the rank 2 daemon's physical process
        self.fs.mds_stop(status.get_rank(self.fs.id, 2)['name'])

        self.fs.wait_for_state('up:active', rank=2, timeout=60)

        time.sleep(30)
        
        status = self.fs.status()
        new_subtrees = self.get_ephemerally_pinned_auth_subtrees(status, 2)

        count = 0
        for subtree_old in original_subtrees:
            for subtree_new in new_subtrees:
                if subtree_old['dir']['path'] == subtree_new['dir']['path']:
                    count = count + 1

        self.assertEqual(count, len(original_subtrees))
