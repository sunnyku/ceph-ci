import os
import errno
import cephfs
import logging

from pathspec import SubvolumeSpec

log = logging.getLogger(__name__)

class SubVolume(object):
    """
    Combine libcephfs and librados interfaces to implement a
    'Subvolume' concept implemented as a cephfs directory.

    Additionally, subvolumes may be in a 'Group'.  Conveniently,
    subvolumes are a lot like manila shares, and groups are a lot
    like manila consistency groups.

    Refer to subvolumes with SubvolumePath, which specifies the
    subvolume and group IDs (both strings).  The group ID may
    be None.

    In general, functions in this class are allowed raise rados.Error
    or cephfs.Error exceptions in unexpected situations.
    """


    def __init__(self, mgr, fs_name=None):
        self.fs = None
        self.fs_name = fs_name
        self.connected = False

        self.rados = mgr.rados

    def _mkdir_p(self, path, mode=0o755):
        try:
            self.fs.stat(path)
        except cephfs.ObjectNotFound:
            pass
        else:
            return

        parts = path.split(os.path.sep)

        for i in range(1, len(parts) + 1):
            subpath = os.path.join(*parts[0:i])
            try:
                self.fs.stat(subpath)
            except cephfs.ObjectNotFound:
                self.fs.mkdir(subpath, mode)

    def _list_dir(self, dir_path):
        """
        Returns a list of directory names within a directory
        """
        dir_handle = self.fs.opendir(dir_path)
        d = self.fs.readdir(dir_handle)
        d_list = []
        while d:
            if d.d_name not in [".", ".."] and d.is_dir():
                d_list.append(d.d_name)
            d = self.fs.readdir(dir_handle)
        self.fs.closedir(dir_handle)
        return d_list

    def create_subvolume(self, spec, size=None, namespace_isolated=True, mode=0o755):
        """
        Set up metadata, pools and auth for a subvolume.

        This function is idempotent.  It is safe to call this again
        for an already-created subvolume, even if it is in use.

        :param spec: subvolume path specification
        :param size: In bytes, or None for no size limit
        :param namespace_isolated: If true, use separate RADOS namespace for this subvolume
        :return: None
        """
        subvolpath = spec.fs_path
        log.info("creating subvolume with path: {0}".format(subvolpath))

        self._mkdir_p(subvolpath, mode)

        if size is not None:
            self.fs.setxattr(subvolpath, 'ceph.quota.max_bytes', str(size).encode('utf-8'), 0)

        xattr_key = xattr_val = None
        if namespace_isolated:
            # enforce security isolation, use separate namespace for this subvolume
            xattr_key = 'ceph.dir.layout.pool_namespace'
            xattr_val = spec.fs_namespace
        else:
            # If subvolume's namespace layout is not set, then the subvolume's pool
            # layout remains unset and will undesirably change with ancestor's
            # pool layout changes.
            xattr_key = 'ceph.dir.layout.pool'
            xattr_val = self._get_ancestor_xattr(subvolpath, "ceph.dir.layout.pool")
        # TODO: handle error...
        self.fs.setxattr(subvolpath, xattr_key, xattr_val.encode('utf-8'), 0)

    def remove_subvolume(self, spec):
        """
        Make a subvolume inaccessible to guests.  This function is idempotent.
        This is the fast part of tearing down a subvolume: you must also later
        call purge_subvolume, which is the slow part.

        :param spec: subvolume path specification
        :return: None
        """

        subvolpath = spec.fs_path
        log.info("deleting subvolume with path: {0}".format(subvolpath))

        # Create the trash directory if it doesn't already exist
        trashdir = spec.trash_dir
        self._mkdir_p(trashdir)

        trashpath = spec.trash_path
        self.fs.rename(subvolpath, trashpath)

    def purge_subvolume(self, spec):
        """
        Finish clearing up a subvolume that was previously passed to delete_subvolume.  This
        function is idempotent.
        """

        def rmtree(root_path):
            log.debug("rmtree {0}".format(root_path))
            try:
                dir_handle = self.fs.opendir(root_path)
            except cephfs.ObjectNotFound:
                return
            d = self.fs.readdir(dir_handle)
            while d:
                d_name = d.d_name.decode('utf-8')
                if d_name not in [".", ".."]:
                    # Do not use os.path.join because it is sensitive
                    # to string encoding, we just pass through dnames
                    # as byte arrays
                    d_full = "{0}/{1}".format(root_path, d_name)
                    if d.is_dir():
                        rmtree(d_full)
                    else:
                        self.fs.unlink(d_full)

                d = self.fs.readdir(dir_handle)
            self.fs.closedir(dir_handle)
            self.fs.rmdir(root_path)

        trashpath = spec.trash_path
        rmtree(trashpath)

    def list_subvolumes(self, spec):
        """
        Returns a list of subvolumes within a subvolume group
        """
        grouppath = spec.group_path
        return self._list_dir(grouppath)

    def get_subvolume_path(self, spec):
        path = spec.fs_path
        try:
            self.fs.stat(path)
        except cephfs.ObjectNotFound:
            return None
        return path

    ### Group operations...

    def create_group(self, spec, mode=0o755):
        path = spec.group_path
        self._mkdir_p(path, mode)

    def remove_group(self, spec):
        path = spec.group_path
        self.fs.rmdir(path)

    def list_groups(self):
        """
        Returns a list of subvolume groups within a FS volume/filesystem
        """
        spec = SubvolumeSpec("", "")
        path = spec.group_dir
        return self._list_dir(path)

    def get_group_path(self, spec, create_default=False):
        path = spec.group_path
        try:
            self.fs.stat(path)
        except cephfs.ObjectNotFound:
            if create_default and spec.is_default_group():
                self._mkdir_p(path)
                return path
            return None
        return path

    def _get_ancestor_xattr(self, path, attr):
        """
        Helper for reading layout information: if this xattr is missing
        on the requested path, keep checking parents until we find it.
        """
        try:
            result = self.fs.getxattr(path, attr).decode('utf-8')
            if result == "":
                # Annoying!  cephfs gives us empty instead of an error when attr not found
                raise cephfs.NoData()
            else:
                return result
        except cephfs.NoData:
            if path == "/":
                raise
            else:
                return self._get_ancestor_xattr(os.path.split(path)[0], attr)

    def _snapshot_create(self, snappath, mode=0o755):
        """
        Create a snapshot, or do nothing if it already exists.
        """
        try:
            self.fs.stat(snappath)
        except cephfs.ObjectNotFound:
            self.fs.mkdir(snappath, mode)
        else:
            log.warn("Snapshot '{0}' already exists".format(snappath))

    def _snapshot_delete(self, snappath):
        """
        Remove a snapshot, or do nothing if it doesn't exist.
        """
        self.fs.stat(snappath)
        self.fs.rmdir(snappath)

    def create_subvolume_snapshot(self, spec, snapname, mode=0o755):
        snappath = spec.make_subvol_snap_path(self.rados.conf_get('client_snapdir'), snapname)
        self._snapshot_create(snappath, mode)

    def remove_subvolume_snapshot(self, spec, snapname):
        snappath = spec.make_subvol_snap_path(self.rados.conf_get('client_snapdir'), snapname)
        self._snapshot_delete(snappath)

    def create_group_snapshot(self, spec, snapname, mode=0o755):
        snappath = spec.make_group_snap_path(self.rados.conf_get('client_snapdir'), snapname)
        self._snapshot_create(snappath, mode)

    def remove_group_snapshot(self, spec, snapname):
        snappath = spec.make_group_snap_path(self.rados.conf_get('client_snapdir'), snapname)
        return self._snapshot_delete(snappath)

    def connect(self):
        log.debug("Connecting to cephfs...")
        self.fs = cephfs.LibCephFS(rados_inst=self.rados)
        log.debug("CephFS initializing...")
        self.fs.init()
        log.debug("CephFS mounting...")
        self.fs.mount(filesystem_name=self.fs_name.encode('utf-8'))
        log.debug("Connection to cephfs complete")

    def disconnect(self):
        log.info("disconnect")
        if self.fs:
            log.debug("Disconnecting cephfs...")
            self.fs.shutdown()
            self.fs = None
            log.debug("Disconnecting cephfs complete")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __del__(self):
        self.disconnect()
