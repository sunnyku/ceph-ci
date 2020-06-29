# vim: expandtab smarttab shiftwidth=4 softtabstop=4
import errno
import functools
import socket
import os

from nose import SkipTest
from nose.tools import eq_ as eq, ok_ as ok
from rados import Rados
import rbd
import rbdx

rados = None
features = None
image_idx = 0
pool_idx = 0
IMG_SIZE = 8 << 20 # 8 MiB
IMG_ORDER = 22 # 4 MiB objects


def setup_module():
    global rados
    rados = Rados(conffile='')
    rados.connect()
    global features
    features = os.getenv("RBD_FEATURES")
    features = int(features) if features is not None else 125


def teardown_module():
    global rados
    rados.shutdown()


def get_temp_pool_name():
    global pool_idx
    pool_idx += 1
    return "test-rbd-api-" + socket.gethostname() + '-' + str(os.getpid()) + \
           '-' + str(pool_idx)


def get_temp_image_name():
    global image_idx
    image_idx += 1
    return "image" + str(image_idx)


def create_image(ioctx):
    image_name = get_temp_image_name()
    rbd.RBD().create(ioctx, image_name, IMG_SIZE, IMG_ORDER, old_format=False,
                 features=int(features))
    return image_name


def remove_image(ioctx, image_name):
    if image_name is not None:
        rbd.RBD().remove(ioctx, image_name)


def require_new_format():
    def wrapper(fn):
        def _require_new_format(*args, **kwargs):
            global features
            if features is None:
                raise SkipTest
            return fn(*args, **kwargs)
        return functools.wraps(fn)(_require_new_format)
    return wrapper


def require_features(required_features):
    def wrapper(fn):
        def _require_features(*args, **kwargs):
            global features
            if features is None:
                raise SkipTest
            for feature in required_features:
                if feature & features != feature:
                    raise SkipTest
            return fn(*args, **kwargs)
        return functools.wraps(fn)(_require_features)
    return wrapper


def find_snap(snaps, name):
        for k, v in snaps.items():
            if (v.name == name):
                return v
        return None


class TestGetInfo_Basic(object):

    def setUp(self):
        self.pool_name = get_temp_pool_name()
        global rados
        rados.create_pool(self.pool_name)
        self.ioctx = rados.open_ioctx(self.pool_name)

        self.rbd = rbd.RBD()
        self.image_name = create_image(self.ioctx)
        self.image = rbd.Image(self.ioctx, self.image_name)

    def tearDown(self):
        self.image.close()
        remove_image(self.ioctx, self.image_name)
        self.image = None

        self.ioctx.close()

        global rados
        rados.delete_pool(self.pool_name)

    @require_new_format()
    def test_basic(self):
        self.image.metadata_set('key1', 'value1')
        self.image.metadata_set('key2', 'value2')
        with rbdx.IoCtx(self.ioctx.ioctx()) as iox:
            (info, r) = rbdx.get_info(iox, '', '')
            eq(r, -errno.EINVAL)
            (info, r) = rbdx.get_info(iox, 'non-exist', '')
            eq(r, -errno.ENOENT)
            (info, r) = rbdx.get_info(iox, '', 'non-exist')
            eq(r, -errno.ENOENT)
            (info, r) = rbdx.get_info(iox, 'non-exist', 'non-exist')
            eq(r, -errno.ENOENT)
            (info, r) = rbdx.get_info(iox, self.image_name, '', 0xffff & ~rbdx.INFO_F_ALL)
            eq(r, -errno.EINVAL)
            (info, r) = rbdx.get_info(iox, self.image_name, '')
            eq(r, 0)
            eq(info.id, self.image.id())
            (info, r) = rbdx.get_info(iox, self.image_name, info.id)
            eq(r, 0)
            (info, r) = rbdx.get_info(iox, '', info.id)
            eq(r, 0)
            eq(info.name, self.image_name)
            eq(info.id, self.image.id())
            eq(info.size, IMG_SIZE)
            eq(info.order, IMG_ORDER)
            ok(info.features is not None)
            ok(info.op_features is not None)
            ok(info.flags is not None)
            ok(type(info.snaps) is dict)
            eq(len(info.snaps), 0)
            eq(info.parent.pool_id, -1)
            eq(info.parent.pool_namespace, '')
            eq(info.parent.image_id, '')
            eq(info.parent.snap_id, rbdx.CEPH_NOSNAP)
            ok(info.create_timestamp != 0)
            ok(info.access_timestamp != 0)
            ok(info.modify_timestamp != 0)
            eq(info.data_pool_id, -1)
            ok(type(info.watchers) is list)
            # image is in open state
            eq(len(info.watchers), 1)
            ok(type(info.metas) is dict)
            eq(len(info.metas), 2)
            eq(info.metas['key1'], 'value1')
            eq(info.metas['key2'], 'value2')
            eq(info.du, -1)
            eq(info.dirty, -1)


class TestGetInfo_data_pool(object):

    def setUp(self):
        self.pool_name = get_temp_pool_name()
        global rados
        rados.create_pool(self.pool_name)
        self.ioctx = rados.open_ioctx(self.pool_name)

        self.rbd = rbd.RBD()

        self.data_pool_name = get_temp_pool_name()
        rados.create_pool(self.data_pool_name)

    def tearDown(self):
        self.ioctx.close()

        global rados
        rados.delete_pool(self.pool_name)

        rados.delete_pool(self.data_pool_name)

    def test_data_pool(self):
        self.image_name = get_temp_image_name()
        self.rbd.create(self.ioctx, self.image_name, IMG_SIZE, IMG_ORDER, old_format=False,
                        features=int(features), data_pool=self.data_pool_name)

        with rbdx.IoCtx(self.ioctx.ioctx()) as iox:
            (info, r) = rbdx.get_info(iox, self.image_name, '', rbdx.INFO_F_ALL)
            eq(r, 0)
            eq(info.data_pool_id, rados.pool_lookup(self.data_pool_name))

        self.rbd.remove(self.ioctx, self.image_name)


class TestGetInfo_children_v1(object):

    def setUp(self):
        self.pool_name = get_temp_pool_name()
        global rados
        rados.create_pool(self.pool_name)
        self.ioctx = rados.open_ioctx(self.pool_name)

        self.rbd = rbd.RBD()
        self.image_name = create_image(self.ioctx)
        self.image = rbd.Image(self.ioctx, self.image_name)

        self.pool_name2 = get_temp_pool_name()
        rados.create_pool(self.pool_name2)
        self.ioctx2 = rados.open_ioctx(self.pool_name2)

    def tearDown(self):
        self.image.close()
        remove_image(self.ioctx, self.image_name)
        self.image = None

        self.ioctx.close()

        global rados
        rados.delete_pool(self.pool_name)

        self.ioctx2.close()
        rados.delete_pool(self.pool_name2)

    @require_new_format()
    @require_features([rbd.RBD_FEATURE_LAYERING])
    def test_children_v1(self):
        with rbdx.IoCtx(self.ioctx.ioctx()) as iox:
            self.image.create_snap('snap1')
            self.image.protect_snap('snap1')

            clone_name = get_temp_image_name()
            rados.conf_set("rbd_default_clone_format", "1")
            self.rbd.clone(self.ioctx, self.image_name, 'snap1', self.ioctx, clone_name, features)
            self.rbd.clone(self.ioctx, self.image_name, 'snap1', self.ioctx2, clone_name, features)
            rados.conf_set("rbd_default_clone_format", "auto")

            (images, r) = rbdx.list(iox)
            eq(r, 0)
            eq(len(images), 2)

            (info, r) = rbdx.get_info(iox, self.image_name, '', rbdx.INFO_F_CHILDREN_V1 | rbdx.INFO_F_SNAP_DU)
            eq(r, 0)
            eq(info.name, self.image_name)
            eq(info.size, IMG_SIZE)
            eq(info.order, IMG_ORDER)
            eq(info.du, 0)
            eq(info.dirty, 0)
            eq(len(info.snaps), 1)
            snap = find_snap(info.snaps, 'snap1')
            ok(snap != None)
            ok(snap.id is not None)
            eq(snap.name, 'snap1')
            eq(snap.snap_type, rbdx.SNAPSHOT_NAMESPACE_TYPE_USER)
            eq(snap.size, IMG_SIZE)
            ok(snap.flags is not None)
            ok(snap.timestamp != 0)
            ok(type(snap.children) is set)
            eq(len(snap.children), 2)
            pool_id = rados.pool_lookup(self.pool_name)
            pool_id2 = rados.pool_lookup(self.pool_name2)
            for child in snap.children:
                ok(child.pool_id in (pool_id, pool_id2))
                if child.pool_id == pool_id:
                    eq(child.pool_id, pool_id)
                    eq(child.pool_namespace, "")
                    eq(child.pool_namespace, self.ioctx.get_namespace())
                    ok(child.image_id != '')
                else:
                    eq(child.pool_id, pool_id2)
                    eq(child.pool_namespace, "")
                    eq(child.pool_namespace, self.ioctx2.get_namespace())
                    ok(child.image_id != '')
            eq(snap.du, 0)
            eq(snap.dirty, 0)

            self.rbd.remove(self.ioctx, clone_name)
            self.rbd.remove(self.ioctx2, clone_name)
            self.image.unprotect_snap('snap1')
            self.image.remove_snap('snap1')
            it = self.image.list_snaps()
            snaps = []
            for s in it:
                snaps.append(s)
            eq(len(snaps), 0)


class TestGetInfo_children_v2(object):

    def setUp(self):
        self.pool_name = get_temp_pool_name()
        global rados
        rados.create_pool(self.pool_name)
        self.ioctx = rados.open_ioctx(self.pool_name)

        self.rbd = rbd.RBD()
        self.image_name = create_image(self.ioctx)
        self.image = rbd.Image(self.ioctx, self.image_name)

        self.pool_name2 = get_temp_pool_name()
        rados.create_pool(self.pool_name2)
        self.ioctx2 = rados.open_ioctx(self.pool_name2)

    def tearDown(self):
        self.image.close()
        remove_image(self.ioctx, self.image_name)
        self.image = None

        self.ioctx.close()

        global rados
        rados.delete_pool(self.pool_name)

        self.ioctx2.close()
        rados.delete_pool(self.pool_name2)

    @require_new_format()
    @require_features([rbd.RBD_FEATURE_LAYERING])
    def test_children_v2(self):
        with rbdx.IoCtx(self.ioctx.ioctx()) as iox:
            self.rbd.namespace_create(self.ioctx, 'ns1')
            ns_ioctx = rados.open_ioctx(self.pool_name)
            ns_ioctx.set_namespace('ns1')

            self.image.create_snap('snap1')
            clone_name = get_temp_image_name()
            rados.conf_set("rbd_default_clone_format", "2")
            self.rbd.clone(self.ioctx, self.image_name, 'snap1', ns_ioctx, clone_name, features)
            self.rbd.clone(self.ioctx, self.image_name, 'snap1', self.ioctx2, clone_name, features)
            rados.conf_set("rbd_default_clone_format", "auto")

            (images, r) = rbdx.list(iox)
            eq(r, 0)
            eq(len(images), 1)

            with rbdx.IoCtx(ns_ioctx.ioctx()) as ns_iox:
                (images, r) = rbdx.list(ns_iox)
                eq(r, 0)
                eq(len(images), 1)

            image_id = self.image.id()

            (info, r) = rbdx.get_info(iox, self.image_name, image_id, rbdx.INFO_F_CHILDREN_V1 | rbdx.INFO_F_SNAP_DU)
            eq(r, 0)
            eq(info.name, self.image_name)
            eq(info.size, IMG_SIZE)
            eq(info.order, IMG_ORDER)
            ok(info.features & rbd.RBD_FEATURE_OPERATIONS)
            ok(info.op_features & rbd.RBD_OPERATION_FEATURE_CLONE_PARENT)
            eq(info.du, 0)
            eq(info.dirty, 0)
            eq(len(info.snaps), 1)
            snap = find_snap(info.snaps, 'snap1')
            ok(snap != None)
            eq(snap.snap_type, rbdx.SNAPSHOT_NAMESPACE_TYPE_USER)
            eq(len(snap.children), 2)
            pool_id = rados.pool_lookup(self.pool_name)
            pool_id2 = rados.pool_lookup(self.pool_name2)
            for child in snap.children:
                ok(child.pool_id in (pool_id, pool_id2))
                if child.pool_id == pool_id:
                    eq(child.pool_id, pool_id)
                    eq(child.pool_namespace, "ns1")
                    eq(child.pool_namespace, ns_ioctx.get_namespace())
                    ok(child.image_id != '')
                else:
                    eq(child.pool_id, pool_id2)
                    eq(child.pool_namespace, "")
                    eq(child.pool_namespace, self.ioctx2.get_namespace())
                    ok(child.image_id != '')
            eq(snap.du, 0)
            eq(snap.dirty, 0)

            self.rbd.remove(ns_ioctx, clone_name)
            ns_ioctx.close()
            self.rbd.namespace_remove(self.ioctx, 'ns1')
            self.rbd.remove(self.ioctx2, clone_name)

            (info, r) = rbdx.get_info(iox, '', image_id, rbdx.INFO_F_ALL)
            eq(r, 0)
            eq(info.features & rbd.RBD_FEATURE_OPERATIONS, 0)
            eq(info.op_features & rbd.RBD_OPERATION_FEATURE_CLONE_PARENT, 0)
            eq(len(info.snaps), 1)
            snap = find_snap(info.snaps, 'snap1')
            ok(snap != None)
            eq(snap.snap_type, rbdx.SNAPSHOT_NAMESPACE_TYPE_USER)
            eq(len(snap.children), 0)

            self.image.remove_snap('snap1')


class TestGetInfo_children_v2_v1(object):

    def setUp(self):
        self.pool_name = get_temp_pool_name()
        global rados
        rados.create_pool(self.pool_name)
        self.ioctx = rados.open_ioctx(self.pool_name)

        self.rbd = rbd.RBD()
        self.image_name = create_image(self.ioctx)
        self.image = rbd.Image(self.ioctx, self.image_name)

        self.pool_name2 = get_temp_pool_name()
        rados.create_pool(self.pool_name2)
        self.ioctx2 = rados.open_ioctx(self.pool_name2)

    def tearDown(self):
        self.image.close()
        remove_image(self.ioctx, self.image_name)
        self.image = None

        self.ioctx.close()

        global rados
        rados.delete_pool(self.pool_name)

        self.ioctx2.close()
        rados.delete_pool(self.pool_name2)

    @require_new_format()
    @require_features([rbd.RBD_FEATURE_LAYERING])
    def test_children_v2_v1(self):
        with rbdx.IoCtx(self.ioctx.ioctx()) as iox:
            self.rbd.namespace_create(self.ioctx, 'ns1')
            ns_ioctx = rados.open_ioctx(self.pool_name)
            ns_ioctx.set_namespace('ns1')

            self.image.create_snap('snap1')
            clone_name = get_temp_image_name()
            rados.conf_set("rbd_default_clone_format", "2")
            self.rbd.clone(self.ioctx, self.image_name, 'snap1', ns_ioctx, clone_name, features)
            rados.conf_set("rbd_default_clone_format", "1")
            self.image.protect_snap('snap1')
            self.rbd.clone(self.ioctx, self.image_name, 'snap1', self.ioctx2, clone_name, features)
            rados.conf_set("rbd_default_clone_format", "auto")

            (images, r) = rbdx.list(iox)
            eq(r, 0)
            eq(len(images), 1)

            with rbdx.IoCtx(ns_ioctx.ioctx()) as ns_iox:
                (images, r) = rbdx.list(ns_iox)
                eq(r, 0)
                eq(len(images), 1)

            image_id = self.image.id()

            (info, r) = rbdx.get_info(iox, self.image_name, image_id, rbdx.INFO_F_CHILDREN_V1 | rbdx.INFO_F_SNAP_DU)
            eq(r, 0)
            eq(info.name, self.image_name)
            eq(info.size, IMG_SIZE)
            eq(info.order, IMG_ORDER)
            ok(info.features & rbd.RBD_FEATURE_OPERATIONS)
            ok(info.op_features & rbd.RBD_OPERATION_FEATURE_CLONE_PARENT)
            eq(info.du, 0)
            eq(info.dirty, 0)
            eq(len(info.snaps), 1)
            snap = find_snap(info.snaps, 'snap1')
            ok(snap != None)
            eq(snap.snap_type, rbdx.SNAPSHOT_NAMESPACE_TYPE_USER)
            eq(len(snap.children), 2)
            pool_id = rados.pool_lookup(self.pool_name)
            pool_id2 = rados.pool_lookup(self.pool_name2)
            for child in snap.children:
                ok(child.pool_id in (pool_id, pool_id2))
                if child.pool_id == pool_id:
                    eq(child.pool_id, pool_id)
                    eq(child.pool_namespace, "ns1")
                    eq(child.pool_namespace, ns_ioctx.get_namespace())
                    ok(child.image_id != '')
                else:
                    eq(child.pool_id, pool_id2)
                    eq(child.pool_namespace, "")
                    eq(child.pool_namespace, self.ioctx2.get_namespace())
                    ok(child.image_id != '')
            eq(snap.du, 0)
            eq(snap.dirty, 0)

            self.rbd.remove(ns_ioctx, clone_name)
            ns_ioctx.close()
            self.rbd.namespace_remove(self.ioctx, 'ns1')
            self.rbd.remove(self.ioctx2, clone_name)
            self.image.unprotect_snap('snap1')

            (info, r) = rbdx.get_info(iox, '', image_id, rbdx.INFO_F_ALL)
            eq(r, 0)
            eq(info.features & rbd.RBD_FEATURE_OPERATIONS, 0)
            eq(info.op_features & rbd.RBD_OPERATION_FEATURE_CLONE_PARENT, 0)
            eq(len(info.snaps), 1)
            snap = find_snap(info.snaps, 'snap1')
            ok(snap != None)
            eq(snap.snap_type, rbdx.SNAPSHOT_NAMESPACE_TYPE_USER)
            eq(len(snap.children), 0)

            self.image.remove_snap('snap1')


class TestList(object):

    def setUp(self):
        self.pool_name = get_temp_pool_name()
        global rados
        rados.create_pool(self.pool_name)
        self.ioctx = rados.open_ioctx(self.pool_name)

        self.rbd = rbd.RBD()
        self.image_name = create_image(self.ioctx)
        self.image = rbd.Image(self.ioctx, self.image_name)

        self.pool_name2 = get_temp_pool_name()
        rados.create_pool(self.pool_name2)
        self.ioctx2 = rados.open_ioctx(self.pool_name2)

    def tearDown(self):
        self.image.close()
        remove_image(self.ioctx, self.image_name)
        self.image = None

        self.ioctx.close()

        global rados
        rados.delete_pool(self.pool_name)

        self.ioctx2.close()
        rados.delete_pool(self.pool_name2)

    @require_new_format()
    def test_list(self):
        self.image_name2 = create_image(self.ioctx2)

        with rbdx.IoCtx(self.ioctx.ioctx()) as iox:
            (images, r) = rbdx.list(iox)
            eq(r, 0)
            eq(len(images), 1)
            eq(images.items()[0][1], self.image_name)

        with rbdx.IoCtx(self.ioctx2.ioctx()) as iox2:
            (images, r) = rbdx.list(iox2)
            eq(r, 0)
            eq(len(images), 1)
            eq(images.items()[0][1], self.image_name2)

        remove_image(self.ioctx2, self.image_name2)


class TestListInfo(object):

    def setUp(self):
        self.pool_name = get_temp_pool_name()
        global rados
        rados.create_pool(self.pool_name)
        self.ioctx = rados.open_ioctx(self.pool_name)

        self.rbd = rbd.RBD()
        self.image_name = create_image(self.ioctx)
        self.image = rbd.Image(self.ioctx, self.image_name)

        self.pool_name2 = get_temp_pool_name()
        rados.create_pool(self.pool_name2)
        self.ioctx2 = rados.open_ioctx(self.pool_name2)

    def tearDown(self):
        self.image.close()
        remove_image(self.ioctx, self.image_name)
        self.image = None

        self.ioctx.close()

        global rados
        rados.delete_pool(self.pool_name)

        self.ioctx2.close()
        rados.delete_pool(self.pool_name2)

    @require_new_format()
    def test_list_info(self):
        self.image_name2 = create_image(self.ioctx2)

        with rbdx.IoCtx(self.ioctx.ioctx()) as iox:
            (infos, r) = rbdx.list_info(iox)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

        with rbdx.IoCtx(self.ioctx.ioctx()) as iox:
            (infos, r) = rbdx.list_info(iox, rbdx.INFO_F_CHILDREN_V1)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

        with rbdx.IoCtx(self.ioctx.ioctx()) as iox:
            (infos, r) = rbdx.list_info(iox, rbdx.INFO_F_IMAGE_DU)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

        with rbdx.IoCtx(self.ioctx.ioctx()) as iox:
            (infos, r) = rbdx.list_info(iox, rbdx.INFO_F_SNAP_DU)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

        with rbdx.IoCtx(self.ioctx2.ioctx()) as iox2:
            (infos, r) = rbdx.list_info(iox2)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

        with rbdx.IoCtx(self.ioctx2.ioctx()) as iox2:
            (infos, r) = rbdx.list_info(iox2, rbdx.INFO_F_CHILDREN_V1 | rbdx.INFO_F_IMAGE_DU)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

        with rbdx.IoCtx(self.ioctx2.ioctx()) as iox2:
            (infos, r) = rbdx.list_info(iox2, rbdx.INFO_F_CHILDREN_V1 | rbdx.INFO_F_IMAGE_DU | rbdx.INFO_F_SNAP_DU)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

        with rbdx.IoCtx(self.ioctx2.ioctx()) as iox2:
            (infos, r) = rbdx.list_info(iox2, rbdx.INFO_F_CHILDREN_V1 | rbdx.INFO_F_SNAP_DU)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

        remove_image(self.ioctx2, self.image_name2)


class TestListInfo_v2(object):

    def setUp(self):
        self.pool_name = get_temp_pool_name()
        global rados
        rados.create_pool(self.pool_name)
        self.ioctx = rados.open_ioctx(self.pool_name)

        self.rbd = rbd.RBD()
        self.image_name = create_image(self.ioctx)
        self.image = rbd.Image(self.ioctx, self.image_name)

        self.pool_name2 = get_temp_pool_name()
        rados.create_pool(self.pool_name2)
        self.ioctx2 = rados.open_ioctx(self.pool_name2)

    def tearDown(self):
        self.image.close()
        remove_image(self.ioctx, self.image_name)
        self.image = None

        self.ioctx.close()

        global rados
        rados.delete_pool(self.pool_name)

        self.ioctx2.close()
        rados.delete_pool(self.pool_name2)

    @require_new_format()
    def test_list_info(self):
        self.image_name2 = create_image(self.ioctx2)

        with rbdx.IoCtx(self.ioctx.ioctx()) as iox:
            (infos, r) = rbdx.list_info(iox, {})
            eq(r, 0)
            eq(len(infos), 0)

            (images, r) = rbdx.list(iox)
            eq(r, 0)
            eq(len(images), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

            (infos, r) = rbdx.list_info(iox, images)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

            (infos, r) = rbdx.list_info(iox, images, rbdx.INFO_F_CHILDREN_V1)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

            (infos, r) = rbdx.list_info(iox, images, rbdx.INFO_F_IMAGE_DU)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

            (infos, r) = rbdx.list_info(iox, images, rbdx.INFO_F_SNAP_DU)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

            (infos, r) = rbdx.list_info(iox, images, rbdx.INFO_F_ALL)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

        with rbdx.IoCtx(self.ioctx2.ioctx()) as iox2:
            (infos, r) = rbdx.list_info(iox2, {})
            eq(r, 0)
            eq(len(infos), 0)

            images = {}
            images_it = self.rbd.list2(self.ioctx2)
            for it in images_it:
                images[it['id']] = it['name']

            (infos, r) = rbdx.list_info(iox2, images)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

            (infos, r) = rbdx.list_info(iox2, {'id1': 'name1'}, rbdx.INFO_F_CHILDREN_V1)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, -errno.ENOENT)

            images2 = {}
            images2.update(images)
            images2.update({'id1': 'name1'})
            (infos, r) = rbdx.list_info(iox2, images2, rbdx.INFO_F_SNAP_DU)
            eq(r, 0)
            eq(len(infos), 2)
            for iid, (info, r) in infos.items():
                if iid == 'id1':
                    eq(r, -errno.ENOENT)
                else:
                    eq(r, 0)

            (infos, r) = rbdx.list_info(iox2, images, rbdx.INFO_F_CHILDREN_V1)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

            (infos, r) = rbdx.list_info(iox2, images, rbdx.INFO_F_IMAGE_DU)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

            (infos, r) = rbdx.list_info(iox2, images, rbdx.INFO_F_SNAP_DU)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

            (infos, r) = rbdx.list_info(iox2, images, rbdx.INFO_F_CHILDREN_V1 | rbdx.INFO_F_IMAGE_DU)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

            (infos, r) = rbdx.list_info(iox2, images, rbdx.INFO_F_CHILDREN_V1 | rbdx.INFO_F_SNAP_DU)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

            (infos, r) = rbdx.list_info(iox2, images, rbdx.INFO_F_CHILDREN_V1 | rbdx.INFO_F_IMAGE_DU | rbdx.INFO_F_SNAP_DU)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

            (infos, r) = rbdx.list_info(iox2, images, rbdx.INFO_F_ALL)
            eq(r, 0)
            eq(len(infos), 1)
            for iid, (info, r) in infos.items():
                eq(r, 0)

        remove_image(self.ioctx2, self.image_name2)


class Test_pool_dne(object):

    def setUp(self):
        self.pool_name = get_temp_pool_name()
        global rados
        rados.create_pool(self.pool_name)
        self.ioctx = rados.open_ioctx(self.pool_name)

        self.rbd = rbd.RBD()
        self.image_name = create_image(self.ioctx)
        self.image = rbd.Image(self.ioctx, self.image_name)

    def tearDown(self):
        self.image.close()
        try:
            remove_image(self.ioctx, self.image_name)
        except rbd.ImageNotFound:
            pass
        self.image = None

        self.ioctx.close()

    @require_new_format()
    def test_pool_dne(self):
        with rbdx.IoCtx(self.ioctx.ioctx()) as iox:
            (infos, r) = rbdx.list_info(iox, {})
            eq(r, 0)
            eq(len(infos), 0)

            (images, r) = rbdx.list(iox)
            eq(r, 0)
            eq(len(images), 1)
            for iid, _ in images.items():
                pass

            global rados
            rados.delete_pool(self.pool_name)

            (info, r) = rbdx.get_info(iox, self.image_name, '')
            eq(r, -errno.ENOENT)

            (info, r) = rbdx.get_info(iox, self.image_name, '', rbdx.INFO_F_CHILDREN_V1)
            eq(r, -errno.ENOENT)

            (info, r) = rbdx.get_info(iox, self.image_name, '', rbdx.INFO_F_IMAGE_DU)
            eq(r, -errno.ENOENT)

            (info, r) = rbdx.get_info(iox, self.image_name, iid, rbdx.INFO_F_SNAP_DU)
            eq(r, -errno.ENOENT)

            (info, r) = rbdx.get_info(iox, self.image_name, iid, rbdx.INFO_F_ALL)
            eq(r, -errno.ENOENT)

            (images, r) = rbdx.list(iox)
            eq(r, 0)
            eq(len(images), 0)

            (infos, r) = rbdx.list_info(iox)
            eq(r, 0)
            eq(len(infos), 0)

            (infos, r) = rbdx.list_info(iox, rbdx.INFO_F_CHILDREN_V1)
            eq(r, 0)
            eq(len(infos), 0)

            (infos, r) = rbdx.list_info(iox, rbdx.INFO_F_IMAGE_DU)
            eq(r, 0)
            eq(len(infos), 0)

            (infos, r) = rbdx.list_info(iox, rbdx.INFO_F_SNAP_DU)
            eq(r, 0)
            eq(len(infos), 0)

            (infos, r) = rbdx.list_info(iox, rbdx.INFO_F_ALL)
            eq(r, 0)
            eq(len(infos), 0)

            (infos, r) = rbdx.list_info(iox, images)
            eq(r, 0)
            eq(len(infos), 0)

            (infos, r) = rbdx.list_info(iox, images, rbdx.INFO_F_CHILDREN_V1)
            eq(r, 0)
            eq(len(infos), 0)

            (infos, r) = rbdx.list_info(iox, images, rbdx.INFO_F_IMAGE_DU)
            eq(r, 0)
            eq(len(infos), 0)

            (infos, r) = rbdx.list_info(iox, images, rbdx.INFO_F_SNAP_DU)
            eq(r, 0)
            eq(len(infos), 0)

            (infos, r) = rbdx.list_info(iox, images, rbdx.INFO_F_ALL)
            eq(r, 0)
            eq(len(infos), 0)
