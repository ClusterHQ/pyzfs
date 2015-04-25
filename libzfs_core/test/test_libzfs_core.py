import unittest
import contextlib
import filecmp
import platform
import resource
import shutil
import stat
import subprocess
import tempfile
import uuid
from ..libzfs_core import *


def _print(*args):
    for arg in args:
        print arg,
    print


@contextlib.contextmanager
def suppress(exceptions = None):
    try:
        yield
    except BaseException as e:
        if exceptions is None or isinstance(e, exceptions):
            pass
        else:
            raise


@contextlib.contextmanager
def zfs_mount(fs):
    mntdir = tempfile.mkdtemp()
    try:
        subprocess.check_output(['mount', '-t', 'zfs', fs, mntdir], stderr = subprocess.STDOUT)
        try:
            yield mntdir
        finally:
            with suppress():
                subprocess.check_output(['umount', '-f', mntdir], stderr = subprocess.STDOUT)
    finally:
        os.rmdir(mntdir)


@contextlib.contextmanager
def cleanup_fd():
    fd = os.open('/dev/zfs', os.O_EXCL)
    try:
        yield fd
    finally:
        os.close(fd)


@contextlib.contextmanager
def os_open(name, mode):
    fd = os.open(name, mode)
    try:
        yield fd
    finally:
        os.close(fd)


@contextlib.contextmanager
def dev_null():
    with os_open('/dev/null', os.O_WRONLY) as fd:
        yield fd


@contextlib.contextmanager
def dev_zero():
    with os_open('/dev/zero', os.O_RDONLY) as fd:
        yield fd


@contextlib.contextmanager
def temp_file_in_fs(fs):
    with zfs_mount(fs) as mntdir:
        with tempfile.NamedTemporaryFile(dir = mntdir) as f:
            for i in range(1024):
                f.write('x' * 1024)
            f.flush()
            yield f.name


def make_snapshots(fs, before, modified, after):
    def _maybe_snap(snap):
        if snap is not None:
            if not snap.startswith(fs):
                snap = fs + '@' + snap
            lzc_snapshot([snap])
        return snap

    before = _maybe_snap(before)
    with temp_file_in_fs(fs) as name:
        modified = _maybe_snap(modified)
    after = _maybe_snap(after)

    return (name, (before, modified, after))


@contextlib.contextmanager
def streams(fs, first, second):
    (filename, snaps) = make_snapshots(fs, None, first, second)
    with tempfile.TemporaryFile(suffix = '.ztream') as full:
        lzc_send(snaps[1], None, full.fileno())
        full.seek(0)
        if snaps[2] is not None:
            with tempfile.TemporaryFile(suffix = '.ztream') as incremental:
                lzc_send(snaps[2], snaps[1], incremental.fileno())
                incremental.seek(0)
                yield (filename, (full, incremental))
        else:
            yield (filename, (full, None))


def runtimeSkipIf(check_method, message):
    def _decorator(f):
        def _f(_self, *args, **kwargs):
            if check_method(_self):
                return _self.skipTest(message)
            else:
                return f(_self, *args, **kwargs)
        _f.__name__ = f.__name__
        return _f
    return _decorator


def skipIfFeatureAvailable(feature, message):
    return runtimeSkipIf(lambda _self: _self.__class__.pool.isPoolFeatureAvailable(feature), message)


def skipUnlessFeatureEnabled(feature, message):
    return runtimeSkipIf(lambda _self: not _self.__class__.pool.isPoolFeatureEnabled(feature), message)


def skipUnlessBookmarksSupported(f):
    return skipUnlessFeatureEnabled('bookmarks', 'bookmarks are not enabled')(f)


def snap_always_unmounted_before_destruction():
    # Apparently ZoL automatically unmounts the snapshot
    # only if it is mounted at its default .zfs/snapshot
    # mountpoint.
    return (platform.system() != 'Linux', 'snapshot is not auto-unmounted')


def ebadf_confuses_dev_zfs_state():
    # For an unknown reason tests that are executed after a test
    # where a bad file descriptor is used are unexpectedly failing
    # on Linux.
    return (platform.system() == 'Linux', 'EBADF confuses /dev/zfs state')


def bug_with_random_file_as_cleanup_fd():
    # BUG: unable to handle kernel NULL pointer dereference at 0000000000000010
    # IP: [<ffffffffa0218aa0>] zfsdev_getminor+0x10/0x20 [zfs]
    # Call Trace:
    #  [<ffffffffa021b4b0>] zfs_onexit_fd_hold+0x20/0x40 [zfs]
    #  [<ffffffffa0214043>] zfs_ioc_hold+0x93/0xd0 [zfs]
    #  [<ffffffffa0215890>] zfsdev_ioctl+0x200/0x500 [zfs]
    return (platform.system() == 'Linux', 'Gets killed')


def lzc_send_honors_file_mode():
    # Apparently there are not enough checks in the kernel code
    # to refuse to write via a file descriptor opened in read-only mode.
    return (platform.system() == 'Linux', 'File mode is not checked')


class ZFSTest(unittest.TestCase):
    POOL_FILE_SIZE = 128 * 1024 * 1024
    FILESYSTEMS = ['fs1', 'fs2', 'fs1/fs']

    pool = None
    misc_pool = None
    readonly_pool = None

    @classmethod
    def setUpClass(cls):
        try:
            cls.pool = _TempPool(filesystems = cls.FILESYSTEMS)
            cls.misc_pool = _TempPool()
            cls.readonly_pool = _TempPool(filesystems = cls.FILESYSTEMS, readonly = True)
            cls.pools = [cls.pool, cls.misc_pool, cls.readonly_pool]
        except:
            cls._cleanUp()
            raise


    @classmethod
    def tearDownClass(cls):
        cls._cleanUp()


    @classmethod
    def _cleanUp(cls):
        for pool in [cls.pool, cls.misc_pool, cls.readonly_pool]:
            if pool is not None:
                pool.cleanUp()


    def setUp(self):
        pass


    def tearDown(self):
        for pool in ZFSTest.pools:
            pool.reset()


    def test_exists(self):
        self.assertTrue(lzc_exists(ZFSTest.pool.makeName()))


    def test_exists_in_ro_pool(self):
        self.assertTrue(lzc_exists(ZFSTest.readonly_pool.makeName()))


    def test_exists_failure(self):
        self.assertFalse(lzc_exists(ZFSTest.pool.makeName('nonexistent')))


    def test_create_fs(self):
        name = ZFSTest.pool.makeName("fs1/fs/test1")

        lzc_create(name)
        self.assertTrue(lzc_exists(name))


    def test_create_fs_with_prop(self):
        name = ZFSTest.pool.makeName("fs1/fs/test2")
        props = { "atime": 0 }

        lzc_create(name, props = props)
        self.assertTrue(lzc_exists(name))


    def test_create_fs_duplicate(self):
        name = ZFSTest.pool.makeName("fs1/fs/test6")

        lzc_create(name)

        with self.assertRaises(FilesystemExists):
            lzc_create(name)


    def test_create_fs_in_ro_pool(self):
        name = ZFSTest.readonly_pool.makeName("fs")

        with self.assertRaises(ReadOnlyPool):
            lzc_create(name)


    def test_create_fs_without_parent(self):
        name = ZFSTest.pool.makeName("fs1/nonexistent/test")

        with self.assertRaises(ParentNotFound):
            lzc_create(name)
        self.assertFalse(lzc_exists(name))


    def test_create_fs_in_nonexistent_pool(self):
        name = "no-such-pool/fs"

        with self.assertRaises(ParentNotFound):
            lzc_create(name)
        self.assertFalse(lzc_exists(name))


    def test_create_fs_with_invalid_prop(self):
        name = ZFSTest.pool.makeName("fs1/fs/test3")
        props = { "BOGUS": 0 }

        with self.assertRaises(PropertyInvalid):
            lzc_create(name, False, props)
        self.assertFalse(lzc_exists(name))


    def test_create_fs_with_invalid_prop_type(self):
        name = ZFSTest.pool.makeName("fs1/fs/test4")
        props = { "atime": "off" }

        with self.assertRaises(PropertyInvalid):
            lzc_create(name, False, props)
        self.assertFalse(lzc_exists(name))


    def test_create_fs_with_invalid_prop_val(self):
        name = ZFSTest.pool.makeName("fs1/fs/test5")
        props = { "atime": 20 }

        with self.assertRaises(PropertyInvalid):
            lzc_create(name, False, props)
        self.assertFalse(lzc_exists(name))


    def test_create_fs_with_invalid_name(self):
        name = ZFSTest.pool.makeName("@badname")

        with self.assertRaises(NameInvalid):
            lzc_create(name)
        self.assertFalse(lzc_exists(name))


    def test_create_fs_with_invalid_pool_name(self):
        name = "bad!pool/fs"

        with self.assertRaises(NameInvalid):
            lzc_create(name)
        self.assertFalse(lzc_exists(name))


    def test_snapshot(self):
        snapname = ZFSTest.pool.makeName("@snap")
        snaps = [ snapname ]

        lzc_snapshot(snaps)
        self.assertTrue(lzc_exists(snapname))


    def test_snapshot_empty_list(self):
        lzc_snapshot([])


    def test_snapshot_user_props(self):
        snapname = ZFSTest.pool.makeName("@snap")
        snaps = [ snapname ]
        props = { "user:foo": "bar" }

        lzc_snapshot(snaps, props)
        self.assertTrue(lzc_exists(snapname))


    def test_snapshot_invalid_props(self):
        snapname = ZFSTest.pool.makeName("@snap")
        snaps = [ snapname ]
        props = { "foo": "bar" }

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps, props)

        self.assertEquals(len(ctx.exception.errors), len(snaps))
        for e in ctx.exception.errors:
            self.assertIsInstance(e, PropertyInvalid)
        self.assertFalse(lzc_exists(snapname))


    def test_snapshot_ro_pool(self):
        snapname1 = ZFSTest.readonly_pool.makeName("@snap")
        snapname2 = ZFSTest.readonly_pool.makeName("fs1@snap")
        snaps = [ snapname1, snapname2 ]

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        # NB: one common error is reported.
        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, ReadOnlyPool)
        self.assertFalse(lzc_exists(snapname1))
        self.assertFalse(lzc_exists(snapname2))


    def test_snapshot_nonexistent_pool(self):
        snapname = "no-such-pool@snap"
        snaps = [snapname]

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, FilesystemNotFound)


    def test_snapshot_nonexistent_fs(self):
        snapname = ZFSTest.pool.makeName("nonexistent@snap")
        snaps = [ snapname ]

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, FilesystemNotFound)


    def test_snapshot_nonexistent_and_existent_fs(self):
        snapname1 = ZFSTest.pool.makeName("@snap")
        snapname2 = ZFSTest.pool.makeName("nonexistent@snap")
        snaps = [ snapname1, snapname2 ]

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, FilesystemNotFound)
        self.assertFalse(lzc_exists(snapname1))
        self.assertFalse(lzc_exists(snapname2))


    def test_multiple_snapshots_nonexistent_fs(self):
        snapname1 = ZFSTest.pool.makeName("nonexistent@snap1")
        snapname2 = ZFSTest.pool.makeName("nonexistent@snap2")
        snaps = [ snapname1, snapname2 ]

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        # XXX two errors should be reported but alas
        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, FilesystemNotFound)
        self.assertFalse(lzc_exists(snapname1))
        self.assertFalse(lzc_exists(snapname2))


    def test_multiple_snapshots_multiple_nonexistent_fs(self):
        snapname1 = ZFSTest.pool.makeName("nonexistent1@snap")
        snapname2 = ZFSTest.pool.makeName("nonexistent2@snap")
        snaps = [ snapname1, snapname2 ]

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        # XXX two errors should be reported but alas
        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, FilesystemNotFound)
        self.assertFalse(lzc_exists(snapname1))
        self.assertFalse(lzc_exists(snapname2))


    def test_snapshot_already_exists(self):
        snapname = ZFSTest.pool.makeName("@snap")
        snaps = [ snapname ]

        lzc_snapshot(snaps)

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, SnapshotExists)


    def test_multiple_snapshots_for_same_fs(self):
        snapname1 = ZFSTest.pool.makeName("@snap1")
        snapname2 = ZFSTest.pool.makeName("@snap2")
        snaps = [ snapname1, snapname2 ]

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, DuplicateSnapshots)
        self.assertFalse(lzc_exists(snapname1))
        self.assertFalse(lzc_exists(snapname2))


    def test_multiple_snapshots(self):
        snapname1 = ZFSTest.pool.makeName("@snap")
        snapname2 = ZFSTest.pool.makeName("fs1@snap")
        snaps = [ snapname1, snapname2 ]

        lzc_snapshot(snaps)
        self.assertTrue(lzc_exists(snapname1))
        self.assertTrue(lzc_exists(snapname2))


    def test_multiple_existing_snapshots(self):
        snapname1 = ZFSTest.pool.makeName("@snap")
        snapname2 = ZFSTest.pool.makeName("fs1@snap")
        snaps = [ snapname1, snapname2 ]

        lzc_snapshot(snaps)

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        self.assertEqual(len(ctx.exception.errors), 2)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, SnapshotExists)


    def test_multiple_new_and_existing_snapshots(self):
        snapname1 = ZFSTest.pool.makeName("@snap")
        snapname2 = ZFSTest.pool.makeName("fs1@snap")
        snapname3 = ZFSTest.pool.makeName("fs2@snap")
        snaps = [ snapname1, snapname2 ]
        more_snaps = snaps + [ snapname3 ]

        lzc_snapshot(snaps)

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(more_snaps)

        self.assertEqual(len(ctx.exception.errors), 2)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, SnapshotExists)
        self.assertFalse(lzc_exists(snapname3))


    def test_snapshot_multiple_errors(self):
        snapname1 = ZFSTest.pool.makeName("@snap")
        snapname2 = ZFSTest.pool.makeName("nonexistent@snap")
        snapname3 = ZFSTest.pool.makeName("fs1@snap")
        snaps = [ snapname1 ]
        more_snaps = [ snapname1, snapname2, snapname3 ]

        # create 'snapname1' snapshot
        lzc_snapshot(snaps)

        # attempt to create 3 snapshots:
        # 1. duplicate snapshot name
        # 2. refers to filesystem that doesn't exist
        # 3. could have succeeded if not for 1 and 2
        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        # XXX FilesystemNotFound is not reported at all.
        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, SnapshotExists)
        self.assertFalse(lzc_exists(snapname2))
        self.assertFalse(lzc_exists(snapname3))


    def test_snapshot_different_pools(self):
        snapname1 = ZFSTest.pool.makeName("@snap")
        snapname2 = ZFSTest.misc_pool.makeName("@snap")
        snaps = [ snapname1, snapname2 ]

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        # NB: one common error is reported.
        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, PoolsDiffer)
        self.assertFalse(lzc_exists(snapname1))
        self.assertFalse(lzc_exists(snapname2))


    def test_snapshot_different_pools_ro_pool(self):
        snapname1 = ZFSTest.pool.makeName("@snap")
        snapname2 = ZFSTest.readonly_pool.makeName("@snap")
        snaps = [ snapname1, snapname2 ]

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        # NB: one common error is reported.
        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            # NB: depending on whether the first attempted snapshot is
            # for the read-only pool a different error is reported.
            self.assertIsInstance(e, (PoolsDiffer, ReadOnlyPool))
        self.assertFalse(lzc_exists(snapname1))
        self.assertFalse(lzc_exists(snapname2))


    def test_snapshot_invalid_name(self):
        snapname1 = ZFSTest.pool.makeName("@bad&name")
        snapname2 = ZFSTest.pool.makeName("fs1@bad*name")
        snapname3 = ZFSTest.pool.makeName("fs2@snap")
        snaps = [snapname1, snapname2, snapname3]

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        # NB: one common error is reported.
        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameInvalid)
            self.assertIsNone(e.filename)


    def test_snapshot_too_long_complete_name(self):
        snapname1 = ZFSTest.pool.makeTooLongName("fs1@")
        snapname2 = ZFSTest.pool.makeTooLongName("fs2@")
        snapname3 = ZFSTest.pool.makeName("@snap")
        snaps = [snapname1, snapname2, snapname3]

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        self.assertEquals(len(ctx.exception.errors), 2)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameTooLong)
            self.assertIsNotNone(e.filename)


    def test_snapshot_too_long_snap_name(self):
        snapname1 = ZFSTest.pool.makeTooLongComponent("fs1@")
        snapname2 = ZFSTest.pool.makeTooLongComponent("fs2@")
        snapname3 = ZFSTest.pool.makeName("@snap")
        snaps = [snapname1, snapname2, snapname3]

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        # NB: one common error is reported.
        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameTooLong)
            self.assertIsNone(e.filename)


    def test_destroy_nonexistent_snapshot(self):
        lzc_destroy_snaps([ZFSTest.pool.makeName("@nonexistent")], False)
        lzc_destroy_snaps([ZFSTest.pool.makeName("@nonexistent")], True)


    def test_destroy_snapshot_of_nonexistent_pool(self):
        with self.assertRaises(SnapshotDestructionFailure) as ctx:
            lzc_destroy_snaps(["no-such-pool@snap"], False)

        for e in ctx.exception.errors:
            self.assertIsInstance(e, PoolNotFound)

        with self.assertRaises(SnapshotDestructionFailure) as ctx:
            lzc_destroy_snaps(["no-such-pool@snap"], True)

        for e in ctx.exception.errors:
            self.assertIsInstance(e, PoolNotFound)


    # NB: note the difference from the nonexistent pool test.
    def test_destroy_snapshot_of_nonexistent_fs(self):
        lzc_destroy_snaps([ZFSTest.pool.makeName("nonexistent@snap")], False)
        lzc_destroy_snaps([ZFSTest.pool.makeName("nonexistent@snap")], True)


    # Apparently the name is not checked for validity.
    @unittest.expectedFailure
    def test_destroy_invalid_snap_name(self):
        with self.assertRaises(SnapshotDestructionFailure) as ctx:
            lzc_destroy_snaps([ZFSTest.pool.makeName("@non$&*existent")], False)
        with self.assertRaises(SnapshotDestructionFailure) as ctx:
            lzc_destroy_snaps([ZFSTest.pool.makeName("@non$&*existent")], True)


    # Apparently the full name is not checked for length.
    @unittest.expectedFailure
    def test_destroy_too_long_full_snap_name(self):
        snapname1 = ZFSTest.pool.makeTooLongName("fs1@")
        snaps = [snapname1]

        with self.assertRaises(SnapshotDestructionFailure) as ctx:
            lzc_destroy_snaps(snaps, False)
        with self.assertRaises(SnapshotDestructionFailure) as ctx:
            lzc_destroy_snaps(snaps, True)


    def test_destroy_too_long_short_snap_name(self):
        snapname1 = ZFSTest.pool.makeTooLongComponent("fs1@")
        snapname2 = ZFSTest.pool.makeTooLongComponent("fs2@")
        snapname3 = ZFSTest.pool.makeName("@snap")
        snaps = [snapname1, snapname2, snapname3]

        with self.assertRaises(SnapshotDestructionFailure) as ctx:
            lzc_destroy_snaps(snaps, False)

        # NB: one common error is reported.
        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameTooLong)


    @unittest.skipUnless(*snap_always_unmounted_before_destruction())
    def test_destroy_mounted_snap(self):
        snap = ZFSTest.pool.getRoot().getSnap()

        lzc_snapshot([snap])
        with zfs_mount(snap):
            # the snapshot should be force-unmounted
            lzc_destroy_snaps([snap], defer = False)
            self.assertFalse(lzc_exists(snap))


    def test_clone(self):
        # NB: note the special name for the snapshot.
        # Since currently we can not destroy filesystems,
        # it would be impossible to destroy the snapshot,
        # so no point in attempting to clean it up.
        snapname = ZFSTest.pool.makeName("fs2@origin1")
        name = ZFSTest.pool.makeName("fs1/fs/clone1")

        lzc_snapshot([snapname])

        lzc_clone(name, snapname)
        self.assertTrue(lzc_exists(name))


    def test_clone_nonexistent_snapshot(self):
        snapname = ZFSTest.pool.makeName("fs2@nonexistent")
        name = ZFSTest.pool.makeName("fs1/fs/clone2")

        # XXX The error should be SnapshotNotFound
        # but limitations of C interface do not allow
        # to differentiate between the errors.
        with self.assertRaises(DatasetNotFound):
            lzc_clone(name, snapname)
        self.assertFalse(lzc_exists(name))


    def test_clone_nonexistent_parent_fs(self):
        snapname = ZFSTest.pool.makeName("fs2@origin3")
        name = ZFSTest.pool.makeName("fs1/nonexistent/clone3")

        lzc_snapshot([snapname])

        with self.assertRaises(DatasetNotFound):
            lzc_clone(name, snapname)
        self.assertFalse(lzc_exists(name))


    def test_clone_to_nonexistent_pool(self):
        snapname = ZFSTest.pool.makeName("fs2@snap")
        name = "no-such-pool/fs"

        lzc_snapshot([snapname])

        with self.assertRaises(DatasetNotFound):
            lzc_clone(name, snapname)
        self.assertFalse(lzc_exists(name))


    def test_clone_invalid_name(self):
        snapname = ZFSTest.pool.makeName("fs2@snap")
        name = ZFSTest.pool.makeName("fs1/bad#name")

        lzc_snapshot([snapname])

        with self.assertRaises(NameInvalid):
            lzc_clone(name, snapname)
        self.assertFalse(lzc_exists(name))


    def test_clone_invalid_pool_name(self):
        snapname = ZFSTest.pool.makeName("fs2@snap")
        name = "bad!pool/fs1"

        lzc_snapshot([snapname])

        with self.assertRaises(NameInvalid):
            lzc_clone(name, snapname)
        self.assertFalse(lzc_exists(name))


    def test_clone_across_pools(self):
        snapname = ZFSTest.pool.makeName("fs2@snap")
        name = ZFSTest.misc_pool.makeName("clone1")

        lzc_snapshot([snapname])

        with self.assertRaises(PoolsDiffer):
            lzc_clone(name, snapname)
        self.assertFalse(lzc_exists(name))


    def test_clone_across_pools_to_ro_pool(self):
        snapname = ZFSTest.pool.makeName("fs2@snap")
        name = ZFSTest.readonly_pool.makeName("fs1/clone1")

        lzc_snapshot([snapname])

        with self.assertRaises(ReadOnlyPool):
            lzc_clone(name, snapname)
        self.assertFalse(lzc_exists(name))


    def test_destroy_cloned_fs(self):
        snapname1 = ZFSTest.pool.makeName("fs2@origin4")
        snapname2 = ZFSTest.pool.makeName("fs1@snap")
        clonename = ZFSTest.pool.makeName("fs1/fs/clone4")
        snaps = [snapname1, snapname2]

        lzc_snapshot(snaps)
        lzc_clone(clonename, snapname1)

        with self.assertRaises(SnapshotDestructionFailure) as ctx:
            lzc_destroy_snaps(snaps, False)

        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, SnapshotIsCloned)
        for snap in snaps:
            self.assertTrue(lzc_exists(snap))


    def test_deferred_destroy_cloned_fs(self):
        snapname1 = ZFSTest.pool.makeName("fs2@origin5")
        snapname2 = ZFSTest.pool.makeName("fs1@snap")
        clonename = ZFSTest.pool.makeName("fs1/fs/clone5")
        snaps = [snapname1, snapname2]

        lzc_snapshot(snaps)
        lzc_clone(clonename, snapname1)

        lzc_destroy_snaps(snaps, defer = True)

        self.assertTrue(lzc_exists(snapname1))
        self.assertFalse(lzc_exists(snapname2))


    def test_rollback(self):
        name = ZFSTest.pool.makeName("fs1")
        snapname = name + "@snap"

        lzc_snapshot([snapname])
        ret = lzc_rollback(name)
        self.assertEqual(ret, snapname)


    def test_rollback_2(self):
        name = ZFSTest.pool.makeName("fs1")
        snapname1 = name + "@snap1"
        snapname2 = name + "@snap2"

        lzc_snapshot([snapname1])
        lzc_snapshot([snapname2])
        ret = lzc_rollback(name)
        self.assertEqual(ret, snapname2)


    def test_rollback_no_snaps(self):
        name = ZFSTest.pool.makeName("fs1")

        with self.assertRaises(SnapshotNotFound) as ctx:
            lzc_rollback(name)


    def test_rollback_non_existent_fs(self):
        name = ZFSTest.pool.makeName("nonexistent")

        with self.assertRaises(FilesystemNotFound) as ctx:
            lzc_rollback(name)


    def test_rollback_invalid_fs_name(self):
        name = ZFSTest.pool.makeName("bad~name")

        with self.assertRaises(NameInvalid) as ctx:
            lzc_rollback(name)


    def test_rollback_snap_name(self):
        name = ZFSTest.pool.makeName("fs1@snap")

        with self.assertRaises(NameInvalid) as ctx:
            lzc_rollback(name)


    def test_rollback_snap_name_2(self):
        name = ZFSTest.pool.makeName("fs1@snap")

        lzc_snapshot([name])
        with self.assertRaises(NameInvalid) as ctx:
            lzc_rollback(name)


    def test_rollback_too_long_fs_name(self):
        name = ZFSTest.pool.makeTooLongName()

        with self.assertRaises(NameTooLong) as ctx:
            lzc_rollback(name)


    @skipUnlessBookmarksSupported
    def test_bookmarks(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1'), ZFSTest.pool.makeName('fs2@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs1#bmark1'), ZFSTest.pool.makeName('fs2#bmark1')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps)
        lzc_bookmark(bmark_dict)


    @skipUnlessBookmarksSupported
    def test_bookmarks_2(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1'), ZFSTest.pool.makeName('fs2@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs1#bmark1'), ZFSTest.pool.makeName('fs2#bmark1')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps)
        lzc_bookmark(bmark_dict)
        lzc_destroy_snaps(snaps, defer = False)


    @skipUnlessBookmarksSupported
    def test_bookmarks_empty(self):
        lzc_bookmark({})


    @skipUnlessBookmarksSupported
    def test_bookmarks_mismatching_name(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs2#bmark1')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps)
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)

        for e in ctx.exception.errors:
            self.assertIsInstance(e, BookmarkMismatch)


    @skipUnlessBookmarksSupported
    def test_bookmarks_invalid_name(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs1#bmark!')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps)
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)

        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameInvalid)


    @skipUnlessBookmarksSupported
    def test_bookmarks_invalid_name_2(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs1@bmark')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps)
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)

        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameInvalid)


    @skipUnlessBookmarksSupported
    def test_bookmarks_too_long_name(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1')]
        bmarks = [ZFSTest.pool.makeTooLongName('fs1#')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps)
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)

        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameTooLong)


    @skipUnlessBookmarksSupported
    def test_bookmarks_too_long_name_2(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1')]
        bmarks = [ZFSTest.pool.makeTooLongComponent('fs1#')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps)
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)

        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameTooLong)


    @skipUnlessBookmarksSupported
    def test_bookmarks_mismatching_names(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1'), ZFSTest.pool.makeName('fs2@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs2#bmark1'), ZFSTest.pool.makeName('fs1#bmark1')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps)
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)

        for e in ctx.exception.errors:
            self.assertIsInstance(e, BookmarkMismatch)


    @skipUnlessBookmarksSupported
    def test_bookmarks_partially_mismatching_names(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1'), ZFSTest.pool.makeName('fs2@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs2#bmark'), ZFSTest.pool.makeName('fs2#bmark1')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps)
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)

        for e in ctx.exception.errors:
            self.assertIsInstance(e, BookmarkMismatch)


    @skipUnlessBookmarksSupported
    def test_bookmarks_cross_pool(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1'), ZFSTest.misc_pool.makeName('@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs1#bmark1'), ZFSTest.misc_pool.makeName('#bmark1')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps[0:1])
        lzc_snapshot(snaps[1:2])
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)

        for e in ctx.exception.errors:
            self.assertIsInstance(e, PoolsDiffer)


    @skipUnlessBookmarksSupported
    def test_bookmarks_missing_snap(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1'), ZFSTest.pool.makeName('fs2@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs1#bmark1'), ZFSTest.pool.makeName('fs2#bmark1')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps[0:1])
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)

        for e in ctx.exception.errors:
            self.assertIsInstance(e, SnapshotNotFound)


    @skipUnlessBookmarksSupported
    def test_bookmarks_missing_snaps(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1'), ZFSTest.pool.makeName('fs2@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs1#bmark1'), ZFSTest.pool.makeName('fs2#bmark1')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)

        for e in ctx.exception.errors:
            self.assertIsInstance(e, SnapshotNotFound)


    @skipUnlessBookmarksSupported
    def test_bookmarks_for_the_same_snap(self):
        snap = ZFSTest.pool.makeName('fs1@snap1')
        bmark1 = ZFSTest.pool.makeName('fs1#bmark1')
        bmark2 = ZFSTest.pool.makeName('fs1#bmark2')
        bmark_dict = {bmark1: snap, bmark2: snap}

        lzc_snapshot([snap])
        lzc_bookmark(bmark_dict)


    @skipUnlessBookmarksSupported
    def test_bookmarks_for_the_same_snap_2(self):
        snap = ZFSTest.pool.makeName('fs1@snap1')
        bmark1 = ZFSTest.pool.makeName('fs1#bmark1')
        bmark2 = ZFSTest.pool.makeName('fs1#bmark2')
        bmark_dict1 = {bmark1: snap}
        bmark_dict2 = {bmark2: snap}

        lzc_snapshot([snap])
        lzc_bookmark(bmark_dict1)
        lzc_bookmark(bmark_dict2)


    def test_bookmarks_duplicate_name(self):
        snap1 = ZFSTest.pool.makeName('fs1@snap1')
        snap2 = ZFSTest.pool.makeName('fs1@snap2')
        bmark = ZFSTest.pool.makeName('fs1#bmark')
        bmark_dict1 = {bmark: snap1}
        bmark_dict2 = {bmark: snap2}

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])
        lzc_bookmark(bmark_dict1)
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict2)

        for e in ctx.exception.errors:
            self.assertIsInstance(e, BookmarkExists)


    @skipUnlessBookmarksSupported
    def test_get_bookmarks(self):
        snap1 = ZFSTest.pool.makeName('fs1@snap1')
        snap2 = ZFSTest.pool.makeName('fs1@snap2')
        bmark = ZFSTest.pool.makeName('fs1#bmark')
        bmark1 = ZFSTest.pool.makeName('fs1#bmark1')
        bmark2 = ZFSTest.pool.makeName('fs1#bmark2')
        bmark_dict1 = {bmark1: snap1, bmark2: snap2}
        bmark_dict2 = {bmark: snap2}

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])
        lzc_bookmark(bmark_dict1)
        lzc_bookmark(bmark_dict2)
        lzc_destroy_snaps([snap1, snap2], defer = False)

        bmarks = lzc_get_bookmarks(ZFSTest.pool.makeName('fs1'))
        self.assertEquals(len(bmarks), 3)
        for b in 'bmark', 'bmark1', 'bmark2':
            self.assertTrue(b in bmarks)
            self.assertIsInstance(bmarks[b], dict)
            self.assertEquals(len(bmarks[b]), 0)

        bmarks = lzc_get_bookmarks(ZFSTest.pool.makeName('fs1'), ['guid', 'createtxg', 'creation'])
        self.assertEquals(len(bmarks), 3)
        for b in 'bmark', 'bmark1', 'bmark2':
            self.assertTrue(b in bmarks)
            self.assertIsInstance(bmarks[b], dict)
            self.assertEquals(len(bmarks[b]), 3)


    @skipUnlessBookmarksSupported
    def test_get_bookmarks_invalid_property(self):
        snap = ZFSTest.pool.makeName('fs1@snap')
        bmark = ZFSTest.pool.makeName('fs1#bmark')
        bmark_dict = {bmark: snap}

        lzc_snapshot([snap])
        lzc_bookmark(bmark_dict)

        bmarks = lzc_get_bookmarks(ZFSTest.pool.makeName('fs1'), ['badprop'])
        self.assertEquals(len(bmarks), 1)
        for b in ('bmark', ):
            self.assertTrue(b in bmarks)
            self.assertIsInstance(bmarks[b], dict)
            self.assertEquals(len(bmarks[b]), 0)


    @skipUnlessBookmarksSupported
    def test_get_bookmarks_nonexistent_fs(self):
        with self.assertRaises(FilesystemNotFound):
            bmarks = lzc_get_bookmarks(ZFSTest.pool.makeName('nonexistent'))


    @skipUnlessBookmarksSupported
    def test_destroy_bookmarks(self):
        snap = ZFSTest.pool.makeName('fs1@snap')
        bmark = ZFSTest.pool.makeName('fs1#bmark')
        bmark_dict = {bmark: snap}

        lzc_snapshot([snap])
        lzc_bookmark(bmark_dict)

        lzc_destroy_bookmarks([bmark, ZFSTest.pool.makeName('fs1#nonexistent')])
        bmarks = lzc_get_bookmarks(ZFSTest.pool.makeName('fs1'))
        self.assertEquals(len(bmarks), 0)


    @skipUnlessBookmarksSupported
    def test_destroy_bookmarks_invalid_name(self):
        snap = ZFSTest.pool.makeName('fs1@snap')
        bmark = ZFSTest.pool.makeName('fs1#bmark')
        bmark_dict = {bmark: snap}

        lzc_snapshot([snap])
        lzc_bookmark(bmark_dict)

        with self.assertRaises(BookmarkDestructionFailure) as ctx:
            lzc_destroy_bookmarks([bmark, ZFSTest.pool.makeName('fs1/nonexistent')])
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameInvalid)

        bmarks = lzc_get_bookmarks(ZFSTest.pool.makeName('fs1'))
        self.assertEquals(len(bmarks), 1)
        self.assertTrue('bmark' in bmarks)


    @skipUnlessBookmarksSupported
    def test_destroy_bookmark_nonexistent_fs(self):
        lzc_destroy_bookmarks([ZFSTest.pool.makeName('nonexistent#bmark')])


    @skipUnlessBookmarksSupported
    def test_destroy_bookmarks_empty(self):
        lzc_bookmark({})


    def test_snaprange_space(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1@snap2")
        snap3 = ZFSTest.pool.makeName("fs1@snap")

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])
        lzc_snapshot([snap3])

        space = lzc_snaprange_space(snap1, snap2)
        self.assertIsInstance(space, (int, long))
        space = lzc_snaprange_space(snap2, snap3)
        self.assertIsInstance(space, (int, long))
        space = lzc_snaprange_space(snap1, snap3)
        self.assertIsInstance(space, (int, long))


    def test_snaprange_space_2(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1@snap2")
        snap3 = ZFSTest.pool.makeName("fs1@snap")

        lzc_snapshot([snap1])
        with zfs_mount(ZFSTest.pool.makeName("fs1")) as mntdir:
            with tempfile.NamedTemporaryFile(dir = mntdir) as f:
                for i in range(1024):
                    f.write('x' * 1024)
                f.flush()
                lzc_snapshot([snap2])
        lzc_snapshot([snap3])

        space = lzc_snaprange_space(snap1, snap2)
        self.assertGreater(space, 1024 * 1024)
        space = lzc_snaprange_space(snap2, snap3)
        self.assertGreater(space, 1024 * 1024)
        space = lzc_snaprange_space(snap1, snap3)
        self.assertGreater(space, 1024 * 1024)


    def test_snaprange_space_same_snap(self):
        snap = ZFSTest.pool.makeName("fs1@snap")

        with zfs_mount(ZFSTest.pool.makeName("fs1")) as mntdir:
            with tempfile.NamedTemporaryFile(dir = mntdir) as f:
                for i in range(1024):
                    f.write('x' * 1024)
                f.flush()
                lzc_snapshot([snap])

        space = lzc_snaprange_space(snap, snap)
        self.assertGreater(space, 1024 * 1024)
        self.assertAlmostEqual(space, 1024 * 1024, delta = 1024 * 1024 / 20)


    def test_snaprange_space_wrong_order(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1@snap2")

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])

        with self.assertRaises(SnapshotMismatch):
            space = lzc_snaprange_space(snap2, snap1)


    def test_snaprange_space_unrelated(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs2@snap2")

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])

        with self.assertRaises(SnapshotMismatch):
            space = lzc_snaprange_space(snap1, snap2)


    def test_snaprange_space_across_pools(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.misc_pool.makeName("@snap2")

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])

        with self.assertRaises(PoolsDiffer):
            space = lzc_snaprange_space(snap1, snap2)


    def test_snaprange_space_nonexistent(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs2@snap2")

        lzc_snapshot([snap1])

        with self.assertRaises(SnapshotNotFound) as ctx:
            space = lzc_snaprange_space(snap1, snap2)
        self.assertEquals(ctx.exception.filename, snap2)

        with self.assertRaises(SnapshotNotFound) as ctx:
            space = lzc_snaprange_space(snap2, snap1)
        self.assertEquals(ctx.exception.filename, snap1)


    def test_snaprange_space_invalid_name(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1@sn#p")

        lzc_snapshot([snap1])

        with self.assertRaises(NameInvalid):
            space = lzc_snaprange_space(snap1, snap2)


    def test_snaprange_space_not_snap(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1")

        lzc_snapshot([snap1])

        with self.assertRaises(NameInvalid):
            space = lzc_snaprange_space(snap1, snap2)
        with self.assertRaises(NameInvalid):
            space = lzc_snaprange_space(snap2, snap1)


    def test_snaprange_space_not_snap_2(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1#bmark")

        lzc_snapshot([snap1])

        with self.assertRaises(NameInvalid):
            space = lzc_snaprange_space(snap1, snap2)
        with self.assertRaises(NameInvalid):
            space = lzc_snaprange_space(snap2, snap1)


    def test_send_space(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1@snap2")
        snap3 = ZFSTest.pool.makeName("fs1@snap")

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])
        lzc_snapshot([snap3])

        space = lzc_send_space(snap2, snap1)
        self.assertIsInstance(space, (int, long))
        space = lzc_send_space(snap3, snap2)
        self.assertIsInstance(space, (int, long))
        space = lzc_send_space(snap3, snap1)
        self.assertIsInstance(space, (int, long))
        space = lzc_send_space(snap1)
        self.assertIsInstance(space, (int, long))
        space = lzc_send_space(snap2)
        self.assertIsInstance(space, (int, long))
        space = lzc_send_space(snap3)
        self.assertIsInstance(space, (int, long))


    def test_send_space_2(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1@snap2")
        snap3 = ZFSTest.pool.makeName("fs1@snap")

        lzc_snapshot([snap1])
        with zfs_mount(ZFSTest.pool.makeName("fs1")) as mntdir:
            with tempfile.NamedTemporaryFile(dir = mntdir) as f:
                for i in range(1024):
                    f.write('x' * 1024)
                f.flush()
                lzc_snapshot([snap2])
        lzc_snapshot([snap3])

        space = lzc_send_space(snap2, snap1)
        self.assertGreater(space, 1024 * 1024)

        space = lzc_send_space(snap3, snap2)

        space = lzc_send_space(snap3, snap1)

        space_empty = lzc_send_space(snap1)

        space = lzc_send_space(snap2)
        self.assertGreater(space, 1024 * 1024)

        space = lzc_send_space(snap3)
        self.assertEquals(space, space_empty)


    def test_send_space_same_snap(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        lzc_snapshot([snap1])
        with self.assertRaises(SnapshotMismatch):
            space = lzc_send_space(snap1, snap1)


    def test_send_space_wrong_order(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1@snap2")

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])

        with self.assertRaises(SnapshotMismatch):
            space = lzc_send_space(snap1, snap2)


    def test_send_space_unrelated(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs2@snap2")

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])

        with self.assertRaises(SnapshotMismatch):
            space = lzc_send_space(snap1, snap2)


    def test_send_space_across_pools(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.misc_pool.makeName("@snap2")

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])

        with self.assertRaises(PoolsDiffer):
            space = lzc_send_space(snap1, snap2)


    def test_send_space_nonexistent(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs2@snap2")

        lzc_snapshot([snap1])

        with self.assertRaises(SnapshotNotFound) as ctx:
            space = lzc_send_space(snap1, snap2)
        self.assertEquals(ctx.exception.filename, snap1)

        with self.assertRaises(SnapshotNotFound) as ctx:
            space = lzc_send_space(snap2, snap1)
        self.assertEquals(ctx.exception.filename, snap2)

        with self.assertRaises(SnapshotNotFound) as ctx:
            space = lzc_send_space(snap2)
        self.assertEquals(ctx.exception.filename, snap2)


    def test_send_space_invalid_name(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1@sn!p")

        lzc_snapshot([snap1])

        with self.assertRaises(NameInvalid) as ctx:
            space = lzc_send_space(snap2, snap1)
        self.assertEquals(ctx.exception.filename, snap2)
        with self.assertRaises(NameInvalid) as ctx:
            space = lzc_send_space(snap2)
        self.assertEquals(ctx.exception.filename, snap2)
        with self.assertRaises(NameInvalid) as ctx:
            space = lzc_send_space(snap1, snap2)
        self.assertEquals(ctx.exception.filename, snap2)


    def test_send_space_not_snap(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1")

        lzc_snapshot([snap1])

        with self.assertRaises(NameInvalid):
            space = lzc_send_space(snap1, snap2)
        with self.assertRaises(NameInvalid):
            space = lzc_send_space(snap2, snap1)
        with self.assertRaises(NameInvalid):
            space = lzc_send_space(snap2)


    def test_send_space_not_snap_2(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1#bmark")

        lzc_snapshot([snap1])

        with self.assertRaises(NameInvalid):
            space = lzc_send_space(snap1, snap2)
        with self.assertRaises(NameInvalid):
            space = lzc_send_space(snap2, snap1)
        with self.assertRaises(NameInvalid):
            space = lzc_send_space(snap2)


    def test_send_full(self):
        snap = ZFSTest.pool.makeName("fs1@snap")

        with zfs_mount(ZFSTest.pool.makeName("fs1")) as mntdir:
            with tempfile.NamedTemporaryFile(dir = mntdir) as f:
                for i in range(1024):
                    f.write('x' * 1024)
                f.flush()
                lzc_snapshot([snap])

        with tempfile.TemporaryFile(suffix = '.ztream') as output:
            estimate = lzc_send_space(snap)

            fd = output.fileno()
            lzc_send(snap, None, fd)
            st = os.fstat(fd)
            # 5%, arbitrary.
            self.assertAlmostEqual(st.st_size, estimate, delta = estimate / 20)


    def test_send_incremental(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1@snap2")

        lzc_snapshot([snap1])
        with zfs_mount(ZFSTest.pool.makeName("fs1")) as mntdir:
            with tempfile.NamedTemporaryFile(dir = mntdir) as f:
                for i in range(1024):
                    f.write('x' * 1024)
                f.flush()
                lzc_snapshot([snap2])

        with tempfile.TemporaryFile(suffix = '.ztream') as output:
            estimate = lzc_send_space(snap2, snap1)

            fd = output.fileno()
            lzc_send(snap2, snap1, fd)
            st = os.fstat(fd)
            # 5%, arbitrary.
            self.assertAlmostEqual(st.st_size, estimate, delta = estimate / 20)


    def test_send_flags(self):
        snap = ZFSTest.pool.makeName("fs1@snap")
        lzc_snapshot([snap])
        with dev_null() as fd:
            lzc_send(snap, None, fd, ['large_blocks'])
            lzc_send(snap, None, fd, ['embedded_data'])
            lzc_send(snap, None, fd, ['embedded_data', 'large_blocks'])


    def test_send_unknown_flags(self):
        snap = ZFSTest.pool.makeName("fs1@snap")
        lzc_snapshot([snap])
        with dev_null() as fd:
            with self.assertRaises(ValueError):
                lzc_send(snap, None, fd, ['embedded_data', 'UNKNOWN'])


    def test_send_same_snap(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        lzc_snapshot([snap1])
        with tempfile.TemporaryFile(suffix = '.ztream') as output:
            fd = output.fileno()
            with self.assertRaises(SnapshotMismatch):
                lzc_send(snap1, snap1, fd)


    def test_send_wrong_order(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1@snap2")

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])

        with tempfile.TemporaryFile(suffix = '.ztream') as output:
            fd = output.fileno()
            with self.assertRaises(SnapshotMismatch):
                lzc_send(snap1, snap2, fd)


    def test_send_unrelated(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs2@snap2")

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])

        with tempfile.TemporaryFile(suffix = '.ztream') as output:
            fd = output.fileno()
            with self.assertRaises(SnapshotMismatch):
                lzc_send(snap1, snap2, fd)


    def test_send_across_pools(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.misc_pool.makeName("@snap2")

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])

        with tempfile.TemporaryFile(suffix = '.ztream') as output:
            fd = output.fileno()
            with self.assertRaises(PoolsDiffer):
                lzc_send(snap1, snap2, fd)


    def test_send_nonexistent(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1@snap2")

        lzc_snapshot([snap1])

        with tempfile.TemporaryFile(suffix = '.ztream') as output:
            fd = output.fileno()
            with self.assertRaises(SnapshotNotFound) as ctx:
                lzc_send(snap1, snap2, fd)
            self.assertEquals(ctx.exception.filename, snap1)

            with self.assertRaises(SnapshotNotFound) as ctx:
                lzc_send(snap2, snap1, fd)
            self.assertEquals(ctx.exception.filename, snap2)

            with self.assertRaises(SnapshotNotFound) as ctx:
                lzc_send(snap2, None, fd)
            self.assertEquals(ctx.exception.filename, snap2)


    def test_send_invalid_name(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1@sn!p")

        lzc_snapshot([snap1])

        with tempfile.TemporaryFile(suffix = '.ztream') as output:
            fd = output.fileno()
            with self.assertRaises(NameInvalid) as ctx:
                lzc_send(snap2, snap1, fd)
            self.assertEquals(ctx.exception.filename, snap2)
            with self.assertRaises(NameInvalid) as ctx:
                lzc_send(snap2, None, fd)
            self.assertEquals(ctx.exception.filename, snap2)
            with self.assertRaises(NameInvalid) as ctx:
                lzc_send(snap1, snap2, fd)
            self.assertEquals(ctx.exception.filename, snap2)


    # XXX Although undocumented the API allows to create an incremental
    # or full stream for a filesystem as if a temporary unnamed snapshot
    # is taken at some time after the call is made and before the stream
    # starts being produced.
    def test_send_filesystem(self):
        snap = ZFSTest.pool.makeName("fs1@snap1")
        fs = ZFSTest.pool.makeName("fs1")

        lzc_snapshot([snap])

        with tempfile.TemporaryFile(suffix = '.ztream') as output:
            fd = output.fileno()
            lzc_send(fs, snap, fd)
            lzc_send(fs, None, fd)


    def test_send_from_filesystem(self):
        snap = ZFSTest.pool.makeName("fs1@snap1")
        fs = ZFSTest.pool.makeName("fs1")

        lzc_snapshot([snap])

        with tempfile.TemporaryFile(suffix = '.ztream') as output:
            fd = output.fileno()
            with self.assertRaises(NameInvalid):
                lzc_send(snap, fs, fd)


    def test_send_bookmark(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1@snap2")
        bmark = ZFSTest.pool.makeName("fs1#bmark")

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])
        lzc_bookmark({bmark: snap2})
        lzc_destroy_snaps([snap2], defer = False)

        with tempfile.TemporaryFile(suffix = '.ztream') as output:
            fd = output.fileno()
            with self.assertRaises(NameInvalid):
                lzc_send(bmark, snap1, fd)
            with self.assertRaises(NameInvalid):
                lzc_send(bmark, None, fd)


    def test_send_from_bookmark(self):
        snap1 = ZFSTest.pool.makeName("fs1@snap1")
        snap2 = ZFSTest.pool.makeName("fs1@snap2")
        bmark = ZFSTest.pool.makeName("fs1#bmark")

        lzc_snapshot([snap1])
        lzc_snapshot([snap2])
        lzc_bookmark({bmark: snap1})
        lzc_destroy_snaps([snap1], defer = False)

        with tempfile.TemporaryFile(suffix = '.ztream') as output:
            fd = output.fileno()
            lzc_send(snap2, bmark, fd)


    @unittest.skipIf(*ebadf_confuses_dev_zfs_state())
    def test_send_bad_fd(self):
        snap = ZFSTest.pool.makeName("fs1@snap")
        lzc_snapshot([snap])

        with tempfile.TemporaryFile() as tmp:
            bad_fd = tmp.fileno()

        with self.assertRaises(IOError) as ctx:
            lzc_send(snap, None, bad_fd)
        self.assertEquals(ctx.exception.errno, errno.EBADF)


    @unittest.skipIf(*ebadf_confuses_dev_zfs_state())
    def test_send_bad_fd_2(self):
        snap = ZFSTest.pool.makeName("fs1@snap")
        lzc_snapshot([snap])

        with tempfile.TemporaryFile() as tmp:
            bad_fd = tmp.fileno()

        with self.assertRaises(IOError) as ctx:
            lzc_send(snap, None, -2)
        self.assertEquals(ctx.exception.errno, errno.EBADF)


    @unittest.skipIf(*ebadf_confuses_dev_zfs_state())
    def test_send_bad_fd_3(self):
        snap = ZFSTest.pool.makeName("fs1@snap")
        lzc_snapshot([snap])

        with tempfile.TemporaryFile() as tmp:
            bad_fd = tmp.fileno()

        (soft, hard) = resource.getrlimit(resource.RLIMIT_NOFILE)
        bad_fd = hard + 1
        with self.assertRaises(IOError) as ctx:
            lzc_send(snap, None, bad_fd)
        self.assertEquals(ctx.exception.errno, errno.EBADF)


    def test_send_to_broken_pipe(self):
        snap = ZFSTest.pool.makeName("fs1@snap")
        lzc_snapshot([snap])

        proc = subprocess.Popen(['true'], stdin = subprocess.PIPE)
        proc.wait()
        with self.assertRaises(IOError) as ctx:
            lzc_send(snap, None, proc.stdin.fileno())
        self.assertEquals(ctx.exception.errno, errno.EPIPE)


    def test_send_to_broken_pipe_2(self):
        snap = ZFSTest.pool.makeName("fs1@snap")
        with zfs_mount(ZFSTest.pool.makeName("fs1")) as mntdir:
            with tempfile.NamedTemporaryFile(dir = mntdir) as f:
                for i in range(1024):
                    f.write('x' * 1024)
                f.flush()
                lzc_snapshot([snap])

        proc = subprocess.Popen(['sleep', '2'], stdin = subprocess.PIPE)
        with self.assertRaises(IOError) as ctx:
            lzc_send(snap, None, proc.stdin.fileno())
        self.assertEquals(ctx.exception.errno, errno.EPIPE)


    @unittest.skipUnless(*lzc_send_honors_file_mode())
    def test_send_to_ro_file(self):
        snap = ZFSTest.pool.makeName("fs1@snap")
        lzc_snapshot([snap])

        with tempfile.NamedTemporaryFile(suffix = '.ztream', delete = False) as output:
            # tempfile always opens a temporary file in read-write mode
            # regardless of the specified mode, so we have to open it again.
            os.chmod(output.name, stat.S_IRUSR)
            fd = os.open(output.name, os.O_RDONLY)
            with self.assertRaises(IOError) as ctx:
                lzc_send(snap, None, fd)
            os.close(fd)
        self.assertEquals(ctx.exception.errno, errno.EBADF)


    def test_recv_full(self):
        src = ZFSTest.pool.makeName("fs1@snap")
        dst = ZFSTest.pool.makeName("fs2/received-1@snap")

        with temp_file_in_fs(ZFSTest.pool.makeName("fs1")) as name:
            lzc_snapshot([src])

        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(src, None, stream.fileno())
            stream.seek(0)
            lzc_receive(dst, stream.fileno())

        name = os.path.basename(name)
        with zfs_mount(src) as mnt1, zfs_mount(dst) as mnt2:
            self.assertTrue(filecmp.cmp(os.path.join(mnt1, name), os.path.join(mnt2, name), False))


    def test_recv_incremental(self):
        src1 = ZFSTest.pool.makeName("fs1@snap1")
        src2 = ZFSTest.pool.makeName("fs1@snap2")
        dst1 = ZFSTest.pool.makeName("fs2/received-2@snap1")
        dst2 = ZFSTest.pool.makeName("fs2/received-2@snap2")

        lzc_snapshot([src1])
        with temp_file_in_fs(ZFSTest.pool.makeName("fs1")) as name:
            lzc_snapshot([src2])

        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(src1, None, stream.fileno())
            stream.seek(0)
            lzc_receive(dst1, stream.fileno())
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(src2, src1, stream.fileno())
            stream.seek(0)
            lzc_receive(dst2, stream.fileno())

        name = os.path.basename(name)
        with zfs_mount(src2) as mnt1, zfs_mount(dst2) as mnt2:
            self.assertTrue(filecmp.cmp(os.path.join(mnt1, name), os.path.join(mnt2, name), False))


    def test_recv_clone(self):
        orig_src = ZFSTest.pool.makeName("fs2@send-origin")
        clone = ZFSTest.pool.makeName("fs1/fs/send-clone")
        clone_snap = clone + "@snap"
        orig_dst = ZFSTest.pool.makeName("fs1/fs/recv-origin@snap")
        clone_dst = ZFSTest.pool.makeName("fs1/fs/recv-clone@snap")

        lzc_snapshot([orig_src])
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(orig_src, None, stream.fileno())
            stream.seek(0)
            lzc_receive(orig_dst, stream.fileno())

        lzc_clone(clone, orig_src)
        lzc_snapshot([clone_snap])
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(clone_snap, orig_src, stream.fileno())
            stream.seek(0)
            lzc_receive(clone_dst, stream.fileno(), origin = orig_dst)


    def test_recv_full_already_existing_empty_fs(self):
        src = ZFSTest.pool.makeName("fs1@snap")
        dstfs = ZFSTest.pool.makeName("fs2/received-3")
        dst = dstfs + '@snap'

        with temp_file_in_fs(ZFSTest.pool.makeName("fs1")) as name:
            lzc_snapshot([src])
        lzc_create(dstfs)
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(src, None, stream.fileno())
            stream.seek(0)
            with self.assertRaises((DestinationModified, DatasetExists)):
                lzc_receive(dst, stream.fileno())


    def test_recv_full_into_root_empty_pool(self):
        empty_pool = None
        try:
            srcfs = ZFSTest.pool.makeName("fs1")
            empty_pool = _TempPool()
            dst = empty_pool.makeName('@snap')

            with streams(srcfs, "snap", None) as (_, (stream, _)):
                with self.assertRaises((DestinationModified, DatasetExists)):
                    lzc_receive(dst, stream.fileno())
        finally:
            if empty_pool is not None:
                empty_pool.cleanUp()


    def test_recv_full_into_ro_pool(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        dst = ZFSTest.readonly_pool.makeName('fs2/received@snap')

        with streams(srcfs, "snap", None) as (_, (stream, _)):
            with self.assertRaises(ReadOnlyPool):
                lzc_receive(dst, stream.fileno())


    def test_recv_full_already_existing_modified_fs(self):
        src = ZFSTest.pool.makeName("fs1@snap")
        dstfs = ZFSTest.pool.makeName("fs2/received-5")
        dst = dstfs + '@snap'

        with temp_file_in_fs(ZFSTest.pool.makeName("fs1")) as name:
            lzc_snapshot([src])
        lzc_create(dstfs)
        with temp_file_in_fs(dstfs):
            with tempfile.TemporaryFile(suffix = '.ztream') as stream:
                lzc_send(src, None, stream.fileno())
                stream.seek(0)
                with self.assertRaises((DestinationModified, DatasetExists)):
                    lzc_receive(dst, stream.fileno())


    def test_recv_full_already_existing_with_snapshots(self):
        src = ZFSTest.pool.makeName("fs1@snap")
        dstfs = ZFSTest.pool.makeName("fs2/received-4")
        dst = dstfs + '@snap'

        with temp_file_in_fs(ZFSTest.pool.makeName("fs1")) as name:
            lzc_snapshot([src])
        lzc_create(dstfs)
        lzc_snapshot([dstfs + "@snap1"])
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(src, None, stream.fileno())
            stream.seek(0)
            with self.assertRaises((StreamMismatch, DatasetExists)):
                lzc_receive(dst, stream.fileno())


    def test_recv_full_already_existing_snapshot(self):
        src = ZFSTest.pool.makeName("fs1@snap")
        dstfs = ZFSTest.pool.makeName("fs2/received-6")
        dst = dstfs + '@snap'

        with temp_file_in_fs(ZFSTest.pool.makeName("fs1")) as name:
            lzc_snapshot([src])
        lzc_create(dstfs)
        lzc_snapshot([dst])
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(src, None, stream.fileno())
            stream.seek(0)
            with self.assertRaises(DatasetExists):
                lzc_receive(dst, stream.fileno())


    def test_recv_full_missing_parent_fs(self):
        src = ZFSTest.pool.makeName("fs1@snap")
        dst = ZFSTest.pool.makeName("fs2/nonexistent/fs@snap")

        with temp_file_in_fs(ZFSTest.pool.makeName("fs1")) as name:
            lzc_snapshot([src])
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(src, None, stream.fileno())
            stream.seek(0)
            with self.assertRaises(DatasetNotFound):
                lzc_receive(dst, stream.fileno())


    def test_recv_full_but_specify_origin(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src = srcfs + "@snap"
        dstfs = ZFSTest.pool.makeName("fs2/received-30")
        dst = dstfs + '@snap'
        origin1 = ZFSTest.pool.makeName("fs2@snap1")
        origin2 = ZFSTest.pool.makeName("fs2@snap2")

        lzc_snapshot([origin1])
        with streams(srcfs, src, None) as (_, (stream, _)):
            with self.assertRaises(StreamMismatch):
                lzc_receive(dst, stream.fileno(), origin = origin1)
            stream.seek(0)
            with self.assertRaises(DatasetNotFound):
                lzc_receive(dst, stream.fileno(), origin = origin2)


    def test_recv_full_existing_empty_fs_and_origin(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src = srcfs + "@snap"
        dstfs = ZFSTest.pool.makeName("fs2/received-31")
        dst = dstfs + '@snap'
        origin = dstfs + '@dummy'

        lzc_create(dstfs)
        with streams(srcfs, src, None) as (_, (stream, _)):
            # because the destination fs already exists and has no snaps
            with self.assertRaises((DestinationModified, DatasetExists)):
                lzc_receive(dst, stream.fileno(), origin = origin)
            lzc_snapshot([origin])
            stream.seek(0)
            # because the destination fs already exists and has the snap
            with self.assertRaises((StreamMismatch, DatasetExists)):
                lzc_receive(dst, stream.fileno(), origin = origin)


    def test_recv_incremental_mounted_fs(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-7")
        dst1 = dstfs + '@snap1'
        dst2 = dstfs + '@snap2'

        with streams(srcfs, src1, src2) as (_, (full, incr)):
            lzc_receive(dst1, full.fileno())
            with zfs_mount(dstfs):
                lzc_receive(dst2, incr.fileno())


    def test_recv_incremental_modified_fs(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-15")
        dst1 = dstfs + '@snap1'
        dst2 = dstfs + '@snap2'

        with streams(srcfs, src1, src2) as (_, (full, incr)):
            lzc_receive(dst1, full.fileno())
            with temp_file_in_fs(dstfs):
                with self.assertRaises(DestinationModified):
                    lzc_receive(dst2, incr.fileno())


    def test_recv_incremental_snapname_used(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-8")
        dst1 = dstfs + '@snap1'
        dst2 = dstfs + '@snap2'

        with streams(srcfs, src1, src2) as (_, (full, incr)):
            lzc_receive(dst1, full.fileno())
            lzc_snapshot([dst2])
            with self.assertRaises(DatasetExists):
                lzc_receive(dst2, incr.fileno())


    def test_recv_incremental_more_recent_snap_with_no_changes(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-9")
        dst1 = dstfs + '@snap1'
        dst2 = dstfs + '@snap2'
        dst_snap = dstfs + '@snap'

        with streams(srcfs, src1, src2) as (_, (full, incr)):
            lzc_receive(dst1, full.fileno())
            lzc_snapshot([dst_snap])
            lzc_receive(dst2, incr.fileno())


    def test_recv_incremental_non_clone_but_set_origin(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-20")
        dst1 = dstfs + '@snap1'
        dst2 = dstfs + '@snap2'
        dst_snap = dstfs + '@snap'

        with streams(srcfs, src1, src2) as (_, (full, incr)):
            lzc_receive(dst1, full.fileno())
            lzc_snapshot([dst_snap])
            lzc_receive(dst2, incr.fileno(), origin = dst1)


    def test_recv_incremental_non_clone_but_set_random_origin(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-21")
        dst1 = dstfs + '@snap1'
        dst2 = dstfs + '@snap2'
        dst_snap = dstfs + '@snap'

        with streams(srcfs, src1, src2) as (_, (full, incr)):
            lzc_receive(dst1, full.fileno())
            lzc_snapshot([dst_snap])
            lzc_receive(dst2, incr.fileno(),
                origin = ZFSTest.pool.makeName("fs2/fs@snap"))


    def test_recv_incremental_more_recent_snap(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-10")
        dst1 = dstfs + '@snap1'
        dst2 = dstfs + '@snap2'
        dst_snap = dstfs + '@snap'

        with streams(srcfs, src1, src2) as (_, (full, incr)):
            lzc_receive(dst1, full.fileno())
            with temp_file_in_fs(dstfs):
                lzc_snapshot([dst_snap])
                with self.assertRaises(DestinationModified):
                    lzc_receive(dst2, incr.fileno())


    def test_recv_incremental_duplicate(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-11")
        dst1 = dstfs + '@snap1'
        dst2 = dstfs + '@snap2'
        dst_snap = dstfs + '@snap'

        with streams(srcfs, src1, src2) as (_, (full, incr)):
            lzc_receive(dst1, full.fileno())
            lzc_receive(dst2, incr.fileno())
            incr.seek(0)
            with self.assertRaises(DestinationModified):
                lzc_receive(dst_snap, incr.fileno())


    def test_recv_incremental_unrelated_fs(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-12")
        dst_snap = dstfs + '@snap'

        with streams(srcfs, src1, src2) as (_, (_, incr)):
            lzc_create(dstfs)
            with self.assertRaises(StreamMismatch):
                lzc_receive(dst_snap, incr.fileno())


    def test_recv_incremental_nonexistent_fs(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-13")
        dst_snap = dstfs + '@snap'

        with streams(srcfs, src1, src2) as (_, (_, incr)):
            with self.assertRaises(DatasetNotFound):
                lzc_receive(dst_snap, incr.fileno())


    def test_recv_incremental_same_fs(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        src_snap = srcfs + '@snap'

        with streams(srcfs, src1, src2) as (_, (_, incr)):
            with self.assertRaises(DestinationModified):
                lzc_receive(src_snap, incr.fileno())


    def test_recv_clone_without_specifying_origin(self):
        orig_src = ZFSTest.pool.makeName("fs2@send-origin-2")
        clone = ZFSTest.pool.makeName("fs1/fs/send-clone-2")
        clone_snap = clone + "@snap"
        orig_dst = ZFSTest.pool.makeName("fs1/fs/recv-origin-2@snap")
        clone_dst = ZFSTest.pool.makeName("fs1/fs/recv-clone-2@snap")

        lzc_snapshot([orig_src])
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(orig_src, None, stream.fileno())
            stream.seek(0)
            lzc_receive(orig_dst, stream.fileno())

        lzc_clone(clone, orig_src)
        lzc_snapshot([clone_snap])
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(clone_snap, orig_src, stream.fileno())
            stream.seek(0)
            with self.assertRaises(BadStream):
                lzc_receive(clone_dst, stream.fileno())


    def test_recv_clone_invalid_origin(self):
        orig_src = ZFSTest.pool.makeName("fs2@send-origin-3")
        clone = ZFSTest.pool.makeName("fs1/fs/send-clone-3")
        clone_snap = clone + "@snap"
        orig_dst = ZFSTest.pool.makeName("fs1/fs/recv-origin-3@snap")
        clone_dst = ZFSTest.pool.makeName("fs1/fs/recv-clone-3@snap")

        lzc_snapshot([orig_src])
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(orig_src, None, stream.fileno())
            stream.seek(0)
            lzc_receive(orig_dst, stream.fileno())

        lzc_clone(clone, orig_src)
        lzc_snapshot([clone_snap])
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(clone_snap, orig_src, stream.fileno())
            stream.seek(0)
            with self.assertRaises(NameInvalid):
                lzc_receive(clone_dst, stream.fileno(), origin = ZFSTest.pool.makeName("fs1/fs"))


    def test_recv_clone_wrong_origin(self):
        orig_src = ZFSTest.pool.makeName("fs2@send-origin-4")
        clone = ZFSTest.pool.makeName("fs1/fs/send-clone-4")
        clone_snap = clone + "@snap"
        orig_dst = ZFSTest.pool.makeName("fs1/fs/recv-origin-4@snap")
        clone_dst = ZFSTest.pool.makeName("fs1/fs/recv-clone-4@snap")
        wrong_origin = ZFSTest.pool.makeName("fs1/fs@snap")

        lzc_snapshot([orig_src])
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(orig_src, None, stream.fileno())
            stream.seek(0)
            lzc_receive(orig_dst, stream.fileno())

        lzc_clone(clone, orig_src)
        lzc_snapshot([clone_snap])
        lzc_snapshot([wrong_origin])
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(clone_snap, orig_src, stream.fileno())
            stream.seek(0)
            with self.assertRaises(StreamMismatch):
                lzc_receive(clone_dst, stream.fileno(), origin = wrong_origin)


    def test_recv_clone_nonexistent_origin(self):
        orig_src = ZFSTest.pool.makeName("fs2@send-origin-5")
        clone = ZFSTest.pool.makeName("fs1/fs/send-clone-5")
        clone_snap = clone + "@snap"
        orig_dst = ZFSTest.pool.makeName("fs1/fs/recv-origin-5@snap")
        clone_dst = ZFSTest.pool.makeName("fs1/fs/recv-clone-5@snap")
        wrong_origin = ZFSTest.pool.makeName("fs1/fs@snap")

        lzc_snapshot([orig_src])
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(orig_src, None, stream.fileno())
            stream.seek(0)
            lzc_receive(orig_dst, stream.fileno())

        lzc_clone(clone, orig_src)
        lzc_snapshot([clone_snap])
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(clone_snap, orig_src, stream.fileno())
            stream.seek(0)
            with self.assertRaises(DatasetNotFound):
                lzc_receive(clone_dst, stream.fileno(), origin = wrong_origin)


    def test_force_recv_full_existing_fs(self):
        src = ZFSTest.pool.makeName("fs1@snap")
        dstfs = ZFSTest.pool.makeName("fs2/received-50")
        dst = dstfs + '@snap'

        with temp_file_in_fs(ZFSTest.pool.makeName("fs1")) as name:
            lzc_snapshot([src])

        lzc_create(dstfs)
        with temp_file_in_fs(dstfs):
            pass # enough to taint the fs

        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(src, None, stream.fileno())
            stream.seek(0)
            lzc_receive(dst, stream.fileno(), force = True)


    def test_force_recv_full_existing_modified_mounted_fs(self):
        src = ZFSTest.pool.makeName("fs1@snap")
        dstfs = ZFSTest.pool.makeName("fs2/received-53")
        dst = dstfs + '@snap'

        with temp_file_in_fs(ZFSTest.pool.makeName("fs1")) as name:
            lzc_snapshot([src])

        lzc_create(dstfs)

        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(src, None, stream.fileno())
            stream.seek(0)
            with zfs_mount(dstfs) as mntdir:
                f = tempfile.NamedTemporaryFile(dir = mntdir, delete = False)
                for i in range(1024):
                    f.write('x' * 1024)
                lzc_receive(dst, stream.fileno(), force = True)
                # The temporary file dissappears and any access, even close(),
                # results in EIO.
                self.assertFalse(os.path.exists(f.name))
                with self.assertRaises(IOError):
                    f.close()


    # This test-case expect the behavior that should be there,
    # at the moment it may fail with DatasetExists or StreamMismatch
    # depending on the implementation.
    @unittest.expectedFailure
    def test_force_recv_full_already_existing_with_snapshots(self):
        src = ZFSTest.pool.makeName("fs1@snap")
        dstfs = ZFSTest.pool.makeName("fs2/received-51")
        dst = dstfs + '@snap'

        with temp_file_in_fs(ZFSTest.pool.makeName("fs1")) as name:
            lzc_snapshot([src])

        lzc_create(dstfs)
        with temp_file_in_fs(dstfs):
            pass # enough to taint the fs
        lzc_snapshot([dstfs + "@snap1"])

        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(src, None, stream.fileno())
            stream.seek(0)
            lzc_receive(dst, stream.fileno(), force = True)


    def test_force_recv_full_already_existing_with_same_snap(self):
        src = ZFSTest.pool.makeName("fs1@snap")
        dstfs = ZFSTest.pool.makeName("fs2/received-52")
        dst = dstfs + '@snap'

        with temp_file_in_fs(ZFSTest.pool.makeName("fs1")) as name:
            lzc_snapshot([src])

        lzc_create(dstfs)
        with temp_file_in_fs(dstfs):
            pass # enough to taint the fs
        lzc_snapshot([dst])

        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(src, None, stream.fileno())
            stream.seek(0)
            with self.assertRaises(DatasetExists):
                lzc_receive(dst, stream.fileno(), force = True)


    def test_force_recv_full_missing_parent_fs(self):
        src = ZFSTest.pool.makeName("fs1@snap")
        dst = ZFSTest.pool.makeName("fs2/nonexistent/fs@snap")

        with temp_file_in_fs(ZFSTest.pool.makeName("fs1")) as name:
            lzc_snapshot([src])
        with tempfile.TemporaryFile(suffix = '.ztream') as stream:
            lzc_send(src, None, stream.fileno())
            stream.seek(0)
            with self.assertRaises(DatasetNotFound):
                lzc_receive(dst, stream.fileno(), force = True)


    def test_force_recv_incremental_modified_fs(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-60")
        dst1 = dstfs + '@snap1'
        dst2 = dstfs + '@snap2'

        with streams(srcfs, src1, src2) as (_, (full, incr)):
            lzc_receive(dst1, full.fileno())
            with temp_file_in_fs(dstfs):
                pass # enough to taint the fs
            lzc_receive(dst2, incr.fileno(), force = True)


    def test_force_recv_incremental_modified_mounted_fs(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-64")
        dst1 = dstfs + '@snap1'
        dst2 = dstfs + '@snap2'

        with streams(srcfs, src1, src2) as (_, (full, incr)):
            lzc_receive(dst1, full.fileno())
            with zfs_mount(dstfs) as mntdir:
                f = tempfile.NamedTemporaryFile(dir = mntdir, delete = False)
                for i in range(1024):
                    f.write('x' * 1024)
                lzc_receive(dst2, incr.fileno(), force = True)
                # The temporary file dissappears and any access, even close(),
                # results in EIO.
                self.assertFalse(os.path.exists(f.name))
                with self.assertRaises(IOError):
                    f.close()


    def test_force_recv_incremental_modified_fs_plus_later_snap(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-61")
        dst1 = dstfs + '@snap1'
        dst2 = dstfs + '@snap2'
        dst3 = dstfs + '@snap'

        with streams(srcfs, src1, src2) as (_, (full, incr)):
            lzc_receive(dst1, full.fileno())
            with temp_file_in_fs(dstfs):
                pass # enough to taint the fs
            lzc_snapshot([dst3])
            lzc_receive(dst2, incr.fileno(), force = True)
        self.assertTrue(lzc_exists(dst1))
        self.assertTrue(lzc_exists(dst2))
        self.assertFalse(lzc_exists(dst3))


    def test_force_recv_incremental_modified_fs_plus_same_name_snap(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-62")
        dst1 = dstfs + '@snap1'
        dst2 = dstfs + '@snap2'

        with streams(srcfs, src1, src2) as (_, (full, incr)):
            lzc_receive(dst1, full.fileno())
            with temp_file_in_fs(dstfs):
                pass # enough to taint the fs
            lzc_snapshot([dst2])
            with self.assertRaises(DatasetExists):
                lzc_receive(dst2, incr.fileno(), force = True)


    def test_force_recv_incremental_modified_fs_plus_held_snap(self):
        srcfs = ZFSTest.pool.makeName("fs1")
        src1 = srcfs + "@snap1"
        src2 = srcfs + "@snap2"
        dstfs = ZFSTest.pool.makeName("fs2/received-63")
        dst1 = dstfs + '@snap1'
        dst2 = dstfs + '@snap2'
        dst3 = dstfs + '@snap'

        with streams(srcfs, src1, src2) as (_, (full, incr)):
            lzc_receive(dst1, full.fileno())
            with temp_file_in_fs(dstfs):
                pass # enough to taint the fs
            lzc_snapshot([dst3])
            with cleanup_fd() as cfd:
                lzc_hold({dst3: 'tag'}, cfd)
                with self.assertRaises(DatasetBusy):
                    lzc_receive(dst2, incr.fileno(), force = True)
        self.assertTrue(lzc_exists(dst1))
        self.assertFalse(lzc_exists(dst2))
        self.assertTrue(lzc_exists(dst3))


    def test_recv_bad_stream(self):
        dstfs = ZFSTest.pool.makeName("fs2/received")
        dst_snap = dstfs + '@snap'

        with dev_zero() as fd:
            with self.assertRaises(BadStream):
                lzc_receive(dst_snap, fd)


    @unittest.skipIf(*ebadf_confuses_dev_zfs_state())
    def test_hold_bad_fd(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        with tempfile.TemporaryFile() as tmp:
            bad_fd = tmp.fileno()

        with self.assertRaises(BadHoldCleanupFD):
            lzc_hold({snap: 'tag'}, bad_fd)


    @unittest.skipIf(*ebadf_confuses_dev_zfs_state())
    def test_hold_bad_fd_2(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        with self.assertRaises(BadHoldCleanupFD):
            lzc_hold({snap: 'tag'}, -2)


    @unittest.skipIf(*ebadf_confuses_dev_zfs_state())
    def test_hold_bad_fd_3(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        (soft, hard) = resource.getrlimit(resource.RLIMIT_NOFILE)
        bad_fd = hard + 1
        with self.assertRaises(BadHoldCleanupFD):
            lzc_hold({snap: 'tag'}, bad_fd)


    @unittest.skipIf(*bug_with_random_file_as_cleanup_fd())
    def test_hold_wrong_fd(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        with tempfile.TemporaryFile() as tmp:
            fd = tmp.fileno()
            with self.assertRaises(BadHoldCleanupFD):
                lzc_hold({snap: 'tag'}, fd)


    def test_hold_fd(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        with cleanup_fd() as fd:
            lzc_hold({snap: 'tag'}, fd)


    def test_hold_empty(self):
        with cleanup_fd() as fd:
            lzc_hold({}, fd)


    def test_hold_empty_2(self):
        lzc_hold({})


    def test_hold_vs_snap_destroy(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        with cleanup_fd() as fd:
            lzc_hold({snap: 'tag'}, fd)

            with self.assertRaises(SnapshotDestructionFailure) as ctx:
                lzc_destroy_snaps([snap], defer = False)
            for e in ctx.exception.errors:
                self.assertIsInstance(e, SnapshotIsHeld)

            lzc_destroy_snaps([snap], defer = True)
            self.assertTrue(lzc_exists(snap))

        # after automatic hold cleanup and deferred destruction
        self.assertFalse(lzc_exists(snap))


    def test_hold_many_tags(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        with cleanup_fd() as fd:
            lzc_hold({snap: 'tag1'}, fd)
            lzc_hold({snap: 'tag2'}, fd)


    def test_hold_many_snaps(self):
        snap1 = ZFSTest.pool.getRoot().getSnap()
        snap2 = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap1])
        lzc_snapshot([snap2])

        with cleanup_fd() as fd:
            lzc_hold({snap1: 'tag', snap2: 'tag'}, fd)


    def test_hold_many_with_one_missing(self):
        snap1 = ZFSTest.pool.getRoot().getSnap()
        snap2 = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap1])

        with cleanup_fd() as fd:
            missing = lzc_hold({snap1: 'tag', snap2: 'tag'}, fd)
        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0], snap2)


    def test_hold_many_with_all_missing(self):
        snap1 = ZFSTest.pool.getRoot().getSnap()
        snap2 = ZFSTest.pool.getRoot().getSnap()

        with cleanup_fd() as fd:
            missing = lzc_hold({snap1: 'tag', snap2: 'tag'}, fd)
        self.assertEqual(len(missing), 2)
        self.assertEqual(sorted(missing), sorted([snap1, snap2]))


    def test_hold_duplicate(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        with cleanup_fd() as fd:
            lzc_hold({snap: 'tag'}, fd)
            with self.assertRaises(HoldFailure) as ctx:
                lzc_hold({snap: 'tag'}, fd)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, HoldExists)


    def test_hold_across_pools(self):
        snap1 = ZFSTest.pool.getRoot().getSnap()
        snap2 = ZFSTest.misc_pool.getRoot().getSnap()
        lzc_snapshot([snap1])
        lzc_snapshot([snap2])

        with cleanup_fd() as fd:
            with self.assertRaises(HoldFailure) as ctx:
                lzc_hold({snap1: 'tag', snap2: 'tag'}, fd)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, PoolsDiffer)


    def test_hold_too_long_tag(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        tag = 't' * 256
        lzc_snapshot([snap])

        with cleanup_fd() as fd:
            with self.assertRaises(HoldFailure) as ctx:
                lzc_hold({snap: tag}, fd)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameTooLong)
            self.assertEquals(e.filename, tag)

    # Apparently the full snapshot name is not checked for length
    # and this snapshot is treated as simply missing.
    @unittest.expectedFailure
    def test_hold_too_long_snap_name(self):
        snap = ZFSTest.pool.getRoot().getTooLongSnap(False)
        with cleanup_fd() as fd:
            with self.assertRaises(HoldFailure) as ctx:
                lzc_hold({snap: 'tag'}, fd)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameTooLong)
            self.assertEquals(e.filename, snap)


    def test_hold_too_long_snap_name_2(self):
        snap = ZFSTest.pool.getRoot().getTooLongSnap(True)
        with cleanup_fd() as fd:
            with self.assertRaises(HoldFailure) as ctx:
                lzc_hold({snap: 'tag'}, fd)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameTooLong)
            self.assertEquals(e.filename, snap)


    def test_hold_invalid_snap_name(self):
        snap = ZFSTest.pool.getRoot().getSnap() + '@bad'
        with cleanup_fd() as fd:
            with self.assertRaises(HoldFailure) as ctx:
                lzc_hold({snap: 'tag'}, fd)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameInvalid)
            self.assertEquals(e.filename, snap)


    def test_hold_invalid_snap_name_2(self):
        snap = ZFSTest.pool.getRoot().getFilesystem().getName()
        with cleanup_fd() as fd:
            with self.assertRaises(HoldFailure) as ctx:
                lzc_hold({snap: 'tag'}, fd)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameInvalid)
            self.assertEquals(e.filename, snap)


    def test_get_holds(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        with cleanup_fd() as fd:
            lzc_hold({snap: 'tag1'}, fd)
            lzc_hold({snap: 'tag2'}, fd)

            holds = lzc_get_holds(snap)
            self.assertEquals(len(holds), 2)
            self.assertTrue('tag1' in holds)
            self.assertTrue('tag2' in holds)
            self.assertIsInstance(holds['tag1'], (int, long))


    def test_get_holds_after_auto_cleanup(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        with cleanup_fd() as fd:
            lzc_hold({snap: 'tag1'}, fd)
            lzc_hold({snap: 'tag2'}, fd)

        holds = lzc_get_holds(snap)
        self.assertEquals(len(holds), 0)
        self.assertIsInstance(holds, dict)


    def test_get_holds_nonexistent_snap(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        with self.assertRaises(SnapshotNotFound) as ctx:
            lzc_get_holds(snap)


    def test_get_holds_too_long_snap_name(self):
        snap = ZFSTest.pool.getRoot().getTooLongSnap(False)
        with self.assertRaises(NameTooLong) as ctx:
            lzc_get_holds(snap)


    def test_get_holds_too_long_snap_name_2(self):
        snap = ZFSTest.pool.getRoot().getTooLongSnap(True)
        with self.assertRaises(NameTooLong) as ctx:
            lzc_get_holds(snap)


    def test_get_holds_invalid_snap_name(self):
        snap = ZFSTest.pool.getRoot().getSnap() + '@bad'
        with self.assertRaises(NameInvalid) as ctx:
            lzc_get_holds(snap)


    # A filesystem-like snapshot name is not recognized as
    # an invalid name.
    @unittest.expectedFailure
    def test_get_holds_invalid_snap_name_2(self):
        snap = ZFSTest.pool.getRoot().getFilesystem().getName()
        with self.assertRaises(NameInvalid) as ctx:
            lzc_get_holds(snap)


    def test_release_hold(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        lzc_hold({snap: 'tag'})
        ret = lzc_release({snap: ['tag']})
        self.assertEquals(len(ret), 0)


    def test_release_hold_empty(self):
        ret = lzc_release({})
        self.assertEquals(len(ret), 0)


    def test_release_hold_complex(self):
        snap1 = ZFSTest.pool.getRoot().getSnap()
        snap2 = ZFSTest.pool.getRoot().getSnap()
        snap3 = ZFSTest.pool.getRoot().getFilesystem().getSnap()
        lzc_snapshot([snap1])
        lzc_snapshot([snap2, snap3])

        lzc_hold({snap1: 'tag1'})
        lzc_hold({snap1: 'tag2'})
        lzc_hold({snap2: 'tag'})
        lzc_hold({snap3: 'tag1'})
        lzc_hold({snap3: 'tag2'})

        holds = lzc_get_holds(snap1)
        self.assertEquals(len(holds), 2)
        holds = lzc_get_holds(snap2)
        self.assertEquals(len(holds), 1)
        holds = lzc_get_holds(snap3)
        self.assertEquals(len(holds), 2)

        release = {
            snap1: ['tag1', 'tag2'],
            snap2: ['tag'],
            snap3: ['tag2'],
        }
        ret = lzc_release(release)
        self.assertEquals(len(ret), 0)

        holds = lzc_get_holds(snap1)
        self.assertEquals(len(holds), 0)
        holds = lzc_get_holds(snap2)
        self.assertEquals(len(holds), 0)
        holds = lzc_get_holds(snap3)
        self.assertEquals(len(holds), 1)

        ret = lzc_release({snap3: ['tag1']})
        self.assertEquals(len(ret), 0)
        holds = lzc_get_holds(snap3)
        self.assertEquals(len(holds), 0)


    def test_release_hold_before_auto_cleanup(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        with cleanup_fd() as fd:
            lzc_hold({snap: 'tag'}, fd)
            ret = lzc_release({snap: ['tag']})
            self.assertEquals(len(ret), 0)


    def test_release_hold_and_snap_destruction(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        with cleanup_fd() as fd:
            lzc_hold({snap: 'tag1'}, fd)
            lzc_hold({snap: 'tag2'}, fd)

            lzc_destroy_snaps([snap], defer = True)
            self.assertTrue(lzc_exists(snap))

            lzc_release({snap: ['tag1']})
            self.assertTrue(lzc_exists(snap))

            lzc_release({snap: ['tag2']})
            self.assertFalse(lzc_exists(snap))


    def test_release_hold_and_multiple_snap_destruction(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        with cleanup_fd() as fd:
            lzc_hold({snap: 'tag'}, fd)

            lzc_destroy_snaps([snap], defer = True)
            self.assertTrue(lzc_exists(snap))

            lzc_destroy_snaps([snap], defer = True)
            self.assertTrue(lzc_exists(snap))

            lzc_release({snap: ['tag']})
            self.assertFalse(lzc_exists(snap))


    def test_release_hold_missing_tag(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        lzc_snapshot([snap])

        ret = lzc_release({snap: ['tag']})
        self.assertEquals(len(ret), 1)
        self.assertEquals(ret[0], snap + '#tag')


    def test_release_hold_missing_snap(self):
        snap = ZFSTest.pool.getRoot().getSnap()

        ret = lzc_release({snap: ['tag']})
        self.assertEquals(len(ret), 1)
        self.assertEquals(ret[0], snap)


    def test_release_hold_missing_snap_2(self):
        snap = ZFSTest.pool.getRoot().getSnap()

        ret = lzc_release({snap: ['tag', 'another']})
        self.assertEquals(len(ret), 1)
        self.assertEquals(ret[0], snap)


    def test_release_hold_across_pools(self):
        snap1 = ZFSTest.pool.getRoot().getSnap()
        snap2 = ZFSTest.misc_pool.getRoot().getSnap()
        lzc_snapshot([snap1])
        lzc_snapshot([snap2])

        with cleanup_fd() as fd:
            lzc_hold({snap1: 'tag'}, fd)
            lzc_hold({snap2: 'tag'}, fd)
            with self.assertRaises(HoldReleaseFailure) as ctx:
                lzc_release({snap1: ['tag'], snap2: ['tag']})
        for e in ctx.exception.errors:
            self.assertIsInstance(e, PoolsDiffer)


    # Apparently the tag name is not verified,
    # only its existence is checked.
    @unittest.expectedFailure
    def test_release_hold_too_long_tag(self):
        snap = ZFSTest.pool.getRoot().getSnap()
        tag = 't' * 256
        lzc_snapshot([snap])

        with self.assertRaises(HoldReleaseFailure) as ctx:
            ret = lzc_release({snap: [tag]})


    # Apparently the full snapshot name is not checked for length
    # and this snapshot is treated as simply missing.
    @unittest.expectedFailure
    def test_release_hold_too_long_snap_name(self):
        snap = ZFSTest.pool.getRoot().getTooLongSnap(False)

        with self.assertRaises(HoldReleaseFailure) as ctx:
            ret = lzc_release({snap: ['tag']})


    def test_release_hold_too_long_snap_name_2(self):
        snap = ZFSTest.pool.getRoot().getTooLongSnap(True)
        with self.assertRaises(HoldReleaseFailure) as ctx:
            lzc_release({snap: ['tag']})
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameTooLong)
            self.assertEquals(e.filename, snap)


    def test_release_hold_invalid_snap_name(self):
        snap = ZFSTest.pool.getRoot().getSnap() + '@bad'
        with self.assertRaises(HoldReleaseFailure) as ctx:
            lzc_release({snap: ['tag']})
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameInvalid)
            self.assertEquals(e.filename, snap)


    def test_release_hold_invalid_snap_name_2(self):
        snap = ZFSTest.pool.getRoot().getFilesystem().getName()
        with self.assertRaises(HoldReleaseFailure) as ctx:
            lzc_release({snap: ['tag']})
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameInvalid)
            self.assertEquals(e.filename, snap)



class _TempPool(object):
    SNAPSHOTS = ['snap', 'snap1', 'snap2']
    BOOKMARKS = ['bmark', 'bmark1', 'bmark2']

    _cachefile_suffix = ".cachefile"

    # XXX Whether to do a sloppy but much faster cleanup
    # or a proper but slower one.
    _recreate_pools = False


    def __init__(self, size = 128 * 1024 * 1024, readonly = False, filesystems = []):
        self._filesystems = filesystems
        self._readonly = readonly
        self._pool_name = 'pool.' + bytes(uuid.uuid4())
        self._root = _Filesystem(self._pool_name)
        (fd, self._pool_file_path) = tempfile.mkstemp(suffix = '.zpool', prefix = 'tmp-')
        if readonly:
            cachefile = self._pool_file_path + _TempPool._cachefile_suffix
        else:
            cachefile = 'none'
        self._zpool_create = ['zpool', 'create', '-o', 'cachefile=' + cachefile, '-O', 'mountpoint=legacy',
                              self._pool_name, self._pool_file_path]
        try:
            os.ftruncate(fd, size)
            os.close(fd)

            subprocess.check_output(self._zpool_create, stderr = subprocess.STDOUT)

            for fs in filesystems:
                lzc_create(self.makeName(fs))

            self._bmarks_supported = self.isPoolFeatureEnabled('bookmarks')

            if readonly:
                # To make a pool read-only it must exported and re-imported with readonly option.
                # The most deterministic way to re-import the pool is by using a cache file.
                # But the cache file has to be stashed away before the pool is exported,
                # because otherwise the pool is removed from the cache.
                shutil.copyfile(cachefile, cachefile + '.tmp')
                subprocess.check_output(['zpool', 'export', '-f', self._pool_name], stderr = subprocess.STDOUT)
                os.rename(cachefile + '.tmp', cachefile)
                subprocess.check_output(['zpool', 'import', '-f', '-N', '-c', cachefile, '-o', 'readonly=on', self._pool_name],
                                        stderr = subprocess.STDOUT)
                os.remove(cachefile)

        except subprocess.CalledProcessError as e:
            self.cleanUp()
            if 'permission denied' in e.output:
                raise unittest.SkipTest('insufficient privileges to run libzfs_core tests')
            print 'command failed: ', e.output
            raise
        except:
            self.cleanUp()
            raise


    def reset(self):
        if self._readonly:
            return

        if not self.__class__._recreate_pools:
            snaps = []
            for fs in [''] + self._filesystems:
                for snap in self.__class__.SNAPSHOTS:
                    snaps.append(self.makeName(fs + '@' + snap))
            self.getRoot().visitSnaps(lambda snap: snaps.append(snap))
            lzc_destroy_snaps(snaps, defer = False)

            if self._bmarks_supported:
                bmarks = []
                for fs in [''] + self._filesystems:
                    for bmark in self.__class__.BOOKMARKS:
                        bmarks.append(self.makeName(fs + '#' + bmark))
                self.getRoot().visitBookmarks(lambda bmark: bmarks.append(bmark))
                lzc_destroy_bookmarks(bmarks)
            self.getRoot().reset()
            return

        try:
            subprocess.check_output(['zpool', 'destroy', '-f', self._pool_name], stderr = subprocess.STDOUT)
            subprocess.check_output(self._zpool_create, stderr = subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            print 'command failed: ', e.output
            raise
        for fs in self._filesystems:
            lzc_create(self.makeName(fs))


    def cleanUp(self):
        try:
            subprocess.check_output(['zpool', 'destroy', '-f', self._pool_name], stderr = subprocess.STDOUT)
        except:
            pass
        try:
            os.remove(self._pool_file_path)
        except:
            pass
        try:
            os.remove(self._pool_file_path + _TempPool._cachefile_suffix)
        except:
            pass
        try:
            os.remove(self._pool_file_path + _TempPool._cachefile_suffix + '.tmp')
        except:
            pass


    def makeName(self, relative = None):
        if not relative:
            return self._pool_name
        if relative.startswith(('@', '#')):
            return self._pool_name + relative
        return self._pool_name + '/' + relative


    def makeTooLongName(self, prefix = None):
        if not prefix:
            prefix = 'x'
        prefix = self.makeName(prefix)
        pad_len = MAXNAMELEN + 1 - len(prefix)
        if pad_len > 0:
            return prefix + 'x' * pad_len
        else:
            return prefix


    def makeTooLongComponent(self, prefix = None):
        padding = 'x' * (MAXNAMELEN + 1)
        if not prefix:
            prefix = padding
        else:
            prefix = prefix + padding
        return self.makeName(prefix)


    def getRoot(self):
        return self._root


    def isPoolFeatureAvailable(self, feature):
        output = subprocess.check_output(['zpool', 'get', '-H', 'feature@' + feature, self._pool_name])
        output = output.strip()
        return output != ''


    def isPoolFeatureEnabled(self, feature):
        output = subprocess.check_output(['zpool', 'get', '-H', 'feature@' + feature, self._pool_name])
        output = output.split()[2]
        return output in ['active', 'enabled']


class _Filesystem(object):
    def __init__(self, name):
        self._name = name
        self.reset()


    def getName(self):
        return self._name


    def reset(self):
        self._children = []
        self._fs_id = 0
        self._snap_id = 0
        self._bmark_id = 0


    def getFilesystem(self):
        self._fs_id += 1
        fsname = self._name + '/fs' + str(self._fs_id)
        fs = _Filesystem(fsname)
        self._children.append(fs)
        return fs


    def _makeSnapName(self, i):
        return self._name + '@snap' + str(i)


    def getSnap(self):
        self._snap_id += 1
        return self._makeSnapName(self._snap_id)


    def _makeBookmarkName(self, i):
        return self._name + '#bmark' + str(i)


    def getBookmark(self):
        self._bmark_id += 1
        return self._makeBookmarkName(self._bmark_id)


    def _makeTooLongName(self, too_long_component):
        if too_long_component:
            return 'x' * (MAXNAMELEN + 1)

        # Note that another character is used for one of '/', '@', '#'.
        comp_len = MAXNAMELEN - len(self._name)
        if comp_len > 0:
            return 'x' * comp_len
        else:
            return 'x'


    def getTooLongFilesystemName(self, too_long_component):
        return self._name + '/' + self._makeTooLongName(too_long_component)


    def getTooLongSnap(self, too_long_component):
        return self._name + '@' + self._makeTooLongName(too_long_component)


    def getTooLongBookmark(self, too_long_component):
        return self._name + '#' + self._makeTooLongName(too_long_component)


    def _visitFilesystems(self, visitor):
        for child in self._children:
            child._visitFilesystems(visitor)
        visitor(self)


    def visitFilesystems(self, visitor):
        def _fsVisitor(fs):
            visitor(fs._name)

        self._visitFilesystems(_fsVisitor)


    def visitSnaps(self, visitor):
        def _snapVisitor(fs):
            for i in range(1, fs._snap_id + 1):
                visitor(fs._makeSnapName(i))

        self._visitFilesystems(_snapVisitor)


    def visitBookmarks(self, visitor):
        def _bmarkVisitor(fs):
            for i in range(1, fs._bmark_id + 1):
                visitor(fs._makeBookmarkName(i))

        self._visitFilesystems(_bmarkVisitor)


# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
