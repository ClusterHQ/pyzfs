
import unittest
import subprocess
import shutil
import tempfile
import uuid
from ..libzfs_core import *

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
        snapname1 = ZFSTest.pool.makeName("fs1@" + "x" * 210)
        snapname2 = ZFSTest.pool.makeName("fs2@" + "x" * 210)
        snapname3 = ZFSTest.pool.makeName("@snap")
        snaps = [snapname1, snapname2, snapname3]

        with self.assertRaises(SnapshotFailure) as ctx:
            lzc_snapshot(snaps)

        self.assertEquals(len(ctx.exception.errors), 2)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameTooLong)
            self.assertIsNotNone(e.filename)


    def test_snapshot_too_long_snap_name(self):
        snapname1 = ZFSTest.pool.makeName("fs1@" + "x" * 256)
        snapname2 = ZFSTest.pool.makeName("fs2@" + "x" * 256)
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
        snapname1 = ZFSTest.pool.makeName("fs1@nonexistent" + "x" * 200)
        snaps = [snapname1]

        with self.assertRaises(SnapshotDestructionFailure) as ctx:
            lzc_destroy_snaps(snaps, False)
        with self.assertRaises(SnapshotDestructionFailure) as ctx:
            lzc_destroy_snaps(snaps, True)


    def test_destroy_too_long_short_snap_name(self):
        snapname1 = ZFSTest.pool.makeName("fs1@nonexistent" + "x" * 245)
        snapname2 = ZFSTest.pool.makeName("fs2@nonexistent" + "x" * 245)
        snapname3 = ZFSTest.pool.makeName("@snap")
        snaps = [snapname1, snapname2, snapname3]

        with self.assertRaises(SnapshotDestructionFailure) as ctx:
            lzc_destroy_snaps(snaps, False)

        # NB: one common error is reported.
        self.assertEquals(len(ctx.exception.errors), 1)
        for e in ctx.exception.errors:
            self.assertIsInstance(e, NameTooLong)


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


    # A snapshot-like filesystem name is not recognized as
    # an invalid name for a filesystem.
    @unittest.expectedFailure
    def test_rollback_invalid_fs_name_2(self):
        name = ZFSTest.pool.makeName("fs1@snap")

        with self.assertRaises(NameInvalid) as ctx:
            lzc_rollback(name)


    def test_rollback_too_long_fs_name(self):
        name = ZFSTest.pool.makeName("x" * 256)

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
    def test_bookmarks_mismatching_name(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs2#bmark1')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps)
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)


    @skipUnlessBookmarksSupported
    def test_bookmarks_invalid_name(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs1#bmark!')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps)
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)


    @skipUnlessBookmarksSupported
    def test_bookmarks_invalid_name_2(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs1@bmark')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps)
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)


    @skipUnlessBookmarksSupported
    def test_bookmarks_mismatching_names(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1'), ZFSTest.pool.makeName('fs2@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs2#bmark1'), ZFSTest.pool.makeName('fs1#bmark1')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps)
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)


    # XXX should this succeed?
    @skipUnlessBookmarksSupported
    def test_bookmarks_partially_mismatching_name(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1'), ZFSTest.pool.makeName('fs2@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs2#bmark1'), ZFSTest.pool.makeName('fs2#bmark1')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps)
        lzc_bookmark(bmark_dict)


    @skipUnlessBookmarksSupported
    def test_bookmarks_missing_snap(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1'), ZFSTest.pool.makeName('fs2@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs1#bmark1'), ZFSTest.pool.makeName('fs2#bmark1')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        lzc_snapshot(snaps[0:1])
        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)


    @skipUnlessBookmarksSupported
    def test_bookmarks_missing_snaps(self):
        snaps = [ZFSTest.pool.makeName('fs1@snap1'), ZFSTest.pool.makeName('fs2@snap1')]
        bmarks = [ZFSTest.pool.makeName('fs1#bmark1'), ZFSTest.pool.makeName('fs2#bmark1')]
        bmark_dict = {x: y for x, y in zip(bmarks, snaps)}

        with self.assertRaises(BookmarkFailure) as ctx:
            lzc_bookmark(bmark_dict)

    @unittest.skip("causes a kernel crash")
    @skipUnlessBookmarksSupported
    def test_bookmarks_for_the_same_snap(self):
        snap = ZFSTest.pool.makeName('fs1@snap1')
        bmark1 = ZFSTest.pool.makeName('fs1#bmark1')
        bmark2 = ZFSTest.pool.makeName('fs1#bmark2')
        bmark_dict = {bmark1: snap, bmark2: snap}

        lzc_snapshot([snap])
        with self.assertRaises(BookmarkFailure) as ctx:
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
        (fd, self._pool_file_path) = tempfile.mkstemp(suffix = '.zpool', prefix = 'tmp-')
        if readonly:
            cachefile = self._pool_file_path + _TempPool._cachefile_suffix
        else:
            cachefile = 'none'

        try:
            os.ftruncate(fd, size)
            os.close(fd)

            subprocess.check_output(['zpool', 'create', '-o', 'cachefile=' + cachefile, self._pool_name, self._pool_file_path],
                                    stderr = subprocess.STDOUT)

            for fs in filesystems:
                lzc_create(self.makeName(fs))

            self._bmarks_supported = self._checkBookmarks()

            if readonly:
                # To make a pool read-only it must exported and re-imported with readonly option.
                # The most deterministic way to re-import the pool is by using a cache file.
                # But the cache file has to be stashed away before the pool is exported,
                # because otherwise the pool is removed from the cache.
                shutil.copyfile(cachefile, cachefile + '.tmp')
                subprocess.check_output(['zpool', 'export', '-f', self._pool_name], stderr = subprocess.STDOUT)
                os.rename(cachefile + '.tmp', cachefile)
                subprocess.check_output(['zpool', 'import', '-N', '-c', cachefile, '-o', 'readonly=on', self._pool_name],
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
            lzc_destroy_snaps(snaps, defer = False)

            if not self._bmarks_supported:
                return
            bmarks = []
            for fs in [''] + self._filesystems:
                for bmark in self.__class__.BOOKMARKS:
                    bmarks.append(self.makeName(fs + '#' + bmark))
            lzc_destroy_bookmarks(bmarks)
            return

        try:
            subprocess.check_output(['zpool', 'destroy', '-f', self._pool_name], stderr = subprocess.STDOUT)
            subprocess.check_output(['zpool', 'create', '-o', 'cachefile=none', self._pool_name, self._pool_file_path],
                                    stderr = subprocess.STDOUT)
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


    def bookmarksSupported(self):
        return self._bmarks_supported


    def _checkBookmarks(self):
        bmarks = {self.makeName('#bmark'): "dummy"}
        try:
            lzc_bookmark(bmarks)
        except BookmarkFailure as e:
            return not isinstance(e.errors[0], BookmarkNotSupported)
        return True


# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
