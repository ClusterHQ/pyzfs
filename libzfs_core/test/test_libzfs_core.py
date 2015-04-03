
import unittest
import subprocess
import tempfile
import uuid
from ..libzfs_core import *

class ZFSTest(unittest.TestCase):
    POOL_FILE_SIZE = 128 * 1024 * 1024
    FILESYSTEMS = ['fs1', 'fs2', 'fs1/fs']
    SNAPSHOTS = ['snap', 'snap1', 'snap2']
    pool_file_path = None
    pool_name = None

    @classmethod
    def setUpClass(cls):
        cls.pool_name = 'pool.' + bytes(uuid.uuid4())
        (pool_file_fd, cls.pool_file_path) = tempfile.mkstemp(suffix = '.zpool', prefix = 'tmp-')
        #print "pool name = ", cls.pool_name
        #print "pool file = ", cls.pool_file_path
        os.ftruncate(pool_file_fd, cls.POOL_FILE_SIZE)
        os.close(pool_file_fd)
        try:
            subprocess.check_output(['zpool', 'create', cls.pool_name, cls.pool_file_path],
                                    stderr = subprocess.STDOUT)
            for fs in cls.FILESYSTEMS:
                lzc_create(cls.pool_name + '/' + fs, False, {})
        except subprocess.CalledProcessError as e:
            cls._cleanUp()
            if 'permission denied' in e.output:
                raise unittest.SkipTest('insufficient privileges to run libzfs_core tests')
            raise
        except:
            cls._cleanUp()
            raise

    @classmethod
    def tearDownClass(cls):
        try:
            subprocess.call(['zpool', 'destroy', '-f', cls.pool_name])
        finally:
            cls._cleanUp()

    @classmethod
    def _cleanUp(cls):
        try:
            os.remove(cls.pool_file_path)
        except:
            pass

    def setUp(self):
        pass

    def tearDown(self):
        snaps = {}
        for fs in [None] + ZFSTest.FILESYSTEMS:
            if not fs:
                fsname = ZFSTest.pool_name
            else:
                fsname = ZFSTest.pool_name + '/' + fs
            for snap in ZFSTest.SNAPSHOTS:
                snaps[fsname + '@' + snap] = None
        lzc_destroy_snaps(snaps, False, {})

    def test_exists(self):
        self.assertTrue(lzc_exists(ZFSTest.pool_name))

    def test_exists_failure(self):
        self.assertFalse(lzc_exists(ZFSTest.pool_name + '/non-existant'))

    def test_snapshot(self):
        snapname = ZFSTest.pool_name + "@snap"
        snaps = { snapname: None }
        errlist = {}

        lzc_snapshot(snaps, {}, errlist)
        self.assertEqual(len(errlist), 0)
        self.assertTrue(lzc_exists(snapname))

    def test_snapshot_already_exists(self):
        snapname = ZFSTest.pool_name + "@snap"
        snaps = { snapname: None }
        errlist = {}

        lzc_snapshot(snaps, {}, errlist)

        with self.assertRaises(SnapshotExists):
            lzc_snapshot(snaps, {}, errlist)
        self.assertEqual(len(errlist), 1)

    def test_multiple_snapshots_for_same_fs(self):
        snapname1 = ZFSTest.pool_name + "@snap1"
        snapname2 = ZFSTest.pool_name + "@snap2"
        snaps = { snapname1: None, snapname2: None }
        errlist = {}

        with self.assertRaises(MultipleSnapshots):
            lzc_snapshot(snaps, {}, errlist)
        self.assertEqual(len(errlist), 0)
        self.assertFalse(lzc_exists(snapname1))
        self.assertFalse(lzc_exists(snapname2))

    def test_multiple_snapshots(self):
        snapname1 = ZFSTest.pool_name + "@snap"
        snapname2 = ZFSTest.pool_name + "/fs1@snap"
        snaps = { snapname1: None, snapname2: None }
        errlist = {}

        lzc_snapshot(snaps, {}, errlist)
        self.assertEqual(len(errlist), 0)
        self.assertTrue(lzc_exists(snapname1))
        self.assertTrue(lzc_exists(snapname2))

    def test_multiple_existing_snapshots(self):
        snapname1 = ZFSTest.pool_name + "@snap"
        snapname2 = ZFSTest.pool_name + "/fs1@snap"
        snaps = { snapname1: None, snapname2: None }
        errlist = {}

        lzc_snapshot(snaps, {}, errlist)

        with self.assertRaises(SnapshotExists):
            lzc_snapshot(snaps, {}, errlist)
        self.assertEqual(len(errlist), 2)

    def test_multiple_new_and_existing_snapshots(self):
        snapname1 = ZFSTest.pool_name + "@snap"
        snapname2 = ZFSTest.pool_name + "/fs1@snap"
        snapname3 = ZFSTest.pool_name + "/fs2@snap"
        snaps = { snapname1: None, snapname2: None }
        more_snaps = { snapname1: None, snapname2: None, snapname3: None }
        errlist = {}

        lzc_snapshot(snaps, {}, errlist)

        with self.assertRaises(SnapshotExists):
            lzc_snapshot(more_snaps, {}, errlist)
        self.assertEqual(len(errlist), 2)
        self.assertFalse(lzc_exists(snapname3))

# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
