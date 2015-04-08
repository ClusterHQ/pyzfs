import threading
from .exceptions import *
from .bindings import libzfs_core
from ._nvlist import nvlist_in, nvlist_out


# TODO: a better way to init and uninit the library
def _initialize():
    class LazyInit(object):

        def __init__(self, lib):
            self._lib = lib
            self._inited = False
            self._lock = threading.Lock()

        def __getattr__(self, name):
            if not self._inited:
                with self._lock:
                    if not self._inited:
                        ret = self._lib.libzfs_core_init()
                        if ret != 0:
                            raise ZFSInitializationFailed(ret)
                        self._inited = True
            return getattr(self._lib, name)

    return LazyInit(libzfs_core.lib)

_ffi = libzfs_core.ffi
_lib = _initialize()


def lzc_create(name, is_zvol = False, props = {}):
    '''
    Create a ZFS filesystem or a ZFS volume ("zvol").

    :param str name: a name of the dataset to be created.
    :param bool is_zvol: whether to create a zvol (false by default).
    :param props: a `dict` of ZFS dataset property name-value pairs (empty by default).
    :type props: dict of str to any

    :raises FilesystemExists: if a dataset with the given name already exists.
    :raises ParentNotFound: if a parent dataset of the requested dataset does not exist.
    :raises PropertyInvalid: if one or more of the specified properties is invalid
                             or has an invalid type or value.
    '''
    if is_zvol:
        ds_type = _lib.DMU_OST_ZVOL
    else:
        ds_type = _lib.DMU_OST_ZFS
    with nvlist_in(props) as nvlist:
        ret = _lib.lzc_create(name, ds_type, nvlist)
    if ret != 0:
        raise {
            errno.EEXIST: FilesystemExists(name),
            errno.ENOENT: ParentNotFound(name),
            errno.EINVAL: PropertyInvalid(name),
        }.get(ret, genericException(ret, name, "Failed to create filesystem"))


def lzc_clone(name, origin, props = {}):
    '''
    Clone a ZFS filesystem or a ZFS volume ("zvol") from a given snapshot.

    :param str name: a name of the dataset to be created.
    :param str origin: a name of the origin snapshot.
    :param props: a `dict` of ZFS dataset property name-value pairs (empty by default).
    :type props: dict of str to Any

    :raises FilesystemExists: if a dataset with the given name already exists.
    :raises DatasetNotFound: if either a parent dataset of the requested dataset
                             or the origin snapshot does not exist.
    :raises PropertyInvalid: if one or more of the specified properties is invalid
                             or has an invalid type or value.

    .. note::
        Because of a deficiency of the underlying C interface
        :py:exc:`.DatasetNotFound` can mean that either a parent filesystem of the target
        or the origin snapshot does not exist.
        It is currently impossible to distinguish between the cases.
        `lzc_hold` can be used to check that the snapshot exists and ensure that
        it is not destroyed before cloning.
    '''
    with nvlist_in(props) as nvlist:
        ret = _lib.lzc_clone(name, origin, nvlist)
    if ret != 0:
        raise {
            errno.EEXIST: FilesystemExists(name),
            errno.ENOENT: DatasetNotFound(name),
            errno.EINVAL: PropertyInvalid(name),
        }.get(ret, genericException(ret, name, "Failed to create clone"))


def lzc_rollback(name):
    '''
    Roll back a filesystem or volume to its most recent snapshot.

    :param str name: a name of the dataset to be rolled back.
    :return: a name of the most recent snapshot.
    :rtype: str

    :raises DatasetNotFound: if either the dataset does not exist
                             or it does not have any snapshots.

    .. note::
        Because of a deficiency of the underlying C interface
        :py:exc:`.DatasetNotFound` can mean that either the dataset does not exist
        or it does not have any snapshots.
        It is currently impossible to distinguish between the cases.
    '''
    snapnamep = _ffi.new('char[]', 256)
    ret = _lib.lzc_rollback(name, snapnamep, 256)
    if ret != 0:
        raise {
            errno.ENOENT: DatasetNotFound(name),
        }.get(ret, genericException(ret, name, "Failed to rollback"))
    return _ffi.string(snapnamep)


def lzc_snapshot(snaps, props = {}):
    '''
    Create snapshots.

    All snapshots must be in the same pool.

    Optionally snapshot properties can be set on all snapshots.
    Currently  only user properties (prefixed with "user:") are supported.

    Either all snapshots are successfully created or none are created if
    an exception is raised.

    :param snaps: a list of names of snapshots to be created.
    :type snaps: list of str
    :param props: a `dict` of ZFS dataset property name-value pairs (empty by default).
    :type props: dict of str to str

    :raises SnapshotFailure: if one or more snapshots could not be created.

    .. note::
        :py:exc:`.SnapshotFailure` is a compound exception that provides at least
        one detailed error object in :py:attr:`SnapshotFailure.errors` `list`.

    .. warning::
        There is an underlying C library bug that affects reporting of
        an error caused by one or more missing filesystems.
        If any other errors are encountered then :py:exc:`.FilesystemNotFound` is
        not reported at all.
        If :py:exc:`.FilesystemNotFound` is reported it is impossible to tell how
        many filesystems are missing and which they are, unless only
        one snapshot has been requested.
    '''
    def _map(ret, name):
        return {
            errno.EEXIST: SnapshotExists(name),
            errno.ENOENT: FilesystemNotFound(name),
            errno.EXDEV:  DuplicateSnapshots(name),
            errno.EINVAL: PropertyInvalid(name),
        }.get(ret, genericException(ret, name, "Failed to create snapshot"))

    snaps_dict = { name: None for name in snaps }
    errlist = {}
    with nvlist_in(snaps_dict) as snaps_nvlist, nvlist_in(props) as props_nvlist:
        with nvlist_out(errlist) as errlist_nvlist:
            ret = _lib.lzc_snapshot(snaps_nvlist, props_nvlist, errlist_nvlist)
    _handleErrList(ret, errlist, snaps, SnapshotFailure, _map)


def lzc_destroy_snaps(snaps, defer):
    '''
    Destroy snapshots.

    They must all be in the same pool.
    Snapshots that do not exist will be silently ignored.

    If 'defer' is not set, and a snapshot has user holds or clones, the
    destroy operation will fail and none of the snapshots will be
    destroyed.

    If 'defer' is set, and a snapshot has user holds or clones, it will be
    marked for deferred destruction, and will be destroyed when the last hold
    or clone is removed/destroyed.

    The operation succeeds if all snapshots were destroyed (or marked for
    later destruction if 'defer' is set) or didn't exist to begin with.

    :param snaps: a list of names of snapshots to be destroyed.
    :type snaps: list of str
    :param bool defer: whether to mark busy snapshots for deferred destruction
                       rather than immediately failing.

    :raises SnapshotDestructionFailure: if one or more snapshots could not be created.

    .. note::
        :py:exc:`.SnapshotDestructionFailure` is a compound exception that provides at least
        one detailed error object in :py:attr:`SnapshotDestructionFailure.errors` `list`.
    '''

    def _map(ret, name):
        return {
            errno.EEXIST: SnapshotIsCloned(name),
        }.get(ret, genericException(ret, name, "Failed to create snapshot"))

    snaps_dict = { name: None for name in snaps }
    errlist = {}
    with nvlist_in(snaps_dict) as snaps_nvlist:
        with nvlist_out(errlist) as errlist_nvlist:
            ret = _lib.lzc_destroy_snaps(snaps_nvlist, defer, errlist_nvlist)
    _handleErrList(ret, errlist, snaps, SnapshotDestructionFailure, _map)


def lzc_bookmark(bookmarks, errlist):
    ret = 0
    with nvlist_in(bookmarks) as nvlist:
        with nvlist_out(errlist) as errlist_nvlist:
            ret = _lib.lzc_bookmark(nvlist, errlist_nvlist)
    return ret


def lzc_get_bookmarks(fsname, props, bmarks):
    ret = 0
    with nvlist_in(props) as nvlist:
        with nvlist_out(bmarks) as bmarks_nvlist:
            ret = _lib.lzc_get_bookmarks(fsname, nvlist, bmarks_nvlist)
    return ret


def lzc_destroy_bookmarks(bookmarks, errlist):
    ret = 0
    with nvlist_in(bookmarks) as nvlist:
        with nvlist_out(errlist) as errlist_nvlist:
            ret = _lib.lzc_destroy_bookmarks(nvlist, errlist_nvlist)
    return ret


def lzc_snaprange_space(firstsnap, lastsnap):
    valp = _ffi.new('uint64_t *')
    ret = _lib.lzc_snaprange_space(firstsnap, lastsnap, valp)
    return (ret, int(valp[0]))


def lzc_hold(holds, fd, errlist):
    ret = 0
    with nvlist_in(holds) as nvlist:
        with nvlist_out(errlist) as errlist_nvlist:
            ret = _lib.lzc_hold(nvlist, fd, errlist_nvlist)
    return ret


def lzc_release(holds, errlist):
    ret = 0
    with nvlist_in(holds) as nvlist:
        with nvlist_out(errlist) as errlist_nvlist:
            ret = _lib.lzc_release(nvlist, errlist_nvlist)
    return ret


def lzc_get_holds(snapname, holds):
    ret = 0
    with nvlist_out(holds) as nvlist:
        ret = _lib.lzc_get_holds(snapname, nvlist)
    return ret



def lzc_send(snapname, fromsnap, fd, flags):
    ret = _lib.lzc_send(snapname, fromsnap, fd, flags)
    return ret


def lzc_send_space(snapname, fromsnap):
    valp = _ffi.new('uint64_t *')
    ret = _lib.lzc_send_space(snapname, fromsnap, valp)
    return (ret, int(valp[0]))


def lzc_receive(snapname, props, origin, force, fd):
    ret = 0
    with nvlist_in(props) as nvlist:
        ret = _lib.lzc_receive(snapname, nvlist, origin, force, fd)
    return ret


def lzc_exists(name):
    ret = _lib.lzc_exists(name)
    return bool(ret)


def _handleErrList(ret, errlist, names, exception, mapper):
    if ret == 0:
        return

    if len(errlist) == 0:
        name = names[0] if len(names) == 1 else None
        errors = [mapper(ret, name)]
    else:
        errors = []
        for name, err in errlist.iteritems():
            errors.append(mapper(err, name))

    raise exception(errors)


# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
