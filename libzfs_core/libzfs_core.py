import re
import string
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
    :type props: dict of str:Any

    :raises FilesystemExists: if a dataset with the given name already exists.
    :raises ParentNotFound: if a parent dataset of the requested dataset does not exist.
    :raises PropertyInvalid: if one or more of the specified properties is invalid
                             or has an invalid type or value.

    Wraps ``int lzc_create(const char *fsname, dmu_objset_type_t type, nvlist_t *props)``.
    '''
    if is_zvol:
        ds_type = _lib.DMU_OST_ZVOL
    else:
        ds_type = _lib.DMU_OST_ZFS
    with nvlist_in(props) as nvlist:
        ret = _lib.lzc_create(name, ds_type, nvlist)
    if ret != 0:
        if ret == errno.EINVAL:
            if not _is_valid_fs_name(name):
                raise NameInvalid(name)
            elif len(name) > 256:
                raise NameTooLong(name)
            else:
                raise PropertyInvalid(name)

        raise {
            errno.EEXIST: FilesystemExists(name),
            errno.ENOENT: ParentNotFound(name),
        }.get(ret, genericException(ret, name, "Failed to create filesystem"))


def lzc_clone(name, origin, props = {}):
    '''
    Clone a ZFS filesystem or a ZFS volume ("zvol") from a given snapshot.

    :param str name: a name of the dataset to be created.
    :param str origin: a name of the origin snapshot.
    :param props: a `dict` of ZFS dataset property name-value pairs (empty by default).
    :type props: dict of str:Any

    :raises FilesystemExists: if a dataset with the given name already exists.
    :raises DatasetNotFound: if either a parent dataset of the requested dataset
                             or the origin snapshot does not exist.
    :raises PropertyInvalid: if one or more of the specified properties is invalid
                             or has an invalid type or value.

    .. note::
        Because of a deficiency of the underlying C interface
        :exc:`.DatasetNotFound` can mean that either a parent filesystem of the target
        or the origin snapshot does not exist.
        It is currently impossible to distinguish between the cases.
        :func:`lzc_hold` can be used to check that the snapshot exists and ensure that
        it is not destroyed before cloning.
    '''
    with nvlist_in(props) as nvlist:
        ret = _lib.lzc_clone(name, origin, nvlist)
    if ret != 0:
        if ret == errno.EINVAL:
            if not _is_valid_fs_name(name):
                raise NameInvalid(name)
            elif not _is_valid_snap_name(origin):
                raise NameInvalid(origin)
            elif len(name) > 256:
                raise NameTooLong(name)
            elif len(origin) > 256:
                raise NameTooLong(origin)
            elif _pool_name(name) != _pool_name(origin):
                raise PoolsDiffer(name) # see https://www.illumos.org/issues/5824
            else:
                raise PropertyInvalid(name)

        raise {
            errno.EEXIST: FilesystemExists(name),
            errno.ENOENT: DatasetNotFound(name),
        }.get(ret, genericException(ret, name, "Failed to create clone"))


def lzc_rollback(name):
    '''
    Roll back a filesystem or volume to its most recent snapshot.

    :param str name: a name of the dataset to be rolled back.
    :return: a name of the most recent snapshot.
    :rtype: str

    :raises FilesystemNotFound: if the dataset does not exist.
    :raises SnapshotNotFound: if the dataset does not have any snapshots.
    :raises NameInvalid: if the dataset name is invalid.
    :raises NameTooLong: if the dataset name is too long.
    '''
    snapnamep = _ffi.new('char[]', 256)
    ret = _lib.lzc_rollback(name, snapnamep, 256)
    if ret != 0:
        if ret == errno.EINVAL:
            if not _is_valid_fs_name(name):
                raise NameInvalid(name)
            elif len(name) > 256:
                raise NameTooLong(name)
            else:
                raise SnapshotNotFound(name)

        raise {
            errno.ENOENT: FilesystemNotFound(name),
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
    :type props: dict of str:str

    :raises SnapshotFailure: if one or more snapshots could not be created.

    .. note::
        :exc:`.SnapshotFailure` is a compound exception that provides at least
        one detailed error object in :attr:`SnapshotFailure.errors` `list`.

    .. warning::
        The underlying implementation reports an individual, per-snapshot error
        only for :exc:`.SnapshotExists` condition and *sometimes* for
        :exc:`.NameTooLong`.
        In all other cases a single error is reported without connection to any
        specific snapshot name(s).

        This has the following implications:

        * if multiple error conditions are encountered only one of them is reported

        * unless only one snapshot is requested then it is impossible to tell
          how many snapshots are problematic and what they are

        * only if there are no other error conditions :exc:`.SnapshotExists`
          is reported for all affected snapshots

        * :exc:`.NameTooLong` can behave either in the same way as
          :exc:`.SnapshotExists` or as all other exceptions.
          The former is the case where the full snapshot name exceeds the maximum
          allowed length but the short snapshot name (after '@') is within
          the limit.
          The latter is the case when the short name alone exceeds the maximum
          allowed length.
    '''
    def _map(ret, name):
        if ret == errno.EXDEV:
            pool_names = map(_pool_name, snaps)
            same_pool = all(x == pool_names[0] for x in pool_names)
            if same_pool:
                return DuplicateSnapshots(name)
            else:
                return PoolsDiffer(name)
        elif ret == errno.EINVAL:
            if any(not _is_valid_snap_name(s) for s in snaps):
                return NameInvalid(name)
            elif any(len(s) > 256 for s in snaps):
                return NameTooLong(name)
            else:
                return PropertyInvalid(name)
        else:
            return {
                errno.EEXIST: SnapshotExists(name),
                errno.ENOENT: FilesystemNotFound(name),
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
        :exc:`.SnapshotDestructionFailure` is a compound exception that provides at least
        one detailed error object in :attr:`SnapshotDestructionFailure.errors` `list`.

        Typical error is :exc:`SnapshotIsCloned` if `defer` is `False`.
        The snapshot names are validated quite loosely and invalid names are typically
        ignored as nonexisiting snapshots.

        A snapshot name referring to a filesystem that doesn't exist is ignored.
        However, non-existent pool name causes :exc:`PoolNotFound`.
    '''

    def _map(ret, name):
        return {
            errno.EEXIST: SnapshotIsCloned(name),
            errno.ENOENT: PoolNotFound(name),
            errno.EBUSY:  SnapshotIsHeld(name),
        }.get(ret, genericException(ret, name, "Failed to destroy snapshot"))

    snaps_dict = { name: None for name in snaps }
    errlist = {}
    with nvlist_in(snaps_dict) as snaps_nvlist:
        with nvlist_out(errlist) as errlist_nvlist:
            ret = _lib.lzc_destroy_snaps(snaps_nvlist, defer, errlist_nvlist)
    _handleErrList(ret, errlist, snaps, SnapshotDestructionFailure, _map)


def lzc_bookmark(bookmarks):
    '''
    Create bookmarks.

    :param bookmarks: a dict that maps names of wanted bookmarks to names of existing snapshots.
    :type bookmarks: dict of str to str

    :raises BookmarkFailure: if any of the bookmarks can not be created for any reason.

    The bookmarks `dict` maps from name of the bookmark (e.g. :file:`{pool}/{fs}#{bmark}`) to
    the name of the snapshot (e.g. :file:`{pool}/{fs}@{snap}`).  All the bookmarks and
    snapshots must be in the same pool.
    '''
    def _map(ret, name):
        if ret == errno.EINVAL:
            if bool(name):
                snap = bookmarks[name]
                pool_names = map(_pool_name, bookmarks.keys())
                if not _is_valid_bmark_name(name):
                    return NameInvalid(name)
                elif not _is_valid_snap_name(snap):
                    return NameInvalid(snap)
                elif _fs_name(name) != _fs_name(snap):
                    return BookmarkMismatch(name)
                elif any(x != _pool_name(name) for x in pool_names):
                    return PoolsDiffer(name)
            else:
                invalid_names = [b for b in bookmarks.keys() if not _is_valid_bmark_name(b)]
                if len(invalid_names) > 0:
                    return NameInvalid(invalid_names[0])
        return {
            errno.EEXIST: BookmarkExists(name),
            errno.ENOENT: SnapshotNotFound(name),
            errno.ENOTSUP: BookmarkNotSupported(name),
        }.get(ret, genericException(ret, name, "Failed to create bookmark"))

    errlist = {}
    with nvlist_in(bookmarks) as nvlist:
        with nvlist_out(errlist) as errlist_nvlist:
            ret = _lib.lzc_bookmark(nvlist, errlist_nvlist)
    _handleErrList(ret, errlist, bookmarks.keys(), BookmarkFailure, _map)


def lzc_get_bookmarks(fsname, props = []):
    '''
    Retrieve a list of bookmarks for the given file system.

    :param str fsname: a name of the filesystem.
    :param props: a `list` of properties that will be returned for each bookmark.
    :type props: list of str
    :return: a `dict` that maps the bookmarks' short names to their properties.
    :rtype: dict of str:dict

    :raises FilesystemNotFound: if the filesystem is not found.

    The following are valid properties on bookmarks:

    guid : integer
        globally unique identifier of the snapshot the bookmark refers to
    createtxg : integer
        txg when the snapshot the bookmark refers to was created
    creation : integer
        timestamp when the snapshot the bookmark refers to was created

    Any other properties passed in ``props`` are ignored without reporting
    any error.
    Values in the returned dictionary map the names of the requested properties
    to their respective values.
    '''
    bmarks = {}
    props_dict = { name: None for name in props }
    with nvlist_in(props_dict) as nvlist:
        with nvlist_out(bmarks) as bmarks_nvlist:
            ret = _lib.lzc_get_bookmarks(fsname, nvlist, bmarks_nvlist)
    if ret != 0:
        raise {
            errno.ENOENT: FilesystemNotFound(fsname),
        }.get(ret, genericException(ret, fsname, "Failed to list bookmarks"))
    return bmarks


def lzc_destroy_bookmarks(bookmarks):
    '''
    Destroy bookmarks.

    :param bookmarks: a list of the bookmarks to be destroyed.
                      The bookmarks are specified as :file:`{fs}#{bmark}`.
    :type bookmarks: list of str

    :raises BookmarkDestructionFailure: if any of the bookmarks may not be destroyed.

    The bookmarks must all be in the same pool.
    Bookmarks that do not exist will be silently ignored.
    This also includes the case where the filesystem component of the bookmark
    name does not exist.
    However, an invalid bookmark name will cause :exc:`.NameInvalid` error
    reported in :attr:`SnapshotDestructionFailure.errors`.

    Either all bookmarks that existed are destroyed or an exception is raised.
    '''
    def _map(ret, name):
        return {
            errno.EINVAL: NameInvalid(name),
        }.get(ret, genericException(ret, name, "Failed to destroy bookmark"))

    errlist = {}
    bmarks_dict = { name: None for name in bookmarks }
    with nvlist_in(bmarks_dict) as nvlist:
        with nvlist_out(errlist) as errlist_nvlist:
            ret = _lib.lzc_destroy_bookmarks(nvlist, errlist_nvlist)
    _handleErrList(ret, errlist, bookmarks, BookmarkDestructionFailure, _map)


def lzc_snaprange_space(firstsnap, lastsnap):
    '''
    Calculate a size of data used by snapshots between
    the firstsnap and lastsnap.

    :param str firstsnap: the name of the first snapshot in the range.
    :param str lastsnap: the name of the last snapshot in the range.
    :return: the calculated stream size, in bytes.
    :rtype: `int` or `long`

    :raises SnapshotNotFound: if either of the snapshots does not exist.
    :raises NameInvalid: if the name of either snapshot is invalid.
    :raises NameTooLong: if the name of either snapshot is too long.
    :raises SnapshotMismatch: if ``fromsnap`` is not an ancestor snapshot of ``snapname``.
    :raises PoolsDiffer: if the snapshots belong to different pools.

    It is not an error to specify the same snapshot as both ``firstsnap``
    and ``lastsnap``, but a result is always zero.
    '''
    valp = _ffi.new('uint64_t *')
    ret = _lib.lzc_snaprange_space(firstsnap, lastsnap, valp)
    if ret != 0:
        if ret == errno.EINVAL:
            if not _is_valid_snap_name(firstsnap):
                raise NameInvalid(firstsnap)
            elif not _is_valid_snap_name(lastsnap):
                raise NameInvalid(lastsnap)
            elif len(firstsnap) > 256:
                raise NameTooLong(firstsnap)
            elif len(lastsnap) > 256:
                raise NameTooLong(lastsnap)
            elif _pool_name(firstsnap) != _pool_name(lastsnap):
                raise PoolsDiffer(lastsnap)
            else:
                raise SnapshotMismatch(lastsnap)
        raise {
            errno.ENOENT: SnapshotNotFound(lastsnap),
        }.get(ret, genericException(ret, lastsnap, "Failed to calculate space used by range of snapshots"))

    return int(valp[0])


def lzc_hold(holds, fd = None):
    '''
    Create *user holds* on snapshots.  If there is a hold on a snapshot,
    the snapshot can not be destroyed.  (However, it can be marked for deletion
    by :func:`lzc_destroy_snaps` ( ``defer`` = `True` ).)

    :param holds: the dictionary of names of the snapshots to hold mapped to the hold names.
    :type holds: dict of str : str
    :type fd: int or None
    :param fd: if not None then it must be the result of :func:`os.open` called as ``os.open("/dev/zfs", O_EXCL)``.
    :type fd: int or None
    :return: a list of the snapshots that do not exist.
    :rtype: list of str

    :raises HoldFailure: if a hold was impossible on one or more of the snapshots.
    :raises BadHoldCleanupFD: if ``fd`` is not a valid file descriptor associated with :file:`/dev/zfs`.

    The snapshots must all be in the same pool.

    If ``fd`` is not None, then when the ``fd`` is closed (including on process
    termination), the holds will be released.  If the system is shut down
    uncleanly, the holds will be released when the pool is next opened
    or imported.

    Holds for snapshots which don't exist will be skipped and have an entry
    added to the return value, but will not cause an overall failure.
    No exceptions is raised if all holds, for snapshots that existed, were succesfully created.
    Otherwise :exc:`.HoldFailure` exception is raised and no holds will be created.
    :attr:`.HoldFailure.errors` may contain a single element for an error that is not
    specific to any hold / snapshot, or it may contain one or more elements
    detailing specific error per each affected hold.
    '''
    def _map(ret, name):
        if ret == errno.EXDEV:
            return PoolsDiffer(name)
        elif ret == errno.EINVAL:
            if bool(name):
                tag = holds[name]
                pool_names = map(_pool_name, holds.keys())
                if not _is_valid_snap_name(name):
                    return NameInvalid(name)
                elif len(name) > 256:
                    return NameTooLong(name)
                elif any(x != _pool_name(name) for x in pool_names):
                    return PoolsDiffer(name)
            else:
                invalid_names = [b for b in holds.keys() if not _is_valid_snap_name(b)]
                if len(invalid_names) > 0:
                    return NameInvalid(invalid_names[0])
        return {
            errno.EEXIST: HoldExists(name),
            errno.E2BIG:  NameTooLong(holds[name]),
        }.get(ret, genericException(ret, name, "Failed to hold snapshot"))

    errlist = {}
    if fd is None:
        fd = -1
    with nvlist_in(holds) as nvlist:
        with nvlist_out(errlist) as errlist_nvlist:
            ret = _lib.lzc_hold(nvlist, fd, errlist_nvlist)

    # XXX ENOENT seems like a FreeBSD quirk
    if ret == errno.EBADF or ret == errno.ENOENT:
        raise BadHoldCleanupFD()
    _handleErrList(ret, errlist, holds.keys(), HoldFailure, _map)

    # If there is no error (no exception raised by _handleErrList), but errlist
    # is not empty, then it contains missing snapshots.
    assert all(x == errno.ENOENT for x in errlist.itervalues())
    return errlist.keys()


def lzc_release(holds):
    '''
    Release *user holds* on snapshots.

    If the snapshot has been marked for
    deferred destroy (by lzc_destroy_snaps(defer=B_TRUE)), it does not have
    any clones, and all the user holds are removed, then the snapshot will be
    destroyed.

    The snapshots must all be in the same pool.

    :param holds: a ``dict`` where keys are snapshot names and values are
                  lists of hold tags to remove.
    :type holds: dict of str : list of str
    :return: a list of any snapshots that do not exist and of any tags that do not
             exist for existing snapshots.
             Such tags are qualified with a corresponding snapshot name
             using the following format :file:`{pool}/{fs}@{snap}#{tag}`
    :rtype: list of str

    :raises HoldReleaseFailure: if one or more existing holds could not be released.

    Holds which failed to release because they didn't exist will have an entry
    added to errlist, but will not cause an overall failure.

    This call is success if ``holds`` was empty or all holds that
    existed, were successfully removed.
    Otherwise an exception will be raised.
    '''
    def _map(ret, name):
        if ret == errno.EXDEV:
            return PoolsDiffer(name)
        elif ret == errno.EINVAL:
            if bool(name):
                pool_names = map(_pool_name, holds.keys())
                if not _is_valid_snap_name(name):
                    return NameInvalid(name)
                elif len(name) > 256:
                    return NameTooLong(name)
                elif any(x != _pool_name(name) for x in pool_names):
                    return PoolsDiffer(name)
            else:
                invalid_names = [b for b in holds.keys() if not _is_valid_snap_name(b)]
                if len(invalid_names) > 0:
                    return NameInvalid(invalid_names[0])
        elif ret == errno.ENOENT:
            return HoldNotFound(name)
        elif ret == errno.E2BIG:
            tag_list = holds[name]
            too_long_tags = [t for t in tag_list if len(t) > 256]
            return NameTooLong(too_long_tags[0])
        else:
            return genericException(ret, name, "Failed to release snapshot hold")

    errlist = {}
    holds_dict = {}
    for snap, hold_list in holds.iteritems():
        if not isinstance(hold_list, list):
            raise TypeError('holds must be in a list')
        holds_dict[snap] = {hold: None for hold in hold_list}
    #holds_dict = {snap: {hold: None for hold in hold_list}
    #                for snap, hold_list in holds.iteritems()}
    with nvlist_in(holds_dict) as nvlist:
        with nvlist_out(errlist) as errlist_nvlist:
            ret = _lib.lzc_release(nvlist, errlist_nvlist)
    _handleErrList(ret, errlist, holds.keys(), HoldReleaseFailure, _map)
    # If there is no error (no exception raised by _handleErrList), but errlist
    # is not empty, then it contains missing snapshots and tags.
    assert all(x == errno.ENOENT for x in errlist.itervalues())
    return errlist.keys()


def lzc_get_holds(snapname):
    '''
    Retrieve list of *user holds* on the specified snapshot.

    :param str snapname: the name of the snapshot.
    :return: holds on the snapshot along with their creation times
             in seconds since the epoch
    :rtype: dict of str : int
    '''
    holds = {}
    with nvlist_out(holds) as nvlist:
        ret = _lib.lzc_get_holds(snapname, nvlist)
    if ret != 0:
        if ret == errno.EINVAL:
            if not _is_valid_snap_name(snapname):
                raise NameInvalid(snapname)
            elif len(snapname) > 256:
                raise NameTooLong(snapname)
        raise {
            errno.ENOENT: SnapshotNotFound(snapname),
        }.get(ret, genericException(ret, snapname, "Failed to get holds on snapshot"))
    return holds


def lzc_send(snapname, fromsnap, fd, flags = 0):
    '''
    Generate a zfs send stream for the specified snapshot and write it to
    the specified file descriptor.

    :param str snapname: the name of the snapshot to send.
    :param fromsnap: if not None the name of the starting snapshot
                     for the incremental stream.
    :type fromsnap: str or None
    :param int fd: the file descriptor to write the send stream to.
    :param flags: the flags that control what enhanced features can be used
                  in the stream.

    :raises SnapshotNotFound: if either the starting snapshot is not `None` and does not exist,
                              or if the ending snapshot does not exist.
    :raises NameInvalid: if the name of either snapshot is invalid.
    :raises NameTooLong: if the name of either snapshot is too long.
    :raises SnapshotMismatch: if ``fromsnap`` is not an ancestor snapshot of ``snapname``.
    :raises PoolsDiffer: if the snapshots belong to different pools.

    If ``fromsnap`` is None, a full (non-incremental) stream will be sent.
    If ``fromsnap`` is not None, it must be the full name of a snapshot or
    bookmark to send an incremental from, e.g. :file:`{pool}/{fs}@{earlier_snap}`
    or :file:`{pool}/{fs}#{earlier_bmark}`.

    The specified snapshot or bookmark must represent an earlier point in the history
    of ``snapname``.
    It can be an earlier snapshot in the same filesystem or zvol as ``snapname``,
    or it can be the origin of ``snapname``'s filesystem, or an earlier
    snapshot in the origin, etc.
    ``fromsnap`` must be strictly an earlier snapshot, specifying the same snapshot
    as both ``fromsnap`` and ``snapname`` is an error.

    If ``flags`` contains LZC_SEND_FLAG_LARGE_BLOCK, the stream is permitted
    to contain DRR_WRITE records with drr_length > 128K, and DRR_OBJECT
    records with drr_blksz > 128K.

    If ``flags`` contains LZC_SEND_FLAG_EMBED_DATA, the stream is permitted
    to contain DRR_WRITE_EMBEDDED records with drr_etype==BP_EMBEDDED_TYPE_DATA,
    which the receiving system must support (as indicated by support
    for the *embedded_data* feature).
    '''
    c_fromsnap = fromsnap if fromsnap is not None else _ffi.NULL
    ret = _lib.lzc_send(snapname, c_fromsnap, fd, flags)
    if ret != 0:
        if ret == errno.EXDEV and fromsnap is not None:
            if _pool_name(fromsnap) != _pool_name(snapname):
                raise PoolsDiffer(snapname)
            else:
                raise SnapshotMismatch(snapname)
        elif ret == errno.EINVAL:
            if fromsnap is not None and not _is_valid_snap_name(fromsnap):
                raise NameInvalid(fromsnap)
            elif not _is_valid_snap_name(snapname):
                raise NameInvalid(snapname)
            elif fromsnap is not None and len(fromsnap) > 256:
                raise NameTooLong(fromsnap)
            elif len(snapname) > 256:
                raise NameTooLong(snapname)
            elif fromsnap is not None and _pool_name(fromsnap) != _pool_name(snapname):
                raise PoolsDiffer(snapname)
        elif ret == errno.ENOENT and fromsnap is not None:
            if not _is_valid_snap_name(fromsnap):
                raise NameInvalid(fromsnap)
        raise {
            errno.ENOENT: SnapshotNotFound(snapname),
        }.get(ret, genericException(ret, snapname, "Failed to create backup stream"))


def lzc_send_space(snapname, fromsnap = None):
    '''
    Estimate size of a full or incremental backup stream
    given the optional starting snapshot and the ending snapshot.

    :param str snapname: the name of the snapshot for which the estimate should be done.
    :param fromsnap: the optional starting snapshot name.
                     If not `None` then an incremental stream size is estimated,
                     otherwise a full stream is esimated.
    :type fromsnap: `str` or `None`
    :return: the estimated stream size, in bytes.
    :rtype: `int` or `long`

    :raises SnapshotNotFound: if either the starting snapshot is not `None` and does not exist,
                              or if the ending snapshot does not exist.
    :raises NameInvalid: if the name of either snapshot is invalid.
    :raises NameTooLong: if the name of either snapshot is too long.
    :raises SnapshotMismatch: if ``fromsnap`` is not an ancestor snapshot of ``snapname``.
    :raises PoolsDiffer: if the snapshots belong to different pools.

    ``fromsnap``, if not ``None``,  must be strictly an earlier snapshot,
    specifying the same snapshot as both ``fromsnap`` and ``snapname`` is an error.
    '''
    c_fromsnap = fromsnap if fromsnap is not None else _ffi.NULL
    valp = _ffi.new('uint64_t *')
    ret = _lib.lzc_send_space(snapname, c_fromsnap, valp)
    if ret != 0:
        if ret == errno.EXDEV and fromsnap is not None:
            if _pool_name(fromsnap) != _pool_name(snapname):
                raise PoolsDiffer(snapname)
            else:
                raise SnapshotMismatch(snapname)
        elif ret == errno.EINVAL:
            if fromsnap is not None and not _is_valid_snap_name(fromsnap):
                raise NameInvalid(fromsnap)
            elif not _is_valid_snap_name(snapname):
                raise NameInvalid(snapname)
            elif fromsnap is not None and len(fromsnap) > 256:
                raise NameTooLong(fromsnap)
            elif len(snapname) > 256:
                raise NameTooLong(snapname)
            elif fromsnap is not None and _pool_name(fromsnap) != _pool_name(snapname):
                raise PoolsDiffer(snapname)
        elif ret == errno.ENOENT and fromsnap is not None:
            if not _is_valid_snap_name(fromsnap):
                raise NameInvalid(fromsnap)
        raise {
            errno.ENOENT: SnapshotNotFound(snapname),
        }.get(ret, genericException(ret, snapname, "Failed to estimate backup stream size"))

    return int(valp[0])


def lzc_receive(snapname, props, origin, force, fd):
    ret = 0
    with nvlist_in(props) as nvlist:
        ret = _lib.lzc_receive(snapname, nvlist, origin, force, fd)
    return ret


def lzc_exists(name):
    '''
    Check if a dataset (a filesystem, or a volume, or a snapshot)
    with the given name exists.

    :param str name: the dataset name to check.
    :return: `True` if the dataset exists, `False` otherwise.
    :rtype: bool

    .. note::
        ``lzc_exists`` can not be used to check for existence of bookmarks.
    '''
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


def _pool_name(name):
    return re.split('[/@#]', name, 1)[0]


def _fs_name(name):
    return re.split('[@#]', name, 1)[0]


def _is_valid_name_component(component):
    allowed = string.ascii_letters + string.digits + '-_.: '
    return bool(component) and all(x in allowed for x in component)


def _is_valid_fs_name(name):
    return bool(name) and all(_is_valid_name_component(c) for c in name.split('/'))


def _is_valid_snap_name(name):
    parts = name.split('@')
    return (len(parts) == 2 and _is_valid_fs_name(parts[0]) and
           _is_valid_name_component(parts[1]))


def _is_valid_bmark_name(name):
    parts = name.split('#')
    return (len(parts) == 2 and _is_valid_fs_name(parts[0]) and
           _is_valid_name_component(parts[1]))


# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
