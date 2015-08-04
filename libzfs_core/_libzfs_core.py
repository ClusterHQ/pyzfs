# Copyright 2015 ClusterHQ. See LICENSE file for details.

"""
Python wrappers for libzfs_core interfaces.

As a rule, there is a Python function for each C function.
The signatures of the Python functions generally follow those of the
functions, but the argument types are natural to Python.
nvlists are wrapped as dictionaries or lists depending on their usage.
Some parameters have default values depending on typical use for
increased convenience.  Output parameters are not used and return values
are directly returned.  Error conditions are signalled by exceptions
rather than by integer error codes.
"""

import errno
import threading
from . import exceptions
from . import _error_translation as xlate
from .bindings import libzfs_core
from ._constants import MAXNAMELEN
from ._nvlist import nvlist_in, nvlist_out


def lzc_create(name, is_zvol = False, props = {}):
    '''
    Create a ZFS filesystem or a ZFS volume ("zvol").

    :param bytes name: a name of the dataset to be created.
    :param bool is_zvol: whether to create a zvol (false by default).
    :param props: a `dict` of ZFS dataset property name-value pairs (empty by default).
    :type props: dict of bytes:Any

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
    nvlist = nvlist_in(props)
    ret = _lib.lzc_create(name, ds_type, nvlist)
    xlate.lzc_create_xlate_error(ret, name, is_zvol, props)


def lzc_clone(name, origin, props = {}):
    '''
    Clone a ZFS filesystem or a ZFS volume ("zvol") from a given snapshot.

    :param bytes name: a name of the dataset to be created.
    :param bytes origin: a name of the origin snapshot.
    :param props: a `dict` of ZFS dataset property name-value pairs (empty by default).
    :type props: dict of bytes:Any

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
    nvlist = nvlist_in(props)
    ret = _lib.lzc_clone(name, origin, nvlist)
    xlate.lzc_clone_xlate_error(ret, name, origin, props)


def lzc_rollback(name):
    '''
    Roll back a filesystem or volume to its most recent snapshot.

    :param bytes name: a name of the dataset to be rolled back.
    :return: a name of the most recent snapshot.
    :rtype: bytes

    :raises FilesystemNotFound: if the dataset does not exist.
    :raises SnapshotNotFound: if the dataset does not have any snapshots.
    :raises NameInvalid: if the dataset name is invalid.
    :raises NameTooLong: if the dataset name is too long.
    '''
    # Account for terminating NUL in C strings.
    snapnamep = _ffi.new('char[]', MAXNAMELEN + 1)
    ret = _lib.lzc_rollback(name, snapnamep, MAXNAMELEN + 1)
    xlate.lzc_rollback_xlate_error(ret, name)
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
    :type snaps: list of bytes
    :param props: a `dict` of ZFS dataset property name-value pairs (empty by default).
    :type props: dict of bytes:bytes

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
    snaps_dict = { name: None for name in snaps }
    errlist = {}
    snaps_nvlist = nvlist_in(snaps_dict)
    props_nvlist = nvlist_in(props)
    with nvlist_out(errlist) as errlist_nvlist:
        ret = _lib.lzc_snapshot(snaps_nvlist, props_nvlist, errlist_nvlist)
    xlate.lzc_snapshot_xlate_errors(ret, errlist, snaps, props)


lzc_snap = lzc_snapshot


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
    :type snaps: list of bytes
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
    snaps_dict = { name: None for name in snaps }
    errlist = {}
    snaps_nvlist = nvlist_in(snaps_dict)
    with nvlist_out(errlist) as errlist_nvlist:
        ret = _lib.lzc_destroy_snaps(snaps_nvlist, defer, errlist_nvlist)
    xlate.lzc_destroy_snaps_xlate_errors(ret, errlist, snaps, defer)


def lzc_bookmark(bookmarks):
    '''
    Create bookmarks.

    :param bookmarks: a dict that maps names of wanted bookmarks to names of existing snapshots.
    :type bookmarks: dict of bytes to bytes

    :raises BookmarkFailure: if any of the bookmarks can not be created for any reason.

    The bookmarks `dict` maps from name of the bookmark (e.g. :file:`{pool}/{fs}#{bmark}`) to
    the name of the snapshot (e.g. :file:`{pool}/{fs}@{snap}`).  All the bookmarks and
    snapshots must be in the same pool.
    '''
    errlist = {}
    nvlist = nvlist_in(bookmarks)
    with nvlist_out(errlist) as errlist_nvlist:
        ret = _lib.lzc_bookmark(nvlist, errlist_nvlist)
    xlate.lzc_bookmark_xlate_errors(ret, errlist, bookmarks)


def lzc_get_bookmarks(fsname, props = []):
    '''
    Retrieve a list of bookmarks for the given file system.

    :param bytes fsname: a name of the filesystem.
    :param props: a `list` of properties that will be returned for each bookmark.
    :type props: list of bytes
    :return: a `dict` that maps the bookmarks' short names to their properties.
    :rtype: dict of bytes:dict

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
    nvlist = nvlist_in(props_dict)
    with nvlist_out(bmarks) as bmarks_nvlist:
        ret = _lib.lzc_get_bookmarks(fsname, nvlist, bmarks_nvlist)
    xlate.lzc_get_bookmarks_xlate_error(ret, fsname, props)
    return bmarks


def lzc_destroy_bookmarks(bookmarks):
    '''
    Destroy bookmarks.

    :param bookmarks: a list of the bookmarks to be destroyed.
                      The bookmarks are specified as :file:`{fs}#{bmark}`.
    :type bookmarks: list of bytes

    :raises BookmarkDestructionFailure: if any of the bookmarks may not be destroyed.

    The bookmarks must all be in the same pool.
    Bookmarks that do not exist will be silently ignored.
    This also includes the case where the filesystem component of the bookmark
    name does not exist.
    However, an invalid bookmark name will cause :exc:`.NameInvalid` error
    reported in :attr:`SnapshotDestructionFailure.errors`.

    Either all bookmarks that existed are destroyed or an exception is raised.
    '''
    errlist = {}
    bmarks_dict = { name: None for name in bookmarks }
    nvlist = nvlist_in(bmarks_dict)
    with nvlist_out(errlist) as errlist_nvlist:
        ret = _lib.lzc_destroy_bookmarks(nvlist, errlist_nvlist)
    xlate.lzc_destroy_bookmarks_xlate_errors(ret, errlist, bookmarks)


def lzc_snaprange_space(firstsnap, lastsnap):
    '''
    Calculate a size of data referenced by snapshots in the inclusive range between
    the ``firstsnap`` and the ``lastsnap`` and not shared with any other datasets.

    :param bytes firstsnap: the name of the first snapshot in the range.
    :param bytes lastsnap: the name of the last snapshot in the range.
    :return: the calculated stream size, in bytes.
    :rtype: `int` or `long`

    :raises SnapshotNotFound: if either of the snapshots does not exist.
    :raises NameInvalid: if the name of either snapshot is invalid.
    :raises NameTooLong: if the name of either snapshot is too long.
    :raises SnapshotMismatch: if ``fromsnap`` is not an ancestor snapshot of ``snapname``.
    :raises PoolsDiffer: if the snapshots belong to different pools.

    ``lzc_snaprange_space`` calculates total size of blocks that exist
    because they are referenced only by one or more snapshots in the given range
    but no other dataset.
    In other words, this is the set of blocks that were born after the snap before
    firstsnap, and died before the snap after the last snap.
    Yet another interpretation is that the result of ``lzc_snaprange_space`` is the size
    of the space that would be freed if the snapshots in the range are destroyed.

    If the same snapshot is given as both the ``firstsnap`` and the ``lastsnap``.
    In that case ``lzc_snaprange_space`` calculates space used by the snapshot.
    '''
    valp = _ffi.new('uint64_t *')
    ret = _lib.lzc_snaprange_space(firstsnap, lastsnap, valp)
    xlate.lzc_snaprange_space_xlate_error(ret, firstsnap, lastsnap)
    return int(valp[0])


def lzc_hold(holds, fd = None):
    '''
    Create *user holds* on snapshots.  If there is a hold on a snapshot,
    the snapshot can not be destroyed.  (However, it can be marked for deletion
    by :func:`lzc_destroy_snaps` ( ``defer`` = `True` ).)

    :param holds: the dictionary of names of the snapshots to hold mapped to the hold names.
    :type holds: dict of bytes : bytes
    :type fd: int or None
    :param fd: if not None then it must be the result of :func:`os.open` called as ``os.open("/dev/zfs", O_EXCL)``.
    :type fd: int or None
    :return: a list of the snapshots that do not exist.
    :rtype: list of bytes

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
    errlist = {}
    if fd is None:
        fd = -1
    nvlist = nvlist_in(holds)
    with nvlist_out(errlist) as errlist_nvlist:
        ret = _lib.lzc_hold(nvlist, fd, errlist_nvlist)
    xlate.lzc_hold_xlate_errors(ret, errlist, holds, fd)
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
    :type holds: dict of bytes : list of bytes
    :return: a list of any snapshots that do not exist and of any tags that do not
             exist for existing snapshots.
             Such tags are qualified with a corresponding snapshot name
             using the following format :file:`{pool}/{fs}@{snap}#{tag}`
    :rtype: list of bytes

    :raises HoldReleaseFailure: if one or more existing holds could not be released.

    Holds which failed to release because they didn't exist will have an entry
    added to errlist, but will not cause an overall failure.

    This call is success if ``holds`` was empty or all holds that
    existed, were successfully removed.
    Otherwise an exception will be raised.
    '''
    errlist = {}
    holds_dict = {}
    for snap, hold_list in holds.iteritems():
        if not isinstance(hold_list, list):
            raise TypeError('holds must be in a list')
        holds_dict[snap] = {hold: None for hold in hold_list}
    nvlist = nvlist_in(holds_dict)
    with nvlist_out(errlist) as errlist_nvlist:
        ret = _lib.lzc_release(nvlist, errlist_nvlist)
    xlate.lzc_release_xlate_errors(ret, errlist, holds)
    # If there is no error (no exception raised by _handleErrList), but errlist
    # is not empty, then it contains missing snapshots and tags.
    assert all(x == errno.ENOENT for x in errlist.itervalues())
    return errlist.keys()


def lzc_get_holds(snapname):
    '''
    Retrieve list of *user holds* on the specified snapshot.

    :param bytes snapname: the name of the snapshot.
    :return: holds on the snapshot along with their creation times
             in seconds since the epoch
    :rtype: dict of bytes : int
    '''
    holds = {}
    with nvlist_out(holds) as nvlist:
        ret = _lib.lzc_get_holds(snapname, nvlist)
    xlate.lzc_get_holds_xlate_error(ret, snapname)
    return holds


def lzc_send(snapname, fromsnap, fd, flags = []):
    '''
    Generate a zfs send stream for the specified snapshot and write it to
    the specified file descriptor.

    :param bytes snapname: the name of the snapshot to send.
    :param fromsnap: if not None the name of the starting snapshot
                     for the incremental stream.
    :type fromsnap: bytes or None
    :param int fd: the file descriptor to write the send stream to.
    :param flags: the flags that control what enhanced features can be used
                  in the stream.
    :type flags: list of bytes

    :raises SnapshotNotFound: if either the starting snapshot is not `None` and does not exist,
                              or if the ending snapshot does not exist.
    :raises NameInvalid: if the name of either snapshot is invalid.
    :raises NameTooLong: if the name of either snapshot is too long.
    :raises SnapshotMismatch: if ``fromsnap`` is not an ancestor snapshot of ``snapname``.
    :raises PoolsDiffer: if the snapshots belong to different pools.
    :raises IOError: if an input / output error occurs while writing to ``fd``.
    :raises ValueError: if the ``flags`` contain an invalid flag name.

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

    If ``flags`` contains *"large_blocks"*, the stream is permitted
    to contain ``DRR_WRITE`` records with ``drr_length`` > 128K, and ``DRR_OBJECT``
    records with ``drr_blksz`` > 128K.

    If ``flags`` contains *"embedded_data"*, the stream is permitted
    to contain ``DRR_WRITE_EMBEDDED`` records with
    ``drr_etype`` == ``BP_EMBEDDED_TYPE_DATA``,
    which the receiving system must support (as indicated by support
    for the *embedded_data* feature).

    .. note::
        ``lzc_send`` can actually accept a filesystem name as the ``snapname``.
        In that case ``lzc_send`` acts as if a temporary snapshot was created
        after the start of the call and before the stream starts being produced.
    '''
    if fromsnap is not None:
        c_fromsnap = fromsnap
    else:
        c_fromsnap = _ffi.NULL
    c_flags = 0
    for flag in flags:
        c_flag = {
            'embedded_data':    _lib.LZC_SEND_FLAG_EMBED_DATA,
            'large_blocks':     _lib.LZC_SEND_FLAG_LARGE_BLOCK,
        }.get(flag)
        if c_flag is None:
            raise ValueError('Unknown flag value ' + flag)
        c_flags |= c_flag

    ret = _lib.lzc_send(snapname, c_fromsnap, fd, c_flags)
    xlate.lzc_send_xlate_error(ret, snapname, fromsnap, fd, flags)


def lzc_send_space(snapname, fromsnap = None):
    '''
    Estimate size of a full or incremental backup stream
    given the optional starting snapshot and the ending snapshot.

    :param bytes snapname: the name of the snapshot for which the estimate should be done.
    :param fromsnap: the optional starting snapshot name.
                     If not `None` then an incremental stream size is estimated,
                     otherwise a full stream is esimated.
    :type fromsnap: `bytes` or `None`
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
    if fromsnap is not None:
        c_fromsnap = fromsnap
    else:
        c_fromsnap = _ffi.NULL
    valp = _ffi.new('uint64_t *')
    ret = _lib.lzc_send_space(snapname, c_fromsnap, valp)
    xlate.lzc_send_space_xlate_error(ret, snapname, fromsnap)
    return int(valp[0])


def lzc_receive(snapname, fd, force = False, origin = None, props = {}):
    '''
    Receive from the specified ``fd``, creating the specified snapshot.

    :param bytes snapname: the name of the snapshot to create.
    :param int fd: the file descriptor from which to read the stream.
    :param bool force: whether to roll back or destroy the target filesystem
                       if that is required to receive the stream.
    :param origin: the optional origin snapshot name if the stream is for a clone.
    :type origin: bytes or None
    :param props: the properties to set on the snapshot as *received* properties.
    :type props: dict of bytes : Any

    :raises IOError: if an input / output error occurs while reading from the ``fd``.
    :raises DatasetExists: if the snapshot named ``snapname`` already exists.
    :raises DatasetExists: if the stream is a full stream and the destination filesystem already exists.
    :raises DatasetExists: if ``force`` is `True` but the destination filesystem could not
                           be rolled back to a matching snapshot because a newer snapshot
                           exists and it is an origin of a cloned filesystem.
    :raises StreamMismatch: if an incremental stream is received and the latest
                            snapshot of the destination filesystem does not match
                            the source snapshot of the stream.
    :raises StreamMismatch: if a full stream is received and the destination
                            filesystem already exists and it has at least one snapshot,
                            and ``force`` is `False`.
    :raises StreamMismatch: if an incremental clone stream is received but the specified
                            ``origin`` is not the actual received origin.
    :raises DestinationModified: if an incremental stream is received and the destination
                                 filesystem has been modified since the last snapshot
                                 and ``force`` is `False`.
    :raises DestinationModified: if a full stream is received and the destination
                                 filesystem already exists and it does not have any
                                 snapshots, and ``force`` is `False`.
    :raises DatasetNotFound: if the destination filesystem and its parent do not exist.
    :raises DatasetNotFound: if the ``origin`` is not `None` and does not exists.
    :raises DatasetBusy: if ``force`` is `True` but the destination filesystem could not
                         be rolled back to a matching snapshot because a newer snapshot
                         is held and could not be destroyed.
    :raises DatasetBusy: if another receive operation is being performed on the
                         destination filesystem and this operation has lost the race.
    :raises BadStream: if the stream is corrupt or it is not recognized or it is
                       a compound stream or it is a clone stream, but ``origin``
                       is `None`.
    :raises BadStream: if a clone stream is received and the destination filesystem
                       already exists.
    :raises StreamFeatureNotSupported: if the stream has a feature that is not
                                       supported on the receiving side.
    :raises PropertyInvalid: if one or more of the specified properties is invalid
                             or has an invalid type or value.
    :raises NameInvalid: if the name of either snapshot is invalid.
    :raises NameTooLong: if the name of either snapshot is too long.

    .. note::
        The ``origin`` is ignored if the actual stream is an incremental stream
        that is not a clone stream and the destination filesystem exists.
        If the stream is a full stream and the destination filesystem does not
        exist then the ``origin`` is checked for existence: if it does not exist
        :exc:`.DatasetNotFound` is raised, otherwise :exc:`.StreamMismatch` is
        raised, because that snapshot can not have any relation to the stream.

    .. note::
        If ``force`` is `True` and the stream is incremental then the destination
        filesystem is rolled back to a matching source snapshot if necessary.
        Intermediate snapshots are destroyed in that case.

        However, none of the existing snapshots must have the same name as
        ``snapname`` even if such a snapshot were to be destroyed.
        The existing ``snapname`` snapshot always causes :exc:`.SnapshotExists`
        to be raised.

        If ``force`` is `True` and the stream is a full stream then the destination
        filesystem is replaced with the received filesystem unless the former
        has any snapshots.
        Those prevent the destination filesystem from being rolled back / replaced.

    .. note::
        This interface does not work on dedup'd streams
        (those with ``DMU_BACKUP_FEATURE_DEDUP``).
    '''

    if origin is not None:
        c_origin = origin
    else:
        c_origin = _ffi.NULL
    nvlist = nvlist_in(props)
    ret = _lib.lzc_receive(snapname, nvlist, c_origin, force, fd)
    xlate.lzc_receive_xlate_error(ret, snapname, fd, force, origin, props)


lzc_recv = lzc_receive


def lzc_exists(name):
    '''
    Check if a dataset (a filesystem, or a volume, or a snapshot)
    with the given name exists.

    :param bytes name: the dataset name to check.
    :return: `True` if the dataset exists, `False` otherwise.
    :rtype: bool

    .. note::
        ``lzc_exists`` can not be used to check for existence of bookmarks.
    '''
    ret = _lib.lzc_exists(name)
    return bool(ret)


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
                            raise exceptions.ZFSInitializationFailed(ret)
                        self._inited = True
            return getattr(self._lib, name)

    return LazyInit(libzfs_core.lib)

_ffi = libzfs_core.ffi
_lib = _initialize()


# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
