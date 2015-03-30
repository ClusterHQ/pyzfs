from .exceptions import *
from .bindings import libzfs_core
from ._nvlist import nvlist_in, nvlist_out

_ffi = libzfs_core.ffi
_lib = libzfs_core.lib

# TODO: a better way to init and uninit the library
_lib.libzfs_core_init()


def lzc_create(name, is_zvol, props):
    ret = 0
    with nvlist_in(props) as nvlist:
        ret = _lib.lzc_create(name, is_zvol, nvlist)
    if ret != 0:
        raise {
            errno.EEXIST: FilesystemExists(name),
            errno.ENOENT: ParentNotFound(name),
        }.get(ret, ZFSError(ret, "Failed to create filesystem", name))


def lzc_snapshot(snaps, props, errlist):
    ret = 0
    with nvlist_in(snaps) as snaps_nvlist, nvlist_in(props) as props_nvlist:
        with nvlist_out(errlist) as errlist_nvlist:
            ret = _lib.lzc_snapshot(snaps_nvlist, props_nvlist, errlist_nvlist)
    if ret != 0:
        raise {
            errno.EEXIST: SnapshotExists(None),
            errno.ENOENT: FilesystemNotFound(None),
        }.get(ret, ZFSError(ret, "Failed to create snapshot", name))


def lzc_promote(name):
    ret = _lib.lzc_promote(name)
    if ret != 0:
        raise {
            errno.EEXIST: SnapshotExists(None),
            errno.ENOENT: FilesystemNotFound(None),
        }.get(ret, ZFSError(ret, "Failed to create snapshot", name))


def lzc_rollback(name):
    snapnamep = _ffi.new('char[]', 256)
    ret = _lib.lzc_rollback(name, snapnamep, 256)
    return (ret, _ffi.string(snapnamep))


def lzc_set_props(name, props, received):
    ret = 0
    with nvlist_in(props) as nvlist:
        ret = _lib.lzc_set_props(name, nvlist, received)
    return ret


def lzc_destroy_snaps(snaps, defer, errlist):
    ret = 0
    with nvlist_in(snaps) as snaps_nvlist:
        with nvlist_out(errlist) as errlist_nvlist:
            ret = _lib.lzc_destroy_snaps(snaps_nvlist, defer, errlist_nvlist)
    return ret


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


def lzc_send_ext(snapname, fromsnap, fd, props):
    ret = 0
    with nvlist_in(props) as nvlist:
        ret = _lib.lzc_send_ext(snapname, fromsnap, fd, props)
    return ret


def lzc_send_space(snapname, fromsnap):
    valp = _ffi.new('uint64_t *')
    ret = _lib.lzc_send_space(snapname, fromsnap, valp)
    return (ret, int(valp[0]))


def lzc_send_progress(snapname, fd):
    valp = _ffi.new('uint64_t *')
    ret = _lib.lzc_send_progress(snapname, fd, valp)
    return (ret, int(valp[0]))


def lzc_receive(snapname, props, origin, force, fd):
    ret = 0
    with nvlist_in(props) as nvlist:
        ret = _lib.lzc_receive(snapname, nvlist, origin, force, fd)
    return ret


def lzc_exists(name):
    ret = _lib.lzc_exists(name)
    return bool(ret)


# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
