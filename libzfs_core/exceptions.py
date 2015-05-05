# Copyright 2015 ClusterHQ. See LICENSE file for details.

import errno
import os

class ZFSError(OSError):
    pass

class ZFSInitializationFailed(ZFSError):
    pass

# Use first of the individual error codes
# as an overall error code.  This is more consistent.
class MultipleOperationsFailure(ZFSError):
    def __init__(self, message, errors, suppressed_count):
        super(MultipleOperationsFailure, self).__init__(errors[0].errno, message)
        #self.message = "Operation on more than one entity failed for one or more reasons"
        self.errors = errors
        #: this many errors were encountered but not placed on the `errors` list
        self.suppressed_count = suppressed_count

class DatasetNotFound(ZFSError):
    """
    This exception is raised when an operation failure can be caused by a missing
    snapshot or a missing filesystem and it is impossible to distinguish between
    the causes.
    """
    def __init__(self, name):
        super(DatasetNotFound, self).__init__(errno.ENOENT, "Dataset not found", name)

class DatasetExists(ZFSError):
    """
    This exception is raised when an operation failure can be caused by an existing
    snapshot or filesystem and it is impossible to distinguish between
    the causes.
    """
    def __init__(self, name):
        super(DatasetExists, self).__init__(errno.EEXIST, "Dataset already exists", name)


class NotClone(ZFSError):
    def __init__(self, name):
        super(NotClone, self).__init__(errno.EINVAL, "Filesystem is not a clone, can not promote", name)

class FilesystemExists(ZFSError):
    def __init__(self, name):
        super(FilesystemExists, self).__init__(errno.EEXIST, "Filesystem already exists", name)

class FilesystemNotFound(ZFSError):
    def __init__(self, name):
        super(FilesystemNotFound, self).__init__(errno.ENOENT, "Filesystem not found", name)

class ParentNotFound(ZFSError):
    def __init__(self, name):
        super(ParentNotFound, self).__init__(errno.ENOENT, "Parent not found", name)

class WrongParent(ZFSError):
    def __init__(self, name):
        super(WrongParent, self).__init__(errno.EINVAL, "Parent dataset is not a filesystem", name)

class SnapshotExists(ZFSError):
    def __init__(self, name):
        super(SnapshotExists, self).__init__(errno.EEXIST, "Snapshot already exists", name)

class SnapshotNotFound(ZFSError):
    def __init__(self, name):
        super(SnapshotNotFound, self).__init__(errno.ENOENT, "Snapshot not found", name)

class SnapshotIsCloned(ZFSError):
    def __init__(self, name):
        super(SnapshotIsCloned, self).__init__(errno.EEXIST, "Snapshot is cloned", name)

class SnapshotIsHeld(ZFSError):
    def __init__(self, name):
        super(SnapshotIsHeld, self).__init__(errno.EBUSY, "Snapshot is held", name)

class DuplicateSnapshots(ZFSError):
    def __init__(self, name):
        super(DuplicateSnapshots, self).__init__(errno.EXDEV, "Requested multiple snapshots of the same filesystem", name)

class SnapshotFailure(MultipleOperationsFailure):
    def __init__(self, errors, suppressed_count):
        super(SnapshotFailure, self).__init__("Creation of snapshot(s) failed for one or more reasons", errors, suppressed_count)

class SnapshotDestructionFailure(MultipleOperationsFailure):
    def __init__(self, errors, suppressed_count):
        super(SnapshotDestructionFailure, self).__init__("Destruction of snapshot(s) failed for one or more reasons", errors, suppressed_count)

class BookmarkExists(ZFSError):
    def __init__(self, name):
        super(BookmarkExists, self).__init__(errno.EEXIST, "Bookmark already exists", name)

class BookmarkNotFound(ZFSError):
    def __init__(self, name):
        super(BookmarkNotFound, self).__init__(errno.ENOENT, "Bookmark not found", name)

class BookmarkMismatch(ZFSError):
    def __init__(self, name):
        super(BookmarkMismatch, self).__init__(errno.EINVAL, "Bookmark is not in snapshot's filesystem", name)

class BookmarkNotSupported(ZFSError):
    def __init__(self, name):
        super(BookmarkNotSupported, self).__init__(errno.ENOTSUP, "Bookmark feature is not supported", name)

class BookmarkFailure(MultipleOperationsFailure):
    def __init__(self, errors, suppressed_count):
        super(BookmarkFailure, self).__init__("Creation of bookmark(s) failed for one or more reasons", errors, suppressed_count)

class BookmarkDestructionFailure(MultipleOperationsFailure):
    def __init__(self, errors, suppressed_count):
        super(BookmarkDestructionFailure, self).__init__("Destruction of bookmark(s) failed for one or more reasons", errors, suppressed_count)

class BadHoldCleanupFD(ZFSError):
    def __init__(self):
        super(BadHoldCleanupFD, self).__init__(errno.EBADF, "Bad file descriptor as cleanup file descriptor")

class HoldExists(ZFSError):
    def __init__(self, name):
        super(HoldExists, self).__init__(errno.EEXIST, "Hold with a given tag already exists on snapshot", name)

class HoldNotFound(ZFSError):
    def __init__(self, name):
        super(HoldNotFound, self).__init__(errno.ENOENT, "Hold with a given tag does not exist on snapshot", name)

class HoldFailure(MultipleOperationsFailure):
    def __init__(self, errors, suppressed_count):
        super(HoldFailure, self).__init__("Placement of hold(s) failed for one or more reasons", errors, suppressed_count)

class HoldReleaseFailure(MultipleOperationsFailure):
    def __init__(self, errors, suppressed_count):
        super(HoldReleaseFailure, self).__init__("Release of hold(s) failed for one or more reasons", errors, suppressed_count)

class SnapshotMismatch(ZFSError):
    def __init__(self, name):
        super(SnapshotMismatch, self).__init__(errno.ENODEV, "Snapshot is not descendant of source snapshot", name)

class StreamMismatch(ZFSError):
    def __init__(self, name):
        super(StreamMismatch, self).__init__(errno.ENODEV, "Stream is not applicable to destination dataset", name)

class DestinationModified(ZFSError):
    def __init__(self, name):
        super(DestinationModified, self).__init__(errno.ETXTBSY, "Destination dataset has modifications that can not be undone", name)

class BadStream(ZFSError):
    def __init__(self):
        super(BadStream, self).__init__(errno.EINVAL, "Bad backup stream")

class StreamFeatureNotSupported(ZFSError):
    def __init__(self):
        super(StreamFeatureNotSupported, self).__init__(errno.ENOTSUP, "Stream contains unsupported feature")

class ZIOError(ZFSError):
    def __init__(self, name):
        super(ZIOError, self).__init__(errno.EIO, "I/O error", name)

class NoSpace(ZFSError):
    def __init__(self, name):
        super(NoSpace, self).__init__(errno.ENOSPC, "No space left", name)

class QuotaExceeded(ZFSError):
    def __init__(self, name):
        super(QuotaExceeded, self).__init__(errno.EDQUOT, "Quouta exceeded", name)

class DatasetBusy(ZFSError):
    def __init__(self, name):
        super(DatasetBusy, self).__init__(errno.EBUSY, "Dataset is busy", name)

class NameTooLong(ZFSError):
    def __init__(self, name):
        super(NameTooLong, self).__init__(errno.ENAMETOOLONG, "Dataset name is too long", name)

class NameInvalid(ZFSError):
    def __init__(self, name):
        super(NameInvalid, self).__init__(errno.EINVAL, "Invalid name", name)

class ReadOnlyPool(ZFSError):
    def __init__(self, name):
        super(ReadOnlyPool, self).__init__(errno.EROFS, "Pool is read-only", name)

class SuspendedPool(ZFSError):
    def __init__(self, name):
        super(SuspendedPool, self).__init__(errno.EROFS, "Pool is suspended", name)

class PoolNotFound(ZFSError):
    def __init__(self, name):
        super(PoolNotFound, self).__init__(errno.EXDEV, "No such pool", name)

class PoolsDiffer(ZFSError):
    def __init__(self, name):
        super(PoolsDiffer, self).__init__(errno.EXDEV, "Source and target belong to different pools", name)

class FeatureNotSupported(ZFSError):
    def __init__(self, name):
        super(FeatureNotSupported, self).__init__(errno.ENOTSUP, "Feature is not supported in this version", name)

class PropertyNotSupported(ZFSError):
    def __init__(self, name):
        super(PropertyNotSupported, self).__init__(errno.ENOTSUP, "Property is not supported in this version", name)

class PropertyInvalid(ZFSError):
    def __init__(self, name):
        super(PropertyInvalid, self).__init__(errno.EINVAL, "Invalid property or property value", name)


def genericException(err, name, message):
    if err in _errToException:
        return _errToException[err](name)
    else:
        return ZFSError(err, message, name)

_errToException = {
    errno.EIO:          ZIOError,
    errno.ENOSPC:       NoSpace,
    errno.EDQUOT:       QuotaExceeded,
    errno.EBUSY:        DatasetBusy,
    errno.ENAMETOOLONG: NameTooLong,
    errno.EROFS:        ReadOnlyPool,
    errno.EAGAIN:       SuspendedPool,
    errno.EXDEV:        PoolsDiffer,
    errno.ENOTSUP:      PropertyNotSupported,
}

# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
