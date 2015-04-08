import errno
import os

class ZFSError(OSError):
    pass

class ZFSInitializationFailed(ZFSError):
    pass

# Use first of the individual error codes
# as an overall error code.  This is more consistent.
class MultipleOperationsFailure(ZFSError):
    def __init__(self, message, errors):
        super(MultipleOperationsFailure, self).__init__(errors[0].errno, message)
        #self.message = "Operation on more than one entity failed for one or more reasons"
        self.errors = errors

class DatasetNotFound(ZFSError):
    """
    This exception is raised when an operation failure can be caused by a missing
    snapshot or a missing filesystem and it is impossible to distinguish between
    the causes.
    """
    def __init__(self, name):
        super(DatasetNotFound, self).__init__(errno.ENOENT, "Dataset not found", name)

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
        super(SnapshotIsCloned, self).__init__(errno.ENOENT, "Snapshot is cloned", name)

class DuplicateSnapshots(ZFSError):
    def __init__(self, name):
        super(DuplicateSnapshots, self).__init__(errno.EXDEV, "Requested multiple snapshots of the same filesystem", name)

class SnapshotFailure(MultipleOperationsFailure):
    def __init__(self, errors):
        super(SnapshotFailure, self).__init__("Creation of snapshot(s) failed for one or more reasons", errors)

class SnapshotDestructionFailure(MultipleOperationsFailure):
    def __init__(self, errors):
        super(SnapshotFailure, self).__init__("Destruction of snapshot(s) failed for one or more reasons", errors)

class BookmarkExists(ZFSError):
    def __init__(self, name):
        super(BookmarkExists, self).__init__(errno.EEXIST, "Bookmark already exists", name)

class BookmarkNotFound(ZFSError):
    def __init__(self, name):
        super(BookmarkNotFound, self).__init__(errno.ENOENT, "Bookmark not found", name)

class UnrelatedSnapshot(ZFSError):
    def __init__(self, name):
        super(UnrelatedSnapshot, self).__init__(errno.EXDEV, "Snapshot is not related to a filesystem", name)

class SnapshotMismatch(ZFSError):
    def __init__(self, name):
        super(SnapshotMismatch, self).__init__(errno.ENODEV, "Snapshot does not match incremental source", name)

class DestinationModified(ZFSError):
    def __init__(self, name):
        super(DestinationModified, self).__init__(errno.ETXTBSY, "Destination modified", name)

class BadStream(ZFSError):
    def __init__(self):
        super(BadStream, self).__init__(errno.EINVAL, "Bad backup stream")

class IOError(ZFSError):
    def __init__(self, name):
        super(IOError, self).__init__(errno.EIO, "I/O error", name)

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

class ReadOnlyDataset(ZFSError):
    def __init__(self, name):
        super(ReadOnlyDataset, self).__init__(errno.EROFS, "Dataset is read-only", name)

class PoolsDiffer(ZFSError):
    def __init__(self, name):
        super(PoolsDiffer, self).__init__(errno.EXDEV, "Source and target belong to different pools", name)

class PropertyNotSupported(ZFSError):
    def __init__(self, name):
        super(PropertyNotSupported, self).__init__(errno.ENOTSUP, "Property is not supported in this verrsion", name)

class PropertyInvalid(ZFSError):
    def __init__(self, name):
        super(PropertyInvalid, self).__init__(errno.EINVAL, "Invalid property or property value", name)


def genericException(err, name, message):
    if err in _errToException:
        return _errToException[err](name)
    else:
        return ZFSError(err, message, name)

_errToException = {
    errno.EIO:          IOError,
    errno.ENOSPC:       NoSpace,
    errno.EDQUOT:       QuotaExceeded,
    errno.EBUSY:        DatasetBusy,
    errno.ENAMETOOLONG: NameTooLong,
    errno.EROFS:        ReadOnlyDataset,
    errno.EXDEV:        PoolsDiffer,
}

# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
