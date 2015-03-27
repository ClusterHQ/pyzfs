import errno

class ZFSError(OSError):
    pass

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
    def __init__(self):
        super(IOError, self).__init__(errno.EIO, "I/O error")

class NoSpace(ZFSError):
    def __init__(self):
        super(NoSpace, self).__init__(errno.ENOSPC, "No space")

class QuotaExceeded(ZFSError):
    def __init__(self):
        super(QuotaExceeded, self).__init__(errno.EDQUOT, "Quouta exceeded")

class Busy(ZFSError):
    def __init__(self):
        super(Busy, self).__init__(errno.EBUSY, "Dataset is busy")

class NameTooLong(ZFSError):
    def __init__(self, name):
        super(NameTooLong, self).__init__(errno.ENAMETOOLONG, "Dataset name is too long", name)

class ReadOnlyDataset(ZFSError):
    def __init__(self, name):
        super(NameTooLong, self).__init__(errno.EROFS, "Dataset is read-only", name)

class PropertyNotSupported(ZFSError):
    def __init__(self, name):
        super(PropertyNotSupported, self).__init__(errno.ENOTSUP, "Property is note supported in this verrsion", name)

class PropertyInvalid(ZFSError):
    def __init__(self, name):
        super(PropertyInvalid, self).__init__(errno.EINVAL, "Invalid value for property", name)

# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
