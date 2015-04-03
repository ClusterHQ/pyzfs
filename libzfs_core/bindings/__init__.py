import sys
import threading

from cffi import FFI


def _setupCFFI():
    # Based on https://caremad.io/2014/11/distributing-a-cffi-project/
    # XXX License?
    class LazyLibrary(object):
        def __init__(self, ffi, libname):
            self._ffi = ffi
            self._libname = libname
            self._lib = None
            self._lock = threading.Lock()

        def __getattr__(self, name):
            if self._lib is None:
                with self._lock:
                    if self._lib is None:
                        self._lib = self._ffi.dlopen(self._libname)

            return getattr(self._lib, name)

    MODULES = [ "libnvpair", "libzfs_core" ]
    ffi = FFI()

    for module_name in MODULES:
        module = __import__(module_name, globals(), locals(), [], -1)
        ffi.cdef(module.CDEF)
        lib = LazyLibrary(ffi, module.LIBRARY)
        setattr(module, "ffi", ffi)
        setattr(module, "lib", lib)


_setupCFFI()

# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
