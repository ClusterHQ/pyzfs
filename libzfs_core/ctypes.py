"""
Utility functions for casting to a specific C type.
"""


class NvInteger(object):
    val = 0

    def __init__(self, val):
        self.val = val

    def _ctype(self):
        raise NotImplementedError("_ctype() must be implemented")

    def suffix(self):
        raise NotImplementedError("suffix() must be implemented")

    def cast(self, ffi):
        return ffi.cast(self._ctype(), self.val)


def _gen_class(ctype, suffix):
    class _class(NvInteger):
        def _ctype(self):
            return ctype
        def suffix(self):
            return suffix
    _class.__name__ = ctype
    return _class


uint8_t =     _gen_class('uint8_t', 'uint8')
int8_t =      _gen_class('int8_t', 'int8')
uint16_t =    _gen_class('uint16_t', 'uint16')
int16_t =     _gen_class('int16_t', 'int16')
uint32_t =    _gen_class('uint32_t', 'uint32')
int32_t =     _gen_class('int32_t', 'int32')
uint64_t =    _gen_class('uint64_t', 'uint64')
int64_t =     _gen_class('int64_t', 'int64')
boolean_t =   _gen_class('boolean_t', 'boolean')
uchar_t =     _gen_class('uchar_t', 'byte')


# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
