"""
Utility functions for casting to a specific C type.
"""

from .bindings.libnvpair import ffi as _ffi


def _ffi_cast(type_name):
    def _func(value):
        # this is for overflow / underflow checking only
        _ffi.new(type_name + '*', value)
        return _ffi.cast(type_name, value)
    _func.__name__ = type_name
    return _func


uint8_t =       _ffi_cast('uint8_t')
int8_t =        _ffi_cast('int8_t')
uint16_t =      _ffi_cast('uint16_t')
int16_t =       _ffi_cast('int16_t')
uint32_t =      _ffi_cast('uint32_t')
int32_t =       _ffi_cast('int32_t')
uint64_t =      _ffi_cast('uint64_t')
int64_t =       _ffi_cast('int64_t')
boolean_t =     _ffi_cast('boolean_t')
uchar_t =       _ffi_cast('uchar_t')


_type_to_suffix = {
    _ffi.typeof('uint8_t'):     'uint8',
    _ffi.typeof('int8_t'):      'int8',
    _ffi.typeof('uint16_t'):    'uint16',
    _ffi.typeof('int16_t'):     'int16',
    _ffi.typeof('uint32_t'):    'uint32',
    _ffi.typeof('int32_t'):     'int32',
    _ffi.typeof('uint64_t'):    'uint64',
    _ffi.typeof('int64_t'):     'int64',
    _ffi.typeof('boolean_t'):   'boolean',
    _ffi.typeof('uchar_t'):     'byte',
}


# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
