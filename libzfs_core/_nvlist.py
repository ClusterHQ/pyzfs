"""
nv_call provides support for converting between
a dictionary on the Python side and an nvlist_t on the C side
with the automatic memory management for C memory allocations.

The dictionary must follow a certain format to be convertible
to the nvlist_t.  The dictionary produced from the nvlist_t
will follow the same format.

Format:
- keys are always byte strings
- a value can be None in which case it represents boolean truth by its mere presence
- a value can be a bool
- a value can be a byte string
- a value can be an integer
- a value can be a CFFI CData object representing one of the following C types:
    int8_t, uint8_t, int16_t, uint16_t, int32_t, uint32_t, int64_t, uint64_t, boolean_t, uchar_t
- a value can be a dictionary that recursively adheres to this format
- a value can be a list of bools, byte strings, integers or CData objects of types specified above
- a value can be a list of dictionaries that adhere to this format
- all elements of a list value must be of the same type
"""

import numbers
from collections import namedtuple
from contextlib import contextmanager
from .bindings import libnvpair
from .ctypes import _type_to_suffix

_ffi = libnvpair.ffi
_lib = libnvpair.lib


def nv_call(func, *args):
    """
    Call func that must be a CFFI C function object with arguments
    specified in args while converting between python dictionary
    objects and nvlist_t C parameters.
    Wherever the C function expects an nvlist_t * parameter the
    corresponding python dictionary argument is converted to an nvlist
    and a pointer to it is passed to the function.
    Wherever the C function expects an nvlist_t ** parameter an nvlist_t
    pointer is allocated and passed to the function.  After the function
    returns the nvlist data is converted to the corresponding python
    dictionary argument.
    """
    new_args = []
    nvpps = []
    for (arg, arg_info) in zip(args, _ffi.typeof(func).args):
        if arg_info.cname == 'nvlist_t *':
            nvpp = _ffi.new("nvlist_t **")
            res = _lib.nvlist_alloc(nvpp, 1, 0) # UNIQUE_NAME == 1
            if res != 0:
                raise MemoryError('nvlist_alloc failed')
            nvp = _ffi.gc(nvpp[0], _lib.nvlist_free)
            _dict_to_nvlist(arg, nvp)
            new_args.append(nvp)
        elif arg_info.cname == 'nvlist_t * *':
            nvpp = _ffi.new("nvlist_t **")
            nvpps.append((nvpp, arg))
            new_args.append(nvpp)
        else:
            new_args.append(arg)

    ret = func(*new_args)

    # associate a destructor with each nvlist_t produced by 'func'
    nvpp_refs = []
    for nvpp, x in nvpps:
        nvpp_refs.append(_ffi.gc(nvpp[0], _lib.nvlist_free))

    # convert data from nvlists to the respective dictionaries
    for (nvpp, out_dict) in nvpps:
        _nvlist_to_dict(nvpp[0], out_dict)

    return ret


def nv_wrap(func):
    """
    This a higher order function that produces a function that calls
    func while performing transformations described in nv_call()
    before and after the call.
    """
    func_type = _ffi.typeof(func)
    args_info = func_type.args

    def _func(*args):
        new_args = []
        nvpps = []
        for (arg, arg_info) in zip(args, args_info):
            if arg_info.cname == 'nvlist_t *':
                nvp = _nvlist_alloc()
                _dict_to_nvlist(arg, nvp)
                new_args.append(nvp)
            elif arg_info.cname == 'nvlist_t * *':
                nvpp = _ffi.new("nvlist_t **")
                nvpps.append((nvpp, arg))
                new_args.append(nvpp)
            else:
                new_args.append(arg)

        ret = func(*new_args)

        # associate a destructor with each nvlist_t produced by 'func'
        nvpp_refs = []
        for nvpp, x in nvpps:
            nvpp_refs.append(_ffi.gc(nvpp[0], _lib.nvlist_free))

        # convert data from nvlists to the respective dictionaries
        for (nvpp, out_dict) in nvpps:
            _nvlist_to_dict(nvpp[0], out_dict)

        return ret

    _func.__name__ = func_type.cname + ' wrapper'
    return _func


_TypeInfo = namedtuple('_TypeInfo', ['suffix', 'ctype', 'convert'])

def _type_info(typeid):
    return {
        _lib.DATA_TYPE_BOOLEAN:         _TypeInfo(None, None, None),
        _lib.DATA_TYPE_BOOLEAN_VALUE:   _TypeInfo("boolean_value", "boolean_t *", bool),
        _lib.DATA_TYPE_BYTE:            _TypeInfo("byte", "uchar_t *", int),
        _lib.DATA_TYPE_INT8:            _TypeInfo("int8", "int8_t *", int),
        _lib.DATA_TYPE_UINT8:           _TypeInfo("uint8", "uint8_t *", int),
        _lib.DATA_TYPE_INT16:           _TypeInfo("int16", "int16_t *", int),
        _lib.DATA_TYPE_UINT16:          _TypeInfo("uint16", "uint16_t *", int),
        _lib.DATA_TYPE_INT32:           _TypeInfo("int32", "int32_t *", int),
        _lib.DATA_TYPE_UINT32:          _TypeInfo("uint32", "uint32_t *", int),
        _lib.DATA_TYPE_INT64:           _TypeInfo("int64", "int64_t *", int),
        _lib.DATA_TYPE_UINT64:          _TypeInfo("uint64", "uint64_t *", int),
        _lib.DATA_TYPE_STRING:          _TypeInfo("string", "char **", _ffi.string),
        _lib.DATA_TYPE_NVLIST:          _TypeInfo("nvlist", "nvlist_t **", lambda x: _nvlist_to_dict(x, {})),
        _lib.DATA_TYPE_BOOLEAN_ARRAY:   _TypeInfo("boolean_array", "boolean_t **", bool),
        _lib.DATA_TYPE_BYTE_ARRAY:      _TypeInfo("byte_array", "uchar_t **", int),            # XXX use bytearray ?
        _lib.DATA_TYPE_INT8_ARRAY:      _TypeInfo("int8_array", "int8_t **", int),
        _lib.DATA_TYPE_UINT8_ARRAY:     _TypeInfo("uint8_array", "uint8_t **", int),
        _lib.DATA_TYPE_INT16_ARRAY:     _TypeInfo("int16_array", "int16_t **", int),
        _lib.DATA_TYPE_UINT16_ARRAY:    _TypeInfo("uint16_array", "uint16_t **", int),
        _lib.DATA_TYPE_INT32_ARRAY:     _TypeInfo("int32_array", "int32_t **", int),
        _lib.DATA_TYPE_UINT32_ARRAY:    _TypeInfo("uint32_array", "uint32_t **", int),
        _lib.DATA_TYPE_INT64_ARRAY:     _TypeInfo("int64_array", "int64_t **", int),
        _lib.DATA_TYPE_UINT64_ARRAY:    _TypeInfo("uint64_array", "uint64_t **", int),
        _lib.DATA_TYPE_STRING_ARRAY:    _TypeInfo("string_array", "char ***", _ffi.string),
        _lib.DATA_TYPE_NVLIST_ARRAY:    _TypeInfo("nvlist_array", "nvlist_t ***", lambda x: _nvlist_to_dict(x, {})),
    }[typeid]

# only integer properties need to be here
_prop_name_to_type_str = {
    "rewind-request":   "uint32",
    "type":             "uint32",
    "N_MORE_ERRORS":    "int32",
    "pool_context":     "int32",
}


def _nvlist_alloc():
    nvpp = _ffi.new("nvlist_t **")
    res = _lib.nvlist_alloc(nvpp, 1, 0) # UNIQUE_NAME == 1
    if res != 0:
        raise MemoryError('nvlist_alloc failed')
    return _ffi.gc(nvpp[0], _lib.nvlist_free)


def _nvlist_add_array(nvlist, key, array):
    ret = 0
    specimen = array[0]

    is_integer = isinstance(specimen, numbers.Integral)
    for i in range(1, len(array)):
        if is_integer and isinstance(array[i], numbers.Integral):
            pass
        elif type(array[i]) is not type(specimen):
            raise TypeError('Array has elements of different types: ' +
                type(specimen).__name__ +
                ' and ' +
                type(array[i]).__name__)

    if isinstance(specimen, dict):
        c_array = []
        for dictionary in array:
            nvlistp = _ffi.new('nvlist_t **')
            res = _lib.nvlist_alloc(nvlistp, 1, 0) # UNIQUE_NAME == 1
            if res != 0:
                raise MemoryError('nvlist_alloc failed')
            nested_nvlist = _ffi.gc(nvlistp[0], _lib.nvlist_free)
            _dict_to_nvlist(dictionary, nested_nvlist)
            c_array.append(nested_nvlist)
        ret = _lib.nvlist_add_nvlist_array(nvlist, key, c_array, len(c_array))
    elif isinstance(specimen, str):
        c_array = []
        for string in array:
            c_array.append(_ffi.new('char[]', string))
        ret = _lib.nvlist_add_string_array(nvlist, key, c_array, len(c_array))
    elif isinstance(specimen, bool):
        ret = _lib.nvlist_add_boolean_array(nvlist, key, array, len(array))
    elif isinstance(specimen, numbers.Integral):
        suffix = _prop_name_to_type_str.get(key, "uint64")
        cfunc = getattr(_lib, "nvlist_add_%s_array" % (suffix,))
        ret = cfunc(nvlist, key, array, len(array))
    elif isinstance(specimen, _ffi.CData) and _ffi.typeof(specimen) in _type_to_suffix:
        suffix = _type_to_suffix[_ffi.typeof(specimen)]
        cfunc = getattr(_lib, "nvlist_add_%s_array" % (suffix,))
        ret = cfunc(nvlist, key, array, len(array))
    else:
        raise TypeError('Unsupported value type ' + type(specimen).__name__)
    if ret != 0:
        raise MemoryError('nvlist_add failed, err = %d' % ret)


def _nvlist_to_dict(nvlist, props):
    pair = _lib.nvlist_next_nvpair(nvlist, _ffi.NULL)
    while pair != _ffi.NULL:
        name = _ffi.string(_lib.nvpair_name(pair))
        typeid = int(_lib.nvpair_type(pair))
        typeinfo = _type_info(typeid)
        is_array = bool(_lib.nvpair_type_is_array(pair))
        cfunc = getattr(_lib, "nvpair_value_%s" % (typeinfo.suffix,), None)
        val = None
        ret = 0
        if is_array:
            valptr = _ffi.new(typeinfo.ctype)
            lenptr = _ffi.new("uint_t *")
            ret = cfunc(pair, valptr, lenptr)
            if ret != 0:
                raise RuntimeError('nvpair_value failed')
            length = int(lenptr[0])
            val = []
            for i in range(length):
                val.append(typeinfo.convert(valptr[0][i]))
        else:
            if typeid == _lib.DATA_TYPE_BOOLEAN:
                val = None # XXX or should it be True ?
            else:
                valptr = _ffi.new(typeinfo.ctype)
                ret = cfunc(pair, valptr)
                if ret != 0:
                    raise RuntimeError('nvpair_value failed')
                val = typeinfo.convert(valptr[0])
        props[name] = val
        pair = _lib.nvlist_next_nvpair(nvlist, pair)
    return props


def _dict_to_nvlist(props, nvlist):
    for k, v in props.items():
        if not isinstance(k, str):
            raise TypeError('Unsupported key type ' + type(k).__name__)
        ret = 0
        if isinstance(v, dict):
            sub_nvlist = _nvlist_alloc()
            _dict_to_nvlist(v, sub_nvlist)
            ret = _lib.nvlist_add_nvlist(nvlist, k, sub_nvlist)
        elif isinstance(v, list):
            _nvlist_add_array(nvlist, k, v)
        elif isinstance(v, str):
            ret = _lib.nvlist_add_string(nvlist, k, v)
        elif isinstance(v, bool):
            ret = _lib.nvlist_add_boolean_value(nvlist, k, v)
        elif v is None:
            ret = _lib.nvlist_add_boolean(nvlist, k)
        elif isinstance(v, numbers.Integral):
            suffix = _prop_name_to_type_str.get(k, "uint64")
            cfunc = getattr(_lib, "nvlist_add_%s" % (suffix,))
            ret = cfunc(nvlist, k, v)
        elif isinstance(v, _ffi.CData) and _ffi.typeof(v) in _type_to_suffix:
            suffix = _type_to_suffix[_ffi.typeof(v)]
            cfunc = getattr(_lib, "nvlist_add_%s" % (suffix,))
            ret = cfunc(nvlist, k, v)
        else:
            raise TypeError('Unsupported value type ' + type(v).__name__)
        if ret != 0:
            raise MemoryError('nvlist_add failed')


# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
