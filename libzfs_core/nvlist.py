"""
nvlist_in and nvlist_out provide support for converting between
a dictionary on the Python side and an nvlist_t on the C side
with the automatic memory management for C memory allocations.

nvlist_in and nvlist_out are to be used with the 'with' statement.

nvlist_in takes a dictionary and produces a CData object corresponding
to a C nvlist_t pointer suitable for passing as an input parameter.
The nvlist_t is populated based on the dictionary.

nvlist_out takes a dictionary and produces a CData object corresponding
to a C nvlist_t pointer to pointer suitable for passing as an output parameter.
Upon exit from a with-block the dictionary is populated based on the nvlist_t.

The dictionary must follow a certain format to be convertible
to the nvlist_t.  The dictionary produced from the nvlist_t
will follow the same format.

Format:
- keys are always strings
- a value can be None in which case it represents boolean truth by its mere presence
- a value can be a bool
- a value can be a string
- a value can be an integer
- a value can be a CFFI CData object representing one of the following C types:
    int8_t, uint8_t, int16_t, uint16_t, int32_t, uint32_t, int64_t, uint64_t, boolean_t, uchar_t
- a value can be a dictionary that recursively adheres to this format
- a value can be a list of bools, strings, integers or CData objects of types specified above
- a value can be a list of dictionaries that adhere to this format
- all elements of a list value must be of the same type
"""

import numbers
from collections import namedtuple
from contextlib import contextmanager
from .bindings import libnvpair

_ffi = libnvpair.ffi
_lib = libnvpair.lib

def _ffi_cast(type_name):
	def _func(value):
		# this is for overflow / underflow checking only
		_ffi.new(type_name + '*', value)
		return _ffi.cast(type_name, value)
	_func.__name__ = type_name
	return _func

# Utility functions for casting to a specific C type
uint8_t =	_ffi_cast('uint8_t')
int8_t =	_ffi_cast('int8_t')
uint16_t =	_ffi_cast('uint16_t')
int16_t =	_ffi_cast('int16_t')
uint32_t =	_ffi_cast('uint32_t')
int32_t =	_ffi_cast('int32_t')
uint64_t =	_ffi_cast('uint64_t')
int64_t =	_ffi_cast('int64_t')
boolean_t =	_ffi_cast('boolean_t')
uchar_t =	_ffi_cast('uchar_t')

_type_to_suffix = {
	_ffi.typeof('uint8_t'):		'uint8',
	_ffi.typeof('int8_t'):		'int8',
	_ffi.typeof('uint16_t'):	'uint16',
	_ffi.typeof('int16_t'):		'int16',
	_ffi.typeof('uint32_t'):	'uint32',
	_ffi.typeof('int32_t'):		'int32',
	_ffi.typeof('uint64_t'):	'uint64',
	_ffi.typeof('int64_t'):		'int64',
	_ffi.typeof('boolean_t'):	'boolean',
	_ffi.typeof('uchar_t'):		'byte',
}

_TypeInfo = namedtuple('_TypeInfo', ['suffix', 'ctype', 'convert'])

def _type_info(typeid):
	return {
		_lib.DATA_TYPE_BOOLEAN:		_TypeInfo(None, None, None),
		_lib.DATA_TYPE_BOOLEAN_VALUE:	_TypeInfo("boolean_value", "boolean_t *", bool),
		_lib.DATA_TYPE_BYTE:		_TypeInfo("byte", "uchar_t *", int),
		_lib.DATA_TYPE_INT8:		_TypeInfo("int8", "int8_t *", int),
		_lib.DATA_TYPE_UINT8:		_TypeInfo("uint8", "uint8_t *", int),
		_lib.DATA_TYPE_INT16:		_TypeInfo("int16", "int16_t *", int),
		_lib.DATA_TYPE_UINT16:		_TypeInfo("uint16", "uint16_t *", int),
		_lib.DATA_TYPE_INT32:		_TypeInfo("int32", "int32_t *", int),
		_lib.DATA_TYPE_UINT32:		_TypeInfo("uint32", "uint32_t *", int),
		_lib.DATA_TYPE_INT64:		_TypeInfo("int64", "int64_t *", int),
		_lib.DATA_TYPE_UINT64:		_TypeInfo("uint64", "uint64_t *", int),
		_lib.DATA_TYPE_STRING:		_TypeInfo("string", "char **", _ffi.string),
		_lib.DATA_TYPE_NVLIST:		_TypeInfo("nvlist", "nvlist_t **", lambda x: _nvlist_to_dict(x, {})),
		_lib.DATA_TYPE_BOOLEAN_ARRAY:	_TypeInfo("boolean_array", "boolean_t **", bool),
		_lib.DATA_TYPE_BYTE_ARRAY:	_TypeInfo("byte_array", "uchar_t **", int),			# XXX use bytearray ?
		_lib.DATA_TYPE_INT8_ARRAY:	_TypeInfo("int8_array", "int8_t **", int),
		_lib.DATA_TYPE_UINT8_ARRAY:	_TypeInfo("uint8_array", "uint8_t **", int),
		_lib.DATA_TYPE_INT16_ARRAY:	_TypeInfo("int16_array", "int16_t **", int),
		_lib.DATA_TYPE_UINT16_ARRAY:	_TypeInfo("uint16_array", "uint16_t **", int),
		_lib.DATA_TYPE_INT32_ARRAY:	_TypeInfo("int32_array", "int32_t **", int),
		_lib.DATA_TYPE_UINT32_ARRAY:	_TypeInfo("uint32_array", "uint32_t **", int),
		_lib.DATA_TYPE_INT64_ARRAY:	_TypeInfo("int64_array", "int64_t **", int),
		_lib.DATA_TYPE_UINT64_ARRAY:	_TypeInfo("uint64_array", "uint64_t **", int),
		_lib.DATA_TYPE_STRING_ARRAY:	_TypeInfo("string_array", "char ***", _ffi.string),
		_lib.DATA_TYPE_NVLIST_ARRAY:	_TypeInfo("nvlist_array", "nvlist_t ***", lambda x: _nvlist_to_dict(x, {})),
	}[typeid]

# only integer properties need to be here
_prop_name_to_type_str = {
	"rewind-request":		"uint32",
	"type":				"uint32",
	"N_MORE_ERRORS":		"int32",
	"pool_context":			"int32",
}


def _nvlist_add_array(nvlist, key, array):
	ret = 0
	length = len(array)
	specimen = array[0]

	is_integer = isinstance(specimen, numbers.Integral)
	for i in range(1, length):
		if is_integer and isinstance(array[i], numbers.Integral):
			pass
		elif type(array[i]) is not type(specimen):
			raise TypeError('Array has elements of different types: ' +
				type(specimen).__name__ +
				' and ' +
				type(array[i]).__name__)

	if isinstance(specimen, dict):
		c_array = _ffi.new('nvlist_t *[]', length)
		# NB: can't use automatic memory management via nvlist_in() here,
		# we have a loop, but 'with' would require recursion
		try:
			for i in range(0, length):
				res = _lib.nvlist_alloc(c_array + i, 1, 0) # UNIQUE_NAME == 1
				if res != 0:
					raise MemoryError('nvlist_alloc failed')
				_dict_to_nvlist(array[i], c_array[i])
			ret = _lib.nvlist_add_nvlist_array(nvlist, key, c_array, length)
		finally:
			for i in range(0, length):
				if c_array[i] != _ffi.NULL:
					_lib.nvlist_free(c_array[i])
	elif isinstance(specimen, basestring):
		c_array = []
		for i in range(0, length):
			c_array.append(_ffi.new('char[]', array[i]))
		ret = _lib.nvlist_add_string_array(nvlist, key, c_array, length)
	elif isinstance(specimen, bool):
		ret = _lib.nvlist_add_boolean_array(nvlist, key, array, length)
	elif isinstance(specimen, numbers.Integral):
		suffix = _prop_name_to_type_str.get(key, "uint64")
		cfunc = getattr(_lib, "nvlist_add_%s_array" % (suffix))
		ret = cfunc(nvlist, key, array, length)
	elif isinstance(specimen, _ffi.CData) and _ffi.typeof(specimen) in _type_to_suffix:
		suffix = _type_to_suffix[_ffi.typeof(specimen)]
		cfunc = getattr(_lib, "nvlist_add_%s_array" % (suffix))
		ret = cfunc(nvlist, key, array, length)
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
		cfunc = getattr(_lib, "nvpair_value_%s" % (typeinfo.suffix), None)
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
		if not isinstance(k, basestring):
			raise TypeError('Unsupported key type ' + type(k).__name__)
		ret = 0
		if isinstance(v, dict):
			with nvlist_in(v) as sub_nvlist:
				ret = _lib.nvlist_add_nvlist(nvlist, k, sub_nvlist)
		elif isinstance(v, list):
			_nvlist_add_array(nvlist, k, v)
		elif isinstance(v, basestring):
			ret = _lib.nvlist_add_string(nvlist, k, v)
		elif isinstance(v, bool):
			ret = _lib.nvlist_add_boolean_value(nvlist, k, v)
		elif v is None:
			ret = _lib.nvlist_add_boolean(nvlist, k)
		elif isinstance(v, numbers.Integral):
			suffix = _prop_name_to_type_str.get(k, "uint64")
			cfunc = getattr(_lib, "nvlist_add_%s" % (suffix))
			ret = cfunc(nvlist, k, v)
		elif isinstance(v, _ffi.CData) and _ffi.typeof(v) in _type_to_suffix:
			suffix = _type_to_suffix[_ffi.typeof(v)]
			cfunc = getattr(_lib, "nvlist_add_%s" % (suffix))
			ret = cfunc(nvlist, k, v)
		else:
			raise TypeError('Unsupported value type ' + type(v).__name__)
		if ret != 0:
			raise MemoryError('nvlist_add failed')


@contextmanager
def nvlist_in(props):
	nvlistp = _ffi.new("nvlist_t **")
	res = _lib.nvlist_alloc(nvlistp, 1, 0) # UNIQUE_NAME == 1
	if res != 0:
		raise MemoryError('nvlist_alloc failed')
	nvlist = nvlistp[0]
	_dict_to_nvlist(props, nvlist)
	try:
		yield nvlist
	finally:
		_lib.nvlist_free(nvlist)


@contextmanager
def nvlist_out(props):
	nvlistp = _ffi.new("nvlist_t **")
	nvlistp[0] = _ffi.NULL # to be sure
	try:
		yield nvlistp
		# clear old entries, if any
		props.clear()
		_nvlist_to_dict(nvlistp[0], props)
	finally:
		if (nvlistp[0] != _ffi.NULL):
			_lib.nvlist_free(nvlistp[0])


