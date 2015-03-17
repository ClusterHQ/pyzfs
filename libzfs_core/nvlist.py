from .bindings import c_libnvpair, ffi_libnvpair, data_type

class NVList(object):
	def __init__(self, props):
		self.props = props

	def get_props(self)
		return self.props:

	@classmethod
	def from_c_ptr(cls, ptr):
		nvlist = cls({})
		nvlist._extract_props(ptr)
		return nvlist

	def get_c_ptr(self):
		ptrptr = ffi_libnvpair.new('nvlist_t **')
		# UNIQUE_NAME == 1
		ptr = c_libnvpair.nvlist_alloc(ptrptr, 1, 0)
		self._populate_nvlist(ptr, self.props)
		return ptr

	def _populate_nvlist(self, arg=None, **kwargs):
		if arg:
			if hasattr(arg, 'keys'):
				for k in arg:
					self._add(k, arg[k])
			else:
				for k, v in arg:
					self._add(k, v)
		for k, v in kwargs.items():
			self._add(k, v)

	def _add(self, key, type, value):
		info = self.info_for_type(type)
		value = info.convert_add(value)
		return not bool(info.nvlist_add(self.ptr, key, value))

	def _extract_props(self, ptr):
		data = {}
		pair = c_libnvpair.nvlist_next_nvpair(ptr, ffi_libnvpair.NULL)
		while pair != ffi_libnvpair.NULL:
			name = ffi_libnvpair.string(c_libnvpair.nvpair_name(pair))
			typeid = c_libnvpair.nvpair_type(pair)
			try:
				dt = data_type(typeid)
				info = self.info_for_type(dt)
			except (ValueError, UnknownValue):
				if not skip_unknown:
					raise UnknownValue("Unknown type: '%r'" % typeid)
				else:
					try:
						dt = data_type(typeid)
					except:
						dt = (None, typeid, None)
					data[name] = dt
					pair = c_libnvpair.nvlist_next_nvpair(self.ptr, pair)
					continue
			valholder = info.create_holder()
			countholder = None
			if info.is_array:
				countholder = info.create_count_holder()
				val = info.nvpair_value(pair, valholder, countholder)
			else:
				val = info.nvpair_value(pair, valholder)
			if not bool(val):
				value = info.convert(valholder, countholder)
				if deep and isinstance(value, NVList):
					value._free = self._free
					with value:
						data[name] = value.to_dict(skip_unknown = skip_unknown)
				elif deep and isinstance(value, list) and isinstance(value[0], NVList):
					temp = data[name] = []
					for item in value:
						item._free = self._free
						with item:
							temp.append(item.to_dict(skip_unknown = skip_unknown))
				else:
					data[name] = value

			pair = c_libnvpair.nvlist_next_nvpair(self.ptr, pair)
		return data

	@classmethod
	def info_for_type(cls, type):
		info = NVLIST_HANDLERS.get(type)
		if info is None:
			raise UnknownValue("Unknown type: '%r'" % type)
		return info


def _to_int(hdl):
	if isinstance(hdl, (int, long)):
		return int(hdl)
	return int(hdl[0])


def _to_long(hdl):
	if isinstance(hdl, (int, long)):
		return long(hdl)
	return long(hdl[0])


class NVListHandler(object):
	def __init__(self, funcname, typename, converter, add_converter = None, is_array = False):
		self._funcname = funcname
		self._typename = typename
		self._converter = converter
		self._add_converter = add_converter
		self._is_array = is_array

	def create_holder(self):
		return ffi_libnvpair.new(self._typename)

	def create_count_holder(self):
		return ffi_libnvpair.new('uint_t *')

	def convert(self, x, count = None):
		if self._converter:
			if self.is_array:
				return self._converter(x, count)
			return self._converter(x)
		return x

	def convert_add(self, x):
		if callable(self._add_converter):
			return self._add_converter(x)
		if self._add_converter is False:
			raise Exception("Unable to convert type")
		return x

	def _get_c_func(self, prefix):
		return getattr(c_libnvpair, '%s_%s' % (prefix, self._funcname))

	@property
	def nvlist_add(self):
		return self._get_c_func('nvlist_add')

	@property
	def nvlist_lookup(self):
		return self._get_c_func('nvlist_lookup')

	@property
	def nvpair_value(self):
		return self._get_c_func('nvpair_value')

	@property
	def is_array(self):
		return self._is_array


def _array_converter(converter):
	def _inner(x, count):
		items = []
		for i in range(count[0]):
			items.append(converter(x[0][i]))
		return items
	return _inner


#
# Key: configuration
#  - add func
#  - lookup func
#  - lookup holder type
#  - add converter
#  - lookup converter
#
NVLIST_HANDLERS = {
	data_type.BOOLEAN:	 NVListHandler('boolean_value', 'boolean_t *', lambda x: bool(x[0]), boolean_t),
	data_type.BOOLEAN_VALUE: NVListHandler('boolean_value', 'boolean_t *', lambda x: bool(x[0]), boolean_t),
	data_type.BYTE:		NVListHandler('byte', 'uchar_t *', _to_int, None),
	data_type.INT8:		NVListHandler('int8', 'int8_t *', _to_int, None),
	data_type.UINT8:	NVListHandler('uint8', 'uint8_t *', _to_int, None),
	data_type.INT16:	NVListHandler('int16', 'int16_t *', _to_int, None),
	data_type.UINT16:	NVListHandler('uint16', 'uint16_t *', _to_int, None),
	data_type.INT32:	NVListHandler('int32', 'int32_t *', _to_int, None),
	data_type.UINT32:	NVListHandler('uint32', 'uint32_t *', _to_int, None),
	data_type.INT64:	NVListHandler('int64', 'int64_t *', _to_int, None),
	data_type.UINT64:	NVListHandler('uint64', 'uint64_t *', _to_int, None),
	data_type.STRING:	NVListHandler('string', 'char **', lambda x: ffi_libnvpair.string(x[0]), None),
	data_type.NVLIST:	NVListHandler('nvlist', 'nvlist_t **', NVList.from_nvlist_handle, False),

	data_type.BYTE_ARRAY:   NVListHandler('byte_array', 'uchar_t **', _array_converter(_to_int), None),
	data_type.INT8_ARRAY:   NVListHandler('int8_array', 'int8_t **', _array_converter(_to_int), False, True),
	data_type.UINT8_ARRAY:  NVListHandler('uint8_array', 'uint8_t **', _array_converter(_to_int), False, True),
	data_type.INT16_ARRAY:  NVListHandler('int16_array', 'int16_t **', _array_converter(_to_int), False, True),
	data_type.UINT16_ARRAY: NVListHandler('uint16_array', 'uint16_t **', _array_converter(_to_int), False, True),
	data_type.INT32_ARRAY:  NVListHandler('int32_array', 'int32_t **', _array_converter(_to_int), False, True),
	data_type.UINT32_ARRAY: NVListHandler('uint32_array', 'uint32_t **', _array_converter(_to_int), False, True),
	data_type.INT64_ARRAY:  NVListHandler('int64_array', 'int64_t **', _array_converter(_to_int), False, True),
	data_type.UINT64_ARRAY: NVListHandler('uint64_array', 'uint64_t **', _array_converter(_to_int), False, True),
	data_type.NVLIST_ARRAY: NVListHandler('nvlist_array', 'nvlist_t ***',
										_array_converter(NVList.from_nvlist_ptr), False, True),
	data_type.STRING_ARRAY: NVListHandler('string_array', 'char ***', 
										_array_converter(lambda x: ffi_libnvpair.string(x)), False, True),
}
