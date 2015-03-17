import errno
from .bindings import libzfs_core
from .nvlist import NVList

_ffi = libzfs_core._ffi
_lib = libzfs_core._lib

_lib.libzfs_core_init()

def lzc_create(name, is_zvol, props):
	nvlist = NVList(props).get_c_ptr()
	ret = _lib.lzc_create(name, is_zvol, nvlist)
	NVList.free_c_ptr(nvlist)
	return ret

def lzc_snapshot(snaps, props):
	snaps = NVList(snaps).get_c_ptr()
	props = NVList(props).get_c_ptr()
	errlist = NVList.alloc_c_ptr()
	ret = _lib.lzc_snapshot(snaps, props, errlist)
	NVList.free_c_ptr(snaps)
	NVList.free_c_ptr(props)
	errlist = NVList.from_c_ptr(errlist[0]).get_props()
	return (ret, errlist)

def lzc_promote(name):
	return _lib.lzc_promote(name)

def lzc_set_props(name, props, received)
	

"""
	int lzc_promote(const char *);
	int lzc_set_props(const char *, nvlist_t *, boolean_t);
	int lzc_destroy_snaps(nvlist_t *, boolean_t, nvlist_t **);
	int lzc_bookmark(nvlist_t *, nvlist_t **);
	int lzc_get_bookmarks(const char *, nvlist_t *, nvlist_t **);
	int lzc_destroy_bookmarks(nvlist_t *, nvlist_t **);

	int lzc_snaprange_space(const char *, const char *, uint64_t *);

	int lzc_hold(nvlist_t *, int, nvlist_t **);
	int lzc_release(nvlist_t *, nvlist_t **);
	int lzc_get_holds(const char *, nvlist_t **);
	    
	enum lzc_send_flags { ... };  
	    
	int lzc_send(const char *, const char *, int, enum lzc_send_flags);
	int lzc_send_ext(const char *, const char *, int, nvlist_t *);
	int lzc_receive(const char *, nvlist_t *, const char *, boolean_t, int);
	int lzc_send_space(const char *, const char *, uint64_t *);
	int lzc_send_progress(const char *, int, uint64_t *);

	boolean_t lzc_exists(const char *);

	int lzc_rollback(const char *, char *, int);
"""

