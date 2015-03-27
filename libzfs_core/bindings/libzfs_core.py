CDEF = """
    int libzfs_core_init(void);
    void libzfs_core_fini(void);

    typedef ... nvlist_t;
    typedef enum { B_FALSE, B_TRUE } boolean_t;

    int lzc_snapshot(nvlist_t *, nvlist_t *, nvlist_t **);
    int lzc_create(const char *, boolean_t, nvlist_t *);
    int lzc_clone(const char *, const char *, nvlist_t *);
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

SOURCE = """
#include <libzfs/libzfs_core.h>
"""

LIBRARY = "zfs_core"
LIBDEPS = [ "zfs" ]

# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
