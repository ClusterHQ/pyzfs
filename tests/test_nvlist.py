import json
from libzfs_core._nvlist import nvlist_in, nvlist_out
from libzfs_core._nvlist import _lib
from libzfs_core.ctypes import uint32_t, boolean_t, uchar_t

props_in = {
    "key1": "str",
    "key2": 10,
    "key3": {
        "skey1": True,
        "skey2": None,
        "skey3": [
            True,
            False,
            True
        ]
    },
    "key4": [
        "ab",
        "bc"
    ],
    "key5": [
        2 ** 64 - 1,
        1,
        2,
        3
    ],
    "key6": [
        uint32_t(10),
        uint32_t(11)
    ],
    "key7": [
        {
            "skey71": "a",
            "skey72": "b",
        },
        {
            "skey71": "c",
            "skey72": "d",
        },
        {
            "skey71": "e",
            "skey72": "f",
        }

    ],
    "key8": [
        boolean_t(True),
        boolean_t(False)
    ],
    "key9": [
        uchar_t(0xa5),
        uchar_t(0x5a)
    ],
    "type": 2 ** 32 - 1,
    "pool_context": -(2 ** 31)
}

props_out = {}

with nvlist_in(props_in) as x:
    print "Dumping a C nvlist_t produced from a python dictionary:"
    print "(ignore 'bad config type 24' message)"
    _lib.dump_nvlist(x, 2)

    with nvlist_out(props_out) as y:
        _lib.nvlist_dup(x, y, 0)
    print "\n\n"
    print "Dumping a dictionary reconstructed from the nvlist_t:"
    print json.dumps(props_out, sort_keys=True, indent=4)

# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
