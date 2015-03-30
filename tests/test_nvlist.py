import json
from libzfs_core._nvlist import nv_call, nv_wrap
from libzfs_core._nvlist import _lib
from libzfs_core.ctypes import uint32_t

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
    "type": 2 ** 32 - 1,
    "pool_context": -(2 ** 31)
}

props_out = {}

nvlist_dup = nv_wrap(_lib.nvlist_dup)

print "Dumping a C nvlist_t produced from a python dictionary:"
print "(ignore 'bad config type 24' message)"
ret = nv_call(_lib.dump_nvlist, props_in, 2)

print "\n\n"
print "Dumping a dictionary reconstructed from the nvlist_t:"
ret = nvlist_dup(props_in, props_out, 0)
print json.dumps(props_out, sort_keys=True, indent=4)

# vim: softtabstop=4 tabstop=4 expandtab shiftwidth=4
