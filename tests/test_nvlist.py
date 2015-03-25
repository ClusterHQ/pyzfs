import json
from libzfs_core.nvlist import *
from libzfs_core.nvlist import _lib

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

	]
}

props_out = {}

with nvlist_in(props_in) as x:
	print "Dumping a C nvlist_t produced from a python dictionary:"
	_lib.dump_nvlist(x, 2)

	with nvlist_out(props_out) as y:
		_lib.nvlist_dup(x, y, 0)
	print "\n\n"
	print "Dumping a dictionary reconstructed from the nvlist_t:"
	print json.dumps(props_out, sort_keys=True, indent=4)

