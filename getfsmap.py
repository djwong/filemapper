#!/usr/bin/env python3
# Python wrapper of GETFSMAP ioctls.
# Copyright (C) 2017 Darrick J. Wong.  All rights reserved.
# Licensed under the GPLv2.

import struct
import collections
import ioctl
import fcntl
import array

# Derived from linux/fsmap.h
_struct_fsmap_string = 'LLQQQQQQQ'
_struct_fsmap = struct.Struct('=' + _struct_fsmap_string)
_struct_fsmap_head_string = 'LLLLQQQQQQ' + (2 * _struct_fsmap_string)
_struct_fsmap_head = struct.Struct('=' + _struct_fsmap_head_string)

FMH_IF_VALID = 0
FMH_OF_DEV_T = 0x1

FMR_OF_PREALLOC = 0x1
FMR_OF_ATTR_FORK = 0x2
FMR_OF_EXTENT_MAP = 0x4
FMR_OF_SHARED = 0x8
FMR_OF_SPECIAL_OWNER = 0x10
FMR_OF_LAST = 0x20

def FMR_OWNER(fstype, code):
	return (fstype << 32) | (code & 0xFFFFFFFF)
def FMR_OWNER_TYPE(owner):
	return owner >> 32
def FMR_OWNER_CODE(owner):
	return owner & 0xFFFFFFFF

FMR_OWN_FREE = FMR_OWNER(0, 1)
FMR_OWN_UNKNOWN = FMR_OWNER(0, 2)
FMR_OWN_METADATA = FMR_OWNER(0, 3)

_FS_IOC_GETFSMAP = ioctl._IOWR(ord('X'), 59, _struct_fsmap_head)

fsmap_key = collections.namedtuple('fsmap_key',
		'device flags physical owner offset')
fsmap_rec = collections.namedtuple('fsmap_rec',
		'device flags physical owner offset length hdr_flags')

def getfsmap(fd, getfsmap_keys = None, count = 10000):
	'''Iterable GETFSMAP generator...'''
	length = 0

	# Prepare keys
	try:
		key0 = getfsmap_keys[0]
	except:
		key0 = fsmap_key(0, 0, 0, 0, 0)
	try:
		key1 = getfsmap_keys[1]
	except:
		key1 = fsmap_key(ioctl._UINT32_MAX, ioctl._UINT32_MAX, \
				ioctl._UINT64_MAX, ioctl._UINT64_MAX, \
				ioctl._UINT64_MAX)

	while True:
		hdr = _struct_fsmap_head.pack(0, 0, count, 0, 0, 0, 0, 0, 0, 0, \
				key0.device, key0.flags, key0.physical, \
				key0.owner, key0.offset, length, 0, 0, 0, \
				key1.device, key1.flags, key1.physical, \
				key1.owner, key1.offset, 0, 0, 0, 0)
		buf = bytearray(hdr) + bytearray(_struct_fsmap.size * count)
		try:
			ret = fcntl.ioctl(fd, _FS_IOC_GETFSMAP, buf)
		except TypeError:
			# Turn into mutable C-level array of chars
			s = '%s%s' % (hdr, '\0' * (_struct_fsmap.size * count))
			buf = array.array('c', s)
			ret = fcntl.ioctl(fd, _FS_IOC_GETFSMAP, buf)
		if ret < 0:
			raise IOError('GETFSMAP')

		meh = _struct_fsmap_head.unpack_from(buf)
		oflags = meh[1]
		entries = meh[3]
		if entries == 0:
			return

		bufsz = _struct_fsmap_head.size + (_struct_fsmap.size * entries)
		assert len(buf) >= bufsz
		for offset in range(_struct_fsmap_head.size, bufsz, _struct_fsmap.size):
			x = _struct_fsmap.unpack_from(buf, offset)
			rec = fsmap_rec(x[0], x[1], x[2], x[3], x[4], x[5], oflags)
			yield rec

		if rec.flags & FMR_OF_LAST:
			return
		key0 = rec
		length = rec.length

XFS_FMR_OWN_TYPE	= ord('X')
XFS_FMR_OWN_FS		= FMR_OWNER(XFS_FMR_OWN_TYPE, 1)
XFS_FMR_OWN_LOG		= FMR_OWNER(XFS_FMR_OWN_TYPE, 2)
XFS_FMR_OWN_AG		= FMR_OWNER(XFS_FMR_OWN_TYPE, 3)
XFS_FMR_OWN_INOBT	= FMR_OWNER(XFS_FMR_OWN_TYPE, 4)
XFS_FMR_OWN_INODES	= FMR_OWNER(XFS_FMR_OWN_TYPE, 5)
XFS_FMR_OWN_REFC	= FMR_OWNER(XFS_FMR_OWN_TYPE, 6)
XFS_FMR_OWN_COW		= FMR_OWNER(XFS_FMR_OWN_TYPE, 7)
XFS_FMR_OWN_DEFECTIVE	= FMR_OWNER(XFS_FMR_OWN_TYPE, 8)

special_owner_types = {
	XFS_FMR_OWN_TYPE:	'XFS',
}

special_owner_codes = {
	XFS_FMR_OWN_FS:		'FS data',
	XFS_FMR_OWN_LOG:	'log',
	XFS_FMR_OWN_AG:		'AG data',
	XFS_FMR_OWN_INOBT:	'inode btree',
	XFS_FMR_OWN_INODES:	'inodes',
	XFS_FMR_OWN_REFC:	'refcount btree',
	XFS_FMR_OWN_COW:	'CoW staging',
	XFS_FMR_OWN_DEFECTIVE:	'defective',
}

def special_owner_name(owner):
	'''Formulate a name for a special owner.'''
	t = FMR_OWNER_TYPE(owner)
	c = FMR_OWNER_CODE(owner)
	if owner in special_owner_codes:
		return '%s:%s' % (special_owner_types[t], \
				special_owner_codes[owner])
	return '%d:%d' % (t, c)

if __name__ == '__main__':
	import sys
	import pprint

	if len(sys.argv) < 2:
		sys.stderr.write('No filename(s) given\n')
		sys.exit(1)

	for file_ in sys.argv[1:]:
		with open(file_, 'r') as fd:
			for fmr in getfsmap(fd):
				pprint.pprint(fmr)
