#!/usr/bin/env python3
# Python wrapper of FIEMAP/FIBMAP ioctls.
# Copyright (C) 2015 Darrick J. Wong.  All rights reserved.
# Licensed under the GPLv2.

import array
import fcntl
import struct
import collections
import os
import errno
import stat
import itertools
import ioctl

# From linux/fiemap.h
FIEMAP_FLAG_SYNC = 0x0001
FIEMAP_FLAG_XATTR = 0x0002
FIEMAP_FLAGS_COMPAT = FIEMAP_FLAG_SYNC | FIEMAP_FLAG_XATTR
# Bogus flag added by djwong
FIEMAP_FLAG_FORCE_FIBMAP = 0x8000000

FIEMAP_EXTENT_LAST = 0x0001
FIEMAP_EXTENT_UNKNOWN = 0x0002
FIEMAP_EXTENT_DELALLOC = 0x0004
FIEMAP_EXTENT_ENCODED = 0x0008
FIEMAP_EXTENT_DATA_ENCRYPTED = 0x0080
FIEMAP_EXTENT_NOT_ALIGNED = 0x0100
FIEMAP_EXTENT_DATA_INLINE = 0x0200
FIEMAP_EXTENT_DATA_TAIL = 0x0400
FIEMAP_EXTENT_UNWRITTEN = 0x0800
FIEMAP_EXTENT_MERGED = 0x1000
FIEMAP_EXTENT_SHARED = 0x2000

# Bogus flag added by djwong
FIEMAP_EXTENT_HOLE = 0x8000

# Derived from linux/fiemap.h
_struct_fiemap = struct.Struct('=QQLLLL')
_struct_fiemap_extent = struct.Struct('=QQQQQLLLL')
_struct_fibmap = struct.Struct('=L')

# From linux/fs.h
_FS_IOC_FIEMAP = ioctl._IOWR(ord('f'), 11, _struct_fiemap)
_FIBMAP = ioctl._IO(0x00, 1)

fiemap_rec = collections.namedtuple('fiemap_rec',
		'logical physical length flags hdr_flags')

MAX_EXTENT_LENGTH = 2**60

def fiemap2(fd, start = 0, length = ioctl._UINT64_MAX, flags = 0, count = 10000):
	while length > 0:
		hdr = _struct_fiemap.pack(start, length, flags, 0, count, 0)
		try:
			buf = bytearray(hdr) + bytearray(_struct_fiemap_extent.size * count)
			ret = fcntl.ioctl(fd, _FS_IOC_FIEMAP, buf)
		except TypeError:
			# Turn into mutable C-level array of chars
			s = '%s%s' % (hdr, '\0' * (_struct_fiemap_extent.size * count))
			buf = array.array('c', s)
			ret = fcntl.ioctl(fd, _FS_IOC_FIEMAP, buf)

		if ret < 0:
			raise IOError('FIEMAP')

		meh = _struct_fiemap.unpack_from(buf)
		oflags = meh[2]
		entries = meh[3]
		if entries == 0:
			return

		bufsz = _struct_fiemap.size + (_struct_fiemap_extent.size * entries)
		assert len(buf) >= bufsz
		for offset in range(_struct_fiemap.size, bufsz, _struct_fiemap_extent.size):
			x = _struct_fiemap_extent.unpack_from(buf, offset)
			rec = fiemap_rec(x[0], x[1], x[2], x[5], oflags)
			yield rec

		if rec.flags & FIEMAP_EXTENT_LAST:
			return
		length -= rec.length
		start += rec.length

def fibmap2(fd, start = 0, end = None, flags = 0):
	xstat = os.fstat(fd)
	if not stat.S_ISREG(xstat.st_mode):
		return
	if flags & FIEMAP_FLAG_XATTR:
		return

	if end is None:
		end = xstat.st_size

	statvfs = os.fstatvfs(fd)
	block_size = statvfs.f_bsize
	start_block = start // block_size
	end_block = (end + block_size - 1) // block_size

	block = start_block
	fe_pblk = None
	fe_lblk = None
	fe_len = None
	max_extent = MAX_EXTENT_LENGTH / block_size
	while block <= end_block:
		indata = struct.pack('i', block)
		res = fcntl.ioctl(fd, _FIBMAP, indata)
		pblock = struct.unpack('i', res)[0]
		if fe_pblk is not None:
			if pblock > 0 and pblock == fe_pblk + fe_len and \
			   fe_len <= max_extent:
				fe_len += 1
			else:
				yield fiemap_rec(fe_lblk * block_size, \
						fe_pblk * block_size, \
						fe_len * block_size, 0, \
						FIEMAP_EXTENT_MERGED)
				fe_pblk = fe_lblk = fe_len = None
		else:
			if pblock > 0:
				fe_pblk = pblock
				fe_lblk = block
				fe_len = 1
		block += 1

	if fe_pblk is not None:
		yield fiemap_rec(fe_lblk * block_size, \
				fe_pblk * block_size, fe_len * block_size, 0, \
				FIEMAP_EXTENT_MERGED)

def file_mappings(fd, start = 0, length = None, flags = 0):
	if flags & FIEMAP_FLAG_FORCE_FIBMAP:
		return fibmap2(fd, start, length, flags)
	try:
		return fiemap2(fd, start, length, flags)
	except:
		return fibmap2(fd, start, length, flags)

if __name__ == '__main__':
	import sys
	import pprint

	if len(sys.argv) < 2:
		sys.stderr.write('No filename(s) given')
		sys.exit(1)

	for file_ in sys.argv[1:]:
		with open(file_, 'r') as fd:
			stat = os.fstat(fd.fileno())
			print(file_)
			print('-' * len(file_))
			for mapping in file_mappings(fd, length = stat.st_size, \
					flags = FIEMAP_FLAG_SYNC):
				pprint.pprint(mapping)
