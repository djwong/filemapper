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

# Internals and plumbing
# From asm-generic/ioctl.h
_IOC_NRBITS = 8
_IOC_TYPEBITS = 8
_IOC_SIZEBITS = 14

_IOC_NRSHIFT = 0
_IOC_TYPESHIFT = _IOC_NRSHIFT + _IOC_NRBITS
_IOC_SIZESHIFT = _IOC_TYPESHIFT + _IOC_TYPEBITS
_IOC_DIRSHIFT = _IOC_SIZESHIFT + _IOC_SIZEBITS

_IOC_TYPECHECK = lambda struct: struct.size

_IOC = lambda dir_, type_, nr, size: \
	(dir_ << _IOC_DIRSHIFT) | (type_ << _IOC_TYPESHIFT) \
	| (nr << _IOC_NRSHIFT) | (size << _IOC_SIZESHIFT)
_IOC_NONE = 0
_IOC_WRITE = 1
_IOC_READ = 2
_IOWR = lambda type_, nr, size: \
	_IOC(_IOC_READ | _IOC_WRITE, type_, nr, _IOC_TYPECHECK(size))
_IO = lambda type_, nr: \
	_IOC(_IOC_NONE, type_, nr, 0)

# Derived from linux/fiemap.h
_struct_fiemap = struct.Struct('=QQLLLL')
_struct_fiemap_extent = struct.Struct('=QQQQQLLLL')
_struct_fibmap = struct.Struct('=L')

# From linux/fs.h
_FS_IOC_FIEMAP = _IOWR(ord('f'), 11, _struct_fiemap)
_FIBMAP = _IO(0x00, 1)

_UINT64_MAX = (2 ** 64) - 1

_fiemap = collections.namedtuple('fiemap',
	'start length flags mapped_extents extent_count extents')
_fiemap_extent = collections.namedtuple('fiemap_extent',
	'logical physical length flags')

def fiemap2(fd, start = 0, length = None, flags = 0):
	fe_logical = start
	fe_length = 0

	if length is None:
		length = _UINT64_MAX

	count = 512
	while True:
		hdr = _struct_fiemap.pack(fe_logical + fe_length, length, flags, 0, count, 0)
		try:
			b = bytearray(hdr)
			b = b + bytearray(_struct_fiemap_extent.size * count)
			buffer_ = b
			ret = fcntl.ioctl(fd, _FS_IOC_FIEMAP, buffer_)
		except OSError:
			raise
		except:
			fiemap_buffer = '%s%s' % (hdr,
				'\0' * (_struct_fiemap_extent.size * count))

			# Turn into mutable C-level array of chars
			buffer_ = array.array('c', fiemap_buffer)
			ret = fcntl.ioctl(fd, _FS_IOC_FIEMAP, buffer_)

		if length != 0 and ret < 0:
			raise IOError('ioctl')

		# Read out fiemap struct
		fm_start, fm_length, fm_flags, fm_mapped_extents, \
				fm_extent_count, fm_reserved = \
				_struct_fiemap.unpack_from(buffer_)
		if fm_mapped_extents == 0:
			return

		offset = _struct_fiemap.size
		for i in range(fm_mapped_extents):
			fe_logical, fe_physical, fe_length, _1, _2, fe_flags, \
					_3, _4, _5 = \
					_struct_fiemap_extent.unpack_from( \
					buffer_[offset:offset + \
						_struct_fiemap_extent.size])
			yield _fiemap_extent(fe_logical, fe_physical, \
					fe_length, fe_flags & ~FIEMAP_EXTENT_LAST)
			if fe_flags & FIEMAP_EXTENT_LAST:
				return
			offset += _struct_fiemap_extent.size

def fibmap2(fd, start = 0, end = None, flags = 0):
	xstat = os.fstat(fd)
	if not stat.S_ISREG(xstat.st_mode):
		return

	if end is None:
		end = xstat.st_size

	statvfs = os.fstatvfs(fd)
	block_size = statvfs.f_bsize
	start_block = start // block_size
	end_block = (end + block_size - 1) // block_size

	if flags & FIEMAP_FLAG_SYNC:
		os.fsync(fd)

	block = start_block
	fe_pblk = None
	fe_lblk = None
	fe_len = None
	while block <= end_block:
		indata = struct.pack('I', block)
		res = fcntl.ioctl(fd, _FIBMAP, indata)
		pblock = struct.unpack('I', res)[0]
		if fe_pblk is not None:
			if pblock != fe_pblk + fe_len:
				yield _fiemap_extent(fe_lblk * block_size, \
						fe_pblk * block_size, \
						fe_len * block_size, 0)
				fe_pblk = fe_lblk = fe_len = None
			else:
				fe_len += 1
		else:
			if pblock != 0:
				fe_pblk = pblock
				fe_lblk = block
				fe_len = 1
		block += 1

	if fe_pblk is not None:
		yield _fiemap_extent(fe_lblk, fe_pblk, fe_len, 0)

def file_mappings(fd, start = 0, length = None, flags = 0):
	if flags & FIEMAP_FLAG_FORCE_FIBMAP:
		yield fibmap2(fd, start, length, flags)
	try:
		yield fiemap2(fd, start, length, flags)
	except:
		yield fibmap2(fd, start, length, flags)

def walk_fs(path, dir_fn, ino_fn, extent_fn):
	'''Iterate the filesystem, looking for extent data.'''
	flags = FIEMAP_FLAG_SYNC
	prefix_len = len(path)
	if path[prefix_len - 1] == '/':
		prefix_len -= 1

	def do_map(stat, path):
		fd = os.open(path, os.O_RDONLY)
		try:
			for extent in fiemap2(fd, flags = flags):
				extent_fn(stat, extent, False)
			for extent in fiemap2(fd, flags = flags | FIEMAP_FLAG_XATTR):
				extent_fn(stat, extent, True)
		except:
			for extent in fibmap2(fd, flags = flags):
				extent_fn(stat, extent, False)
		os.close(fd)

	dev = os.lstat(path).st_dev
	# Careful - we have to pass a byte string to os.walk so that
	# it'll return byte strings, which we can then decode ourselves.
	# Otherwise the automatic Unicode decoding will error out.
	for root, dirs, files in os.walk(path.encode('utf-8', 'surrogateescape')):
		rstat = os.lstat(root)
		ino_fn(rstat, root[prefix_len:].decode('utf-8', 'replace'))
		do_map(rstat, root)
		dentries = []
		for xdir in dirs:
			dname = os.path.join(root, xdir)
			try:
				dstat = os.lstat(dname)
			except Exception as e:
				print(e)
				continue

			if dstat.st_dev != dev:
				dirs.remove(xdir)
				continue
			dentries.append((xdir.decode('utf-8', 'replace'), dstat))
		for xfile in files:
			fname = os.path.join(root, xfile)
			try:
				fstat = os.lstat(fname)
			except Exception as e:
				print(e)
				continue
	
			if not stat.S_ISREG(fstat.st_mode) and \
			   not stat.S_ISDIR(fstat.st_mode):
				continue

			if fstat.st_dev != dev:
				continue
			ino_fn(fstat, fname[prefix_len:].decode('utf-8', 'replace'))
			do_map(fstat, fname)
			dentries.append((xfile.decode('utf-8', 'replace'), fstat))
		dir_fn(rstat, dentries)

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
			map_ = file_mappings(fd, length = stat.st_size,
					flags = FIEMAP_FLAG_SYNC)
			pprint.pprint(map_)
			print()
