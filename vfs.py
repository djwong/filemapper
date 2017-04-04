#!/usr/bin/env python3
# online filesystem mapper scanning
# Copyright (C) 2017 Darrick J. Wong.  All rights reserved.
# Licensed under the GPLv2.

import fiemap
#import array
#import fcntl
#import struct
#import collections
import os
#import errno
import stat
#import itertools

def walk_fs(path, dir_fn, ino_fn, extent_fn):
	'''Iterate the filesystem, looking for extent data.'''
	def do_map(fstat, path):
		if fstat.st_ino in seen:
			return
		seen.add(fstat.st_ino)
		fd = os.open(path, os.O_RDONLY)
		try:
			if do_map.fiemap_broken:
				for extent in fiemap.fibmap2(fd, flags = flags):
					extent_fn(fstat, extent, False)
				return
			try:
				for extent in fiemap.fiemap2(fd, flags = flags):
					extent_fn(fstat, extent, False)
			except:
				if stat.S_ISREG(fstat.st_mode):
					do_map.fiemap_broken = True
				for extent in fiemap.fibmap2(fd, flags = flags):
					extent_fn(fstat, extent, False)
			try:
				for extent in fiemap.fiemap2(fd, flags = flags | fiemap.FIEMAP_FLAG_XATTR):
					extent_fn(fstat, extent, True)
			except:
				pass
		finally:
			os.close(fd)

	do_map.fiemap_broken = False
	seen = set()
	# Careful - we have to pass a byte string to os.walk so that
	# it'll return byte strings, which we can then decode ourselves.
	# Otherwise the automatic Unicode decoding will error out.
	#
	# Strip out the trailing / so that root is ''
	if path != os.sep and path[-1] == os.sep:
		path = path[:-1]
	prefix_len = 0 if path == os.sep else len(path)
	dev = os.lstat(path).st_dev
	flags = fiemap.FIEMAP_FLAG_SYNC
	for root, dirs, files in os.walk(path.encode('utf-8', 'surrogateescape')):
		rstat = os.lstat(root)
		if rstat.st_dev != dev:
			continue
		if root.decode('utf-8', 'replace') == os.sep:
			plen = 1
		else:
			plen = prefix_len
		ino_fn(rstat, root[plen:].decode('utf-8', 'replace'))
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
