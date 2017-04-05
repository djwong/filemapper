#!/usr/bin/env python3
# online filesystem mapper scanning
# Copyright (C) 2017 Darrick J. Wong.  All rights reserved.
# Licensed under the GPLv2.

import os
import stat
import fiemap
import getfsmap
import collections
import fmdb

fake_stat = collections.namedtuple('fake_stat',
		'st_ino st_mode st_size st_atime st_mtime st_ctime')

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

	def ensure_metadir(stat_dict):
		if fmdb.metadata_dir in stat_dict:
			return stat_dict[fmdb.metadata_dir]
		metadir_stat = fake_stat(-1, -fmdb.INO_TYPE_DIR, None, None, None, None)
		ino_fn(metadir_stat, '/' + fmdb.metadata_dir)
		root_stat = os.lstat(path)
		dir_fn(root_stat, [(fmdb.metadata_dir, metadir_stat)])
		stat_dict[fmdb.metadata_dir] = metadir_stat
		return metadir_stat

	def ensure_metadir_file(stat_dict, owner, mode, name):
		if owner in stat_dict:
			return stat_dict[owner]
		sb = fake_stat(owner, mode, None, None, None, None)
		metadir_stat = ensure_metadir(stat_dict)
		ino_fn(sb, '/' + fmdb.metadata_dir + '/' + name)
		dir_fn(metadir_stat, [(name, sb)])
		stat_dict[owner] = sb
		return sb

	def ensure_unlinked_file(stat_dict, owner):
		if owner in stat_dict:
			return stat_dict[owner]
		sb = fake_stat(owner, stat.S_IFREG, None, None, None, None)
		udir_stat = ensure_metadir_file(stat_dict, -4, stat.S_IFDIR, fmdb.unlinked_dir)
		ino_fn(sb, '/' + fmdb.metadata_dir + '/' + fmdb.unlinked_dir + '/%d' % owner)
		dir_fn(udir_stat, [('%d' % owner, sb)])
		stat_dict[owner] = sb
		return sb

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
	root_stat = os.lstat(path)
	flags = fiemap.FIEMAP_FLAG_SYNC

	# Walk the directory tree for dir and extent information
	seen_inodes = set()
	for root, dirs, files in os.walk(path.encode('utf-8', 'surrogateescape')):
		rstat = os.lstat(root)
		if rstat.st_dev != root_stat.st_dev:
			continue
		if root.decode('utf-8', 'replace') == os.sep:
			plen = 1
		else:
			plen = prefix_len
		seen_inodes.add(rstat.st_ino)
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

			if dstat.st_dev != root_stat.st_dev:
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

			if fstat.st_dev != root_stat.st_dev:
				continue
			seen_inodes.add(fstat.st_ino)
			ino_fn(fstat, fname[prefix_len:].decode('utf-8', 'replace'))
			do_map(fstat, fname)
			dentries.append((xfile.decode('utf-8', 'replace'), fstat))
		dir_fn(rstat, dentries)

	# Now try to walk the space map for metadata and unlinked inodes
	stat_dict = {}
	try:
		fd = os.open(path, os.O_RDONLY)
		for rmap in getfsmap.getfsmap(fd):
			hdr_flags = 0
			if (rmap.hdr_flags & getfsmap.FMH_OF_DEV_T) and \
			   rmap.device != root_stat.st_dev:
				# Not this device; skip
				continue
			elif (rmap.flags & getfsmap.FMR_OF_SPECIAL_OWNER) == 0:
				if rmap.owner in seen_inodes:
					continue
				# Capture unlinked inode extents
				if rmap.flags & getfsmap.FMR_OF_ATTR_FORK:
					hdr_flags |= fiemap.FIEMAP_FLAG_XATTR
				sb = ensure_unlinked_file(stat_dict, rmap.owner)
			elif rmap.owner == getfsmap.FMR_OWN_FREE:
				# Free space
				sb = ensure_metadir_file(stat_dict, -2, -fmdb.INO_TYPE_FREESP, fmdb.freespace_file)
			elif rmap.owner == getfsmap.FMR_OWN_METADATA:
				# Generic metadata
				sb = ensure_metadir_file(stat_dict, -3, -fmdb.INO_TYPE_METADATA, fmdb.metadata_file)
			elif getfsmap.FMR_OWNER_TYPE(rmap.owner) != 0:
				# FS-specific metadata
				sb = ensure_metadir_file(stat_dict, rmap.owner, -fmdb.INO_TYPE_METADATA, getfsmap.special_owner_name(rmap.owner))
			else:
				# Unknown generic special owner
				continue
			ext = fiemap.fiemap_rec(rmap.offset, rmap.physical, \
					rmap.length, 0, hdr_flags)
			extent_fn(sb, ext, False)
	except OSError:
		pass
	finally:
		os.close(fd)
