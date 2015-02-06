#!/usr/bin/env python3
# Database routines for filemapper
# Copyright(C) 2015 Darrick J. Wong
# Licensed under GPLv2.

import sqlite3
import os
import datetime
import stat
import array
import fiemap
from collections import namedtuple
from abc import ABCMeta, abstractmethod

fs_summary = namedtuple('fs_summary', ['path', 'block_size', 'frag_size',
				       'total_bytes', 'free_bytes',
				       'avail_bytes', 'total_inodes',
				       'free_inodes', 'avail_inodes',
				       'extents', 'pathsep', 'inodes',
				       'date'])

# Extent types
EXT_TYPE_FILE		= 0
EXT_TYPE_DIR		= 1
EXT_TYPE_EXTENT		= 2
EXT_TYPE_METADATA	= 3
EXT_TYPE_XATTR		= 4
EXT_TYPE_SYMLINK	= 5

extent_types = {
	EXT_TYPE_FILE:		'File',
	EXT_TYPE_DIR:		'Directory',
	EXT_TYPE_EXTENT:	'Extent Map',
	EXT_TYPE_METADATA:	'Metadata',
	EXT_TYPE_XATTR:		'Extended Attribute',
	EXT_TYPE_SYMLINK:	'Symbolic Link',
}

def stmode_to_type(xstat, is_xattr):
	'''Convert a stat mode to a type code.'''
	if is_xattr:
		return EXT_TYPE_XATTR
	elif stat.S_ISREG(xstat.st_mode):
		return EXT_TYPE_FILE
	elif stat.S_ISDIR(xstat.st_mode):
		return EXT_TYPE_DIR
	elif stat.S_ISLNK(xstat.st_mode):
		return EXT_TYPE_SYMLINK

# Extent flags
EXT_FLAG_UNKNOWN = fiemap.FIEMAP_EXTENT_UNKNOWN
EXT_FLAG_DELALLOC = fiemap.FIEMAP_EXTENT_DELALLOC
EXT_FLAG_ENCODED = fiemap.FIEMAP_EXTENT_ENCODED
EXT_FLAG_DATA_ENCRYPTED = fiemap.FIEMAP_EXTENT_DATA_ENCRYPTED
EXT_FLAG_NOT_ALIGNED = fiemap.FIEMAP_EXTENT_NOT_ALIGNED
EXT_FLAG_DATA_INLINE = fiemap.FIEMAP_EXTENT_DATA_INLINE
EXT_FLAG_DATA_TAIL = fiemap.FIEMAP_EXTENT_DATA_TAIL
EXT_FLAG_UNWRITTEN = fiemap.FIEMAP_EXTENT_UNWRITTEN
EXT_FLAG_MERGED = fiemap.FIEMAP_EXTENT_MERGED
EXT_FLAG_SHARED = fiemap.FIEMAP_EXTENT_SHARED

extent_flags = {
	EXT_FLAG_UNKNOWN:		'n',
	EXT_FLAG_DELALLOC:		'd',
	EXT_FLAG_ENCODED:		'c',
	EXT_FLAG_DATA_ENCRYPTED:	'E',
	EXT_FLAG_NOT_ALIGNED:		'a',
	EXT_FLAG_DATA_INLINE:		'i',
	EXT_FLAG_DATA_TAIL:		't',
	EXT_FLAG_UNWRITTEN:		'U',
	EXT_FLAG_MERGED:		'm',
	EXT_FLAG_SHARED:		's',
}

extent_flags_long = {
	EXT_FLAG_UNKNOWN:		'U(n)known',
	EXT_FLAG_DELALLOC:		'Not Allocate(d)',
	EXT_FLAG_ENCODED:		'(C)ompressed',
	EXT_FLAG_DATA_ENCRYPTED:	'(E)ncrypted',
	EXT_FLAG_NOT_ALIGNED:		'Un(a)ligned',
	EXT_FLAG_DATA_INLINE:		'(I)nline',
	EXT_FLAG_DATA_TAIL:		'(T)ail-Packed',
	EXT_FLAG_UNWRITTEN:		'(U)nwritten',
	EXT_FLAG_MERGED:		'(M)erged',
	EXT_FLAG_SHARED:		'(S)hared',
}

extent_strings = {extent_flags[i] for i in extent_flags}
extent_strings_long = {extent_flags_long[i] for i in extent_flags_long}

def extent_flags_to_str(flags):
	'''Convert an extent flags number into a string.'''
	return ''.join([extent_flags[f] for f in extent_flags if flags & f > 0])

def extent_str_to_flags(string):
	'''Convert an extent string into a flags number.'''
	ret = 0
	for s in string:
		ret |= extent_strings[s]
	return ret

# An extent
class poff_row(object):
	def __init__(self, path, p_off, l_off, length, flags, type):
		self.path = path
		self.p_off = p_off
		self.l_off = l_off
		self.length = length
		self.flags = flags
		self.type = type

	def flagstr(self):
		return extent_flags_to_str(self.flags)

	def typestr(self):
		return extent_types[self.type]

# Inode type codes
INO_TYPE_FILE		= 0
INO_TYPE_DIR		= 1
INO_TYPE_METADATA	= 2
INO_TYPE_SYMLINK	= 3

inode_types = {
	INO_TYPE_FILE:		'file',
	INO_TYPE_DIR:		'directory',
	INO_TYPE_METADATA:	'metadata',
	INO_TYPE_SYMLINK:	'symbolic link',
}

# Directory entry
class dentry(object):
	'''Directory entry.'''
	def __init__(self, name, ino, type):
		self.name = name
		self.ino = ino
		self.type = type

	def typestr(self):
		return inode_types[self.type]

# Database strings
APP_ID = 0xEF54
def generate_op_sql():
	'''Generate per-connection database settings.'''
	return '''PRAGMA cache_size = 65536;
PRAGMA mmap_size = 1073741824;
PRAGMA synchronous = OFF;
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA threads = 8;'''

def generate_schema_sql():
	'''Generate the database schema.'''
	x = ['''PRAGMA page_size = 4096;
PRAGMA application_id = %d;
PRAGMA journal_mode = WAL;
DROP VIEW IF EXISTS dentry_t;
DROP VIEW IF EXISTS path_extent_v;
DROP TABLE IF EXISTS dentry_t;
DROP TABLE IF EXISTS extent_t;
DROP TABLE IF EXISTS extent_type_t;
DROP TABLE IF EXISTS inode_t;
DROP TABLE IF EXISTS inode_type_t;
DROP TABLE IF EXISTS path_t;
DROP TABLE IF EXISTS dir_t;
DROP TABLE IF EXISTS fs_t;
CREATE TABLE fs_t(path TEXT PRIMARY KEY NOT NULL, block_size INTEGER NOT NULL, frag_size INTEGER NOT NULL, total_bytes INTEGER NOT NULL, free_bytes INTEGER NOT NULL, avail_bytes INTEGER NOT NULL, total_inodes INTEGER NOT NULL, free_inodes INTEGER NOT NULL, avail_inodes INTEGER NOT NULL, max_len INTEGER NOT NULL, timestamp TEXT NOT NULL, finished INTEGER NOT NULL, path_separator TEXT NOT NULL);
CREATE TABLE inode_type_t(id INTEGER PRIMARY KEY UNIQUE, code TEXT NOT NULL);
CREATE TABLE inode_t(ino INTEGER PRIMARY KEY UNIQUE NOT NULL, type INTEGER NOT NULL, FOREIGN KEY(type) REFERENCES inode_type_t(id));
CREATE TABLE dir_t(dir_ino INTEGER NOT NULL, name TEXT NOT NULL, name_ino INTEGER NOT NULL, FOREIGN KEY(dir_ino) REFERENCES inode_t(ino), FOREIGN KEY(name_ino) REFERENCES inode_t(ino));
CREATE TABLE path_t(path TEXT PRIMARY KEY UNIQUE NOT NULL, ino INTEGER NOT NULL, FOREIGN KEY(ino) REFERENCES inode_t(ino));
CREATE TABLE extent_type_t (id INTEGER PRIMARY KEY UNIQUE, code TEXT NOT NULL);
CREATE TABLE extent_t(ino INTEGER NOT NULL, p_off INTEGER NOT NULL, l_off INTEGER NOT NULL, flags INTEGER NOT NULL, length INTEGER NOT NULL, type INTEGER NOT NULL, p_end INTEGER NOT NULL, FOREIGN KEY(ino) REFERENCES inode_t(ino), FOREIGN KEY(type) REFERENCES extent_type_t(id));
CREATE VIEW path_extent_v AS SELECT path_t.path, extent_t.p_off, extent_t.l_off, extent_t.length, extent_t.flags, extent_t.type, extent_t.p_end, extent_t.ino FROM extent_t, path_t WHERE extent_t.ino = path_t.ino;
CREATE VIEW dentry_t AS SELECT dir_t.dir_ino, dir_t.name, dir_t.name_ino, inode_t.type FROM dir_t, inode_t WHERE dir_t.name_ino = inode_t.ino;''' % APP_ID]
	y = ["INSERT INTO inode_type_t VALUES (%d, '%s');" % (x, inode_types[x]) for x in sorted(inode_types.keys())]
	z = ["INSERT INTO extent_type_t VALUES (%d, '%s');" % (x, extent_types[x]) for x in sorted(extent_types.keys())]
	return '\n'.join(x + y + z)

def generate_index_sql():
	'''Generate SQL for the indexes.'''
	return '''CREATE INDEX inode_i ON inode_t(ino);
CREATE INDEX path_ino_i ON path_t(ino);
CREATE INDEX path_path_i ON path_t(path);
CREATE INDEX dir_ino_i ON dir_t(dir_ino);
CREATE INDEX dir_nino_i ON dir_t(name_ino);
CREATE INDEX extent_poff_i ON extent_t(p_off, p_end);
CREATE INDEX extent_loff_i ON extent_t(l_off, length);
CREATE INDEX extent_ino_i ON extent_t(ino);
PRAGMA foreign_key_check;
'''

def print_times(label, times):
	'''Print some profiling data.'''
	l = [str(times[i] - times[i - 1]) for i in range(1, len(times))]
	print('%s: %s' % (label, ', '.join(l)))

class overview_block(object):
	def __init__(self):
		self.files = self.dirs = self.mappings = self.metadata = self.xattrs = self.symlink = 0

	def add(self, value):
		'''Add another overview block to this one.'''
		self.files += value.files
		self.dirs += value.dirs
		self.mappings += value.mappings
		self.metadata += value.metadata
		self.xattrs += value.xattrs
		self.symlink += value.symlink

	def to_letter(ov):
		'''Render this overview block as a string.'''
		tot = ov.files + ov.dirs + ov.mappings + ov.metadata + ov.xattrs + ov.symlink
		if tot == 0:
			return '.'
		elif ov.files == tot:
			return 'F'
		elif ov.dirs == tot:
			return 'D'
		elif ov.mappings == tot:
			return 'E'
		elif ov.metadata == tot:
			return 'M'
		elif ov.xattrs == tot:
			return 'X'
		elif ov.symlink == tot:
			return 'S'

		x = ov.files
		letter = 'f'
		if ov.dirs > x:
			x = ov.dirs
			letter = 'd'
		if ov.mappings > x:
			x = ov.mappings
			letter = 'e'
		if ov.metadata > x:
			x = ov.metadata
			letter = 'm'
		if ov.xattrs > x:
			x = ov.xattrs
			letter = 'x'
		if ov.symlink > x:
			x = ov.symlink
			letter = 's'
		return letter

	def __str__(ov):
		return '(f:%d d:%d e:%d m:%d x:%d s:%d)' % (ov.files, ov.dirs, ov.mappings, ov.metadata, ov.xattrs, ov.symlink)

class fmdb(object):
	'''filemapper database'''
	def __init__(self, fspath, dbpath):
		'''Initialize a database object.'''
		if dbpath == ':memory:':
			self.writable = True
			db = dbpath
		elif fspath is None:
			self.writable = False
			db = 'file:%s?mode=ro' % dbpath
		else:
			self.writable = True
			db = 'file:%s' % dbpath
		self.conn = None
		try:
			self.conn = sqlite3.connect(db, uri = True)
		except TypeError:
			# In Python 2.6 there's no uri parameter support
			self.conn = sqlite3.connect(dbpath)
		self.fs = None
		self.overview_len = None
		self.cached_overview = []
		self.result_batch_size = 512

		# Check the database sanity if we're reading it.
		if not self.writable:
			cur = self.conn.execute('PRAGMA application_id;')
			appid = cur.fetchall()[0][0]
			if appid != 61268:
				print('WARNING: This might not be a FileMapper database!')
		self.conn.executescript(generate_op_sql())
		if not self.writable:
			cur = self.conn.cursor()
			try:
				cur.execute('SELECT path, finished FROM fs_t')
			except:
				raise ValueError('Database is empty.')
			results = cur.fetchall()
			if len(results) != 1:
				raise ValueError('Database is empty.')
			if results[0][1] == 0:
				raise ValueError('Database is incomplete.')
			self.fspath = results[0][0]
		else:
			self.fspath = fspath

	def __del__(self):
		'''Destroy database object.'''
		if self.conn is not None:
			self.conn.close()

	def start_update(self):
		'''Prepare a database for new data.'''
		if self.fspath is None:
			raise ValueError('fspath must be specified.')
		self.conn.executescript(generate_schema_sql())

		self.fs = None
		statfs = os.statvfs(self.fspath)
		self.conn.execute('INSERT INTO fs_t VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?);', \
				(self.fspath, statfs.f_bsize, \
				 statfs.f_frsize, \
				 statfs.f_blocks * statfs.f_bsize, \
				 statfs.f_bfree * statfs.f_bsize, \
				 statfs.f_bavail * statfs.f_bsize, \
				 statfs.f_files, statfs.f_ffree, \
				 statfs.f_favail, statfs.f_namemax, \
				 str(datetime.datetime.today()), \
				 os.sep))

	def end_update(self):
		'''Finish updating a database.'''
		self.conn.executescript(generate_index_sql())
		self.conn.execute('UPDATE fs_t SET finished = 1 WHERE path = ?;', (self.fspath,))
		self.conn.commit()

	@abstractmethod
	def analyze(self, force = False):
		'''Analyze the filesystem.'''
		raise NotImplementedError()

	@abstractmethod
	def is_stale(self):
		'''Decide if we need to reanalyze the database.'''
		raise NotImplementedError()

	def insert_dir(self, root, dentries):
		'''Insert a directory record into the database.'''
		self.conn.executemany('INSERT INTO dir_t VALUES(?, ?, ?);', \
				[(root.st_ino, name, stat.st_ino) for name, stat in dentries])

	def insert_inode(self, xstat, path):
		'''Insert an inode record into the database.'''
		if path == '/':
			raise ValueError("'/' is an invalid path.  Check code.")
		if stat.S_ISDIR(xstat.st_mode):
			xtype = INO_TYPE_DIR
		else:
			xtype = INO_TYPE_FILE
		self.conn.execute('INSERT OR REPLACE INTO inode_t VALUES(?, ?);', \
				(xstat.st_ino, xtype))
		self.conn.execute('INSERT INTO path_t VALUES(?, ?);', \
				(path, xstat.st_ino))

	def insert_extent(self, stat, extent, is_xattr):
		'''Insert an extent record into the database.'''
		code = stmode_to_type(stat, is_xattr)
		if extent.flags & (fiemap.FIEMAP_EXTENT_UNKNOWN | \
				   fiemap.FIEMAP_EXTENT_DELALLOC):
			return
		self.conn.execute('INSERT INTO extent_t VALUES(?, ?, ?, ?, ?, ?, ?);', \
			    (stat.st_ino, extent.physical, extent.logical, \
			     extent.flags, extent.length, \
			     code, extent.physical + extent.length - 1))

	def set_overview_length(self, length):
		'''Set the overview length.'''
		length = int(length)
		self.query_summary()
		if length > self.fs.total_bytes or length < 1:
			length = self.fs.total_bytes
		if self.overview_len == length:
			return
		self.overview_len = length
		self.bytes_per_cell = int(self.fs.total_bytes / self.overview_len)

	def query_overview(self):
		'''Create the overview.'''
		for c in self.cached_overview:
			if c[0] == self.overview_len:
				return c[1]

		self.query_summary()
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		overview = [overview_block() for x in range(0, self.overview_len)]

		t0 = datetime.datetime.today()
		cur.execute('SELECT p_off, length, type FROM extent_t;')
		t1 = datetime.datetime.today()
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for (e_p_off, e_len, e_type) in rows:
				start_cell = int(e_p_off // self.bytes_per_cell)
				end_cell = int((e_p_off + e_len - 1) // self.bytes_per_cell)
				if e_type == EXT_TYPE_FILE:
					for i in range(start_cell, end_cell + 1):
						overview[i].files += 1
				elif e_type == EXT_TYPE_DIR:
					for i in range(start_cell, end_cell + 1):
						overview[i].dirs += 1
				elif e_type == EXT_TYPE_EXTENT:
					for i in range(start_cell, end_cell + 1):
						overview[i].mappings += 1
				elif e_type == EXT_TYPE_METADATA:
					for i in range(start_cell, end_cell + 1):
						overview[i].metadata += 1
				elif e_type == EXT_TYPE_XATTR:
					for i in range(start_cell, end_cell + 1):
						overview[i].xattrs += 1
				elif e_type == EXT_TYPE_SYMLINK:
					for i in range(start_cell, end_cell + 1):
						overview[i].symlink += 1
		t2 = datetime.datetime.today()
		print_times('overview', [t0, t1, t2])
		self.cached_overview.append([self.overview_len, overview])
		return overview

	def query_summary(self):
		'''Fetch the filesystem summary.'''
		if self.fs is not None:
			return self.fs

		cur = self.conn.cursor()
		etypes = ', '.join(map(str, [EXT_TYPE_FILE, EXT_TYPE_DIR, EXT_TYPE_XATTR, EXT_TYPE_SYMLINK]))
		qstr = 'SELECT COUNT(p_off) FROM extent_t WHERE type IN (%s);' % etypes
		cur.execute(qstr)
		rows = cur.fetchall()
		extents = rows[0][0]
		cur.execute('SELECT COUNT(ino) FROM inode_t;')
		rows = cur.fetchall()
		inodes = rows[0][0]

		cur.execute('SELECT path, block_size, frag_size, total_bytes, free_bytes, avail_bytes, total_inodes, free_inodes, avail_inodes, path_separator, timestamp FROM fs_t;')
		rows = cur.fetchall()
		assert len(rows) == 1
		res = rows[0]

		self.fs = fs_summary(res[0], int(res[1]), int(res[2]), \
				 int(res[3]), int(res[4]), int(res[5]), \
				 int(res[6]), int(res[7]), int(res[8]),
				 int(extents), res[9], int(inodes), res[10])
		return self.fs

	def pick_cells(self, ranges):
		'''Convert ranges of cells to ranges of bytes.'''
		sbc = self.bytes_per_cell

		for i in ranges:
			if type(i) == int:
				if i > self.overview_len:
					raise ValueError("range %d outside of overview" % i)
				yield (i * sbc, (i + 1) * sbc - 1)
			else:
				if i[0] > self.overview_len:
					raise ValueError("range %d outside of overview" % i[0])
				if i[1] > self.overview_len:
					raise ValueError("range %d outside of overview" % i[1])
				yield (i[0] * sbc, (i[1] + 1) * sbc - 1)

	def pick_bytes(self, ranges):
		'''Convert ranges of bytes to ranges of cells.'''
		sbc = self.bytes_per_cell
		self.query_summary()

		for i in ranges:
			if type(i) == int:
				if i > self.fs.total_bytes:
					raise ValueError("range %d outside of fs" % i)
				yield int(float(i) / sbc)
			else:
				if i[0] > self.fs.total_bytes:
					raise ValueError("range %d outside of fs" % i)
				if i[1] > self.fs.total_bytes:
					raise ValueError("range %d outside of fs" % i)
				yield (int(float(i[0]) / sbc), int(float(i[1]) / sbc))

	def query_loff_range(self, ranges):
		'''Query extents spanning ranges of logical bytes.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		qstr = 'SELECT path, p_off, l_off, length, flags, type FROM path_extent_v'
		qarg = []
		cond = 'WHERE'
		for r in ranges:
			if type(r) == int:
				qstr = qstr + ' %s (l_off <= ? AND l_off + length - 1 >= ?)' % cond
				cond = 'OR'
				qarg.append(r)
				qarg.append(r)
			else:
				qstr = qstr + ' %s (l_off <= ? AND l_off + length - 1 >= ?)' % cond
				cond = 'OR'
				qarg.append(r[1])
				qarg.append(r[0])
		qstr = qstr + " ORDER BY path, l_off"
		cur.execute(qstr, qarg)
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for row in rows:
				yield poff_row(row[0], row[1], row[2], row[3], \
						row[4], row[5])


	def query_poff_range(self, ranges):
		'''Query extents spanning ranges of physical bytes.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		qstr = 'SELECT path, p_off, l_off, length, flags, type FROM path_extent_v'
		qarg = []
		cond = 'WHERE'
		for r in ranges:
			if type(r) == int:
				qstr = qstr + ' %s (p_off <= ? AND p_end >= ?)' % cond
				cond = 'OR'
				qarg.append(r)
				qarg.append(r)
			else:
				qstr = qstr + ' %s (p_off <= ? AND p_end >= ?)' % cond
				cond = 'OR'
				qarg.append(r[1])
				qarg.append(r[0])
		qstr = qstr + " ORDER BY path, l_off"
		#print(qstr, qarg)
		t0 = datetime.datetime.today()
		cur.execute(qstr, qarg)
		t1 = datetime.datetime.today()
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for row in rows:
				yield poff_row(row[0], row[1], row[2], row[3], \
						row[4], row[5])
		t2 = datetime.datetime.today()
		print_times('poff_range', [t0, t1, t2])

	def query_paths(self, paths):
		'''Query extents used by a given path.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		qstr = 'SELECT path, p_off, l_off, length, flags, type FROM path_extent_v'
		qarg = []
		cond = 'WHERE'
		for p in paths:
			if p == self.fs.pathsep:
				p = ''
			if '*' in p:
				op = 'LIKE'
			else:
				op = '='
			qstr = qstr + ' %s path %s ?' % (cond, op)
			cond = 'OR'
			qarg.append(p.replace('*', '%'))
		qstr = qstr + " ORDER BY path, l_off"
		cur.execute(qstr, qarg)
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				return
			for row in rows:
				yield poff_row(row[0], row[1], row[2], row[3], \
						row[4], row[5])

	def query_inodes(self, ranges):
		'''Query extents given ranges of inodes.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		qstr = 'SELECT path, p_off, l_off, length, flags, type FROM path_extent_v'
		qarg = []
		cond = 'WHERE'
		for r in ranges:
			if type(r) == int:
				qstr = qstr + ' %s ino = ?' % cond
				cond = 'OR'
				qarg.append(r)
			else:
				qstr = qstr + ' %s ino BETWEEN ? AND ?' % cond
				cond = 'OR'
				qarg.append(r[0])
				qarg.append(r[1])
		qstr = qstr + " ORDER BY path, l_off"
		cur.execute(qstr, qarg)
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for row in rows:
				yield poff_row(row[0], row[1], row[2], row[3], \
						row[4], row[5])

	def query_lengths(self, ranges):
		'''Query extents given ranges of lengths.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		qstr = 'SELECT path, p_off, l_off, length, flags, type FROM path_extent_v'
		qarg = []
		cond = 'WHERE'
		for r in ranges:
			if type(r) == int:
				qstr = qstr + ' %s length = ?' % cond
				cond = 'OR'
				qarg.append(r)
			else:
				qstr = qstr + ' %s length BETWEEN ? AND ?' % cond
				cond = 'OR'
				qarg.append(r[0])
				qarg.append(r[1])
		qstr = qstr + " ORDER BY path, l_off"
		cur.execute(qstr, qarg)
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for row in rows:
				yield poff_row(row[0], row[1], row[2], row[3], \
						row[4], row[5])

	def query_extent_types(self, types):
		'''Query extents given a set of type codes.'''
		if len(types) == 0:
			return
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		qstr = 'SELECT path, p_off, l_off, length, flags, type FROM path_extent_v'
		qarg = []
		cond = 'WHERE type IN ('
		for r in types:
			qstr = qstr + ' %s ?' % cond
			cond = ', '
			qarg.append(r)
		if len(qarg) > 0:
			qstr = qstr + ')'
		qstr = qstr + ' ORDER BY path, l_off'
		cur.execute(qstr, qarg)
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for row in rows:
				yield poff_row(row[0], row[1], row[2], row[3], \
						row[4], row[5])

	def query_extent_flags(self, flags, exact = True):
		'''Query extents given a set of type codes.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		qstr = 'SELECT path, p_off, l_off, length, flags, type FROM path_extent_v WHERE flags'
		if exact:
			qstr = qstr + ' = ?'
		else:
			qstr = qstr + ' & ? > 0'
		cur.execute(qstr, [flags])
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for row in rows:
				yield poff_row(row[0], row[1], row[2], row[3], \
						row[4], row[5])

	def query_root(self):
		'''Retrieve a dentry for root.'''
		cur = self.conn.cursor()
		qstr = 'SELECT inode_t.ino, inode_t.type FROM inode_t, path_t WHERE inode_t.ino = path_t.ino AND path_t.path = ?'
		qarg = ['']
		cur.execute(qstr, qarg)
		rows = cur.fetchall()
		if len(rows) > 1:
			raise ValueError('More than one root dentry?')
		elif len(rows) < 1:
			raise ValueError('Less than one root dentry?')
		return dentry('', rows[0][0], rows[0][1])

	def query_ls(self, paths):
		'''Query all paths available under a given path.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		qstr = 'SELECT dentry_t.name, dentry_t.name_ino, dentry_t.type FROM dentry_t, path_t WHERE dentry_t.dir_ino = path_t.ino'
		cond = 'AND ('
		qarg = []
		for p in paths:
			if '*' in p:
				op = 'LIKE'
			else:
				op = '='
			qstr = qstr + ' %s path_t.path %s ?' % (cond, op)
			cond = 'OR'
			qarg.append(p.replace('*', '%'))
		if len(qarg) == 0:
			cond = ''
		else:
			cond = ')'
		qstr = qstr + '%s ORDER by dentry_t.name' % cond
		cur.execute(qstr, qarg)
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for row in rows:
				yield dentry(row[0], row[1], row[2])

class fiemap_db(fmdb):
	'''FileMapper database based on FIEMAP.'''
	def __init__(self, fspath, dbpath):
		if fspath is None:
			raise ValueError('Please specify a FS path.')
		super(fiemap_db, self).__init__(fspath, dbpath)

	def is_stale(self):
		'''Decide if the FS should be re-analyzed.'''
		if self.fspath == None:
			return False
		try:
			cur = self.conn.cursor()
			cur.execute('SELECT path, finished FROM fs_t WHERE path = ?', (self.fspath,))
			results = cur.fetchall()
			if len(results) != 1:
				return True
			if results[0][1] == 0:
				return True
			return False
		except:
			return True

	def analyze(self, force = False):
		'''Regenerate the database.'''
		if not force and not self.must_regenerate():
			return
		self.start_update()
		fiemap.walk_fs(self.fspath,
			self.insert_dir,
			self.insert_inode,
			self.insert_extent)
		self.end_update()

