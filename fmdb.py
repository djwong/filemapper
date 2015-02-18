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

file_stats = namedtuple('file_stats', ['paths', 'ino', 'type', 'nr_extents', 'travel_score'])

# Extent types
EXT_TYPE_FILE		= 0
EXT_TYPE_DIR		= 1	# User visible extents are kept < 2
EXT_TYPE_EXTENT		= 2
EXT_TYPE_METADATA	= 3
EXT_TYPE_XATTR		= 4
EXT_TYPE_SYMLINK	= 5

extent_types = {
	EXT_TYPE_FILE:		'f',
	EXT_TYPE_DIR:		'd',
	EXT_TYPE_EXTENT:	'e',
	EXT_TYPE_METADATA:	'm',
	EXT_TYPE_XATTR:		'x',
	EXT_TYPE_SYMLINK:	's',
}

extent_types_long = {
	EXT_TYPE_FILE:		'File',
	EXT_TYPE_DIR:		'Directory',
	EXT_TYPE_EXTENT:	'Extent Map',
	EXT_TYPE_METADATA:	'Metadata',
	EXT_TYPE_XATTR:		'Extended Attribute',
	EXT_TYPE_SYMLINK:	'Symbolic Link',
}

extent_type_strings = {extent_types[i]: i for i in extent_types}
extent_type_strings_long = {extent_types_long[i]: i for i in extent_types_long}

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

extent_flags_strings = {extent_flags[i]: i for i in extent_flags}
extent_flags_strings_long = {extent_flags_long[i]: i for i in extent_flags_long}

def extent_flags_to_str(flags):
	'''Convert an extent flags number into a string.'''
	return ''.join([extent_flags[f] for f in extent_flags if flags & f > 0])

def extent_str_to_flags(string):
	'''Convert an extent string into a flags number.'''
	ret = 0
	for s in string:
		ret |= extent_flags_strings[s]
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
		return extent_types_long[self.type]

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

primary_extent_type_for_inode = {
	INO_TYPE_FILE:		EXT_TYPE_FILE,
	INO_TYPE_DIR:		EXT_TYPE_DIR,
	INO_TYPE_METADATA:	EXT_TYPE_METADATA,
	INO_TYPE_SYMLINK:	EXT_TYPE_SYMLINK,
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
PRAGMA case_sensitive_like = ON;
PRAGMA threads = 8;'''

def generate_schema_sql():
	'''Generate the database schema.'''
	x = ['''PRAGMA page_size = 4096;
PRAGMA application_id = %d;
PRAGMA journal_mode = WAL;
DROP VIEW IF EXISTS dentry_t;
DROP VIEW IF EXISTS path_extent_v;
DROP TABLE IF EXISTS overview_t;
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
CREATE TABLE inode_t(ino INTEGER PRIMARY KEY UNIQUE NOT NULL, type INTEGER NOT NULL, nr_extents INTEGER, travel_score REAL, FOREIGN KEY(type) REFERENCES inode_type_t(id));
CREATE TABLE dir_t(dir_ino INTEGER NOT NULL, name TEXT NOT NULL, name_ino INTEGER NOT NULL, FOREIGN KEY(dir_ino) REFERENCES inode_t(ino), FOREIGN KEY(name_ino) REFERENCES inode_t(ino));
CREATE TABLE path_t(path TEXT PRIMARY KEY UNIQUE NOT NULL, ino INTEGER NOT NULL, FOREIGN KEY(ino) REFERENCES inode_t(ino));
CREATE TABLE extent_type_t (id INTEGER PRIMARY KEY UNIQUE, code TEXT NOT NULL);
CREATE TABLE extent_t(ino INTEGER NOT NULL, p_off INTEGER NOT NULL, l_off INTEGER NOT NULL, flags INTEGER NOT NULL, length INTEGER NOT NULL, type INTEGER NOT NULL, p_end INTEGER NOT NULL, FOREIGN KEY(ino) REFERENCES inode_t(ino), FOREIGN KEY(type) REFERENCES extent_type_t(id));
CREATE TABLE overview_t(length INTEGER NOT NULL, cell_no INTEGER NOT NULL, files INTEGER NOT NULL, dirs INTEGER NOT NULL, mappings INTEGER NOT NULL, metadata INTEGER NOT NULL, xattrs INTEGER NOT NULL, symlinks INTEGER NOT NULL, CONSTRAINT pk_overview PRIMARY KEY (length, cell_no));
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
CREATE INDEX overview_cell_i ON overview_t(length, cell_no);
PRAGMA foreign_key_check;
'''

def print_times(label, times):
	'''Print some profiling data.'''
	l = [str(times[i] - times[i - 1]) for i in range(1, len(times))]
	print('%s: %s' % (label, ', '.join(l)))

class overview_block(object):
	def __init__(self, extents_to_show, files = 0, dirs = 0, mappings = 0, \
		     metadata = 0, xattrs = 0, symlinks = 0):
		self.ets = extents_to_show
		self.files = files
		self.dirs = dirs
		self.mappings = mappings
		self.metadata = metadata
		self.xattrs = xattrs
		self.symlinks = symlinks

	def add(self, value):
		'''Add another overview block to this one.'''
		self.files += value.files
		self.dirs += value.dirs
		self.mappings += value.mappings
		self.metadata += value.metadata
		self.xattrs += value.xattrs
		self.symlinks += value.symlinks

	def to_letter(ov):
		def on(t):
			return ov.ets is None or t in ov.ets
		'''Render this overview block as a string.'''
		tot = ov.files + ov.dirs + ov.mappings + ov.metadata + ov.xattrs + ov.symlinks
		if tot == 0:
			return '.'
		elif ov.files == tot and on(EXT_TYPE_FILE):
			return 'F'
		elif ov.dirs == tot and on(EXT_TYPE_DIR):
			return 'D'
		elif ov.mappings == tot and on(EXT_TYPE_EXTENT):
			return 'E'
		elif ov.metadata == tot and on(EXT_TYPE_METADATA):
			return 'M'
		elif ov.xattrs == tot and on(EXT_TYPE_XATTR):
			return 'X'
		elif ov.symlinks == tot and on(EXT_TYPE_SYMLINK):
			return 'S'

		x = 0
		if ov.files > x and on(EXT_TYPE_FILE):
			x = ov.files
			letter = 'f'
		if ov.dirs > x and on(EXT_TYPE_DIR):
			x = ov.dirs
			letter = 'd'
		if ov.mappings > x and on(EXT_TYPE_EXTENT):
			x = ov.mappings
			letter = 'e'
		if ov.metadata > x and on(EXT_TYPE_METADATA):
			x = ov.metadata
			letter = 'm'
		if ov.xattrs > x and on(EXT_TYPE_XATTR):
			x = ov.xattrs
			letter = 'x'
		if ov.symlinks > x and on(EXT_TYPE_SYMLINK):
			x = ov.symlinks
			letter = 's'
		if x == 0:
			letter = '.'
		return letter

	def __str__(ov):
		return '(f:%d d:%d e:%d m:%d x:%d s:%d)' % (ov.files, ov.dirs, \
				ov.mappings, ov.metadata, ov.xattrs, ov.symlinks)

class fmdb(object):
	'''filemapper database'''
	def __init__(self, fspath, dbpath, dbwrite):
		'''Initialize a database object.'''
		self.writable = dbwrite
		if dbpath == ':memory:':
			self.writable = True
			db = dbpath
		elif dbwrite:
			self.writable = True
			db = 'file:%s?mode=rwc' % dbpath
		else:
			self.writable = False
			db = 'file:%s?mode=ro' % dbpath
		self.conn = None
		try:
			self.conn = sqlite3.connect(db, uri = True)
		except TypeError:
			# In Python 2.6 there's no uri parameter support
			self.conn = sqlite3.connect(dbpath)
		self.conn.isolation_level = None
		self.fs = None
		self.overview_len = None
		self.result_batch_size = 512

		# Check the database sanity if we're reading it.
		if fspath is None:
			cur = self.conn.execute('PRAGMA application_id;')
			appid = cur.fetchall()[0][0]
			if appid != APP_ID:
				print('WARNING: This might not be a FileMapper database!')
		self.conn.executescript(generate_op_sql())
		if fspath is None:
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
		self.extent_types_to_show = None

	def __del__(self):
		'''Destroy database object.'''
		if self.conn is not None:
			self.conn.close()

	def start_update(self):
		'''Start an update process.'''
		self.conn.execute('BEGIN TRANSACTION;')
		pass

	def finish_update(self):
		'''End the update process.'''
		self.conn.execute('END TRANSACTION;')
		pass

	def clear_database(self):
		'''Erase the database and prepare it for new data.'''
		if self.fspath is None:
			raise ValueError('fspath must be specified.')
		self.conn.executescript(generate_schema_sql())

	def collect_fs_stats(self):
		'''Store filesystem stats in the database.'''
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

	def finalize_fs_stats(self):
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
		self.conn.execute('INSERT OR REPLACE INTO inode_t VALUES(?, ?, NULL, NULL);', \
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
		self.bytes_per_cell = int(float(self.fs.total_bytes) / self.overview_len)

	def generate_overview(self, length):
		'''Generate the overview for a given length.'''
		if length < 1:
			raise ValueError('Cannot create overviews of negative length.')
		self.query_summary()
		bytes_per_cell = float(self.fs.total_bytes) / length
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size

		t0 = datetime.datetime.today()
		overview = [overview_block(self.extent_types_to_show) for x in range(0, length)]
		t1 = datetime.datetime.today()
		cur.execute('SELECT p_off, p_end, type FROM extent_t;')
		t2 = datetime.datetime.today()
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for (e_p_off, e_p_end, e_type) in rows:
				start_cell = int(e_p_off / bytes_per_cell)
				end_cell = int(e_p_end / bytes_per_cell)
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
						overview[i].symlinks += 1
		t3 = datetime.datetime.today()
		print_times('generate_overview', [t0, t1, t2, t3])
		return overview

	def cache_overview(self, length):
		'''Generate and cache overview data.'''
		# Generate the data.
		length = int(length)
		overview = self.generate_overview(length)

		# Try to stuff it in the database.
		try:
			if not self.writable:
				raise Exception('Read-only database.')
			self.start_update()
			cur = self.conn.cursor()
			cur.arraysize = self.result_batch_size
			t0 = datetime.datetime.today()
			qstr = 'INSERT OR REPLACE INTO overview_t VALUES (?, ?, ?, ?, ?, ?, ?, ?);'
			qarg = [(length, i, overview[i].files, overview[i].dirs, \
				 overview[i].mappings, overview[i].metadata, \
				 overview[i].xattrs, overview[i].symlinks) \
				 for i in range(0, length)]
			cur.executemany(qstr, qarg)
			self.finish_update()

			t1 = datetime.datetime.today()
			print_times('store_overview', [t0, t1])
		except Exception as e:
			print(e)
			pass
		return overview

	def query_overview(self):
		'''Generate an overview report.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size

		# Do we already have it in the database?
		qstr = 'SELECT COUNT(cell_no) FROM overview_t WHERE length = ?'
		qarg = [self.overview_len]
		cur.execute(qstr, qarg)
		if cur.fetchall()[0][0] == self.overview_len:
			t0 = datetime.datetime.today()
			qstr = 'SELECT files, dirs, mappings, metadata, xattrs, symlinks FROM overview_t WHERE length = ?'
			qarg = [self.overview_len]
			cur.execute(qstr, qarg)
			while True:
				rows = cur.fetchmany()
				if len(rows) == 0:
					break
				for r in rows:
					yield overview_block(self.extent_types_to_show, \
							r[0], r[1], \
							r[2], r[3], r[4], \
							r[5])
			t1 = datetime.datetime.today()
			print_times('cached_overview', [t0, t1])
			return

		# Generate and cache it, then.
		for row in self.cache_overview(self.overview_len):
			yield row

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
		if self.extent_types_to_show is not None:
			if len(self.extent_types_to_show) == 0:
				return
			ets = list(self.extent_types_to_show)
			qstr += ' %s type IN (%s)' % (cond, ', '.join(['?' for x in ets]))
			cond = ' AND ('
		else:
			ets = []
		for r in ranges:
			if type(r) == int:
				qstr += ' %s (l_off <= ? AND l_off + length - 1 >= ?)' % cond
				cond = 'OR'
				qarg.append(r)
				qarg.append(r)
			else:
				qstr += ' %s (l_off <= ? AND l_off + length - 1 >= ?)' % cond
				cond = 'OR'
				qarg.append(r[1])
				qarg.append(r[0])
		if len(ets) > 0 and len(qarg) > 0:
			qstr += ')'
		qstr += ' ORDER BY path, l_off'
		cur.execute(qstr, ets + qarg)
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
		if self.extent_types_to_show is not None:
			if len(self.extent_types_to_show) == 0:
				return
			ets = list(self.extent_types_to_show)
			qstr += ' %s type IN (%s)' % (cond, ', '.join(['?' for x in ets]))
			cond = ' AND ('
		else:
			ets = []
		for r in ranges:
			if type(r) == int:
				qstr += ' %s (p_off <= ? AND p_end >= ?)' % cond
				cond = 'OR'
				qarg.append(r)
				qarg.append(r)
			else:
				qstr += ' %s (p_off <= ? AND p_end >= ?)' % cond
				cond = 'OR'
				qarg.append(r[1])
				qarg.append(r[0])
		if len(ets) > 0 and len(qarg) > 0:
			qstr += ')'
		qstr += ' ORDER BY path, l_off'
		#print(qstr, ets + qarg)
		t0 = datetime.datetime.today()
		cur.execute(qstr, ets + qarg)
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
		if self.extent_types_to_show is not None:
			if len(self.extent_types_to_show) == 0:
				return
			ets = list(self.extent_types_to_show)
			qstr += ' %s type IN (%s)' % (cond, ', '.join(['?' for x in ets]))
			cond = ' AND ('
		else:
			ets = []
		if '*' not in paths and '/*' not in paths:
			for p in paths:
				if p == self.fs.pathsep:
					p = ''
				if '*' in p:
					op = 'GLOB'
				else:
					op = '='
				qstr = qstr + ' %s path %s ?' % (cond, op)
				cond = 'OR'
				qarg.append(p)
			if len(ets) > 0 and len(qarg) > 0:
				qstr += ')'
		qstr += ' ORDER BY path, l_off'
		#print(qstr, ets + qarg)
		cur.execute(qstr, ets + qarg)
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
		if self.extent_types_to_show is not None:
			if len(self.extent_types_to_show) == 0:
				return
			ets = list(self.extent_types_to_show)
			qstr += ' %s type IN (%s)' % (cond, ', '.join(['?' for x in ets]))
			cond = ' AND ('
		else:
			ets = []
		for r in ranges:
			if type(r) == int:
				qstr += ' %s ino = ?' % cond
				cond = 'OR'
				qarg.append(r)
			else:
				qstr += ' %s ino BETWEEN ? AND ?' % cond
				cond = 'OR'
				qarg.append(r[0])
				qarg.append(r[1])
		if len(ets) > 0 and len(qarg) > 0:
			qstr += ')'
		qstr += ' ORDER BY path, l_off'
		cur.execute(qstr, ets + qarg)
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
		if self.extent_types_to_show is not None:
			if len(self.extent_types_to_show) == 0:
				return
			ets = list(self.extent_types_to_show)
			qstr += ' %s type IN (%s)' % (cond, ', '.join(['?' for x in ets]))
			cond = ' AND ('
		else:
			ets = []
		for r in ranges:
			if type(r) == int:
				qstr += ' %s length = ?' % cond
				cond = 'OR'
				qarg.append(r)
			else:
				qstr += ' %s length BETWEEN ? AND ?' % cond
				cond = 'OR'
				qarg.append(r[0])
				qarg.append(r[1])
		if len(ets) > 0 and len(qarg) > 0:
			qstr += ')'
		qstr += ' ORDER BY path, l_off'
		#print(qstr, ets + qarg)
		cur.execute(qstr, ets + qarg)
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
		all_types = set(extent_types)
		cond = 'WHERE'
		if set(types) != all_types:
			qstr += ' %s type IN (%s)' % (cond, ', '.join(['?' for x in types]))
			qarg = types
		qstr += ' ORDER BY path, l_off'
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
		if '*' not in paths and '/*' not in paths:
			for p in paths:
				if p == self.fs.pathsep:
					p = ''
				if '*' in p:
					op = 'GLOB'
				else:
					op = '='
				qstr += ' %s path_t.path %s ?' % (cond, op)
				cond = 'OR'
				qarg.append(p)
			if len(qarg) > 0:
				qstr += ')'
		qstr += ' ORDER by dentry_t.name'
		cur.execute(qstr, qarg)
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for row in rows:
				yield dentry(row[0], row[1], row[2])

	def set_extent_types_to_show(self, types):
		'''Restrict the overview and queries to showing these types of extents.'''
		self.extent_types_to_show = types

	def get_extent_types_to_show(self):
		'''Retrieve the types of extents to show in the overview and queries.'''
		return self.extent_types_to_show

	def count_inode_extents(self, ino, type):
		'''Count the number of extents of a given type and attached to an inode.'''
		cur = self.conn.cursor()
		qstr = 'SELECT COUNT(extent_t.p_off) FROM extent_t WHERE extent_t.ino = ? AND extent_t.type = ?'
		qarg = [ino, primary_extent_type_for_inode[type]]
		cur.execute(qstr, qarg)
		r = cur.fetchall()
		if r is not None and len(r) == 1 and len(r[0]) == 1:
			return r[0][0]
		return None

	def analyze_inode_extents(self, ino, type, ignore_sparse = False):
		'''Calculate the average distance and extent count for a given inode.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		def get_extents():
			qstr = 'SELECT p_off, l_off, length FROM extent_t WHERE extent_t.ino = ? AND extent_t.type = ?'
			qarg = [ino, primary_extent_type_for_inode[type]]
			#print(qstr, qarg)
			cur.execute(qstr, qarg)
			while True:
				rows = cur.fetchmany()
				if len(rows) == 0:
					return
				for row in rows:
					yield row

		extents = p_dist = l_dist = 0
		last_poff = last_loff = None
		for (p_off, l_off, length) in get_extents():
			if last_poff is not None:
				p_dist += abs(p_off - last_poff)
				if not ignore_sparse:
					l_dist += l_off - last_loff
			extents += 1
			p_dist += length
			l_dist += length
			last_poff = p_off
			last_loff = l_off
		if extents == 0:
			return (0, 0.0)
		return (extents, float(p_dist) / l_dist)

	def query_inodes_stats(self, ino_sql, ino_sql_args, resolve_paths, count_extents, analyze_extents):
		'''Retrieve the inode statistic data, given a query to select inodes.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size

		# Figure out what to do with the inode sql
		if ino_sql is not None and ino_sql != '':
			isql = 'WHERE inode_t.ino IN (%s)' % ino_sql
			rpsql = 'WHERE path_t.ino IN (%s)' % ino_sql
			iargs = ino_sql_args
		else:
			isql = ''
			rpsql = ''
			iargs = []

		# Do we need to reverse-map inodes to paths?
		path_map = {}
		if resolve_paths:
			qstr = 'SELECT path, ino FROM path_t %s' % rpsql
			cur.execute(qstr, iargs)
			while True:
				rows = cur.fetchmany()
				if len(rows) == 0:
					break
				for (path, ino) in rows:
					if ino in path_map:
						path_map[ino].append(path)
					else:
						path_map[ino] = [path]

		# Go for the main query
		qstr = 'SELECT ino, type, nr_extents, travel_score FROM inode_t %s' % isql
		cur.execute(qstr, iargs)
		upd = []
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for (ino, type, nr_extents, travel_score) in rows:
				nr_extents0 = nr_extents
				travel_score0 = travel_score
				if (travel_score is None or nr_extents is None) and analyze_extents:
					nr_extents, travel_score = self.analyze_inode_extents(ino, type)
				if nr_extents is None and count_extents:
					nr_extents = self.count_inode_extents(ino, type)
				if self.writable and \
				   ((nr_extents0 is None and nr_extents != nr_extents0) or \
				    (travel_score0 is None and travel_score != travel_score0)):
					upd.append((nr_extents, travel_score, ino))
				p = path_map[ino] if ino in path_map else []
				yield file_stats(p, ino, type, nr_extents, travel_score)
		if len(upd) > 0:
			cur.execute('BEGIN TRANSACTION')
			qstr = 'UPDATE inode_t SET nr_extents = ?, travel_score = ? WHERE ino = ?'
			cur.executemany(qstr, upd)
			cur.execute('END TRANSACTION')

	def clear_calculated_values(self):
		'''Remove all calculated values from the database.'''
		if not self.writable:
			raise Exception('Database is not writable.')
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		qstr = 'UPDATE inode_t SET nr_extents = NULL, travel_score = NULL'
		cur.execute('BEGIN TRANSACTION')
		cur.execute(qstr)
		cur.execute('END TRANSACTION')

	def query_paths_stats(self, paths, resolve_paths = False, count_extents = False, analyze_extents = False):
		'''Retrieve the inode data for the given path specifications.'''
		qstr = 'SELECT DISTINCT ino FROM path_t'
		qarg = []
		cond = ' WHERE '
		for p in paths:
			if p == self.fs.pathsep:
				p = ''
			if '*' in p:
				op = 'GLOB'
			else:
				op = '='
			qstr += ' %s path %s ?' % (cond, op)
			cond = 'OR'
			qarg.append(p)
		if '*' in paths or '/*' in paths:
			qstr = qarg = None
		for x in self.query_inodes_stats(qstr, qarg, resolve_paths, count_extents, analyze_extents):
			yield x

	def query_dir_contents_stats(self, paths, resolve_paths = False, count_extents = False, analyze_extents = False):
		'''Retrieve the inode data of all files in the given directories.'''
		qstr = 'SELECT DISTINCT name_ino FROM dir_t, path_t WHERE dir_t.dir_ino = path_t.ino'
		qarg = []
		cond = ' AND '
		for p in paths:
			if p == self.fs.pathsep:
				p = ''
			if '*' in p:
				op = 'GLOB'
			else:
				op = '='
			qstr += ' %s path_t.path %s ?' % (cond, op)
			cond = 'OR'
			qarg.append(p)
		if '*' in paths or '/*' in paths:
			qstr = qarg = None
		for x in self.query_inodes_stats(qstr, qarg, resolve_paths, count_extents, analyze_extents):
			yield x

class fiemap_db(fmdb):
	'''FileMapper database based on FIEMAP.'''
	def __init__(self, fspath, dbpath, dbwrite):
		if fspath is None:
			raise ValueError('Please specify a FS path.')
		super(fiemap_db, self).__init__(fspath, dbpath, dbwrite)

	def is_stale(self):
		'''Decide if the FS should be re-analyzed.'''
		if not self.writable:
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
		if not force and not self.is_stale():
			return
		self.clear_database()
		self.start_update()
		self.collect_fs_stats()
		fiemap.walk_fs(self.fspath,
			self.insert_dir,
			self.insert_inode,
			self.insert_extent)
		self.finish_update()
		self.finalize_fs_stats()
