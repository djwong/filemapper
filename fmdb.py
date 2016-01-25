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
from dateutil import tz

# Debugging stuff
def print_times(label, times):
	'''Print some profiling data.'''
	l = ['%0.2fs' % (times[i] - times[i - 1]).total_seconds() for i in range(1, len(times))]
	print('%s: %.02fs (%s)' % (label, (times[-1] - times[0]).total_seconds(), ', '.join(l)))

def print_sql(qstr, qarg = None):
	'''Print some debug stuff.'''
	return
	if qarg is None:
		print(qstr)
	else:
		print(qstr, qarg)

# SQL generator modes
FMDB_EXTENT_SQL	= 1
FMDB_INODE_SQL	= 2

# FS summary
fs_summary = namedtuple('fs_summary', ['path', 'block_size', 'frag_size',
				       'total_bytes', 'free_bytes',
				       'avail_bytes', 'total_inodes',
				       'free_inodes', 'avail_inodes',
				       'extents', 'pathsep', 'inodes',
				       'date', 'fstype', 'extents_bytes'])

# Inode type codes
INO_TYPE_FILE		= 0
INO_TYPE_DIR		= 1
INO_TYPE_METADATA	= 2
INO_TYPE_SYMLINK	= 3

inode_types = {
	INO_TYPE_FILE:		'f',
	INO_TYPE_DIR:		'd',
	INO_TYPE_METADATA:	'm',
	INO_TYPE_SYMLINK:	's',
}

inode_types_long = {
	INO_TYPE_FILE:		'File',
	INO_TYPE_DIR:		'Directory',
	INO_TYPE_METADATA:	'Metadata',
	INO_TYPE_SYMLINK:	'Symbolic Link',
}

inode_type_strings = {inode_types[i]: i for i in inode_types}

# Inode stat data; use a named tuple to reduce memory use
inode_stats = namedtuple('inode_stats', ['fs', 'path', 'ino', 'itype',
					 'nr_extents', 'travel_score', 'atime',
					 'crtime', 'ctime', 'mtime', 'size'])

def inode_typestr(self):
	'''Return a string representing the inode type.'''
	return inode_types_long[self.itype]

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

primary_extent_type_for_inode = {
	INO_TYPE_FILE:		EXT_TYPE_FILE,
	INO_TYPE_DIR:		EXT_TYPE_DIR,
	INO_TYPE_METADATA:	EXT_TYPE_METADATA,
	INO_TYPE_SYMLINK:	EXT_TYPE_SYMLINK,
}

extent_type_strings = {extent_types[i]: i for i in extent_types}
extent_type_strings_long = {extent_types_long[i]: i for i in extent_types_long}

all_extent_types = set(extent_types.keys())

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

# An extent; named tuples consume far less memory...
extent = namedtuple('extent', ['path', 'ino', 'p_off', 'l_off', 'length', 'flags', 'type'])

def extent_flagstr(self):
	'''Generate a string representing an extent's flags.'''
	return extent_flags_to_str(self.flags)

def extent_typestr(self):
	'''Generate a string representing an extent's type.'''
	return extent_types_long[self.type]

# Directory entry
dentry = namedtuple('dentry', ['name', 'ino', 'type'])

def dentry_typestr(self):
	'''Generate a string representation of a dentry's type.'''
	return inode_types_long[self.type]

# Database strings
APP_ID = 61272
PAGE_SIZE = 65536
CACHE_SIZE = 256 * 1048576
CACHE_PAGES = CACHE_SIZE / PAGE_SIZE
def generate_op_sql():
	'''Generate per-connection database settings.'''
	return '''PRAGMA cache_size = %d;
PRAGMA mmap_size = 1073741824;
PRAGMA journal_mode = MEMORY;
PRAGMA synchronous = OFF;
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA case_sensitive_like = ON;''' % CACHE_PAGES

def generate_schema_sql():
	'''Generate the database schema.'''
	a = ['''PRAGMA page_size = %d;
PRAGMA application_id = %d;
PRAGMA journal_mode = MEMORY;
DROP VIEW IF EXISTS dentry_t;
DROP VIEW IF EXISTS path_extent_v;
DROP VIEW IF EXISTS path_inode_v;
DROP TABLE IF EXISTS overview_t;
DROP TABLE IF EXISTS dentry_t;
DROP TABLE IF EXISTS extent_t;
DROP TABLE IF EXISTS extent_type_t;
DROP TABLE IF EXISTS inode_t;
DROP TABLE IF EXISTS inode_type_t;
DROP TABLE IF EXISTS path_t;
DROP TABLE IF EXISTS dir_t;
DROP TABLE IF EXISTS fs_t;
CREATE TABLE fs_t(path TEXT PRIMARY KEY NOT NULL, block_size INTEGER NOT NULL, frag_size INTEGER NOT NULL, total_bytes INTEGER NOT NULL, free_bytes INTEGER NOT NULL, avail_bytes INTEGER NOT NULL, total_inodes INTEGER NOT NULL, free_inodes INTEGER NOT NULL, avail_inodes INTEGER NOT NULL, max_len INTEGER NOT NULL, timestamp INTEGER NOT NULL, finished INTEGER NOT NULL, path_separator TEXT NOT NULL, fstype TEXT);
CREATE TABLE inode_type_t(id INTEGER PRIMARY KEY UNIQUE, code TEXT NOT NULL);
CREATE TABLE inode_t(ino INTEGER PRIMARY KEY UNIQUE NOT NULL, type INTEGER NOT NULL, nr_extents INTEGER, travel_score REAL, atime INTEGER, crtime INTEGER, ctime INTEGER, mtime INTEGER, size INTEGER, FOREIGN KEY(type) REFERENCES inode_type_t(id));
CREATE TABLE dir_t(dir_ino INTEGER NOT NULL, name TEXT NOT NULL, name_ino INTEGER NOT NULL, FOREIGN KEY(dir_ino) REFERENCES inode_t(ino), FOREIGN KEY(name_ino) REFERENCES inode_t(ino));
CREATE TABLE path_t(path TEXT PRIMARY KEY UNIQUE NOT NULL, ino INTEGER NOT NULL, FOREIGN KEY(ino) REFERENCES inode_t(ino));
CREATE TABLE extent_type_t (id INTEGER PRIMARY KEY UNIQUE, code TEXT NOT NULL);
CREATE TABLE extent_t(ino INTEGER NOT NULL, p_off INTEGER NOT NULL, l_off INTEGER, flags INTEGER NOT NULL, length INTEGER NOT NULL, type INTEGER NOT NULL, p_end INTEGER NOT NULL, FOREIGN KEY(ino) REFERENCES inode_t(ino), FOREIGN KEY(type) REFERENCES extent_type_t(id));
CREATE TABLE overview_t(length INTEGER NOT NULL, cell_no INTEGER NOT NULL, files INTEGER NOT NULL, dirs INTEGER NOT NULL, mappings INTEGER NOT NULL, metadata INTEGER NOT NULL, xattrs INTEGER NOT NULL, symlinks INTEGER NOT NULL, CONSTRAINT pk_overview PRIMARY KEY (length, cell_no));
CREATE VIEW path_extent_v AS SELECT path_t.path, extent_t.p_off, extent_t.l_off, extent_t.length, extent_t.flags, extent_t.type, extent_t.p_end, extent_t.ino FROM extent_t, path_t WHERE extent_t.ino = path_t.ino;
CREATE VIEW path_inode_v AS SELECT path_t.path, inode_t.ino, inode_t.type, inode_t.nr_extents, inode_t.travel_score, inode_t.atime, inode_t.crtime, inode_t.ctime, inode_t.mtime, inode_t.size FROM path_t, inode_t WHERE inode_t.ino = path_t.ino;
CREATE VIEW dentry_t AS SELECT dir_t.dir_ino, dir_t.name, dir_t.name_ino, inode_t.type FROM dir_t, inode_t WHERE dir_t.name_ino = inode_t.ino;''' % (PAGE_SIZE, APP_ID)]
	y = ["INSERT INTO inode_type_t VALUES (%d, '%s');" % (x, inode_types[x]) for x in sorted(inode_types.keys())]
	z = ["INSERT INTO extent_type_t VALUES (%d, '%s');" % (x, extent_types[x]) for x in sorted(extent_types.keys())]
	return '\n'.join(a + y + z)

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
CREATE INDEX inode_ino_i ON inode_t(ino);
CREATE INDEX extent_type_i ON extent_t(type);
PRAGMA foreign_key_check;
'''

### Date handling functions

tz_gmt = tz.gettz('UTC')
tz_local = tz.gettz()
def utctimestamp_to_datetime(t):
	'''Convert a UTC timestamp to a date object.'''
	if t is None:
		return None
	return datetime.datetime.utcfromtimestamp(t).replace(tzinfo = tz_gmt)

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

## Main database object
#
# Manage FM data.  That means handling the overview, updating data,
# calculating missing pieces, and (mostly) making queries.
#
# Main query plan:
# a. SELECT <extentcolumns> FROM path_extent_v WHERE	<extentcriteria>;
# b. SELECT <inodecolumns> FROM path_inode_v WHERE	<inodecriteria>;
#
# c. SELECT <inodecolumns> FROM path_inode_v WHERE	ino IN (SELECT DISTINCT ino FROM extent_t WHERE <extentcriteria>);
# d. SELECT <extentcolumns> FROM path_extent_v WHERE	ino IN (SELECT DISTINCT ino FROM inode_t WHERE <inodecriteria>);
#
# e. SELECT <inodecolumns> FROM path_inode_v WHERE	path GLOB <fubar>;
# f. SELECT <extentcolumns> FROM path_extent_v WHERE	path GLOB <fubar>;
#
# g. SELECT <inodecolumns> FROM path_inode_v WHERE	ino IN (SELECT DISTINCT ino FROM extent_t WHERE <extenttype>) AND path GLOB <fubar>;
# h. SELECT <extentcolumns> FROM path_extent_v WHERE	<extenttype> AND path GLOB <fubar>;
#
#a/c: overview/poff/loff/length/type/flags
#f/e: paths
#d/b: nrextents/travelscore/times/size
#h/g: paths w/ extents to show
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

	## Functions for updating the database

	def start_update(self):
		'''Start an update process.'''
		print_sql('BEGIN TRANSACTION')
		self.conn.execute('BEGIN TRANSACTION;')
		pass

	def finish_update(self):
		'''End the update process.'''
		print_sql('END TRANSACTION')
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
		qstr = 'INSERT INTO fs_t VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)'
		nowgmt = datetime.datetime.utcnow().replace(microsecond = 0, tzinfo = tz_gmt)
		qarg = (self.fspath, statfs.f_bsize, \
			statfs.f_frsize, \
			statfs.f_blocks * statfs.f_bsize, \
			statfs.f_bfree * statfs.f_bsize, \
			statfs.f_bavail * statfs.f_bsize, \
			statfs.f_files, statfs.f_ffree, \
			statfs.f_favail, statfs.f_namemax, \
			int(nowgmt.timestamp()), \
			os.sep, 'fiemap')
		print_sql(qstr, qarg)
		self.conn.execute(qstr, qarg)
		self.query_summary()

	def finalize_fs_stats(self):
		'''Finish updating a database.'''
		self.conn.executescript(generate_index_sql())
		self.conn.execute('UPDATE fs_t SET finished = 1 WHERE path = ?;', (self.fspath,))
		self.conn.commit()

		cur = self.conn.cursor()
		cur.execute('SELECT MAX(p_end) FROM extent_t')
		max_extent_byte = cur.fetchall()[0][0]

		cur.execute('SELECT total_bytes FROM fs_t')
		total_bytes = cur.fetchall()[0][0]

		if total_bytes <= max_extent_byte:
			cur.execute('UPDATE fs_t SET total_bytes = ? WHERE path = ?', (max_extent_byte + 1, self.fspath))
			self.conn.commit()
			self.fs = None
			self.query_summary()

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
		qstr = 'INSERT INTO dir_t VALUES(?, ?, ?)'
		qarg = [(root.st_ino, name, stat.st_ino) for name, stat in dentries]
		print_sql(qstr, qarg)
		self.conn.executemany(qstr, qarg)

	def insert_inode(self, xstat, path):
		'''Insert an inode record into the database.'''
		if path == '/' or path == self.fs.pathsep:
			raise ValueError("'%s' is an invalid path.  Check code." % path)
		if stat.S_ISDIR(xstat.st_mode):
			xtype = INO_TYPE_DIR
		else:
			xtype = INO_TYPE_FILE
		qstr = 'INSERT OR REPLACE INTO inode_t VALUES(?, ?, NULL, NULL, ?, ?, ?, ?, ?)'
		qarg = (xstat.st_ino, xtype, xstat.st_atime, None, \
			xstat.st_ctime, xstat.st_mtime, xstat.st_size)
		print_sql(qstr, qarg)
		self.conn.execute(qstr, qarg)
		qstr = 'INSERT INTO path_t VALUES(?, ?)'
		qarg = (path, xstat.st_ino)
		print_sql(qstr, qarg)
		self.conn.execute(qstr, qarg)

	def insert_extent(self, stat, extent, is_xattr):
		'''Insert an extent record into the database.'''
		code = stmode_to_type(stat, is_xattr)
		if extent.flags & (fiemap.FIEMAP_EXTENT_UNKNOWN | \
				   fiemap.FIEMAP_EXTENT_DELALLOC):
			return
		qstr = 'INSERT INTO extent_t VALUES(?, ?, ?, ?, ?, ?, ?)'
		qarg = (stat.st_ino, extent.physical, extent.logical, \
			extent.flags, extent.length, \
			code, extent.physical + extent.length - 1)
		print_sql(qstr, qarg)
		self.conn.execute(qstr, qarg)

	## Overview control

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
				if start_cell >= len(overview) or \
				   end_cell >= len(overview):
					raise ValueError('Extent goes past end of FS? p_off=%d p_end=%d' % (e_p_off, e_p_end))
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
			print_sql(qstr, [])
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

	## Show or hide different types of extents in the queries

	def set_extent_types_to_show(self, types):
		'''Restrict the overview and queries to showing these types of extents.'''
		if types == all_extent_types:
			types = None
		self.extent_types_to_show = types

	def get_extent_types_to_show(self):
		'''Retrieve the types of extents to show in the overview and queries.'''
		t = self.extent_types_to_show
		return all_extent_types if t is None else t

	## FS Summary

	def query_summary(self):
		'''Fetch the filesystem summary.'''
		if self.fs is not None:
			return self.fs

		cur = self.conn.cursor()
		etypes = ', '.join(map(str, [EXT_TYPE_FILE, EXT_TYPE_DIR, EXT_TYPE_XATTR, EXT_TYPE_SYMLINK]))

		cur.execute('SELECT COUNT(ino) FROM extent_t WHERE type IN (%s)' % etypes)
		rows = cur.fetchall()
		extents = rows[0][0]

		cur.execute('SELECT SUM(length) FROM extent_t WHERE type IN (%s)' % etypes)
		rows = cur.fetchall()
		extent_bytes = rows[0][0]
		if extent_bytes is None:
			extent_bytes = 0

		cur.execute('SELECT COUNT(ino) FROM inode_t WHERE ino IN (SELECT DISTINCT ino FROM extent_t WHERE extent_t.type IN (%s))' % etypes)
		rows = cur.fetchall()
		inodes = rows[0][0]
		#print(extents, inodes)

		cur.execute('SELECT path, block_size, frag_size, total_bytes, free_bytes, avail_bytes, total_inodes, free_inodes, avail_inodes, path_separator, timestamp, fstype FROM fs_t;')
		rows = cur.fetchall()
		assert len(rows) == 1
		res = rows[0]

		# In the old days, the date was a string instead of Epoch seconds
		if type(res[10]) == str:
			d = datetime.datetime.strptime(res[10], '%Y-%m-%d %H:%M:%S').replace(tzinfo = tz_gmt)
		else:
			d = utctimestamp_to_datetime(res[10])
		self.fs = fs_summary(res[0], int(res[1]), int(res[2]), \
				 int(res[3]), int(res[4]), int(res[5]), \
				 int(res[6]), int(res[7]), int(res[8]),
				 int(extents), res[9], int(inodes), d, res[11], int(extent_bytes))
		return self.fs

	## Querying extents and inodes with extents that happen to overlap a range

	def __query_extent_ranges_sql(self, ranges, mode, field_start, field_end):
		'''Generate SQL criteria for an extent range being within a range.'''
		if len(ranges) == 0:
			return (None, None)
		qstr = ''
		qarg = []
		close_paren = False
		cond = ''
		ets = self.extent_types_to_show
		if ets is not None:
			if len(ets) == 0:
				return (None, None)
			qstr += ' %s type IN (%s)' % (cond, ', '.join(['?' for x in ets]))
			qarg += list(ets)
			cond = ' AND ('
			close_paren = True
		for r in ranges:
			if type(r) == int:
				qstr += ' %s (%s <= ? AND %s >= ?)' % (cond, field_start, field_end)
				cond = 'OR'
				qarg.append(r)
				qarg.append(r)
			else:
				qstr += ' %s (%s <= ? AND %s >= ?)' % (cond, field_start, field_end)
				cond = 'OR'
				qarg.append(r[1])
				qarg.append(r[0])
		if close_paren:
			qstr += ')'
		if mode == FMDB_INODE_SQL:
			qstr = 'ino IN (SELECT DISTINCT ino FROM extent_t WHERE %s)' % qstr
		return (qstr, qarg)

	def query_poff_range(self, ranges, **kwargs):
		'''Query extents covering ranges of physical bytes.'''
		qstr, qarg = self. __query_extent_ranges_sql(ranges, FMDB_EXTENT_SQL, 'p_off', 'p_end')
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_poff_range_inodes(self, ranges, **kwargs):
		'''Query inodes with extents covering ranges of physical bytes.'''
		qstr, qarg = self. __query_extent_ranges_sql(ranges, FMDB_INODE_SQL, 'p_off', 'p_end')
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	def query_loff_range(self, ranges, **kwargs):
		'''Query extents covering ranges of logical bytes.'''
		qstr, qarg = self. __query_extent_ranges_sql(ranges, FMDB_EXTENT_SQL, 'l_off', 'l_off + length - 1')
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_loff_range_inodes(self, ranges, **kwargs):
		'''Query inodes with extents covering ranges of logical bytes.'''
		qstr, qarg = self. __query_extent_ranges_sql(ranges, FMDB_INODE_SQL, 'l_off', 'l_off + length - 1')
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	## Querying extents and inodes by path

	def __query_paths_sql(self, paths, mode):
		'''Generate SQL criteria for data appearing at a given path.'''
		if len(paths) == 0:
			return (None, None)
		ets_str = None
		ets_arg = []
		ets = self.extent_types_to_show
		if ets is not None:
			if len(ets) == 0:
				return (None, None)
			ets_str = 'type IN (%s)' % (', '.join(['?' for x in ets]))
			ets_arg = list(ets)
		glob_str = None
		glob_arg = []
		cond = ''
		if '*' not in paths and '%s*' % self.fs.pathsep not in paths:
			glob_str = ''
			for p in paths:
				if p == self.fs.pathsep:
					p = ''
				if '*' in p:
					op = 'GLOB'
				else:
					op = '='
				glob_str += '%spath %s ?' % (cond, op)
				cond = ' OR '
				glob_arg.append(p)
		if ets_str is None:
			# glob_str can be set or None
			return (glob_str, glob_arg)
		if glob_str is None: # and ets is not None
			if mode == FMDB_INODE_SQL:
				ets_str = 'ino IN (SELECT DISTINCT INO FROM extent_t WHERE %s)' % ets_str
			return (ets_str, ets_arg)
		# ets and glob are both set
		if mode == FMDB_INODE_SQL:
			qstr = 'ino IN (SELECT DISTINCT INO FROM extent_t WHERE %s) AND (%s)' % (ets_str, glob_str)
		else:
			qstr = '%s AND (%s)' % (ets_str, glob_str)
		return (qstr, ets_arg + glob_arg)

	def query_paths(self, paths, **kwargs):
		'''Query extents with inodes appearing at the given paths.'''
		qstr, qarg = self.__query_paths_sql(paths, FMDB_EXTENT_SQL)
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_paths_inodes(self, paths, **kwargs):
		'''Query inodes appearing at the given paths.'''
		qstr, qarg = self.__query_paths_sql(paths, FMDB_INODE_SQL)
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	## Querying extents and inodes with extents with a field inside a range

	def __query_extent_range_sql(self, ranges, mode, field):
		'''Generate SQL criteria for an extent field being within a range.'''
		if len(ranges) == 0:
			return (None, None)
		qstr = ''
		qarg = []
		close_paren = False
		cond = ''
		ets = self.extent_types_to_show
		if ets is not None:
			if len(ets) == 0:
				return (None, None)
			qstr += ' %s type IN (%s)' % (cond, ', '.join(['?' for x in ets]))
			qarg += list(ets)
			cond = ' AND ('
			close_paren = True
		for r in ranges:
			if type(r) == int:
				qstr += ' %s %s = ?' % (cond, field)
				cond = 'OR'
				qarg.append(r)
			else:
				qstr += ' %s %s BETWEEN ? AND ?' % (cond, field)
				cond = 'OR'
				qarg.append(r[0])
				qarg.append(r[1])
		if close_paren:
			qstr += ')'
		if mode == FMDB_INODE_SQL:
			qstr = 'ino IN (SELECT DISTINCT ino FROM extent_t WHERE %s)' % qstr
		return (qstr, qarg)

	def query_inums(self, ranges, **kwargs):
		'''Query extents with inodes having ranges of inode numbers.'''
		qstr, qarg = self.__query_extent_range_sql(ranges, FMDB_EXTENT_SQL, 'ino')
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_inums_inodes(self, ranges, **kwargs):
		'''Query inodes having ranges of inode numbers.'''
		qstr, qarg = self.__query_inode_range_sql(ranges, FMDB_INODE_SQL, 'ino')
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	def query_lengths(self, ranges, **kwargs):
		'''Query extents having ranges of extent lengths.'''
		qstr, qarg = self.__query_extent_range_sql(ranges, FMDB_EXTENT_SQL, 'length')
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_lengths_inodes(self, ranges, **kwargs):
		'''Query inodes with extents having ranges of extent lengths.'''
		qstr, qarg = self.__query_extent_range_sql(ranges, FMDB_INODE_SQL, 'length')
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	## Querying extents and inodes with extents with enumerable quantities

	def __query_extent_types_sql(self, types, mode):
		'''Generate SQL criteria for extents having one of a set of type codes.'''
		if set(types) == set(extent_types):
			qstr = None
			qarg = []
		elif len(types) == 1:
			qstr = 'type = ?'
			qarg = types
		else:
			qstr = 'type IN (%s)' % (', '.join(['?' for x in types]))
			qarg = types
		if mode == FMDB_INODE_SQL and qstr is not None:
			qstr = 'ino IN (SELECT DISTINCT ino FROM extent_t WHERE %s)' % qstr
		return (qstr, qarg)

	def query_extent_types(self, types, **kwargs):
		'''Query extents having a set of type codes.'''
		if len(types) == 0:
			return
		qstr, qarg = self.__query_extent_types_sql(types, FMDB_EXTENT_SQL)
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_extent_types_inodes(self, types, **kwargs):
		'''Query inodes with extents having a set of type codes.'''
		if len(types) == 0:
			return
		qstr, qarg = self.__query_extent_types_sql(types, FMDB_INODE_SQL)
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	def __query_extent_flags_sql(self, flags, exact, mode):
		'''Generate SQL criteria for extents having a set of flags.'''
		if exact:
			qstr = 'flags = ?'
		else:
			qstr = 'flags & ? > 0'
		qarg = [flags]
		ets = self.extent_types_to_show
		if ets is not None:
			if len(ets) == 0:
				return (None, None)
			qarg += list(ets)
			qstr += ' AND type IN (%s)' % (', '.join(['?' for x in ets]))
		if mode == FMDB_INODE_SQL:
			qstr = 'ino IN (SELECT DISTINCT ino FROM extent_t WHERE %s)' % qstr
		return (qstr, qarg)

	def query_extent_flags(self, flags, exact = True, **kwargs):
		'''Query extents having a set of type codes.'''
		qstr, qarg = self.__query_extent_flags_sql(flags, exact, FMDB_EXTENT_SQL)
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_extent_flags_inodes(self, flags, exact = True, **kwargs):
		'''Query inodes with extents having a set of type codes.'''
		qstr, qarg = self.__query_extent_flags_sql(flags, exact, FMDB_INODE_SQL)
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	## Querying parts of the FS tree.  We do not hide by extent type here.

	def query_root(self):
		'''Retrieve the directory entry for root.'''
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
		'''Query all directory entries available under the given paths.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		qstr = 'SELECT dentry_t.name, dentry_t.name_ino, dentry_t.type FROM dentry_t, path_t WHERE dentry_t.dir_ino = path_t.ino'
		qarg = []
		cond = ' AND ('
		close_paren = False
		if '*' not in paths and '%s*' % self.fs.pathsep not in paths:
			close_paren = True
			for p in paths:
				if p == self.fs.pathsep:
					p = ''
				if '*' in p:
					op = 'GLOB'
				else:
					op = '='
				qstr += '%spath_t.path %s ?' % (cond, op)
				cond = ' OR '
				qarg.append(p)
			if close_paren:
				qstr += ')'
		print_sql(qstr, qarg)
		cur.execute(qstr, qarg)
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for row in rows:
				yield dentry(row[0], row[1], row[2])

	## Query inode features

	def __query_inode_range_sql(self, ranges, mode, field):
		'''Generate SQL criteria for an inode field being within a range.'''
		if len(ranges) == 0:
			return (None, None)
		qstr = ''
		qarg = []
		close_paren = False
		cond = ''
		ets = self.extent_types_to_show
		if ets is not None:
			if len(ets) == 0:
				return (None, None)
			qstr += ' %s type IN (%s)' % (cond, ', '.join(['?' for x in ets]))
			qarg += list(ets)
			cond = ' AND ('
			close_paren = True
		for r in ranges:
			if type(r) == int:
				qstr += ' %s %s = ?' % (cond, field)
				cond = 'OR'
				qarg.append(r)
			else:
				qstr += ' %s %s BETWEEN ? AND ?' % (cond, field)
				cond = 'OR'
				qarg.append(r[0])
				qarg.append(r[1])
		if close_paren:
			qstr += ')'
		if mode == FMDB_EXTENT_SQL:
			qstr = 'ino IN (SELECT DISTINCT ino FROM inode_t WHERE %s)' % qstr
		return (qstr, qarg)

	def query_travel_scores(self, ranges, **kwargs):
		'''Query extents with inodes having ranges of inode travel scores.'''
		qstr, qarg = self.__query_inode_range_sql(ranges, FMDB_EXTENT_SQL, 'travel_score')
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_travel_scores_inodes(self, ranges, **kwargs):
		'''Query inodes having ranges of inode travel scores.'''
		qstr, qarg = self.__query_inode_range_sql(ranges, FMDB_INODE_SQL, 'travel_score')
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	def query_nr_extents(self, ranges, **kwargs):
		'''Query extents with inodes having ranges of inode primary extent counts.'''
		qstr, qarg = self.__query_inode_range_sql(ranges, FMDB_EXTENT_SQL, 'nr_extents')
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_nr_extents_inodes(self, ranges, **kwargs):
		'''Query inodes having ranges of inode primary extent counts.'''
		qstr, qarg = self.__query_inode_range_sql(ranges, FMDB_INODE_SQL, 'nr_extents')
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	def query_sizes(self, ranges, **kwargs):
		'''Query extents with inodes having ranges of inode sizes.'''
		qstr, qarg = self.__query_inode_range_sql(ranges, FMDB_EXTENT_SQL, 'size')
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_sizes_inodes(self, ranges, **kwargs):
		'''Query inodes having ranges of inode sizes.'''
		qstr, qarg = self.__query_inode_range_sql(ranges, FMDB_INODE_SQL, 'size')
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	## Query inode timestamps

	def __query_inode_times_sql(self, ranges, mode, field):
		'''Generate SQL criteria for an inode timestamp being within a range.'''
		if len(ranges) == 0:
			return (None, None)
		qstr = ''
		qarg = []
		close_paren = False
		cond = ''
		ets = self.extent_types_to_show
		if ets is not None:
			if len(ets) == 0:
				return (None, None)
			qstr += ' %s type IN (%s)' % (cond, ', '.join(['?' for x in ets]))
			qarg += list(ets)
			cond = ' AND ('
			close_paren = True
		for r in ranges:
			if type(r) == datetime.datetime:
				qstr += ' %s %s = ?' % (cond, field)
				cond = 'OR'
				qarg.append(r.timestamp())
			else:
				qstr += ' %s %s BETWEEN ? AND ?' % (cond, field)
				cond = 'OR'
				qarg.append(r[0].timestamp())
				qarg.append(r[1].timestamp())
		if close_paren:
			qstr += ')'
		if mode == FMDB_EXTENT_SQL:
			qstr = 'ino IN (SELECT DISTINCT ino FROM inode_t WHERE %s)' % qstr
		return (qstr, qarg)

	def query_atimes(self, ranges, **kwargs):
		'''Query extents with inodes having ranges of inode last access times.'''
		qstr, qarg = self.__query_inode_times_sql(ranges, FMDB_EXTENT_SQL, 'atime')
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_atimes_inodes(self, ranges, **kwargs):
		'''Query inodes having ranges of inode last access times.'''
		qstr, qarg = self.__query_inode_times_sql(ranges, FMDB_INODE_SQL, 'atime')
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	def query_ctimes(self, ranges, **kwargs):
		'''Query extents with inodes having ranges of inode change times.'''
		qstr, qarg = self.__query_inode_times_sql(ranges, FMDB_EXTENT_SQL, 'ctime')
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_ctimes_inodes(self, ranges, **kwargs):
		'''Query inodes having ranges of inode change times.'''
		qstr, qarg = self.__query_inode_times_sql(ranges, FMDB_INODE_SQL, 'ctime')
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	def query_mtimes(self, ranges, **kwargs):
		'''Query extents with inodes having ranges of inode data change times.'''
		qstr, qarg = self.__query_inode_times_sql(ranges, FMDB_EXTENT_SQL, 'mtime')
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_mtimes_inodes(self, ranges, **kwargs):
		'''Query inodes having ranges of inode data change times.'''
		qstr, qarg = self.__query_inode_times_sql(ranges, FMDB_INODE_SQL, 'mtime')
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	def query_crtimes(self, ranges, **kwargs):
		'''Query extents with inodes having ranges of inode creation times.'''
		qstr, qarg = self.__query_inode_times_sql(ranges, FMDB_EXTENT_SQL, 'crtime')
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_crtimes_inodes(self, ranges, **kwargs):
		'''Query inodes having ranges of inode creation times.'''
		qstr, qarg = self.__query_inode_times_sql(ranges, FMDB_INODE_SQL, 'crtime')
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	## Querying extents and inodes for inodes with enumerable quantities

	def __query_inode_types_sql(self, types, mode):
		'''Generate SQL criteria for inodes having one of a set of inode type codes.'''
		if set(types) == set(extent_types):
			qstr = None
			qarg = []
		elif len(types) == 1:
			qstr = 'type = ?'
			qarg = types
		else:
			qstr = 'type IN (%s)' % (', '.join(['?' for x in types]))
			qarg = types
		if mode == FMDB_EXTENT_SQL and qstr is not None:
			qstr = 'ino IN (SELECT DISTINCT ino FROM inode_t WHERE %s)' % qstr
		return (qstr, qarg)

	def query_inode_types(self, types, **kwargs):
		'''Query extents with inodes having one of a set of type codes.'''
		if len(types) == 0:
			return
		qstr, qarg = self.__query_inode_types_sql(types, FMDB_EXTENT_SQL)
		for x in self.query_extents(qstr, qarg, **kwargs):
			yield x

	def query_inode_types_inodes(self, types, **kwargs):
		'''Query inodes having one of a set of type codes.'''
		if len(types) == 0:
			return
		qstr, qarg = self.__query_inode_types_sql(types, FMDB_INODE_SQL)
		for x in self.query_inodes_stats(qstr, qarg, **kwargs):
			yield x

	## More in-depth inode analysis

	def calc_inode_stats(self, force = False):
		'''Analyze the extent/inode relations.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size

		t0 = datetime.datetime.now()
		ino_str = 'AND inode_t.ino IN (SELECT ino FROM inode_t WHERE travel_score IS NULL OR nr_extents IS NULL)' if not force else ''
		qstr = 'SELECT extent_t.ino, inode_t.type AS itype, extent_t.type AS etype, p_off, l_off, length FROM extent_t INNER JOIN inode_t WHERE extent_t.l_off IS NOT NULL AND extent_t.ino = inode_t.ino %s ORDER BY extent_t.ino, l_off' % ino_str
		print_sql(qstr)
		cur.execute(qstr)
		upd = []
		t1 = datetime.datetime.now()
		last_ino = None
		extents = p_dist = l_dist = 0
		last_poff = last_loff = None
		for ino, itype, etype, p_off, l_off, length in cur.fetchall():
			if etype != primary_extent_type_for_inode[itype]:
				continue
			if ino != last_ino:
				if last_ino is not None:
					travel_score = float(p_dist) / l_dist if l_dist != 0 else 0
					upd.append((extents, travel_score, last_ino))
				extents = p_dist = l_dist = 0
				last_poff = last_loff = None
				last_ino = ino
			if last_poff is not None:
				p_dist += abs(p_off - last_poff)
				l_dist += l_off - last_loff
			extents += 1
			p_dist += length
			l_dist += length
			last_poff = p_off + length - 1
			last_loff = l_off + length - 1
		if last_ino is not None:
			travel_score = float(p_dist) / l_dist if l_dist != 0 else 0
			upd.append((extents, travel_score, last_ino))
		t2 = datetime.datetime.now()
		if len(upd) > 0:
			print_sql('BEGIN TRANSACTION')
			cur.execute('BEGIN TRANSACTION')
			qstr = 'UPDATE inode_t SET nr_extents = ?, travel_score = ? WHERE ino = ?'
			print_sql(qstr, upd)
			cur.executemany(qstr, upd)
			print_sql('END TRANSACTION')
			cur.execute('END TRANSACTION')
		t3 = datetime.datetime.now()
		print_times('calc_inode_stats', [t0, t1, t2, t3])

	def clear_calculated_values(self):
		'''Remove all calculated values from the database.'''
		if not self.writable:
			raise Exception('Database is not writable.')
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		qstr = 'UPDATE inode_t SET nr_extents = NULL, travel_score = NULL'
		print_sql('BEGIN TRANSACTION')
		cur.execute('BEGIN TRANSACTION')
		print_sql(qstr)
		cur.execute(qstr)
		print_sql('END TRANSACTION')
		cur.execute('END TRANSACTION')

	## Return inodes or extents, given some query parameters.
	## (These are internal functions)

	def query_inodes_stats(self, ino_sql, ino_sql_args):
		'''Retrieve the inode statistic data, given a query to select inodes.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size

		t0 = datetime.datetime.now()
		# Figure out what to do with the inode sql
		if ino_sql is not None and ino_sql != '':
			isql = 'WHERE %s' % ino_sql
			qarg = ino_sql_args
		else:
			isql = ''
			qarg = []

		t1 = datetime.datetime.now()
		# Go for the main query
		qstr = 'SELECT path, ino, type, nr_extents, travel_score, atime, crtime, ctime, mtime, size FROM path_inode_v %s' % isql
		print_sql(qstr, qarg)
		cur.execute(qstr, qarg)
		upd = []
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for (p, ino, itype, nr_extents, travel_score, atime, \
			     crtime, ctime, mtime, size) in rows:
				yield inode_stats(self.fs, p, ino, itype, \
						  nr_extents, travel_score, \
						  utctimestamp_to_datetime(atime), \
						  utctimestamp_to_datetime(crtime), \
						  utctimestamp_to_datetime(ctime), \
						  utctimestamp_to_datetime(mtime), \
						  size)
		t2 = datetime.datetime.now()
		print_times('query_inode_stat', [t0, t1, t2])

	def query_extents(self, ext_sql, ext_sql_args):
		'''Retrieve extent data given some criteria.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size

		t0 = datetime.datetime.now()
		# Figure out what to do with the extent sql
		if ext_sql is not None and ext_sql != '':
			isql = 'WHERE %s' % ext_sql
			qarg = ext_sql_args
		else:
			isql = ''
			qarg = []

		t1 = datetime.datetime.now()
		# Go for the main query
		qstr = 'SELECT path, ino, p_off, l_off, length, flags, type FROM path_extent_v %s ORDER BY ino, l_off' % isql
		print_sql(qstr, qarg)
		cur.execute(qstr, qarg)
		upd = []
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for row in rows:
				yield extent(row[0], row[1], row[2], row[3], \
						row[4], row[5], row[6])
		t2 = datetime.datetime.now()
		print_times('query_extents', [t0, t1, t2])

	def query_avg_travel_score(self):
		'''Query the average travel score for all files and directories.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size

		qstr = 'SELECT AVG(travel_score) FROM inode_t WHERE type < %d' % INO_TYPE_METADATA
		print_sql(qstr)
		cur.execute(qstr)
		ret = cur.fetchall()[0][0]
		if ret is None:
			return 0.0
		return float(ret)

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
		t0 = datetime.datetime.now()
		self.clear_database()
		t1 = datetime.datetime.now()
		self.start_update()
		self.collect_fs_stats()
		t2 = datetime.datetime.now()
		fiemap.walk_fs(self.fspath,
			self.insert_dir,
			self.insert_inode,
			self.insert_extent)
		t3 = datetime.datetime.now()
		self.finish_update()
		self.finalize_fs_stats()
		t4 = datetime.datetime.now()
		print_times('fiemap_analyze', [t0, t1, t2, t3, t4])
