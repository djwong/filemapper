# Database routines for filemapper
# Copyright(C) 2015 Darrick J. Wong
# Licensed under GPLv2.

import sqlite3
import os
import datetime
import stat
import array
from collections import namedtuple

def stmode_to_type(xstat, is_xattr):
	'''Convert a stat mode to a type code.'''
	if is_xattr:
		return 'x'
	elif stat.S_ISREG(xstat.st_mode):
		return 'f'
	elif stat.S_ISDIR(xstat.st_mode):
		return 'd'

fs_summary = namedtuple('fs_summary', ['path', 'block_size', 'frag_size', \
				       'total_bytes', 'free_bytes', \
				       'avail_bytes', 'total_inodes', \
				       'free_inodes', 'avail_inodes'])

poff_row = namedtuple('poff_row', ['path', 'p_off', 'l_off', 'length', \
				   'flags', 'type'])

class overview_block:
	def __init__(self):
		self.files = self.dirs = self.mappings = self.metadata = self.xattrs = 0

class fmdb:
	'''filemapper database'''
	def __init__(self, fspath, dbpath):
		'''Initialize a database object.'''
		self.fspath = fspath
		self.conn = sqlite3.connect(dbpath)
		self.fs = None
		self.overview_len = None
		self.cached_overview = None
		self.result_batch_size = 512

	def __del__(self):
		'''Destroy database object.'''
		self.conn.close()

	def start_update(self):
		'''Prepare a database for new data.'''
		self.conn.executescript("""
PRAGMA page_size = 4096;
DROP VIEW IF EXISTS path_extent_v;
DROP TABLE IF EXISTS extent_t;
DROP TABLE IF EXISTS inode_t;
DROP TABLE IF EXISTS path_t;
DROP TABLE IF EXISTS dir_t;
DROP TABLE IF EXISTS fs_t;
CREATE TABLE fs_t(path TEXT PRIMARY KEY NOT NULL, block_size INTEGER NOT NULL, frag_size INTEGER NOT NULL, total_bytes INTEGER NOT NULL, free_bytes INTEGER NOT NULL, avail_bytes INTEGER NOT NULL, total_inodes INTEGER NOT NULL, free_inodes INTEGER NOT NULL, avail_inodes INTEGER NOT NULL, max_len INTEGER NOT NULL, timestamp TEXT NOT NULL, finished INTEGER NOT NULL);
CREATE TABLE inode_t(ino INTEGER PRIMARY KEY UNIQUE NOT NULL);
CREATE TABLE dir_t(dir_ino INTEGER REFERENCES inode_t(ino) NOT NULL, name TEXT NOT NULL, name_ino INTEGER REFERENCES inode_t(ino) NOT NULL);
CREATE TABLE path_t(path TEXT PRIMARY KEY UNIQUE NOT NULL, ino INTEGER REFERENCES inode_t(ino));
CREATE TABLE extent_t(ino INTEGER REFERENCES inode_t(ino), p_off INTEGER NOT NULL, l_off INTEGER NOT NULL, flags INTEGER NOT NULL, length INTEGER NOT NULL, type TEXT NOT NULL CHECK (type in ('f', 'd', 'e', 'm', 'x')), p_end INTEGER NOT NULL);
CREATE VIEW path_extent_v AS SELECT path_t.path, extent_t.p_off, extent_t.l_off, extent_t.length, extent_t.flags, extent_t.type, extent_t.p_end FROM extent_t, path_t WHERE extent_t.ino = path_t.ino;
		""")

		self.fs = None
		statfs = os.statvfs(self.fspath)
		self.conn.execute('INSERT INTO fs_t VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0);', \
				(self.fspath, statfs.f_bsize, \
				 statfs.f_frsize, \
				 statfs.f_blocks * statfs.f_bsize, \
				 statfs.f_bfree * statfs.f_bsize, \
				 statfs.f_bavail * statfs.f_bsize, \
				 statfs.f_files, statfs.f_ffree, \
				 statfs.f_favail, statfs.f_namemax, \
				 str(datetime.datetime.today())))

	def end_update(self):
		'''Finish updating a database.'''

		self.conn.executescript("""
CREATE INDEX path_ino_i ON path_t(ino);
CREATE INDEX extent_poff_i ON extent_t(p_off, p_end);
		""")
		self.conn.execute('UPDATE fs_t SET finished = 1 WHERE path = ?;', (self.fspath,))
		self.conn.commit()

	def must_regenerate(self):
		'''Decide if we need to regenerate the database.'''
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

	def insert_dir(self, root, dentries):
		'''Insert a directory record into the database.'''
		self.conn.executemany('INSERT INTO dir_t VALUES(?, ?, ?);', \
				[(root.st_ino, name, stat.st_ino) for name, stat in dentries])

	def insert_inode(self, stat, path):
		'''Insert an inode record into the database.'''
		self.conn.execute('INSERT OR REPLACE INTO inode_t VALUES(?);', \
				(stat.st_ino,))
		self.conn.execute('INSERT INTO path_t VALUES(?, ?);', \
				(path, stat.st_ino))
		
	def insert_extent(self, stat, extent, is_xattr):
		'''Insert an extent record into the database.'''
		code = stmode_to_type(stat, is_xattr)
		self.conn.execute('INSERT INTO extent_t VALUES(?, ?, ?, ?, ?, ?, ?);', \
			    (stat.st_ino, extent.physical, extent.logical, \
			     extent.flags, extent.length, \
			     code, extent.physical + extent.length - 1))

	def set_overview_length(self, length):
		'''Set the overview length.'''
		length = int(length)
		self.query_summary()
		if length > self.fs.total_bytes or length < 1:
			length = total_bytes
		if self.overview_len == length:
			return
		self.overview_len = length
		self.cached_overview = None
		self.bytes_per_cell = int(self.fs.total_bytes / self.overview_len)

	def query_overview(self):
		'''Create the overview.'''
		if self.cached_overview is not None:
			return self.cached_overview

		self.query_summary()
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		overview = [overview_block() for x in range(1, self.overview_len)]

		t0 = datetime.datetime.today()
		cur.execute('SELECT p_off, length, flags, type FROM path_extent_v;')
		t1 = datetime.datetime.today()
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				break
			for (e_p_off, e_len, e_flags, e_type) in rows:
				start_cell = int(e_p_off // self.bytes_per_cell)
				end_cell = int((e_p_off + e_len) // self.bytes_per_cell)
				for i in range(start_cell, end_cell + 1):
					if e_type == 'f':
						overview[i].files += 1
					elif e_type == 'd':
						overview[i].dirs += 1
					elif e_type == 'e':
						overview[i].mappings += 1
					elif e_type == 'm':
						overview[i].metadata += 1
					elif e_type == 'x':
						overview[i].xattrs += 1
		t2 = datetime.datetime.today()
		self.cached_overview = overview
		return self.cached_overview

	def query_summary(self):
		'''Fetch the filesystem summary.'''
		if self.fs is not None:
			return self.fs

		cur = self.conn.cursor()
		cur.execute('SELECT path, block_size, frag_size, total_bytes, free_bytes, avail_bytes, total_inodes, free_inodes, avail_inodes FROM fs_t;')
		rows = cur.fetchall()
		assert len(rows) == 1
		res = rows[0]

		self.fs = fs_summary(res[0], int(res[1]), int(res[2]), \
				 int(res[3]), int(res[4]), int(res[5]), \
				 int(res[6]), int(res[7]), int(res[8]))
		return self.fs

	def pick_cells(self, ranges):
		'''Convert a range of cells to byte ranges.'''
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

	def query_poff_range(self, ranges):
		'''Query extents spanning a range of bytes.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		qstr = 'SELECT path, p_off, l_off, length, flags, type FROM path_extent_v'
		qarg = []
		cond = 'WHERE'
		for r in ranges:
			qstr = qstr + ' %s (p_off <= ? AND p_end >= ?)' % cond
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

	def query_paths(self, paths, wildcards = True):
		'''Query extents used by a given path.'''
		cur = self.conn.cursor()
		cur.arraysize = self.result_batch_size
		qstr = 'SELECT path, p_off, l_off, length, flags, type FROM path_extent_v'
		if wildcards:
			op = 'LIKE'
		else:
			op = '='
		qarg = []
		cond = 'WHERE'
		for p in paths:
			qstr = qstr + ' %s path %s ?' % (cond, op)
			cond = 'OR'
			qarg.append(p)
		qstr = qstr + " ORDER BY path, l_off"
		print(qstr, qarg)
		cur.execute(qstr, qarg)
		while True:
			rows = cur.fetchmany()
			if len(rows) == 0:
				return
			for row in rows:
				yield poff_row(row[0], row[1], row[2], row[3], \
						row[4], row[5])

