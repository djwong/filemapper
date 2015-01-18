# Database routines for filemapper
# Copyright(C) 2015 Darrick J. Wong
# Licensed under GPLv2.

import sqlite3
import os
import datetime

class fmdb:
	'''filemapper database'''
	def __init__(self, fspath, dbpath):
		'''Initialize a database object.'''
		self.fspath = fspath
		self.prefix_len = len(self.fspath)
		if self.fspath[self.prefix_len - 1] == '/':
			self.prefix_len -= 1
		self.statfs = os.statvfs(fspath)
		self.conn = sqlite3.connect(dbpath)

	def __del__(self):
		'''Destroy database object.'''
		self.conn.close()

	def reset(self):
		'''Prepare a database for new data.'''
		self.conn.executescript("""
PRAGMA page_size = 4096;
DROP VIEW IF EXISTS path_extent_v;
DROP TABLE IF EXISTS extent_t;
DROP TABLE IF EXISTS inode_t;
DROP TABLE IF EXISTS path_t;
DROP TABLE IF EXISTS dir_t;
DROP TABLE IF EXISTS fs_t;
CREATE TABLE fs_t(path TEXT PRIMARY KEY NOT NULL, bsize INTEGER NOT NULL, frsize INTEGER NOT NULL, total_blocks INTEGER NOT NULL, free_blocks INTEGER NOT NULL, avail_blocks INTEGER NOT NULL, total_inodes INTEGER NOT NULL, free_inodes INTEGER NOT NULL, avail_inodes INTEGER NOT NULL, max_len INTEGER NOT NULL, timestamp BLOB);
CREATE TABLE inode_t(ino INTEGER PRIMARY KEY UNIQUE NOT NULL, type INTEGER NOT NULL CHECK (type in (0, 1, 2)));
CREATE TABLE dir_t(dir_ino INTEGER REFERENCES inode_t(ino), name TEXT NOT NULL);
CREATE TABLE path_t(path TEXT PRIMARY KEY UNIQUE NOT NULL, ino INTEGER REFERENCES inode_t(ino));
CREATE TABLE extent_t(ino INTEGER REFERENCES inode_t(ino), pblk INTEGER NOT NULL, lblk INTEGER NOT NULL, flags INTEGER NOT NULL, length INTEGER NOT NULL);
CREATE VIEW path_extent_v AS SELECT path_t.path, extent_t.pblk, extent_t.lblk, extent_t.length, extent_t.flags FROM extent_t, path_t WHERE extent_t.ino = path_t.ino;
		""")

		self.conn.execute('INSERT INTO fs_t VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (self.fspath, self.statfs.f_bsize, self.statfs.f_frsize, self.statfs.f_blocks, self.statfs.f_bfree, self.statfs.f_bavail, self.statfs.f_files, self.statfs.f_ffree, self.statfs.f_favail, self.statfs.f_namemax, datetime.datetime.today()))

	def must_regenerate(self):
		'''Decide if we need to regenerate the database.'''
		try:
			cur = self.conn.cursor()
			cur.execute('SELECT path, timestamp FROM fs_t WHERE path = ?', (self.fspath,))
			results = cur.fetchall()
			return len(results) != 1
		except:
			return True

	def insert_dir(self, root, dentries):
		'''Insert a directory record into the database.'''
		self.conn.executemany('INSERT INTO dir_t VALUES(?, ?);', \
				[(root.st_ino, x) for x in dentries])

	def insert_inode(self, stat, path, is_dir):
		'''Insert an inode record into the database.'''
		if is_dir:
			ftype = 1
		else:
			ftype = 0

		self.conn.execute('INSERT OR REPLACE INTO inode_t VALUES(?, ?);', \
				(stat.st_ino, ftype))
		self.conn.execute('INSERT INTO path_t VALUES(?, ?);', \
				(path[self.prefix_len:], stat.st_ino))

	def insert_extent(self, stat, extent):
		'''Insert an extent record into the database.'''
		self.conn.execute('INSERT INTO extent_t VALUES(?, ?, ?, ?, ?);', \
			    (stat.st_ino, extent.physical, extent.logical, \
			     extent.flags, extent.length))

	def query_path(self, path):
		'''Query extents used by a particular path.'''
		cur = self.conn.cursor()
		cur.execute('SELECT * FROM path_extent_v WHERE path LIKE ?', (path, ))
		return cur.fetchall()

	def query_pblocks(self, start, end):
		'''Query inodes spanning a range of blocks.'''
		cur = self.conn.cursor()
		cur.execute("SELECT * FROM path_extent_v WHERE pblk < ? OR pblk + length > ?", (end, start))
		return cur.fetchall()
