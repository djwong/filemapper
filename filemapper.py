#!/usr/bin/env python3
# Generate a sqlite database of FS data via FIEMAP

import os
import fiemap
import sqlite3
import sys

def walk_fs(path, dir_fn, ino_fn, extent_fn):
	'''Iterate the filesystem, looking for extent data.'''
	def do_map(stat, path):
		fd = os.open(path, os.O_RDONLY)
		for extent in fiemap.fiemap2(fd):
			extent_fn(stat, extent)
		os.close(fd)

	os.sync()
	dev = os.stat(path).st_dev
	for root, dirs, files in os.walk(path):
		rstat = os.stat(root)
		ino_fn(rstat, root, True)
		do_map(rstat, root)
		for xdir in dirs:
			dname = os.path.join(root, xdir)
			try:
				dstat = os.stat(dname)
			except Exception as e:
				print(e)
				continue

			if dstat.st_dev != dev:
				print("REMOVE %s" % dname)
				dirs.remove(xdir)
				continue
		for xfile in files:
			fname = os.path.join(root, xfile)
			try:
				fstat = os.stat(fname)
			except Exception as e:
				print(e)
				continue
	
			if fstat.st_dev != dev:
				continue
			ino_fn(fstat, fname, False)
			do_map(fstat, fname)
		dir_fn(rstat, dirs + files)

def setup_db(dbpath):
	'''Initialize a sqlite database.'''
	con = sqlite3.connect(dbpath)

	con.execute('DROP TABLE IF EXISTS dir_t;')
	con.execute('DROP TABLE IF EXISTS path_t;')
	con.execute('DROP TABLE IF EXISTS inode_t;')
	con.execute('DROP TABLE IF EXISTS extent_t;')
	con.execute('CREATE TABLE inode_t(ino INTEGER PRIMARY KEY UNIQUE NOT NULL, type INTEGER NOT NULL CHECK (type in (0, 1, 2)));')
	con.execute('CREATE TABLE dir_t(dir_ino INTEGER REFERENCES inode_t(ino), name TEXT NOT NULL);')
	con.execute('CREATE TABLE path_t(path TEXT PRIMARY KEY UNIQUE NOT NULL, ino INTEGER REFERENCES inode_t(ino));')
	con.execute('CREATE TABLE extent_t(ino INTEGER REFERENCES inode_t(ino), pblk INTEGER NOT NULL, lblk INTEGER NOT NULL, flags INTEGER NOT NULL, length INTEGER NOT NULL);')

	return con

def insert_dir(con, root, dentries):
	con.executemany('INSERT INTO dir_t VALUES(?, ?);', [(root.st_ino, x) for x in dentries])

def insert_inode(con, stat, path, is_dir):
	if is_dir:
		ftype = 1
	else:
		ftype = 0

	con.execute('INSERT OR REPLACE INTO inode_t VALUES(?, ?);', (stat.st_ino, ftype))
	con.execute('INSERT INTO path_t VALUES(?, ?);', (path, stat.st_ino))

def insert_extent(con, stat, extent):
	con.execute('INSERT INTO extent_t VALUES(?, ?, ?, ?, ?);', \
		    (stat.st_ino, extent.physical, extent.logical, \
		     extent.flags, extent.length))

if __name__ == "__main__":
	con = setup_db("/tmp/test.db")
	walk_fs(sys.argv[1],
		lambda x, y: insert_dir(con, x, y),
		lambda x, y, z: insert_inode(con, x, y, z),
		lambda x, y: insert_extent(con, x, y))
	print("ENJOY")
	con.commit()
	con.close()
