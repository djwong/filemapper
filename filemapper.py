#!/usr/bin/env python
# Generate a sqlite database of FS data via FIEMAP

import os
import fiemap
import fmdb
import sys

class fiemap_db(fmdb.fmdb):
	def regenerate(self, force = False):
		'''Regenerate the database.'''
		if not force and not self.must_regenerate():
			return
		self.reset()
		fiemap.walk_fs(self.fspath,
			lambda x, y: self.insert_dir(x, y),
			lambda x, y, z: self.insert_inode(x, y, z),
			lambda x, y: self.insert_extent(x, y))
		self.conn.commit()

if __name__ == "__main__":
	fmdb = fiemap_db(sys.argv[1], '/tmp/test.db')
	fmdb.regenerate(force = True)

	if len(sys.argv) > 3:
		print(fmdb.query_pblocks(sys.argv[2], sys.argv[3]))
	elif len(sys.argv) > 2:
		print(fmdb.query_path(sys.argv[2]))
