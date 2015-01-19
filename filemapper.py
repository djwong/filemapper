#!/usr/bin/env python3
# Generate a sqlite database of FS data via FIEMAP

import os
import fiemap
import fmdb
import fmcli
import sys
import argparse

class fiemap_db(fmdb.fmdb):
	def regenerate(self, force = False):
		'''Regenerate the database.'''
		if not force and not self.must_regenerate():
			return
		self.reset()
		fiemap.walk_fs(self.fspath,
			self.insert_dir,
			self.insert_inode,
			self.insert_extent)
		self.conn.commit()

if __name__ == "__main__":
	parser = argparse.ArgumentParser(prog = sys.argv[0],
		description = 'Display an overview of commands.')
	parser.add_argument('-m', default = 0, action = 'count', help = 'Enable machine-friendly outputs.')
	parser.add_argument('-l', default = 2048, type = int, help = 'Initial overview length.')
	parser.add_argument('-d', default = '/tmp/test.db', help = 'Database file.')
	parser.add_argument('-r', default = 0, action = 'count', help = 'Regenerate the database.')
	parser.add_argument('fspath', help = 'Filesystem path to examine.')
	parser.add_argument('commands', nargs = '*', \
		help = 'Commands to run up.')
	args = parser.parse_args(sys.argv[1:])
	fmdb = fiemap_db(args.fspath, args.d)
	f = False
	if args.r > 0:
		f = True
	fmdb.regenerate(f)
	fmdb.set_overview_length(args.l)
	fmcli = fmcli.fmcli(fmdb)
	if args.m > 0:
		fmcli.machine = True
	fmcli.interact('filemapper v0.5')
