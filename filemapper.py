#!/usr/bin/env python3
# Generate/view a sqlite database of FS data via FIEMAP or other.
# Copyright (C) 2015 Darrick J. Wong.  All rights reserved.
# Licensed under the GPLv2.

import os
import fmdb
import fmcli
import sys
import argparse

if __name__ == "__main__":
	parser = argparse.ArgumentParser(prog = sys.argv[0],
		description = 'Display an overview of commands.')
	parser.add_argument('-m', action = 'store_true', help = 'Enable machine-friendly outputs (CLI).')
	parser.add_argument('-l', default = 2048, metavar = 'length', type = int, help = 'Initial overview length (CLI).')
	parser.add_argument('-r', nargs = 1, metavar = 'fspath', help = 'Analyze a filesystem using the FIEMAP backend.')
	parser.add_argument('-q', action = 'store_true', help = 'If -r is specified, exit after analyzing.')
	parser.add_argument('-g', action = 'store_true', help = 'Start the GUI.')
	parser.add_argument('-s', action = 'store_true', help = 'Generate SQL schemas and index definitions.')
	parser.add_argument('database', help = 'Database file to store snapshots.')
	parser.add_argument('commands', nargs = '*', \
		help = 'Commands to run (CLI).  -g cannot be specified.')
	args = parser.parse_args(sys.argv[1:])

	if args.s:
		print(fmdb.generate_op_sql())
		print(fmdb.generate_schema_sql())
		print(fmdb.generate_index_sql())

	if args.r is not None:
		fmdb = fmdb.fiemap_db(args.r[0], args.database, True)
		fmdb.analyze(True)
		fmdb.cache_overview(2048)
		fmdb.cache_overview(65536)
		fmdb.calc_inode_stats()
		if args.q:
			sys.exit(0)
	else:
		fmdb = fmdb.fmdb(None, args.database, os.access(args.database, os.W_OK))

	if args.g and len(args.commands) > 0:
		print('-g cannot be specified with commands to run.')
		parser.print_help()
		sys.exit(1)

	if args.g:
		import fmgui
		from PyQt4 import QtGui, uic
		app = QtGui.QApplication([])
		fmgui = fmgui.fmgui(fmdb)
		sys.exit(app.exec_())
	else:
		fmdb.set_overview_length(args.l)
		fmcli = fmcli.fmcli(fmdb)
		fmcli.machine = args.m
		for c in args.commands:
			fmcli.push(c)
		if len(args.commands) == 0:
			fmcli.interact('filemapper v0.6')
