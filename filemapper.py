#!/usr/bin/env python3
# Generate a sqlite database of FS data via FIEMAP
# Copyright (C) 2015 Darrick J. Wong.  All rights reserved.
# Licensed under the GPLv2.

import os
import fiemap
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
	parser.add_argument('-g', action = 'store_true', help = 'Run the GUI.')
	parser.add_argument('database', help = 'Database file to store snapshots.')
	parser.add_argument('commands', nargs = '*', \
		help = 'Commands to run (CLI).')
	args = parser.parse_args(sys.argv[1:])

	if args.r is not None:
		fmdb = fiemap.fiemap_db(args.r[0], args.database)
		fmdb.analyze(True)
	else:
		fmdb = fmdb.fmdb(None, args.database)

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
		fmcli.interact('filemapper v0.5')
