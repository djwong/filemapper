#!/usr/bin/env python3
# filemapper CLI
# Copyright (C) 2015 Darrick J. Wong
# Licensed under GPLv2.

import code
import readline
import atexit
import os
import argparse
import sys
import fmdb
from collections import namedtuple
import datetime
from dateutil import tz

units = namedtuple('units', ['abbrev', 'label', 'factor'])

# Units for regular numbers
units_none = units('', '', 1)
units_k = units('K', 'K', 10 ** 3)
units_m = units('M', 'M', 10 ** 6)
units_g = units('G', 'G', 10 ** 9)
units_t = units('T', 'T', 10 ** 12)

# Units for storage quantities
units_bytes = units('', 'bytes', 1)
units_sectors = units('s', 'sectors', 2 ** 9)
units_kib = units('K', 'KiB', 2 ** 10)
units_mib = units('M', 'MiB', 2 ** 20)
units_gib = units('G', 'GiB', 2 ** 30)
units_tib = units('T', 'TiB', 2 ** 40)

units_auto = units('a', 'auto', None)

def format_size(units, num):
	'''Pretty-format a number with base-2 suffixes.'''
	if num is None:
		return None
	if units.factor is not None:
		if units.factor == 1:
			return "{:,}{}{}".format(int(num), \
				' ' if len(units.label) > 0 else '', units.label[:-1] if num == 1 else units.label)
		return "{:,.1f} {}".format(float(num) / units.factor, units.label)
	units_scale = [units_bytes, units_kib, units_mib, units_gib, units_tib]
	for i in range(0, len(units_scale) - 1):
		if num < units_scale[i + 1].factor:
			return format_size(units_scale[i], num)
	return format_size(units_scale[-1], num)

def format_number(units, num):
	'''Pretty-format a number with base-10 suffixes.'''
	if num is None:
		return None
	if units.factor is not None:
		if units.factor == 1:
			return "{:,}{}{}".format(int(num), \
				' ' if len(units.label) > 0 else '', units.label)
		return "{:,.1f} {}".format(float(num) / units.factor, units.label)
	units_scale = [units_none, units_k, units_m, units_g, units_t]
	for i in range(0, len(units_scale) - 1):
		if num < units_scale[i + 1].factor:
			return format_number(units_scale[i], num)
	return format_number(units_scale[-1], num)

gmt = tz.gettz('UTC')
def posix_timestamp_str(t, pretty = False):
	'''Generate a string from a POSIX UTC timestamp.'''
	if t is None:
		return None
	dt = datetime.datetime.utcfromtimestamp(t).replace(tzinfo = gmt)
	if pretty:
		dt = dt.astimezone(tz.gettz())
		return dt.strftime('%x %X')
	return dt.isoformat()

def n2p(maximum, num):
	'''Convert a suffixed number to an integer.'''
	conv = [
		units('%', 'percent', maximum / 100.0),
		units_none,
		units_k,
		units_m,
		units_g,
		units_t,
	]
	for unit in conv:
		if num[-1].lower() == unit.abbrev.lower():
			return int(unit.factor * float(num[:-1]))
	return int(num)

def s2p(fs, num):
	'''Convert a suffixed size to an integer.'''
	conv = [
		units('%', 'percent', fs.total_bytes / 100.0),
		units('B', 'blocks', fs.block_size),
		units_bytes,
		units_sectors,
		units_kib,
		units_mib,
		units_gib,
		units_tib,
	]
	for unit in conv:
		if num[-1].lower() == unit.abbrev.lower():
			return int(unit.factor * float(num[:-1]))
	return int(num)

def parse_ranges(args, fn):
	'''Parse string arguments into numeric ranges.'''
	ranges = []
	if 'all' in args:
		return ranges
	for arg in args:
		if ':' in arg:
			pos = arg.index(':')
			ranges.append((fn(arg[:pos]), fn(arg[pos+1:])))
		elif '-' in arg:
			start = 1 if arg[0] == '-' else 0
			pos = arg.index('-', start)
			ranges.append((fn(arg[:pos]), fn(arg[pos+1:])))
		else:
			ranges.append(fn(self.fs, arg))
	return ranges

def split_unescape(s, delim, str_delim, escape='\\', unescape=True):
	"""Split a string into a an argv array, with string support.
	>>> split_unescape('foo,bar', ',')
	['foo', 'bar']
	>>> split_unescape('foo$,bar', ',', '$')
	['foo,bar']
	>>> split_unescape('foo$$,bar', ',', '$', unescape=True)
	['foo$', 'bar']
	>>> split_unescape('foo$$,bar', ',', '$', unescape=False)
	['foo$$', 'bar']
	>>> split_unescape('foo$', ',', '$', unescape=True)
	['foo$']
	"""
	ret = []
	current = []
	in_str = False
	itr = iter(s)
	for ch in itr:
		if ch == escape:
			try:
				n = next(itr)
				if (in_str and n in str_delim) or \
				   (not in_str and (n in delim or n in str_delim)):
					if not unescape:
						current.append(ch)
					current.append(n)
				else:
					current.append(ch)
					current.append(n)
			except StopIteration:
				current.append(escape)
		elif ch in str_delim:
			in_str = not in_str
		elif ch == delim and not in_str:
			# split! (add current to the list and reset it)
			if len(current) > 0:
				ret.append(''.join(current))
			current = []
		else:
			current.append(ch)
	if len(current) > 0:
		ret.append(''.join(current))
	return ret

class fmcli(code.InteractiveConsole):
	'''Interactive command line client.'''
	def __init__(self, fmdb, locals=None, filename="<console>", \
		     histfile=os.path.join(os.path.expanduser('~'), '.config', \
					   'fmcli-history')):
		# In Python 2.x this didn't inherit from object, so we're
		# stuck with the old superclass syntax for now.
		# super(fmcli, self).__init__(locals, filename)
		code.InteractiveConsole.__init__(self, locals, filename)
		self.init_history(histfile)
		self.fmdb = fmdb
		readline.set_history_length(1000)
		self.commands = {
			('cache', 'a'): self.do_cache_overview,
			('cell', 'c'): self.do_cell_to_extents,
			('overview_types', 'ot'): self.do_overview_extent_types,
			('help', 'h', '?'): self.do_help,
			('file', 'f'): self.do_paths,
			('fstat', 'fs'): self.do_paths_stats,
			('clear', 'cc'): self.do_clear_calculated,
			('flag', 'g'): self.do_extent_flag,
			('inode', 'i'): self.do_inodes,
			('logical', 'l'): self.do_loff_to_extents,
			('ls', ): self.do_ls,
			('machine', 'm'): self.do_machine,
			('length', 'n'): self.do_lengths,
			('primary_extents', 'pe'): self.do_nr_extents,
			('overview', 'o'): self.do_overview,
			('physical', 'p'): self.do_poff_to_extents,
			('quit', 'exit', 'q'): self.do_exit,
			('summary', 's'): self.do_summary,
			('size', 'sz'): self.do_inode_sizes,
			('type', 't'): self.do_extent_type,
			('travel_score', 'ts'): self.do_travel_scores,
			('units', 'u'): self.do_set_units,
		}
		self.done = False
		self.units = units_auto
		self.machine = False
		self.fs = self.fmdb.query_summary()

	## Interpreter stuff

	def init_history(self, histfile):
		'''Initializes readline history.'''
		readline.parse_and_bind("tab: complete")
		if not hasattr(readline, "read_history_file"):
			return
		try:
			readline.read_history_file(histfile)
		except IOError:
			pass
		atexit.register(self.save_history, histfile)

	def save_history(self, histfile):
		'''Save readline history to disk.'''
		readline.write_history_file(histfile)

	def raw_input(self, prompt):
		return code.InteractiveConsole.raw_input(self, 'fm' + prompt)

	def runsource(self, source, filename='<stdin>'):
		args = split_unescape(source, ' ', ('"', "'"))
		if len(args) == 0:
			return
		for key in self.commands:
			if args[0] in key:
				try:
					self.commands[key](args)
				except SystemExit:
					return
				if self.done:
					sys.exit(0)
				return
		print("Command '%s' not recognized." % args[0])
		self.do_help(args)

	## Utility

	def parse_size_ranges(self, args):
		'''Parse string arguments into size ranges.'''
		return parse_ranges(args, lambda x: s2p(self.fs, x))

	def parse_number_ranges(self, args, maximum):
		'''Parse string arguments into number ranges.'''
		return parse_ranges(args, lambda x: n2p(maximum, x))

	## Misc. Commands

	def do_help(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Display an overview of commands.')
		parser.add_argument('commands', nargs = '*', \
			help = 'Commands to look up.')
		args = parser.parse_args(argv[1:])
		print_cmds = False
		if len(args.commands) == 0:
			print_cmds = True
		for key in self.commands:
			for cmd in args.commands:
				if cmd in key:
					self.commands[key]([cmd, '-h'])			
				else:
					print_cmds = True
		if print_cmds:
			print("Available commands:")
			for key in sorted(self.commands):
				print(key[0])

	def do_overview(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Show the block overview.')
		parser.add_argument('blocks', nargs='?', metavar = 'N', \
			type = int, default = None, \
			help = 'Number of blocks to print.  Default is 2048.')
		args = parser.parse_args(argv[1:])
		if args.blocks is not None:
			self.fmdb.set_overview_length(args.blocks)
		for ov in self.fmdb.query_overview():
			sys.stdout.write(ov.to_letter())
		sys.stdout.write('\n')

	def do_exit(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Exit the program.')
		parser.parse_args(argv[1:])
		self.done = True

	def do_summary(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Display a summary of the filesystem.')
		parser.parse_args(argv[1:])
		res = self.fs
		tb = self.fs.total_bytes
		fb = self.fs.free_bytes
		if tb == 0:
			fb = 1
			tb = 1
		ti = self.fs.total_inodes
		fi = self.fs.free_inodes
		if ti == 0:
			fi = 1
			ti = 1
		print("Summary of '%s':" % res.path)
		print("Block size:\t\t%s" % format_size(units_auto, res.block_size))
		print("Fragment size:\t\t%s" % format_size(units_auto, res.frag_size))
		print("Total space:\t\t%s" % format_size(self.units, res.total_bytes))
		print("Used space:\t\t%s (%.0f%%)" % \
			(format_size(self.units, res.total_bytes - res.free_bytes), \
			 100 * (1.0 - (float(fb) / tb))))
		print("Free space:\t\t%s" % format_size(self.units, res.free_bytes))
		print("Total inodes:\t\t%s" % format_number(units_auto, res.total_inodes))
		print("Used inodes:\t\t%s (%.0f%%)" % \
			(format_number(units_auto, res.total_inodes - res.free_inodes), \
			100 * (1.0 - (float(fi) / ti))))
		print("Free inodes:\t\t%s" % format_number(units_auto, res.free_inodes))
		print("Overview cells:\t\t%s each" % format_size(units_auto, float(res.total_bytes) / self.fmdb.overview_len))
		print("Extents:\t\t%s" % format_number(units_auto, res.extents))
		print("Inodes w/ extents:\t%s" % format_number(units_auto, res.inodes))
		inodes = res.inodes if res.inodes != 0 else 1
		extents = res.extents if res.extents != 0 else 1
		print("Fragmentation:\t\t%.1f%%" % ((100.0 * extents / inodes) - 100))

	def print_extent(self, ext):
		'''Pretty-print an extent.'''
		if self.machine:
			print("'%s',%d,%d,%d,'%s','%s'" % \
				(ext.path if ext.path != '' else self.fs.pathsep, \
				 ext.p_off, ext.l_off, ext.length, \
				 fmdb.extent_flagstr(ext), \
				 fmdb.extent_typestr(ext)))
			return
		print("'%s', %s, %s, %s, '%s', '%s'" % \
			(ext.path if ext.path != '' else self.fs.pathsep, \
			 format_size(self.units, ext.p_off), \
			 format_size(self.units, ext.l_off), \
			 format_size(self.units, ext.length), \
			 fmdb.extent_flagstr(ext), \
			 fmdb.extent_typestr(ext)))

	def print_dentry(self, de):
		'''Pretty-print a dentry.'''
		if self.machine:
			print("'%s',%d,'%s'" % \
				(de.name, de.ino, fmdb.dentry_typestr(de)))
			return
		print("'%s', %s, '%s'" % \
			(de.name, format_number(units_none, de.ino), \
			 fmdb.dentry_typestr(de)))

	def do_loff_to_extents(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents of a given range of logical offsets.')
		parser.add_argument('offsets', nargs = '+', \
			help = 'Logical offsets to look up.  This can be a single number or a range (e.g. 0-10m).')
		args = parser.parse_args(argv[1:])
		ranges = self.parse_size_ranges(args.offsets)
		for x in self.fmdb.query_loff_range(ranges):
			self.print_extent(x)

	def do_poff_to_extents(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents of a given range of physical offsets.')
		parser.add_argument('offsets', nargs = '+', \
			help = 'Physical offsets to look up.  This can be a single number or a range (e.g. 0-10k).')
		args = parser.parse_args(argv[1:])
		ranges = self.parse_size_ranges(args.offsets)
		for x in self.fmdb.query_poff_range(ranges):
			self.print_extent(x)

	def do_cache_overview(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Create caches of the overview table.')
		parser.add_argument('lengths', nargs = '+', \
			help = 'Lengths of the overview tables.')
		args = parser.parse_args(argv[1:])
		for arg in args.lengths:
			self.fmdb.cache_overview(arg)

	def do_cell_to_extents(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents of a given range of overview cells.')
		parser.add_argument('cells', nargs = '+', \
			help = 'Cell ranges to look up.  This can be a single number or a range (e.g. 0-10).')
		args = parser.parse_args(argv[1:])
		ranges = self.parse_number_args(args.cells, self.fmdb.overview_len)
		r = list(self.fmdb.pick_cells(ranges))
		for x in self.fmdb.query_poff_range(r):
			self.print_extent(x)

	def do_set_units(self, argv):
		avail_units = [
			units_auto,
			units_bytes,
			units_sectors,
			units('B', 'blocks', self.fs.block_size),
			units_kib,
			units_mib,
			units_gib,
			units_tib,
		]
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Set display units.')
		unit_list = [x[1] for x in avail_units]
		parser.add_argument('units', \
			help = 'Units for display output.  Default is bytes.', \
			choices = [x[1] for x in avail_units])
		args = parser.parse_args(argv[1:])
		for u in avail_units:
			if args.units.lower() == u.abbrev.lower() or \
			   args.units.lower() == u.label.lower():
				self.units = u
				print("Units set to '%s'." % self.units.label)
				return
		print("Unrecognized unit '%s'.  Available units:" % args.units)
		print(', '.join(unit_list))

	def do_paths(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents of a given path.')
		parser.add_argument('paths', nargs = '+', \
			help = 'Paths to look up.')
		parser.add_argument('-q', action = 'store_true', help = 'Quiet mode.')
		args = parser.parse_args(argv[1:])
		it = self.fmdb.query_paths(args.paths)
		if args.q:
			x = list(it)
			return
		for ext in it:
			self.print_extent(ext)

	def do_machine(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Toggle machine-friendly output mode.')
		parser.add_argument('mode', choices = ['yes', 'no', 'on', 'off', '1', '0'], \
			help = 'Paths to look up.')
		args = parser.parse_args(argv[1:])
		if args.mode == 'yes' or args.mode == 'on' or args.mode == '1':
			self.machine = True
		else:
			self.machine = False		

	def do_inodes(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents of a given range of inodes.')
		parser.add_argument('inodes', nargs = '+', \
			help = 'Inodes to look up.  This can be a single number or a range (e.g. 0-10).')
		args = parser.parse_args(argv[1:])
		ranges = self.parse_number_ranges(args.inodes, self.fs.total_inodes)
		for x in self.fmdb.query_inums(ranges):
			self.print_extent(x)

	def do_lengths(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents of a given range of lengths.')
		parser.add_argument('lengths', nargs = '+', \
			help = 'Lengths to look up.  This can be a single number or a range (e.g. 0-16k).')
		args = parser.parse_args(argv[1:])
		ranges = self.parse_size_ranges(args.lengths)
		for x in self.fmdb.query_lengths(ranges):
			self.print_extent(x)

	def do_extent_type(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents with a particular type.')
		parser.add_argument('types', nargs = '+', \
			help = 'Type codes to look up.  Valid values are: (d)irectory, (e)xtent map, (f)ile, FS (m)etadata, (s)ymbolic links, and e(x)tended attributes.', \
			choices = [x for x in sorted(fmdb.extent_type_strings.keys())])
		args = parser.parse_args(argv[1:])
		types = set()
		for arg in args.types:
			types.add(fmdb.extent_type_strings[arg])
		for x in self.fmdb.query_extent_types(list(types)):
			self.print_extent(x)

	def do_extent_flag(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents with a particular set of flags.')
		parser.add_argument('flags', nargs = '*', \
			help = 'Flag codes to look up.  Valid values are: u(n)known, (d)elayed allocation, (e)ncoded, (E)ncrypted, (u)naligned, (i)nline, (t)ail-packed, (U)nwritten, (m)erged, (s)hared, or no flag code at all.', \
			choices = [x for x in sorted(fmdb.extent_flags_strings.keys())])
		parser.add_argument('-e', action = 'store_true', help = 'Flags must match exactly.')
		args = parser.parse_args(argv[1:])
		flags = 0
		for arg in args.flags:
			flags |= fmdb.extent_flags_strings[arg]
		for x in self.fmdb.query_extent_flags(flags, args.e):
			self.print_extent(x)

	def do_ls(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up directories in the filesystem tree.')
		parser.add_argument('dirnames', nargs = '+', \
			help = 'Directory names to look up.')
		args = parser.parse_args(argv[1:])
		x = [x if x[-1] != '/' else x[:-1] for x in args.dirnames]
		for de in self.fmdb.query_ls(x):
			self.print_dentry(de)

	def do_overview_extent_types(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Restrict the overview display to particular types of extents.')
		t = [x for x in sorted(fmdb.extent_type_strings.keys())]
		t.append('all')
		parser.add_argument('types', nargs = '+', \
			help = 'Type codes to look up.  Valid values are: (d)irectory, (e)xtent map, (f)ile, FS (m)etadata, (s)ymbolic links, and e(x)tended attributes.  Use "all" to display all types.', \
			choices = t)
		args = parser.parse_args(argv[1:])
		if 'all' in args:
			self.fmdb.set_extent_types_to_show(None)
			return
		types = set()
		for arg in args.types:
			types.add(fmdb.extent_type_strings[arg])
		self.fmdb.set_extent_types_to_show(types)

	def print_inode_stats(self, inode):
		'''Pretty-print inode statistics.'''
		p = inode.path if inode.path != '' else self.fs.pathsep
		if self.machine:
			print("'%s',%d,%s,%0.2f,%s,%s,%s,%s,%s,%s" % \
				(p, inode.ino, inode.nr_extents, inode.travel_score, \
				 fmdb.inode_typestr(inode), \
				 posix_timestamp_str(inode.atime), \
				 posix_timestamp_str(inode.crtime), \
				 posix_timestamp_str(inode.ctime), \
				 posix_timestamp_str(inode.mtime), \
				 inode.size))
			return
		print("'%s', %d, %s, %0.2f, %s, '%s', '%s', '%s', '%s', %s" % \
			(p, inode.ino, inode.nr_extents, inode.travel_score, \
			 fmdb.inode_typestr(inode), \
			 posix_timestamp_str(inode.atime, True), \
			 posix_timestamp_str(inode.crtime, True), \
			 posix_timestamp_str(inode.ctime, True), \
			 posix_timestamp_str(inode.mtime, True), \
			 format_size(self.units, inode.size)))

	def do_paths_stats(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Calculate inode statistics for given paths.')
		parser.add_argument('paths', nargs = '+', \
			help = 'Paths to look up.')
		parser.add_argument('-q', action = 'store_true', help = 'Quiet mode.  Calculate and cache the results, but do not print them.')
		args = parser.parse_args(argv[1:])
		i = self.fmdb.query_paths_inodes(args.paths, analyze_extents = True)
		if args.q:
			list(i)
			return
		for x in i:
			self.print_inode_stats(x)

	def do_travel_scores(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up inodes of a given range of travel scores.')
		parser.add_argument('scores', nargs = '+', \
			help = 'Scores to look up.  This can be a single number or a range (e.g. 0-16k).')
		args = parser.parse_args(argv[1:])
		ranges = self.parse_size_ranges(args.scores)
		for x in self.fmdb.query_travel_scores_inodes(ranges):
			self.print_inode_stats(x)

	def do_nr_extents(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up inodes of a given range of primary extent counts.  The extent type of a primary extent matches the type of the inode, e.g. file extents for files.')
		parser.add_argument('counts', nargs = '+', \
			help = 'Extent counts to look up.  This can be a single number or a range (e.g. 0-16k).')
		args = parser.parse_args(argv[1:])
		ranges = self.parse_number_ranges(args.counts, 2**64)
		for x in self.fmdb.query_nr_extents_inodes(ranges):
			self.print_inode_stats(x)

	def do_inode_sizes(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up inodes of a given range of inode sizes.')
		parser.add_argument('sizes', nargs = '+', \
			help = 'Sizes to look up.  This can be a single number or a range (e.g. 0-16k).')
		args = parser.parse_args(argv[1:])
		ranges = self.parse_size_ranges(args.sizes)
		for x in self.fmdb.query_sizes_inodes(ranges):
			self.print_inode_stats(x)

	def do_clear_calculated(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Erase all calculated values.')
		args = parser.parse_args(argv[1:])
		self.fmdb.clear_calculated_values()
