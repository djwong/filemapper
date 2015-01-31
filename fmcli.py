# filemapper CLI
# Copyright (C) 2015 Darrick J. Wong
# Licensed under GPLv2.

import code
import readline
import atexit
import os
import argparse
import sys
import fiemap
from collections import namedtuple

typecodes = {
	'f': 'file',
	'd': 'directory',
	'e': 'file map',
	'm': 'metadata',
	'x': 'extended attribute',
}

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
	if units.factor is not None:
		if units.factor == 1:
			return "{:,}{}{}".format(int(num / units.factor), \
				' ' if len(units.label) > 0 else '', units.label)
		return "{:,.1f} {}".format(num / units.factor, units.label)
	units_scale = [units_bytes, units_kib, units_mib, units_gib, units_tib]
	for i in range(0, len(units_scale) - 1):
		if num < units_scale[i + 1].factor:
			return format_size(units_scale[i], num)
	return format_size(units_scale[-1], num)

def format_number(units, num):
	'''Pretty-format a number with base-10 suffixes.'''
	if units.factor is not None:
		if units.factor == 1:
			return "{:,}{}{}".format(int(num / units.factor), \
				' ' if len(units.label) > 0 else '', units.label)
		return "{:,.1f} {}".format(num / units.factor, units.label)
	units_scale = [units_none, units_k, units_m, units_g, units_t]
	for i in range(0, len(units_scale) - 1):
		if num < units_scale[i + 1].factor:
			return format_number(units_scale[i], num)
	return format_number(units_scale[-1], num)

def n2p(fs, num):
	'''Convert a suffixed number to an integer.'''
	conv = [
		units('%', 'percent', fs.total_bytes / 100),
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
		     histfile=os.path.expanduser("~/.config/fmcli-history")):
		# In Python 2.x this didn't inherit from object, so we're
		# stuck with the old superclass syntax for now.
		# super(fmcli, self).__init__(locals, filename)
		code.InteractiveConsole.__init__(self, locals, filename)
		self.init_history(histfile)
		self.fmdb = fmdb
		readline.set_history_length(1000)
		self.commands = {
			('cell', 'c'): self.do_cell_to_extents,
			('help', 'h', '?'): self.do_help,
			('file', 'f'): self.do_paths,
			('flag', 'g'): self.do_extent_flag,
			('inode', 'i'): self.do_inodes,
			('logical', 'l'): self.do_loff_to_extents,
			('ls', ): self.do_ls,
			('machine', 'm'): self.do_machine,
			('length', 'n'): self.do_lengths,
			('overview', 'o'): self.do_overview,
			('physical', 'p'): self.do_poff_to_extents,
			('quit', 'exit', 'q'): self.do_exit,
			('summary', 's'): self.do_summary,
			('type', 't'): self.do_extent_type,
			('units', 'u'): self.do_set_units,
		}
		self.done = False
		self.units = units_auto
		self.machine = False
		self.fs = self.fmdb.query_summary()

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

	# Commands
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
		def overview_to_letter(ov):
			tot = ov.files + ov.dirs + ov.mappings + ov.metadata + ov.xattrs
			if tot == 0:
				return '.'
			elif ov.files == tot:
				return 'F'
			elif ov.dirs == tot:
				return 'D'
			elif ov.mappings == tot:
				return 'E'
			elif ov.metadata == tot:
				return 'M'
			elif ov.xattrs == tot:
				return 'X'

			x = ov.files
			letter = 'f'
			if ov.dirs > x:
				x = ov.dirs
				letter = 'd'
			if ov.mappings > x:
				x = ov.mappings
				letter = 'e'
			if ov.metadata > x:
				x = ov.metadata
				letter = 'm'
			if ov.xattrs > x:
				letter = 'x'
			return letter
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Show the block overview.')
		parser.add_argument('blocks', nargs='?', metavar = 'N', \
			type = int, default = None, \
			help = 'Number of blocks to print.  Default is 2048.')
		args = parser.parse_args(argv[1:])
		if args.blocks is not None:
			self.fmdb.set_overview_length(args.blocks)
		for ov in self.fmdb.query_overview():
			sys.stdout.write(overview_to_letter(ov))
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
		print("Summary of '%s':" % res.path)
		print("Block size:\t\t%s" % format_size(units_auto, res.block_size))
		print("Fragment size:\t\t%s" % format_size(units_auto, res.frag_size))
		print("Total space:\t\t%s" % format_size(self.units, res.total_bytes))
		print("Used space:\t\t%s (%.0f%%)" % \
			(format_size(self.units, res.total_bytes - res.free_bytes), \
			 100 * (1.0 - (res.free_bytes / res.total_bytes))))
		print("Free space:\t\t%s" % format_size(self.units, res.free_bytes))
		print("Total inodes:\t\t%s" % format_number(units_auto, res.total_inodes))
		print("Used inodes:\t\t%s (%.0f%%)" % \
			(format_number(units_auto, res.total_inodes - res.free_inodes), \
			100 * (1.0 - (res.free_inodes / res.total_inodes))))
		print("Free inodes:\t\t%s" % format_number(units_auto, res.free_inodes))
		print("Overview cells:\t\t%s each" % format_size(units_auto, res.total_bytes / self.fmdb.overview_len))
		print("Extents:\t\t%s" % format_number(units_auto, res.extents))
		print("Inodes w/ extents:\t%s" % format_number(units_auto, res.inodes))
		print("Fragmentation:\t\t%.1f%%" % ((100.0 * res.extents / res.inodes) - 100))

	def print_extent(self, ext):
		'''Pretty-print an extent.'''
		if self.machine:
			print("'%s',%d,%d,%d,'%s','%s'" % \
				(ext.path if ext.path != '' else self.fs.pathsep, \
				 ext.p_off, ext.l_off, ext.length, \
				 ext.flags_to_str(), \
				 typecodes[ext.type]))
			return
		print("'%s', %s, %s, %s, '%s', '%s'" % \
			(ext.path if ext.path != '' else self.fs.pathsep, \
			 format_size(self.units, ext.p_off), \
			 format_size(self.units, ext.l_off), \
			 format_size(self.units, ext.length), \
			 ext.flags_to_str(), \
			 typecodes[ext.type]))

	def print_dentry(self, de):
		'''Pretty-print a dentry.'''
		if self.machine:
			print("'%s',%d,'%s'" % \
				(de.name, de.ino, de.type))
			return
		print("'%s', %s, '%s'" % \
			(de.name, format_number(units_none, de.ino), de.type))

	def do_loff_to_extents(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents of a given range of logical offsets.')
		parser.add_argument('offsets', nargs = '+', \
			help = 'Logical offsets to look up.')
		args = parser.parse_args(argv[1:])
		ranges = []
		for arg in args.offsets:
			if arg == 'all':
				for x in self.fmdb.query_loff_range([]):
					self.print_extent(x)
				return
			elif '-' in arg:
				pos = arg.index('-')
				ranges.append((n2p(self.fs, arg[:pos]), n2p(self.fs, arg[pos+1:])))
			else:
				ranges.append(n2p(self.fs, arg))
		for x in self.fmdb.query_loff_range(ranges):
			self.print_extent(x)

	def do_poff_to_extents(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents of a given range of physical offsets.')
		parser.add_argument('offsets', nargs = '+', \
			help = 'Physical offsets to look up.')
		args = parser.parse_args(argv[1:])
		ranges = []
		for arg in args.offsets:
			if arg == 'all':
				for x in self.fmdb.query_poff_range([]):
					self.print_extent(x)
				return
			elif '-' in arg:
				pos = arg.index('-')
				ranges.append((n2p(self.fs, arg[:pos]), n2p(self.fs, arg[pos+1:])))
			else:
				ranges.append(n2p(self.fs, arg))
		for x in self.fmdb.query_poff_range(ranges):
			self.print_extent(x)

	def do_cell_to_extents(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents of a given range of overview cells.')
		parser.add_argument('cells', nargs = '+', \
			help = 'Cell ranges to look up.')
		args = parser.parse_args(argv[1:])
		ranges = []
		for arg in args.cells:
			if arg == 'all':
				for x in self.fmdb.query_poff_range([]):
					self.print_extent(x)
				return
			elif '-' in arg:
				pos = arg.index('-')
				ranges.append((int(arg[:pos]), int(arg[pos+1:])))
			else:
				ranges.append(int(arg))
		r = self.fmdb.pick_cells(ranges)
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
		parser.add_argument('units', \
			help = 'Units for display output.  Default is bytes.')
		args = parser.parse_args(argv[1:])
		for u in avail_units:
			if args.units.lower() == u.abbrev.lower() or \
			   args.units.lower() == u.label.lower():
				self.units = u
				print("Units set to '%s'." % self.units.label)
				return
		print("Unrecognized unit '%s'.  Available units:" % args.units)
		print(', '.join([x[1] for x in avail_units]))

	def do_paths(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents of a given path.')
		parser.add_argument('paths', nargs = '+', \
			help = 'Paths to look up.')
		args = parser.parse_args(argv[1:])
		if '*' in args.paths:
			for ext in self.fmdb.query_paths([]):
				self.print_extent(ext)
			return
		for ext in self.fmdb.query_paths(args.paths):
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
			help = 'Inodes to look up.')
		args = parser.parse_args(argv[1:])
		ranges = []
		for arg in args.inodes:
			if arg == 'all':
				for x in self.fmdb.query_inodes([]):
					self.print_extent(x)
				return
			elif '-' in arg:
				pos = arg.index('-')
				ranges.append((int(arg[:pos]), int(arg[pos+1:])))
			else:
				ranges.append(int(arg))
		for x in self.fmdb.query_inodes(ranges):
			self.print_extent(x)

	def do_lengths(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents of a given range of lengths.')
		parser.add_argument('lengths', nargs = '+', \
			help = 'Lengths to look up.')
		args = parser.parse_args(argv[1:])
		ranges = []
		for arg in args.lengths:
			if arg == 'all':
				for x in self.fmdb.query_lengths([]):
					self.print_extent(x)
				return
			elif '-' in arg:
				pos = arg.index('-')
				ranges.append((n2p(self.fs, arg[:pos]), n2p(self.fs, arg[pos+1:])))
			else:
				ranges.append(n2p(self.fs, arg))
		for x in self.fmdb.query_lengths(ranges):
			self.print_extent(x)

	def do_extent_type(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents with a particular type.')
		parser.add_argument('types', nargs = '+', \
			help = 'Type codes to look up.  Valid values are: (f)ile, (d)irectory, (e)xtent map, FS (m)etadata, and e(x)tended attributes.', \
			choices = ['f', 'd', 'e', 'm', 'x'])
		args = parser.parse_args(argv[1:])
		types = set()
		for arg in args.types:
			for l in arg:
				types.add(l)
		for x in self.fmdb.query_extent_types(types):
			self.print_extent(x)

	def do_extent_flag(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents with a particular set of flags.')
		parser.add_argument('flags', nargs = '*', \
			help = 'Flag codes to look up.  Valid values are: u(n)known, (d)elayed allocation, (e)ncoded, (E)ncrypted, (u)naligned, (i)nline, (t)ail-packed, (U)nwritten, (m)erged, (s)hared, or no flag code at all.', \
			choices = ['n', 'd', 'e', 'E', 'u', 'i', 't', 'U', 'm', 's', []])
		parser.add_argument('-e', default = 0, action = 'count', help = 'Flags must match exactly.')
		args = parser.parse_args(argv[1:])
		flags = 0
		for arg in args.flags:
			flags |= fiemap.extent_str_to_flags(arg)
		for x in self.fmdb.query_extent_flags(flags, args.e > 0):
			self.print_extent(x)

	def do_ls(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up directories in the filesystem tree.')
		parser.add_argument('dirnames', nargs = '+', \
			help = 'Directory names to look up.')
		args = parser.parse_args(argv[1:])
		if '*' in args.dirnames:
			for de in self.fmdb.query_ls([]):
				self.print_dentry(de)
			return
		dnames = ['' if p == self.fs.pathsep else p for p in args.dirnames]
		for de in self.fmdb.query_ls(dnames):
			self.print_dentry(de)
