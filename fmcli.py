# filemapper CLI
# Copyright (C) 2015 Darrick J. Wong
# Licensed under GPLv2.

import code
import readline
import atexit
import os
import argparse
import sys
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

def split_unescape(s, delim, str_delim, escape='\\', unescape=True):
	"""
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
	def __init__(self, fmdb, locals=None, filename="<console>", \
		     histfile=os.path.expanduser("~/.config/fmcli-history")):
		code.InteractiveConsole.__init__(self, locals, filename)
		self.init_history(histfile)
		self.fmdb = fmdb
		readline.set_history_length(1000)
		self.commands = {
			('cell', 'c'): self.do_cell_to_extents,
			('help', 'h', '?'): self.do_help,
			('file', 'f'): self.do_paths,
			('inode', 'i'): self.do_inodes,
			('ls', 'l'): self.do_ls,
			('machine', 'm'): self.do_machine,
			('overview', 'o'): self.do_overview,
			('phys', 'p'): self.do_poff_to_extents,
			('quit', 'exit', 'q'): self.do_exit,
			('summary', 's'): self.do_summary,
			('units', 'u'): self.do_set_units,
		}
		self.done = False
		self.units = units_auto
		self.machine = False

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
		res = self.fmdb.query_summary()
		print("Summary of '%s':" % res.path)
		print("Block size:\t%s" % format_size(units_auto, res.block_size))
		print("Fragment size:\t%s" % format_size(units_auto, res.frag_size))
		print("Total space:\t%s" % format_size(self.units, res.total_bytes))
		print("Used space:\t%s (%.0f%%)" % \
			(format_size(self.units, res.total_bytes - res.free_bytes), \
			 100 * (1.0 - (res.free_bytes / res.total_bytes))))
		print("Free space:\t%s" % format_size(self.units, res.free_bytes))
		print("Total inodes:\t%s" % format_number(units_auto, res.total_inodes))
		print("Used inodes:\t%s (%.0f%%)" % \
			(format_number(units_auto, res.total_inodes - res.free_inodes), \
			100 * (1.0 - (res.free_inodes / res.total_inodes))))
		print("Free inodes:\t%s" % format_number(units_auto, res.free_inodes))
		print("Extents:\t%s" % format_number(units_auto, res.extents))

	def print_extent(self, ext):
		if self.machine:
			print("'%s',%d,%d,%d,0x%x,'%s'" % \
				(ext.path if ext.path != '' else '/', \
				 ext.p_off, ext.l_off, ext.length, \
				 ext.flags_to_str(), \
				 typecodes[ext.type]))
			return
		print("'%s', %s, %s, %s, 0x%x, '%s'" % \
			(ext.path if ext.path != '' else '/', \
			 format_size(self.units, ext.p_off), \
			 format_size(self.units, ext.l_off), \
			 format_size(self.units, ext.length), \
			 ext.flags_to_str(), \
			 typecodes[ext.type]))

	def print_dentry(self, de):
		if self.machine:
			print("'%s',%d,'%s'" % \
				(de.name, de.ino, de.type))
			return
		print("'%s', %s, '%s'" % \
			(de.name, format_number(units_none, de.ino), de.type))

	def do_poff_to_extents(self, argv):
		def n2p(num):
			conv = [
				units('%', 'percent', None, res.total_bytes / 100),
				units('B', 'blocks', None, res.block_size),
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

		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents of a given range of physical offsets.')
		parser.add_argument('offsets', nargs = '+', \
			help = 'Physical offsets to look up.')
		args = parser.parse_args(argv[1:])
		ranges = []
		res = self.fmdb.query_summary()
		for arg in args.offsets:
			if arg == 'all':
				for x in self.fmdb.query_poff_range([]):
					self.print_extent(x)
				return
			elif '-' in arg:
				pos = arg.index('-')
				ranges.append((n2p(arg[:pos]), n2p(arg[pos+1:])))
			else:
				ranges.append((n2p(arg), n2p(arg)))
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
		res = self.fmdb.query_summary()
		avail_units = [
			units_auto,
			units_bytes,
			units_sectors,
			units('B', 'blocks', res.block_size),
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
				ranges.append((int(arg), int(arg)))
		for x in self.fmdb.query_inodes(ranges):
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
		dnames = ['' if p == '/' else p for p in args.dirnames]
		for de in self.fmdb.query_ls(dnames):
			self.print_dentry(de)
