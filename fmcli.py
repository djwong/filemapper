# filemapper CLI
# Copyright (C) 2015 Darrick J. Wong
# Licensed under GPLv2

import code
import readline
import atexit
import os
import argparse
import sys

typecodes = {
	'f': 'file',
	'd': 'directory',
	'e': 'file map',
	'm': 'metadata',
	'x': 'extended attribute',
}

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

def format_number(units, num):
	return "{:,} {}".format(int(units[2](num)), units[1])

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
			('machine', 'm'): self.do_machine,
			('overview', 'o'): self.do_overview,
			('phys', 'p'): self.do_poff_to_extents,
			('quit', 'exit', 'q'): self.do_exit,
			('summary', 's'): self.do_summary,
			('units', 'u'): self.do_set_units,
		}
		self.done = False
		self.units = ['', 'bytes', lambda x: x]
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
		print("Block size:\t{:,}".format(res.block_size))
		print("Fragment size:\t{:,}".format(res.frag_size))
		print("Total space:\t%s" % format_number(self.units, res.total_bytes))
		print("Used space:\t%s (%.0f%%)" % \
			(format_number(self.units, res.total_bytes - res.free_bytes), \
			 100 * (1.0 - (res.free_bytes / res.total_bytes))))
		print("Free space:\t%s" % format_number(self.units, res.free_bytes))
		print("Total inodes:\t{:,}".format(res.total_inodes))
		print("Used inodes:\t{:,} ({:.0%})".format(res.total_inodes - res.free_inodes, 1.0 - (res.free_inodes / res.total_inodes)))
		print("Free inodes:\t{:,}".format(res.free_inodes))

	def print_extent(self, ext):
		if self.machine:
			print("'%s',%d,%d,%d,0x%x,'%s'" % \
				(ext.path, \
				 ext.p_off, ext.l_off, ext.length, \
				 ext.flags, typecodes[ext.type]))
			return
		print("'%s', %s, %s, %s, 0x%x, '%s'" % \
			(ext.path, \
			 format_number(self.units, ext.p_off), \
			 format_number(self.units, ext.l_off), \
			 format_number(self.units, ext.length), \
			 ext.flags, typecodes[ext.type]))

	def do_poff_to_extents(self, argv):
		def n2p(num):
			conv = {
				'%': lambda x: x * res.total_bytes / 100,
				'b': lambda x: x * res.block_size,
				'B': lambda x: x * res.block_size,
				's': lambda x: x * (2 ** 9),
				'S': lambda x: x * (2 ** 9),
				'k': lambda x: x * (2 ** 10),
				'K': lambda x: x * (2 ** 10),
				'M': lambda x: x * (2 ** 20),
				'm': lambda x: x * (2 ** 20),
				'G': lambda x: x * (2 ** 30),
				'g': lambda x: x * (2 ** 30),
				'T': lambda x: x * (2 ** 40),
				't': lambda x: x * (2 ** 40),
			}
			if num[-1] in conv:
				fn = conv[num[-1]]
				return int(fn(int(num[:-1])))
			else:
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
		units = [
			['', 'bytes', lambda x: x],
			['s', 'sectors', lambda x: x // (2 ** 9)],
			['B', 'blocks', lambda x: x // (res.block_size)],
			['K', 'KiB', lambda x: x / (2 ** 10)],
			['M', 'MiB', lambda x: x / (2 ** 20)],
			['G', 'GiB', lambda x: x / (2 ** 30)],
			['T', 'TiB', lambda x: x / (2 ** 40)],
		]
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Set display units.')
		parser.add_argument('units', \
			help = 'Units for display output.  Default is bytes.')
		args = parser.parse_args(argv[1:])
		for u in units:
			if args.units.lower() == u[0].lower() or \
			   args.units.lower() == u[1].lower():
				self.units = u
				print("Units set to '%s'." % self.units[1])
				return
		print("Unrecognized unit '%s'.  Available units:" % args.units)
		print(', '.join([x[1] for x in units]))

	def do_paths(self, argv):
		parser = argparse.ArgumentParser(prog = argv[0],
			description = 'Look up extents of a given path.')
		parser.add_argument('-e', action = 'count', default = 0, \
			help = 'Exact matches only.')
		parser.add_argument('paths', nargs = '+', \
			help = 'Paths to look up.')
		args = parser.parse_args(argv[1:])
		if '*' in args.paths:
			for ext in self.fmdb.query_paths([], False):
				self.print_extent(ext)
			return
		arg = [x.replace('*', '%') for x in args.paths]
		if args.e > 0:
			exact = False
		else:
			exact = True
		for ext in self.fmdb.query_paths(arg, exact):
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

if __name__ == '__main__':
	fmcli().interact()
