#!/usr/bin/python

# Crappy program to drive filemapper via GUI.
# Copyright (C) 2010 Darrick J. Wong.  All rights reserved.
# This program is licensed under the GNU General Public License, version 2.

try:
 	import pygtk
  	pygtk.require("2.0")
except:
  	pass
try:
	import sys
	import gtk
	import gtk.glade
	import gobject
	import os
	import vte
	import pango
	import subprocess
	import fcntl
	import thread
	import select
	import threading
	import time
except ImportError, e:
	print "Import error gfilemapper cannot start:", e
	sys.exit(1)

class gfilemapper_window(object):
	"""Main gfilemapper window."""

	def __init__(self, driver, title):
		self.title = title
		self.window_tree = gtk.glade.XML("gfilemapper.glade", "main_window")
		self.window = self.window_tree.get_widget("main_window")
		events = {
			"on_main_window_destroy": self.close_app,
			"on_filter_btn_clicked": self.set_filter,
			"on_restriction_list_changed": self.set_filter_type,
			"on_pull_selection_btn_clicked": self.pull_selection_btn,
			"on_addfile_btn_clicked": self.add_file_btn}
		self.window_tree.signal_autoconnect(events)
		self.window.connect("destroy", self.close_app)

		self.map = self.window_tree.get_widget("map_text")
		self.map.modify_font(pango.FontDescription("Anonymous Pro,DejaVu Sans Mono,Consolas,Lucida Console,monospace 8"))
		self.map_buffer = self.map.get_buffer()

		self.restriction_list = self.window_tree.get_widget("restriction_list")
		self.restriction_text = self.window_tree.get_widget("restriction_text")
		self.addfile_btn = self.window_tree.get_widget("addfile_btn")
		self.pull_selection_btn = self.window_tree.get_widget("pull_selection_btn")
		self.detail_list = self.window_tree.get_widget("detail_list")
		self.detail_list.set_headers_clickable(True)

		self.driver = driver
		self.read_map()

		self.window.show_all()
		self.restriction_list.set_active(4)
		self.set_filter_type(None)
		self.filter_descr = "Overview"
		self.set_window_title()

	def set_filter_type(self, widget):
		ui_elements = [[False, False, False], [True, False, False], [True, False, True], [True, False, False], [True, True, False], [True, False, True]]
		idx = self.restriction_list.get_active()
		self.set_filter_ui(ui_elements[idx])

	def pull_selection_btn(self, widget):
		iter = self.map.get_buffer().get_selection_bounds()
		if len(iter) == 0:
			x = self.map.get_buffer().get_property("cursor-position")
			y = x
		else:
			x = iter[0].get_offset()
			y = iter[1].get_offset() - 1
		text = "%d" % x
		if y != x:
			text = text + "-%d" % y
		x = self.restriction_text.get_text()
		if len(x):
			x = x + " "
		self.restriction_text.set_text(x + text)

	def add_file_btn(self, widget):
		fcd = gtk.FileChooserDialog("View File Blocks")
		fcd.add_button("OK", 0)
		fcd.add_button("Cancel", 1)
		fcd.set_default_response(0)
		fcd.set_local_only(True)
		fcd.set_select_multiple(True)
		result = fcd.run()
		fcd.hide()
		if result != 0:
			return

		x = self.restriction_text.get_text()
		for fname in fcd.get_filenames():
			if len(x):
				x = x + " "
			x = x + fname
		self.restriction_text.set_text(x)

	def set_filter_ui(self, flags):
		if flags[0]:
			self.restriction_text.show()
		else:
			self.restriction_text.hide()

		if flags[1]:
			self.pull_selection_btn.show()
		else:
			self.pull_selection_btn.hide()

		if flags[2]:
			self.addfile_btn.show()
		else:
			self.addfile_btn.hide()

	def close_app(self, widget):
		gtk.main_quit()

	def read_map(self):
		self.read_until_prompt()
		str = self.driver.read_error_line()
		str = self.driver.read_output_line()
		if len(str) and str != "Map:":
			return 1
		self.read_map_to_display()

	def read_map_to_display(self):
		str = self.driver.read_output_line()
		str = str.replace('.', '-')
		self.map_buffer.set_text(str)

	def read_until_prompt(self):
		self.driver.wait_for_prompt("filemapper> ")
		print "prompt!"

	def overview_filter(self, args):
		self.driver.writeln("o")
		self.set_detail_columns(None, [])
		self.read_map()

	def files_filter(self, args):
		self.driver.writeln("f " + args)
		self.read_until_prompt()
		str = self.driver.read_output_line()

		model = gtk.TreeStore(gobject.TYPE_STRING, gobject.TYPE_UINT64, gobject.TYPE_UINT64)
		self.set_detail_columns(model, ["File", "Start", "End"])

		while len(str) and str != "Map:":
			cols = str.split()
			blocks = cols[5].split("-")
			model.append(None, [cols[1], int(blocks[0]), int(blocks[1].strip("."))])
			str = self.driver.read_output_line()

		self.read_map_to_display()

	def file_trees_filter(self, args):
		self.driver.writeln("r " + args)
		self.read_until_prompt()

		model = gtk.TreeStore(gobject.TYPE_STRING, gobject.TYPE_UINT64, gobject.TYPE_UINT64)
		self.set_detail_columns(model, ["File", "Start", "End"])

		str = self.driver.read_output_line()
		while len(str) and str != "Map:":
			cols = str.split()
			blocks = cols[5].split("-")
			model.append(None, [cols[1], int(blocks[0]), int(blocks[1].strip("."))])
			str = self.driver.read_output_line()

		self.read_map_to_display()

	def set_detail_columns(self, model, columns):
		for column in self.detail_list.get_columns():
			self.detail_list.remove_column(column)

		renderer = gtk.CellRendererText()
		for i in range(0, len(columns)):
			column = gtk.TreeViewColumn(columns[i], renderer, text = i)
			self.detail_list.append_column(column)

		self.detail_list.set_model(model)

	def map_blocks_filter(self, args):
		self.driver.writeln("m " + args)
		self.read_until_prompt()

		model = gtk.TreeStore(gobject.TYPE_UINT64, gobject.TYPE_STRING)
		self.set_detail_columns(model, ["Block", "File"])

		str = self.driver.read_output_line()
		while len(str) and str != "Map:":
			cols = str.split()
			model.append(None, [int(cols[1]), cols[4].strip(".")])
			str = self.driver.read_output_line()

		self.read_map_to_display()

	def blocks_filter(self, args):
		self.driver.writeln("b " + args)
		self.read_until_prompt()

		model = gtk.TreeStore(gobject.TYPE_UINT64, gobject.TYPE_STRING)
		self.set_detail_columns(model, ["Block", "File"])

		str = self.driver.read_output_line()
		while len(str) and str != "Map:":
			cols = str.split()
			model.append(None, [int(cols[1]), cols[4].strip(".")])
			str = self.driver.read_output_line()

		self.read_map_to_display()

	def inodes_filter(self, args):
		self.driver.writeln("i " + args)
		self.read_until_prompt()

		model = gtk.TreeStore(gobject.TYPE_STRING, gobject.TYPE_UINT64, gobject.TYPE_UINT64)
		self.set_detail_columns(model, ["Inode", "Start", "End"])

		str = self.driver.read_output_line()
		while len(str) and str != "Map:":
			cols = str.split()
			blocks = cols[5].split("-")
			model.append(None, [cols[1], int(blocks[0]), int(blocks[1].strip("."))])
			str = self.driver.read_output_line()

		self.read_map_to_display()

	def set_filter(self, widget):
		filters = [self.overview_filter, self.blocks_filter, self.files_filter, self.inodes_filter, self.map_blocks_filter, self.file_trees_filter]
		idx = self.restriction_list.get_active()
		filter = filters[idx]
		args = self.restriction_text.get_text()
		filter(args)
		self.filter_descr = self.restriction_list.get_active_text() + ": " + self.restriction_text.get_text()
		self.set_window_title()

	def set_window_title(self):
		self.window.set_title(self.title + " - " + self.filter_descr)

class process_driver:
	def __init__(self, subprocess):
		self.outputstr = ""
		self.errorstr = ""
		self.outputs = []
		self.errors = []
		self.process = subprocess
		self.outeof = False
		self.erreof = False
		self.output_waiter = threading.Condition()

		fd = self.process.stdout.fileno()
		flags = fcntl.fcntl(fd, fcntl.F_GETFL)
		fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

		fd = self.process.stderr.fileno()
		flags = fcntl.fcntl(fd, fcntl.F_GETFL)
		fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

	def start(self):
		thread.start_new_thread(self.loop, ())

	def read_output_line(self):
		if len(self.outputs) == 0:
			return self.outputstr
		return self.outputs.pop(0).strip()

	def read_error_line(self):
		if len(self.errors) == 0:
			return self.errorstr
		return self.errors.pop(0).strip()

	def writeln(self, str):
		cmd = str + "\n"
		print ["command", cmd]
		self.clear_reads()
		self.process.stdin.write(cmd)
		self.process.stdin.flush()

	def clear_reads(self):
		self.outputs = []
		self.errors = []
		self.outputstr = ""
		self.errorstr = ""

	def prompt_seen(self, prompt):
		if len(self.outputstr) > 0:
			if self.outputstr == prompt:
				return True
			return False
		if len(self.outputs) > 0:
			if self.outputs[-1] == prompt:
				return True
		return False

	def wait_for_prompt(self, prompt):
		while not self.prompt_seen(prompt):
			self.output_waiter.acquire()
			self.output_waiter.wait()
			self.output_waiter.release()

	def loop(self):
		while True:
			self.loop_once()
			if self.outeof and self.erreof:
				break

	def loop_once(self):
		tocheck = [self.process.stdout, self.process.stderr]
		ready = select.select(tocheck, [], [], 1)
		if len(ready[0]) == 0:
			return

		if self.process.stdout in ready[0]:
			x = self.process.stdout.read(10)
			if x == "":
				self.outeof = True
				if len(self.outputstr) > 0:
					self.outputs.append(self.outputstr)
				self.outputstr = ""
			else:
				x = self.outputstr + x
				strings = x.splitlines(True)
				last = strings.pop()
				self.outputs = self.outputs + strings
				if last[-1] == "\n":
					self.outputs.append(last)
					self.outputstr = ""
				else:
					self.outputstr = last
			self.output_waiter.acquire()
			self.output_waiter.notify()
			self.output_waiter.release()
		if self.process.stderr in ready[0]:
			x = self.process.stderr.read()
			if x == "":
				self.erreof = True
				if len(self.errorstr) > 0:
					self.errors.append(self.errorstr)
				self.errorstr = ""
			else:
				x = self.errorstr + x
				strings = x.splitlines(True)
				last = strings.pop()
				self.errors = self.errors + strings
				if last[-1] == "\n":
					self.errors.append(last)
					self.errorstr = ""
				else:
					self.errorstr = last

if __name__ == "__main__":
	cmd = ["gksudo", "./filemapper"]
	cmd = ["./filemapper"]
	title = "FileMapper -"
	if len(sys.argv) > 1:
		cmd = cmd + sys.argv[1:]
		for arg in sys.argv[1:]:
			title = title + " " + arg
	else:
		cmd.append("/")
		title = title + " /"
	print ["command", cmd]
	filemapper = subprocess.Popen(cmd, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE, close_fds = 1)
	driver = process_driver(filemapper)
	driver.start()

	gtk.gdk.threads_init()
	gfw = gfilemapper_window(driver, title)
	gtk.gdk.threads_enter()
	gtk.main()
	gtk.gdk.threads_leave()
