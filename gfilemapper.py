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

VERSION = "gfilemapper v0.40"
PROMPT_STRING = "filemapper> "
DEFAULT_MAP_WIDTH = 3600

class gfilemapper_window(object):
	"""Main gfilemapper window."""

	def __init__(self, driver, title):
		default_restriction = 4
		default_font = "Source Code Pro,Anonymous Pro,DejaVu Sans Mono,Consolas,Lucida Console,monospace 8"

		self.title = title
		self.window_tree = gtk.glade.XML("gfilemapper.glade", "main_window")
		self.window = self.window_tree.get_widget("main_window")
		events = {
			"on_main_window_destroy": self.close_app,
			"on_filter_btn_clicked": self.set_filter_clicked,
			"on_restriction_list_changed": self.set_filter_type,
			"on_pull_selection_btn_clicked": self.pull_selection_btn_clicked,
			"on_addfile_btn_clicked": self.add_file_btn_clicked,
			"on_set_font_btn_clicked": self.set_font_btn_clicked,
			"on_clear_btn_clicked": self.clear_btn_clicked}
		self.window_tree.signal_autoconnect(events)
		self.window.connect("destroy", self.close_app)

		self.map = self.window_tree.get_widget("map_text")
		self.map.get_buffer().connect("notify::has-selection", self.map_selection_changed)
		self.map.modify_font(pango.FontDescription(default_font))
		self.map_buffer = self.map.get_buffer()

		self.restriction_list = self.window_tree.get_widget("restriction_list")
		self.restriction_text = self.window_tree.get_widget("restriction_text")
		self.addfile_btn = self.window_tree.get_widget("addfile_btn")
		self.filter_btn = self.window_tree.get_widget("filter_btn")
		self.pull_selection_btn = self.window_tree.get_widget("pull_selection_btn")
		self.set_font_btn = self.window_tree.get_widget("set_font_btn")
		self.detail_list = self.window_tree.get_widget("detail_list")
		self.status_bar = self.window_tree.get_widget("status_bar")
		self.clear_btn = self.window_tree.get_widget("clear_btn")
		self.detail_label = self.window_tree.get_widget("detail_label")
		self.detail_frame = self.window_tree.get_widget("detail_frame")
		self.status_bar.push(0, "Working...")
		self.detail_list.set_headers_clickable(True)
		self.old_restrictions = ["", "", "", "", "", "", "%d" % DEFAULT_MAP_WIDTH, default_font]
		self.old_restriction = default_restriction
		self.have_filter = 0

		self.driver = driver
		self.read_until_prompt()
		self.read_map()

		self.window.show_all()
		self.restriction_list.set_active(default_restriction)
		self.set_filter_type(None)
		self.filter_descr = "Overview"
		self.set_window_title()

	def set_filter_type(self, widget):
		idx = self.restriction_list.get_active()
		self.set_filter_ui(idx)

	def pull_selection_btn_clicked(self, widget):
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

	def add_file_btn_clicked(self, widget):
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

	def set_font_btn_clicked(self, widget):
		fsd = gtk.FontSelectionDialog("Set Font")
		fsd.set_font_name(self.restriction_text.get_text())
		result = fsd.run()
		fsd.hide()
		if result == gtk.RESPONSE_OK:
			self.restriction_text.set_text(fsd.get_font_name())
			self.map.modify_font(pango.FontDescription(fsd.get_font_name()))

	def set_filter_ui(self, idx):
		ui_elements = [ [False, False, False, False, "_See Overview", False, ""],
				[True, False, False, False, "_Set Criteria", True, "Block to File Mapping"],
				[True, False, True, False, "_Set Criteria", True, "File to Block Mapping"],
				[True, False, False, False, "_Set Criteria", True, "Inode to Block Mapping"],
				[True, True, False, False, "_Set Criteria", True, "Map Block to File Mapping"],
				[True, False, True, False, "_Set Criteria", True, "File to Block Mapping"],
				[True, False, True, False, "_Set Criteria", True, "File to Block Mapping"],
				[True, False, False, False, "_Set Map Length", False, None],
				[False, False, False, True, None, False, None]]
		flags = ui_elements[idx]

		self.old_restrictions[self.old_restriction] = self.restriction_text.get_text()
		self.restriction_text.set_text(self.old_restrictions[idx])
		self.old_restriction = idx

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

		if flags[3]:
			self.set_font_btn.show()
		else:
			self.set_font_btn.hide()

		if flags[4] != None:
			self.filter_btn.set_label(flags[4])
			self.filter_btn.show()
		else:
			self.filter_btn.hide()

		if flags[5]:
			self.clear_btn.show()
		else:
			self.clear_btn.hide()

		self.validate_map_selection()

		# Now do the UI elements for the current filter
		flags = ui_elements[self.have_filter]
		if flags[6] != None and flags[6] != "":
			self.detail_label.set_label("<b>" + flags[6] + "</b>")
			self.detail_frame.show()
		elif flags[6] == "":
			self.detail_frame.hide()

	def close_app(self, widget):
		self.driver.terminate()
		gtk.main_quit()

	def read_map(self):
		str = self.driver.read_output_line()
		if len(str) and str != "Map:":
			return 1
		self.read_map_to_display()

	def read_map_to_display(self):
		str = self.driver.read_output_line()
		self.map_buffer.set_text(str)
		str = self.driver.read_output_line()
		self.status_bar.pop(0)
		self.status_bar.push(0, str)

	def report_errors(self, willdie):
		msg = ""
		x = self.driver.read_error_line()
		while x != "":
			msg = msg + x + "\n"
			x = self.driver.read_error_line()
		msg = msg + "\n"
		if willdie:
			msg = msg + "FileMapper will now exit."
		else:
			msg = msg + "FileMapper will try to continue."
		md = gtk.MessageDialog(type = gtk.MESSAGE_ERROR, buttons = gtk.BUTTONS_CLOSE, message_format = msg)
		md.set_title("FileMapper Errors")
		md.run()
		md.hide()

	def is_prompt(self, str):
		if str == PROMPT_STRING:
			return True
		return False

	def read_until_prompt(self):
		res = self.driver.wait_for_prompt(PROMPT_STRING)
		if res == False:
			self.report_errors(True)
			sys.exit(1)
		if self.driver.has_errors():
			self.report_errors(False)

	def overview_filter(self, args):
		self.driver.writeln("o")
		self.read_until_prompt()
		self.set_detail_columns(None, [])
		self.read_map()
		self.filter_descr = "Overview"
		self.have_filter = 0

	def walk_path_filter(self, args):
		self.driver.writeln("t " + args)
		self.read_until_prompt()

		model = gtk.TreeStore(gobject.TYPE_STRING, gobject.TYPE_UINT64, gobject.TYPE_UINT64)
		self.set_detail_columns(model, ["File", "Start", "End"])

		str = self.driver.read_output_line()
		if self.is_prompt(str):
			return
		while len(str) and str != "Map:":
			cols = str.split("|")
			model.append(None, [cols[0], int(cols[1]), int(cols[2])])
			str = self.driver.read_output_line()

		self.detail_list.set_model(model)
		self.read_map_to_display()
		self.have_filter = 6

	def files_filter(self, args):
		self.driver.writeln("f " + args)
		self.read_until_prompt()

		model = gtk.TreeStore(gobject.TYPE_STRING, gobject.TYPE_UINT64, gobject.TYPE_UINT64)
		self.set_detail_columns(model, ["File", "Start", "End"])

		str = self.driver.read_output_line()
		if self.is_prompt(str):
			return
		while len(str) and str != "Map:":
			cols = str.split("|")
			model.append(None, [cols[0], int(cols[1]), int(cols[2])])
			str = self.driver.read_output_line()

		self.detail_list.set_model(model)
		self.read_map_to_display()
		self.have_filter = 2

	def file_trees_filter(self, args):
		self.driver.writeln("r " + args)
		self.read_until_prompt()

		model = gtk.TreeStore(gobject.TYPE_STRING, gobject.TYPE_UINT64, gobject.TYPE_UINT64)
		self.set_detail_columns(model, ["File", "Start", "End"])

		str = self.driver.read_output_line()
		if self.is_prompt(str):
			return
		while len(str) and str != "Map:":
			cols = str.split("|")
			model.append(None, [cols[0], int(cols[1]), int(cols[2])])
			str = self.driver.read_output_line()

		self.detail_list.set_model(model)
		self.read_map_to_display()
		self.have_filter = 5

	def set_detail_columns(self, model, columns):
		for column in self.detail_list.get_columns():
			self.detail_list.remove_column(column)
		self.detail_list.set_model(None)

		renderer = gtk.CellRendererText()
		for i in range(0, len(columns)):
			column = gtk.TreeViewColumn(columns[i], renderer, text = i)
			column.set_clickable(True)
			column.set_resizable(True)
			column.set_sort_column_id(i)
			self.detail_list.append_column(column)

	def map_blocks_filter(self, args):
		self.driver.writeln("m " + args)
		self.read_until_prompt()

		model = gtk.TreeStore(gobject.TYPE_UINT64, gobject.TYPE_UINT64, gobject.TYPE_STRING)
		self.set_detail_columns(model, ["Start", "End", "File"])

		str = self.driver.read_output_line()
		if self.is_prompt(str):
			return
		while len(str) and str != "Map:":
			cols = str.split("|")
			model.append(None, [int(cols[0]), int(cols[1]), cols[2]])
			str = self.driver.read_output_line()

		self.detail_list.set_model(model)
		self.read_map_to_display()
		self.have_filter = 4

	def blocks_filter(self, args):
		self.driver.writeln("b " + args)
		self.read_until_prompt()

		model = gtk.TreeStore(gobject.TYPE_UINT64, gobject.TYPE_UINT64, gobject.TYPE_STRING)
		self.set_detail_columns(model, ["Start", "End", "File"])

		str = self.driver.read_output_line()
		if self.is_prompt(str):
			return
		while len(str) and str != "Map:":
			cols = str.split("|")
			model.append(None, [int(cols[0]), int(cols[1]), cols[2]])
			str = self.driver.read_output_line()

		self.detail_list.set_model(model)
		self.read_map_to_display()
		self.have_filter = 1

	def inodes_filter(self, args):
		self.driver.writeln("i " + args)
		self.read_until_prompt()

		model = gtk.TreeStore(gobject.TYPE_STRING, gobject.TYPE_UINT64, gobject.TYPE_UINT64)
		self.set_detail_columns(model, ["Inode", "Start", "End"])

		str = self.driver.read_output_line()
		if self.is_prompt(str):
			return
		while len(str) and str != "Map:":
			cols = str.split("|")
			model.append(None, [cols[0], int(cols[1]), int(cols[2])])
			str = self.driver.read_output_line()

		self.detail_list.set_model(model)
		self.read_map_to_display()
		self.have_filter = 3

	def set_map_width(self, args):
		self.driver.writeln("w " + args)
		self.read_until_prompt()
		self.clear_btn_clicked(None)

	def set_font(self, args):
		pass

	def set_filter_clicked(self, widget):
		filters = [self.overview_filter, self.blocks_filter,
			   self.files_filter, self.inodes_filter,
			   self.map_blocks_filter, self.file_trees_filter,
			   self.walk_path_filter,
			   self.set_map_width, self.set_font]
		idx = self.restriction_list.get_active()
		filter = filters[idx]
		args = self.restriction_text.get_text()
		self.filter_descr = self.restriction_list.get_active_text() + ": " + self.restriction_text.get_text()
		filter(args)
		self.set_window_title()
		idx = self.restriction_list.get_active()
		self.set_filter_ui(idx)

	def clear_btn_clicked(self, widget):
		self.restriction_list.set_active(0)
		self.set_filter_clicked(None)

	def set_window_title(self):
		self.window.set_title(self.title + " - " + self.filter_descr)

	def map_selection_changed(self, widget, buf):
		self.validate_map_selection()

	def validate_map_selection(self):
		make_sel = self.map.get_buffer().get_has_selection()
		self.pull_selection_btn.set_sensitive(make_sel)

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
		self.killer = threading.Condition()

		fd = self.process.stdout.fileno()
		flags = fcntl.fcntl(fd, fcntl.F_GETFL)
		fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

		fd = self.process.stderr.fileno()
		flags = fcntl.fcntl(fd, fcntl.F_GETFL)
		fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

	def terminate(self):
		if self.process.poll() != None:
			return
		self.should_kill = True
		self.killer.acquire()
		self.killer.wait()
		self.killer.release()

	def start(self):
		self.should_kill = False
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

	def has_errors(self):
		if len(self.errorstr) > 0 or len(self.errors) > 0:
			return True
		return False

	def prompt_seen(self, prompt):
		if len(self.outputstr) > 0:
			if self.outputstr == prompt:
				return True
			return False

		if len(self.outputs) > 0:
			if self.outputs[-1] == prompt:
				return True

		if self.has_errors():
			return None

		return False

	def wait_for_prompt(self, prompt):
		while True:
			res = self.prompt_seen(prompt)
			if self.process.poll() != None or res == None:
				return False
			if res == True:
				print "prompt!"
				return True
			self.output_waiter.acquire()
			self.output_waiter.wait()
			self.output_waiter.release()

	def loop(self):
		while True:
			self.loop_once()
			if self.outeof and self.erreof:
				break
			if self.should_kill:
				self.process.terminate()
		self.killer.acquire()
		self.killer.notify()
		self.killer.release()

	def loop_once(self):
		tocheck = [self.process.stdout, self.process.stderr]
		ready = select.select(tocheck, [], [], 1)
		if len(ready[0]) == 0:
			return

		if self.process.stdout in ready[0]:
			x = self.process.stdout.read()
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
			self.output_waiter.acquire()
			self.output_waiter.notify()
			self.output_waiter.release()

if __name__ == "__main__":
	print VERSION
	cmd = ["./filemapper", "-m", "-w", "%d" % DEFAULT_MAP_WIDTH]
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
