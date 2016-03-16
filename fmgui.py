#!/usr/bin/env python3
# QT GUI routines for filemapper
# Copyright(C) 2016 Darrick J. Wong
# Licensed under GPLv2.

import sys
try:
	from PyQt5 import QtGui, uic, QtCore, QtWidgets, Qt
	print("Loading qt5...")
except:
	from PyQt4 import QtGui, uic, QtCore, Qt
	from PyQt4 import QtGui as QtWidgets
	print("Loading qt4...")
import fmcli
import datetime
import fmdb
import math
import os.path
import json
import base64
from abc import ABCMeta, abstractmethod
import dateutil.parser

null_model = QtCore.QModelIndex()
bold_font = QtGui.QFont()
bold_font.setBold(True)

def addReturnPressedEvents(widget):
	'''Add a returnPressed event to a Qt widget.'''
	class ReturnKeyEater(QtCore.QObject):
		__rpsig = QtCore.pyqtSignal()

		def __init__(self, widget):
			super(ReturnKeyEater, self).__init__(widget)
			self.widget = widget
			self.widget.returnPressed = self.__rpsig

		def eventFilter(self, obj, event):
			if event.type() == QtCore.QEvent.KeyRelease and \
			   event.key() in [QtCore.Qt.Key_Return, QtCore.Qt.Key_Enter]:
				self.__rpsig.emit()
				return True
			return False

	m = ReturnKeyEater(widget)
	widget.installEventFilter(m)

def sort_dentry(dentry):
	if dentry.type == fmdb.INO_TYPE_DIR:
		return '0' + dentry.name
	else:
		return '1' + dentry.name

class MessagePump(object):
	'''Helper class to prime the Qt message queue periodically.'''
	def __init__(self, on_fn, off_fn):
		self.last = None
		self.interval = None
		self.i_start = datetime.timedelta(seconds = 1)
		self.i_run = datetime.timedelta(milliseconds = 50)
		self.on_fn = on_fn
		self.off_fn = off_fn

	def start(self):
		'''Set ourselves up for periodic message pumping.'''
		self.last = datetime.datetime.today()
		self.interval = self.i_start

	def pump(self):
		'''Actually pump messages.'''
		now = datetime.datetime.today()
		if now > self.last + self.interval:
			if self.interval == self.i_start:
				self.on_fn()
			self.interval = self.i_run
			QtWidgets.QApplication.processEvents()
			self.last = now

	def stop(self):
		'''Tear down message pumping.'''
		self.off_fn()
		self.last = None
		self.interval = None

## Data models

class ExtentTableModel(QtCore.QAbstractTableModel):
	'''Render and highlight an extent table.'''
	def __init__(self, fs, data, units, rows_to_show=500, parent=None, *args):
		super(ExtentTableModel, self).__init__(parent, *args)
		self.__data = data
		self.headers = ['Physical Offset', 'Logical Offset', \
				'Length', 'Flags', 'Type', 'Path']
		self.header_map = [
			lambda x: fmcli.format_size(self.units, x.p_off),
			lambda x: fmcli.format_size(self.units, x.l_off),
			lambda x: fmcli.format_size(self.units, x.length),
			lambda x: fmdb.extent_flagstr(x),
			lambda x: fmdb.extent_typestr(x),
			lambda x: x.path if x.path != '' else fs.pathsep
		]
		self.sort_keys = [
			lambda x: x.p_off,
			lambda x: -1 if x.l_off is None else x.l_off,
			lambda x: x.length,
			lambda x: fmdb.extent_flagstr(x),
			lambda x: fmdb.extent_typestr(x),
			lambda x: x.path,
		]
		self.align_map = [
			QtCore.Qt.AlignRight,
			QtCore.Qt.AlignRight,
			QtCore.Qt.AlignRight,
			QtCore.Qt.AlignLeft,
			QtCore.Qt.AlignLeft,
			QtCore.Qt.AlignLeft,
		]
		self.units = units
		self.rows_to_show = rows_to_show
		self.rows = min(rows_to_show, len(data))
		self.name_highlight = None

	def change_units(self, new_units):
		'''Change the display units of the size and length columns.'''
		self.units = new_units
		tl = self.createIndex(0, 0)
		br = self.createIndex(len(self.__data) - 1, 2)
		self.dataChanged.emit(tl, br)

	def revise(self, new_data):
		'''Update the extent table and redraw.'''
		olen = self.rows
		nlen = min(len(new_data), self.rows_to_show)
		self.rows = nlen
		parent = self.createIndex(-1, -1)
		self.layoutAboutToBeChanged.emit()
		if olen > nlen:
			self.beginRemoveRows(parent, nlen, olen)
		elif nlen > olen:
			self.beginInsertRows(parent, olen, nlen)
		self.__data = new_data
		if olen > nlen:
			self.endRemoveRows()
		elif nlen > olen:
			self.endInsertRows()
		tl = self.createIndex(0, 0)
		br = self.createIndex(olen - 1, len(self.headers) - 1)
		self.dataChanged.emit(tl, br)
		self.layoutChanged.emit()

	def canFetchMore(self, parent):
		return self.rows < len(self.__data)

	def fetchMore(self, parent):
		'''Reduce load times by rendering subsets selectively.'''
		nlen = min(len(self.__data) - self.rows, self.rows_to_show)
		self.beginInsertRows(parent, self.rows, self.rows + nlen)
		self.rows += nlen
		self.endInsertRows()

	def rowCount(self, parent):
		if not parent.isValid():
			return self.rows
		return 0

	def columnCount(self, parent):
		return len(self.headers)

	def data(self, index, role):
		def is_name_highlighted(name):
			if self.name_highlight is None:
				return False
			return name in self.name_highlight
		if not index.isValid():
			return None
		i = index.row()
		j = index.column()
		row = self.__data[i]
		if role == QtCore.Qt.DisplayRole:
			return self.header_map[j](row)
		elif role == QtCore.Qt.FontRole:
			if is_name_highlighted(row.path):
				return bold_font
			return None
		elif role == QtCore.Qt.TextAlignmentRole:
			return self.align_map[j]
		return None

	def headerData(self, col, orientation, role):
		if orientation == QtCore.Qt.Horizontal and \
		   role == QtCore.Qt.DisplayRole:
			return self.headers[col]
		return None

	def highlight_names(self, names = None):
		'''Highlight rows corresponding to some FS paths.'''
		self.name_highlight = names
		# Skip the re-render since we're just about to requery anyway.

	def extents(self, rows):
		'''Retrieve a range of extents.'''
		if rows is None or len(rows) == 0:
			for r in self.__data:
				yield r
			return
		for r in rows:
			yield self.__data[r]

	def inodes_extents(self, inodes):
		'''Retrieve a range of extents for some inodes.'''
		if inodes is None or len(inodes) == 0:
			return
		for r in self.__data:
			if r.ino in inodes:
				yield r

	def extent_count(self):
		'''Return the number of rows in the dataset.'''
		return len(self.__data)

	def sort(self, column, order):
		if column < 0:
			return
		self.__data.sort(key = self.sort_keys[column], reverse = order == 1)
		tl = self.createIndex(0, 0)
		br = self.createIndex(self.rows - 1, len(self.headers) - 1)
		self.dataChanged.emit(tl, br)

class InodeTableModel(QtCore.QAbstractTableModel):
	'''Render and highlight an inode table.'''
	def __init__(self, fs, data, units, rows_to_show=500, parent=None, *args):
		super(InodeTableModel, self).__init__(parent, *args)
		self.__data = data
		self.fs = fs
		self.headers = ['Inode', 'Extents', \
				'Travel Score', 'Type', 'Size', 'Last Access', \
				'Creation', 'Last Metadata Change', 'Last Data Change', \
				'Paths']
		self.header_map = [
			lambda x: fmcli.format_number(fmcli.units_none, x.ino),
			lambda x: fmcli.format_number(fmcli.units_none, x.nr_extents),
			lambda x: fmcli.format_size(self.units, x.travel_score),
			lambda x: fmdb.inode_typestr(x),
			lambda x: fmcli.format_size(self.units, x.size),
			lambda x: fmcli.posix_timestamp_str(x.atime, True),
			lambda x: fmcli.posix_timestamp_str(x.crtime, True),
			lambda x: fmcli.posix_timestamp_str(x.ctime, True),
			lambda x: fmcli.posix_timestamp_str(x.mtime, True),
			lambda x: self.fs.pathsep if x.path == '' else x.path,
		]
		self.sort_keys = [
			lambda x: x.ino,
			lambda x: -1 if x.nr_extents is None else x.nr_extents,
			lambda x: -1 if x.travel_score is None else x.travel_score,
			lambda x: fmdb.inode_typestr(x),
			lambda x: -1 if x.size is None else x.size,
			lambda x: -1 if x.atime is None else x.atime,
			lambda x: -1 if x.crtime is None else x.crtime,
			lambda x: -1 if x.ctime is None else x.ctime,
			lambda x: -1 if x.mtime is None else x.mtime,
			lambda x: x.path,
		]
		self.align_map = [
			QtCore.Qt.AlignRight,
			QtCore.Qt.AlignRight,
			QtCore.Qt.AlignRight,
			QtCore.Qt.AlignLeft,
			QtCore.Qt.AlignRight,
			QtCore.Qt.AlignRight,
			QtCore.Qt.AlignRight,
			QtCore.Qt.AlignRight,
			QtCore.Qt.AlignRight,
			QtCore.Qt.AlignLeft,
		]
		self.units = units
		self.rows_to_show = rows_to_show
		self.rows = min(rows_to_show, len(data))
		self.name_highlight = None

	def change_units(self, new_units):
		'''Change the display units of the size and length columns.'''
		self.units = new_units
		tl = self.createIndex(0, 2)
		br = self.createIndex(len(self.__data) - 1, 2)
		self.dataChanged.emit(tl, br)

	def revise(self, new_data):
		'''Update the inode table and redraw.'''
		olen = self.rows
		nlen = min(len(new_data), self.rows_to_show)
		self.rows = nlen
		parent = self.createIndex(-1, -1)
		self.layoutAboutToBeChanged.emit()
		if olen > nlen:
			self.beginRemoveRows(parent, nlen, olen)
		elif nlen > olen:
			self.beginInsertRows(parent, olen, nlen)
		self.__data = new_data
		if olen > nlen:
			self.endRemoveRows()
		elif nlen > olen:
			self.endInsertRows()
		tl = self.createIndex(0, 0)
		br = self.createIndex(olen - 1, len(self.headers) - 1)
		self.dataChanged.emit(tl, br)
		self.layoutChanged.emit()

	def canFetchMore(self, parent):
		return self.rows < len(self.__data)

	def fetchMore(self, parent):
		'''Reduce load times by rendering subsets selectively.'''
		nlen = min(len(self.__data) - self.rows, self.rows_to_show)
		self.beginInsertRows(parent, self.rows, self.rows + nlen)
		self.rows += nlen
		self.endInsertRows()

	def rowCount(self, parent):
		if not parent.isValid():
			return self.rows
		return 0

	def columnCount(self, parent):
		return len(self.headers)

	def data(self, index, role):
		def is_name_highlighted(name):
			if self.name_highlight is None:
				return False
			return name in self.name_highlight
		if not index.isValid():
			return None
		i = index.row()
		j = index.column()
		row = self.__data[i]
		if role == QtCore.Qt.DisplayRole:
			return self.header_map[j](row)
		elif role == QtCore.Qt.FontRole:
			if is_name_highlighted(row.path):
				return bold_font
			return None
		elif role == QtCore.Qt.TextAlignmentRole:
			return self.align_map[j]
		return None

	def headerData(self, col, orientation, role):
		if orientation == QtCore.Qt.Horizontal and \
		   role == QtCore.Qt.DisplayRole:
			return self.headers[col]
		return None

	def highlight_names(self, names = None):
		'''Highlight rows corresponding to some FS paths.'''
		self.name_highlight = names
		# Skip the re-render since we're just about to requery anyway.
		# XXX: are we?

	def inodes(self, rows):
		'''Retrieve a range of extents.'''
		if rows is None or len(rows) == 0:
			for r in self.__data:
				yield r
			return
		for r in rows:
			yield self.__data[r]

	def inode_count(self):
		'''Return the number of rows in the dataset.'''
		return len(self.__data)

	def sort(self, column, order):
		if column < 0:
			return
		self.__data.sort(key = self.sort_keys[column], reverse = order == 1)
		tl = self.createIndex(0, 0)
		br = self.createIndex(self.rows - 1, len(self.headers) - 1)
		self.dataChanged.emit(tl, br)

class FsTreeNode(object):
	'''A node in the recorded filesystem.'''
	def __init__(self, path, ino, type, load_fn = None, parent = None, fs = None):
		if load_fn is None and parent is None:
			raise ValueError('Supply a dentry loading function or a parent node.')
		if fs is None and parent is None:
			raise ValueError('Supply a FS summary object or a parent node.')
		self.path = path
		self.type = type
		self.ino = ino
		self.parent = parent
		if load_fn is not None:
			self.load_fn = load_fn
		else:
			self.load_fn = parent.load_fn
		if fs is not None:
			self.fs = fs
		else:
			self.fs = parent.fs
		self.loaded = False
		self.children = None
		self.__row = None
		if self.type != fmdb.INO_TYPE_DIR:
			self.loaded = True
			self.children = []

	def load(self):
		'''Query the database for child nodes.'''
		if self.loaded:
			return
		self.loaded = True
		self.children = [FsTreeNode(self.path + self.fs.pathsep + de.name, de.ino, de.type, parent = self) for de in self.load_fn(self.path)]

	def row(self):
		if self.__row is None:
			if self.parent is None:
				self.__row = 0
			else:
				self.__row = self.parent.children.index(self)
		return self.__row

	def hasChildren(self):
		return self.type == fmdb.INO_TYPE_DIR

class FsTreeModel(QtCore.QAbstractItemModel):
	'''Model the filesystem tree recorded in the database.'''
	def __init__(self, fs, root, parent=None, *args):
		super(FsTreeModel, self).__init__(parent, *args)
		self.root = root
		self.headers = ['Name']
		self.fs = fs

	def index(self, row, column, parent):
		if not parent.isValid():
			return self.createIndex(row, column, self.root)
		parent = parent.internalPointer()
		parent.load()
		return self.createIndex(row, column, parent.children[row])

	def root_index(self):
		'''Return the index of the root of the model.'''
		return self.createIndex(0, 0, self.root)

	def parent(self, index):
		'''Create an index for the parent of an indexed cell, from 
		   the perspective of the grandparent node.'''
		if not index.isValid():
			return null_model
		node = index.internalPointer()
		if node.parent is None:
			return null_model
		return self.createIndex(node.parent.row(), 0, node.parent)

	def hasChildren(self, parent):
		if not parent.isValid():
			return True
		node = parent.internalPointer()
		return node.hasChildren()

	def rowCount(self, parent):
		if not parent.isValid():
			return 1
		node = parent.internalPointer()
		node.load()
		return len(node.children)

	def columnCount(self, parent):
		return len(self.headers)

	def data(self, index, role):
		if not index.isValid():
			return None
		node = index.internalPointer()
		if role == QtCore.Qt.DisplayRole:
			node.load()
			if index.column() == 0:
				if len(node.path) == 0:
					return self.fs.pathsep
				r = node.path.rindex(self.fs.pathsep)
				return node.path[r + 1:]
			else:
				return node.ino
		elif role == QtCore.Qt.DecorationRole:
			if node.type == fmdb.INO_TYPE_DIR:
				return QtGui.QIcon.fromTheme('folder')
			else:
				return QtGui.QIcon.fromTheme('text-x-generic')
		return None

	def headerData(self, col, orientation, role):
		if orientation == QtCore.Qt.Horizontal and \
		   role == QtCore.Qt.DisplayRole:
			return self.headers[col]
		return None

class ChecklistModel(QtCore.QAbstractTableModel):
	'''A list model for checkable items.'''
	def __init__(self, items, parent=None, *args):
		super(ChecklistModel, self).__init__(parent, *args)
		self.rows = items

	def rowCount(self, parent):
		return len(self.rows)

	def columnCount(self, parent):
		return 1

	def data(self, index, role):
		i = index.row()
		j = index.column()
		if j != 0:
			return None
		if role == QtCore.Qt.DisplayRole:
			return self.rows[i][0]
		elif role == QtCore.Qt.CheckStateRole:
			return QtCore.Qt.Checked if self.rows[i][1] else QtCore.Qt.Unchecked
		else:
			return None

	def flags(self, index):
		return super(ChecklistModel, self).flags(index) | QtCore.Qt.ItemIsUserCheckable

	def setData(self, index, value, role):
		if role != QtCore.Qt.CheckStateRole:
			return None
		row = index.row()
		# N.B. Weird comparison because Python2 returns QVariant, not bool
		self.rows[row][1] = not (value == False)
		return True

	def items(self):
		return self.rows

class OverviewModel(QtCore.QObject):
	'''Render the overview into a text field.'''
	rendered = QtCore.pyqtSignal()

	def __init__(self, fmdb, ctl, precision = 65536, parent = None, yield_fn = None):
		super(OverviewModel, self).__init__(parent)
		self.fmdb = fmdb
		self.fs = self.fmdb.query_summary()
		self.ctl = ctl
		self.ctl.resizeEvent = self.resize_ctl
		self.length = None
		self.zoom = 1.0
		self.precision = precision
		self.overview_big = None
		self.rst = QtCore.QTimer()
		self.rst.timeout.connect(self.delayed_resize)
		self.range_highlight = None
		self.yield_fn = yield_fn
		self.auto_size = True
		self.has_rendered = False

		self.resize_viewport()

	def load(self):
		'''Query the DB for the high-res overview data.'''
		olen = min(self.precision, self.fs.total_bytes // self.fs.block_size)
		self.fmdb.set_overview_length(olen)
		self.overview_big = [ov for ov in self.fmdb.query_overview()]
		self.has_rendered = False

	def set_zoom(self, zoom):
		'''Set the zoom factor for the overview.'''
		new_as = True
		if type(zoom) == int or type(zoom) == float:
			new_as = False
			new_zoom = 1.0
			new_length = int(float(self.fs.total_bytes) / zoom)
		elif type(zoom) == str:
			if zoom[-1] == '%':
				new_zoom = float(zoom[:-1]) / 100.0
				new_length = 0
			else:
				new_as = False
				new_zoom = 1.0
				cell_length = fmcli.s2p(self.fs, zoom)
				new_length = int(float(self.fs.total_bytes) / cell_length)
		else:
			raise ValueError("Unknown zoom factor '%s'" % zoom)
		self.auto_size = new_as
		self.zoom = new_zoom
		self.length = new_length
		self.resize_viewport()
		self.render()

	def total_length(self):
		'''The total length of the overview.'''
		return self.length * self.zoom

	def render_html(self, length):
		'''Render the overview for a given length.'''
		def is_highlighted(cell):
			if range_highlight is None:
				return False
			for start, end in range_highlight:
				if cell >= start and cell <= end:
					return True
			return False

		t0 = datetime.datetime.today()
		if self.overview_big is None:
			return None
		olen = int(length)
		#print(self.length, self.zoom)
		o2s = float(len(self.overview_big)) / olen
		ov_str = []
		t1 = datetime.datetime.today()
		h = False
		for i in range(0, olen):
			x = int(round(i * o2s))
			y = int(round((i + 1) * o2s))
			ss = self.overview_big[x:y]
			rh = self.range_highlight[x:y] if self.range_highlight is not None else [0]
			ovs = fmdb.overview_block(self.fmdb.get_extent_types_to_show())
			for s in ss:
				ovs.add(s)
			if sum(rh) > 0:
				if not h:
					ov_str.append('<span style="background: #e0e0e0; font-weight: bold;">')
				h = True
			else:
				if h:
					ov_str.append('</span>')
				h = False
			ov_str.append(ovs.to_letter())
		if h:
			ov_str.append('</span>')
		t2 = datetime.datetime.today()
		fmdb.print_times('render', [t0, t1, t2])
		return ''.join(ov_str)

	def render(self):
		'''Render the overview into the text view.'''
		html = self.render_html(int(self.length * self.zoom))
		if html is None:
			return
		cursor = self.ctl.textCursor()
		start = cursor.selectionStart()
		end = cursor.selectionEnd()
		vs = self.ctl.verticalScrollBar()
		old_v = vs.value()
		old_max = vs.maximum()
		self.ctl.setText(html)
		if old_max > 0:
			if vs.maximum() == old_max:
				vs.setValue(old_v)
			else:
				vs.setValue(int(vs.maximum() * float(old_v) / old_max))
		self.rendered.emit()
		self.has_rendered = True

	def resize_ctl(self, event):
		'''Handle the resizing of the text view control.'''
		QtWidgets.QTextEdit.resizeEvent(self.ctl, event)
		if self.resize_viewport():
			self.rst.start(40)

	def font_changed(self):
		'''Call this if the font changes.'''
		if self.resize_viewport():
			self.render()

	def resize_viewport(self):
		'''Recalculate the overview size.'''
		sz = self.ctl.viewport().size()
		qfm = QtGui.QFontMetrics(self.ctl.document().defaultFont())
		overview_font_width = qfm.width('M')
		overview_font_height = qfm.height()
		# Cheat with the textedit width/height -- use one less
		# column than we probably could, and force wrapping at
		# that column.
		w = (sz.width() // overview_font_width) - 1
		h = (sz.height() // overview_font_height) - 1
		self.ctl.setLineWrapColumnOrWidth(w)
		#print("overview; %f x %f = %f" % (w, h, w * h))
		if self.auto_size:
			self.length = w * h
		return self.auto_size or not self.has_rendered

	def delayed_resize(self):
		self.rst.stop()
		self.render()

	def highlight_ranges(self, ranges):
		'''Highlight a range of physical extents in the overview.'''
		old_highlight = self.range_highlight
		# Figure out which ranges we're playing with
		olen = min(self.precision, self.fs.total_bytes // self.fs.block_size)
		self.fmdb.set_overview_length(olen)

		# Compress that into a set
		rset = set()
		n = 0
		for x in self.fmdb.pick_bytes(ranges):
			if type(x) == int:
				start = end = x
			else:
				start, end = x
			for x in range(start, end + 1):
				rset.add(x)
			if n > 1000:
				self.yield_fn()
				n = 0
			n += 1
		self.range_highlight =  [(1 if n in rset else 0) for n in range(0, olen)]
		if old_highlight == self.range_highlight:
			return
		self.render()

## Query classes

class FmQuery(object):
	'''Abstract base class to manage query context and UI.'''
	def __init__(self, label, ctl, query_fn):
		self.label = label
		self.ctl = ctl
		self.query_fn = query_fn

	@abstractmethod
	def load_query(self):
		'''Load ourselves into the UI.'''
		raise NotImplementedError()

	@abstractmethod
	def save_query(self):
		'''Save UI contents.'''
		raise NotImplementedError()

	@abstractmethod
	def parse_query(self):
		'''Parse query contents into some meaningful form.'''
		raise NotImplementedError()

	def run_query(self):
		'''Run a query.'''
		args = self.parse_query()
		#print("QUERY:", self.label, args)
		self.query_fn(args)

	@abstractmethod
	def export_state(self):
		'''Export state data for serializeation.'''
		raise NotImplementedError()

	@abstractmethod
	def import_state(self, data):
		'''Import state data for serialization.'''
		raise NotImplementedError()

	def summarize(self):
		'''Summarize the state of this query as a string.'''
		return self.label + ': '

class StringQuery(FmQuery):
	'''Handle queries that are free-form text.'''
	def __init__(self, label, ctl, query_fn, edit_string = '', history = None, parent=None, *args):
		super(StringQuery, self).__init__(label, ctl, query_fn)
		if history is None:
			self.history = []
		else:
			self.history = history
		self.edit_string = edit_string

	def load_query(self):
		self.ctl.clear()
		self.ctl.addItems(self.history)
		if self.edit_string in self.history:
			self.ctl.setCurrentIndex(self.history.index(self.edit_string))
		else:
			self.ctl.setEditText(self.edit_string)
		self.ctl.show()

	def save_query(self):
		self.ctl.hide()
		self.edit_string = str(self.ctl.currentText())

	def parse_query(self):
		a = str(self.ctl.currentText())
		self.edit_string = a
		self.add_to_history(a)
		return fmcli.split_unescape(str(a), ' ', ('"', "'"))

	def add_to_history(self, string):
		'''Add a string to the history.'''
		if len(self.history) > 0 and self.history[0] == string:
			return
		if string in self.history:
			self.history.remove(string)
		r = self.ctl.findText(string)
		if r >= 0:
			self.ctl.removeItem(r)
		self.history.insert(0, string)
		self.ctl.insertItem(0, string)
		self.ctl.setCurrentIndex(self.history.index(string))

	def export_state(self):
		return {'edit_string': str(self.edit_string), 'history': self.history[:100]}

	def import_state(self, data):
		self.edit_string = data['edit_string']
		self.history = data['history']

	def summarize(self):
		x = super(StringQuery, self).summarize()
		return x + self.edit_string

class ChecklistQuery(FmQuery):
	'''Handle queries comprising a selection of discrete items.'''
	def __init__(self, label, ctl, query_fn, items, parent=None, *args):
		super(ChecklistQuery, self).__init__(label, ctl, query_fn)
		self.items = items
		self.model = ChecklistModel(items)

	def load_query(self):
		self.ctl.setModel(self.model)
		self.ctl.show()

	def save_query(self):
		self.ctl.hide()

	def parse_query(self):
		return self.items

	def export_state(self):
		return [{'label': x[0], 'state': x[1]} for x in self.items]

	def import_state(self, data):
		for d in data:
			for i in self.items:
				if i[0] == d['label']:
					i[1] = d['state']
					break

	def summarize(self):
		x = super(StringQuery, self).summarize()
		return x + ', '.join([x for x in self.items if x[1]])

class TimestampQuery(FmQuery):
	'''Handle queries comprising a range of timestamps.'''
	def __init__(self, label, ctl, query_fn, start_ctl, end_ctl, range_ctl, start = None, end = None, range_enabled = False, parent=None, *args):
		super(TimestampQuery, self).__init__(label, ctl, query_fn)
		self.nowgmt = datetime.datetime.utcnow().replace(microsecond = 0, tzinfo = fmdb.tz_gmt)
		self.start = self.nowgmt if start is None else start
		self.end = self.nowgmt if end is None else end
		self.range_enabled = range_enabled
		self.start_ctl = start_ctl
		self.end_ctl = end_ctl
		self.range_ctl = range_ctl

	def load_query(self):
		self.start_ctl.setDateTime(self.start.astimezone(fmdb.tz_local))
		self.end_ctl.setDateTime(self.end.astimezone(fmdb.tz_local))
		self.range_ctl.setCheckState(QtCore.Qt.Checked if self.range_enabled else QtCore.Qt.Unchecked)
		self.ctl.show()

	def save_query(self):
		self.ctl.hide()
		self.parse_query()

	def parse_query(self):
		def x(d):
			return d.dateTime().toPyDateTime().replace(microsecond = 0, tzinfo = fmdb.tz_local).astimezone(fmdb.tz_gmt)
		self.range_enabled = self.range_ctl.checkState() == QtCore.Qt.Checked
		self.start = x(self.start_ctl)
		self.end = x(self.end_ctl)
		if self.range_enabled:
			return (self.start, self.end)
		return self.start

	def export_state(self):
		a = 'None' if self.start == self.nowgmt else self.start.isoformat()
		b = 'None' if self.end == self.nowgmt else self.end.isoformat()
		return [a, b, self.range_enabled]

	def import_state(self, data):
		if data[0] != 'None':
			self.start = dateutil.parser.parse(data[0])
		if data[1] != 'None':
			self.end = dateutil.parser.parse(data[1])
		self.range_enabled = data[2] == True

	def summarize(self):
		x = super(TimestampQuery, self).summarize()
		if not self.range_enabled:
			return x + fmcli.posix_timestamp_str(self.start, True)
		return x + fmcli.posix_timestamp_str(self.start, True) + ' to ' + fmcli.posix_timestamp_str(self.end, True)

## Custom widgets

class XLineEdit(QtWidgets.QLineEdit):
	'''QLineEdit with clear button, which appears when user enters text.'''
	def __init__(self, parent=None):
		super(XLineEdit, self).__init__(parent)
		self.layout = QtWidgets.QHBoxLayout(self)
		self.image = QtWidgets.QLabel(self)
		self.image.setCursor(QtCore.Qt.ArrowCursor)
		self.image.setFocusPolicy(QtCore.Qt.NoFocus)
		self.image.setStyleSheet("border: none;")
		pixmap = QtGui.QIcon.fromTheme('locationbar-erase').pixmap(16, 16)
		self.image.setPixmap(pixmap)
		self.image.setSizePolicy(
			QtWidgets.QSizePolicy.Expanding,
			QtWidgets.QSizePolicy.Expanding)
		self.image.adjustSize()
		self.image.setScaledContents(True)
		self.layout.addWidget(
			self.image, alignment=QtCore.Qt.AlignRight)
		self.textChanged.connect(self.changed)
		self.image.hide()
		self.image.mouseReleaseEvent = self.clear_mouse_release
		qm = self.textMargins()
		qm.setRight(qm.right() + 24)
		self.setTextMargins(qm)

	def clear_mouse_release(self, ev):
		QtWidgets.QLabel.mouseReleaseEvent(self.image, ev)
		if ev.button() == QtCore.Qt.LeftButton:
			self.clear()

	def changed(self, text):
		if len(text) > 0:
			self.image.show()
		else:
			self.image.hide()

### Validator for the zoom combobox

class ZoomValidator(QtGui.QValidator):
	'''Validate a zoom size.'''
	def __init__(self, fs, parent = None):
		super(ZoomValidator, self).__init__(parent)
		self.fs = fs

	def validate(self, string, pos = None):
		if string == '':
			return (QtGui.QValidator.Intermediate, string, pos)
		try:
			self.try_validate(string)
			return (QtGui.QValidator.Acceptable, string, pos)
		except:
			return (QtGui.QValidator.Invalid, string, pos)

	def try_validate(self, zoom):
		'''Try to validate the string.'''
		if type(zoom) == int or type(zoom) == float:
			pass
		elif type(zoom) == str:
			if zoom[-1] == '%':
				zoom = float(zoom[:-1]) / 100.0
			else:
				cell_length = fmcli.s2p(self.fs, zoom)
				zoom = int(self.fs.total_bytes / cell_length)
		else:
			raise ValueError("Unknown zoom factor '%s'" % zoom)
		return zoom

## GUI

class fmgui(QtWidgets.QMainWindow):
	'''Manage the GUI widgets and interactions.'''
	def __init__(self, fmdbX, histfile=os.path.join(os.path.expanduser('~'), \
						'.config', 'fmgui-history')):
		super(fmgui, self).__init__()
		self.json_version = 1
		self.fmdb = fmdbX
		try:
			uic.loadUi('%s/filemapper.ui' % os.environ['FM_LIB_DIR'], self)
		except:
			uic.loadUi('filemapper.ui', self)
		self.fs = self.fmdb.query_summary()
		self.setWindowTitle('%s (%s) - FileMapper' % (self.fs.path, self.fs.fstype))
		self.histfile = histfile
		self.mp = MessagePump(self.mp_start, self.mp_stop)

		# Set up the menu
		units = fmcli.units_auto
		self.unit_actions = self.menuUnits.actions()
		ag = QtWidgets.QActionGroup(self)
		for u in self.unit_actions:
			u.setActionGroup(ag)
		ag.triggered.connect(self.change_units)
		self.actionExportExtents.triggered.connect(self.export_extents)
		self.actionExportExtents.setIcon(QtGui.QIcon.fromTheme('document-save'))
		self.actionExportInodes.triggered.connect(self.export_inodes)
		self.actionExportInodes.setIcon(QtGui.QIcon.fromTheme('document-save'))
		self.actionExportOverview.triggered.connect(self.export_overview)
		self.actionExportOverview.setIcon(QtGui.QIcon.fromTheme('document-save'))
		self.actionChangeFont.triggered.connect(self.change_font)
		self.actionChangeFont.setIcon(QtGui.QIcon.fromTheme('preferences-desktop-font'))
		self.actionQuit.setIcon(QtGui.QIcon.fromTheme('application-exit'))

		ag = QtWidgets.QActionGroup(self)
		ag.setExclusive(False)
		self.extent_type_actions = self.menuOverview.actions()[2:2 + len(fmdb.extent_types)]
		for a in self.extent_type_actions:
			a.setActionGroup(ag)
		ag.triggered.connect(self.change_extent_type)
		self.extent_type_actions = ag

		# Set up the overview
		self.overview = OverviewModel(self.fmdb, self.overview_text, yield_fn = self.mp.pump)
		self.overview.rendered.connect(self.do_summary)
		self.overview_text.selectionChanged.connect(self.select_overview)
		self.ost = QtCore.QTimer()
		self.ost.timeout.connect(self.run_query)
		self.old_ostart = None
		self.old_oend = None

		# Set up the extent view
		self.etm = ExtentTableModel(self.fs, [], units)
		self.unit_actions[0].setChecked(True)
		self.extent_table.setModel(self.etm)
		self.extent_table.selectionModel().selectionChanged.connect(self.pick_extent_table)
		self.extent_table.sortByColumn(-1, 0) #Qt.AscendingOrder)

		# Set up the inode view
		self.itm = InodeTableModel(self.fs, [], units)
		self.inode_table.setModel(self.itm)
		self.inode_table.selectionModel().selectionChanged.connect(self.pick_inode_table)
		self.extent_table.sortByColumn(-1, 0)

		# Set up the fs tree view
		de = self.fmdb.query_root()
		root = FsTreeNode(de.name, de.ino, de.type, \
				  lambda x: sorted(self.fmdb.query_ls([x]), key = sort_dentry), \
				  fs = self.fs)

		self.ftm = FsTreeModel(self.fs, root)
		self.fs_tree.setModel(self.ftm)
		self.fs_tree.selectionModel().selectionChanged.connect(self.pick_fs_tree)
		self.fs_tree.setRootIsDecorated(False)
		self.fs_tree.expand(self.ftm.root_index())

		# Set up the query UI
		# First, the combobox-lineedit widget weirdness
		self.query_text.setDuplicatesEnabled(False)
		self.xle = XLineEdit(self.query_text)
		self.xle.returnPressed.connect(self.run_query)
		self.query_text.setLineEdit(self.xle)
		self.query_text.completer().setCompletionMode(QtWidgets.QCompleter.PopupCompletion)

		# Next the checklist thing
		addReturnPressedEvents(self.query_checklist)
		self.query_checklist.returnPressed.connect(self.run_query)

		# Next, the query button
		self.query_btn.clicked.connect(self.run_query)
		self.query_btn.setIcon(QtGui.QIcon.fromTheme('system-search'))

		# Then the check-list query data
		extent_types = [
			[fmdb.extent_types_long[x], True, x] for x in sorted(fmdb.extent_types.keys())
		]
		extent_flags = [
			[fmdb.extent_flags_long[x], False, x] for x in sorted(fmdb.extent_flags_long.keys())
		]
		inode_types = [
			[fmdb.inode_types_long[x], True, x] for x in sorted(fmdb.inode_types.keys())
		]
		extent_flags.append(['Exact Match', False])
		def sq(l, q):
			return StringQuery(l, self.query_text, q)
		def cq(l, q, x):
			return ChecklistQuery(l, self.query_checklist, q, x)
		def tq(l, q):
			return TimestampQuery(l, self.query_timestamp, q, self.query_startTimeStamp, self.query_endTimeStamp, self.query_endTimeEnabled)
		self.query_types = [
			sq('Overview Cells', self.query_overview),
			sq('Physical Offsets', self.query_poff),
			sq('Logical Offsets', self.query_loff),
			sq('Inode Numbers', self.query_inodes),
			sq('Paths', self.query_paths),
			cq('Extent Types', self.query_extent_type, extent_types),
			cq('Extent Flags', self.query_extent_flags, extent_flags),
			sq('Extent Lengths', self.query_lengths),
			sq('Travel Scores', self.query_travel_scores),
			sq('# Primary Extents', self.query_nr_extents),
			sq('File Sizes', self.query_sizes),
			tq('Data Changed', self.query_mtime),
			tq('Last Access', self.query_atime),
			tq('Inode Changed', self.query_ctime),
			tq('Creation Time', self.query_crtime),
			cq('Inode Type', self.query_inode_type, inode_types),
		]

		# Then the query type selector
		self.querytype_combo.setMaxVisibleItems(len(self.query_types))
		self.querytype_combo.insertItems(0, [x.label for x in self.query_types])
		self.querytype_combo.currentIndexChanged.connect(self.change_querytype)
		self.query_checklist.hide()
		self.query_text.hide()
		self.query_timestamp.hide()
		self.old_querytype = None

		# Finally move the query UI to the toolbar
		self.toolBar.addWidget(self.query_frame)

		# Set up the zoom control
		self.zoom_levels = ['100%', '200%', '400%', '800%']
		self.zoom_combo.insertItems(0, self.zoom_levels)
		self.zoom_combo.currentIndexChanged.connect(self.change_zoom)
		self.zoom_combo.setValidator(ZoomValidator(self.fs))

		# Set up the status bar
		self.status_label = QtWidgets.QLabel()
		self.status_bar.addWidget(self.status_label)

		# Here we go!
		self.load_state()
		self.overview.load()
		self.show()

	## Utility

	def parse_size_ranges(self, args):
		'''Parse string arguments into size ranges.'''
		return fmcli.parse_ranges(args, lambda x: fmcli.s2p(self.fs, x))

	def parse_number_ranges(self, args, maximum):
		'''Parse string arguments into number ranges.'''
		return fmcli.parse_ranges(args, lambda x: fmcli.n2p(maximum, x))

	def mp_start(self):
		'''Disable UI elements during message pumping.'''
		self.query_frame.setEnabled(False)
		self.overview_text.setEnabled(False)
		self.fs_tree.setEnabled(False)
		self.extent_type_actions.setEnabled(False)

	def mp_stop(self):
		'''Enable UI elements after message pumping.'''
		self.query_frame.setEnabled(True)
		self.overview_text.setEnabled(True)
		self.fs_tree.setEnabled(True)
		self.extent_type_actions.setEnabled(True)

	def do_summary(self):
		'''Load the FS summary into the status line.'''
		s = self.summary_text()
		self.status_label.setText(s)

	def summary_text(self, overview_len = None):
		'''Summarize the filesystem contents.'''
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
		if overview_len is None:
			overview_len = self.overview.total_length()
		inodes = self.fs.inodes if self.fs.inodes != 0 else 1
		extents = self.fs.extents if self.fs.extents != 0 else 1
		extents_blocks = self.fs.extents_bytes / self.fs.block_size if self.fs.extents_bytes != 0 else 1
		s = "%s of %s (%.0f%%) used; %s of %s (%.0f%%) inodes; %s extents; %s/cell; %.1f%% frag; %s avg. travel" % \
			(fmcli.format_size(fmcli.units_auto, self.fs.total_bytes - self.fs.free_bytes), \
			 fmcli.format_size(fmcli.units_auto, self.fs.total_bytes), \
			 100 * (1.0 - (float(fb) / tb)), \
			 fmcli.format_number(fmcli.units_auto, self.fs.total_inodes - self.fs.free_inodes), \
			 fmcli.format_number(fmcli.units_auto, self.fs.total_inodes), \
			 100 * (1.0 - (float(fi) / ti)), \
			 fmcli.format_number(fmcli.units_auto, self.fs.extents), \
			 fmcli.format_size(fmcli.units_auto, float(self.fs.total_bytes) / overview_len), \
			 100.0 * extents / extents_blocks, \
			 fmcli.format_size(fmcli.units_auto, self.fmdb.query_avg_travel_score()))
		return s

	## Load and save UI state

	def save_state(self):
		'''Save the state of the UI.'''
		def eta():
			actions = self.extent_type_actions.actions()
			return [fmdb.extent_types_long[x] for x in range(0, len(actions)) if actions[x].isChecked()]
		of = self.overview_text.document().defaultFont()
		zs = list()
		for x in range(0, self.zoom_combo.count()):
			s = self.zoom_combo.itemText(x)
			if s not in self.zoom_levels:
				zs.append(s)
		data = {
			'version': self.json_version,
			'zoom': self.zoom_combo.currentText(),
			'zoom_levels': zs[:28],
			'query_type': self.query_types[self.querytype_combo.currentIndex()].label,
			'units': self.etm.units.label,
			'window_state': base64.b64encode(self.saveState()).decode('utf-8'),
			'window_geometry': base64.b64encode(self.saveGeometry()).decode('utf-8'),
			'overview_font_family': str(of.family()),
			'overview_font_points': of.pointSizeF(),
			'extent_types': eta(),
			'results_tab': self.results_tab.currentIndex(),
			'extent_headers': base64.b64encode(self.extent_table.header().saveState()).decode('utf-8'),
			'inode_headers': base64.b64encode(self.inode_table.header().saveState()).decode('utf-8'),
		}
		qtdata = {}
		for qt in self.query_types:
			qtdata[qt.label] = qt.export_state()
		data['query_data'] = qtdata
		with open(self.histfile, 'w') as fd:
			json.dump(data, fd, indent = 4)

	def load_state(self):
		'''Load the state of the UI.'''
		failed = False
		try:
			fd = open(self.histfile, 'r')
			data = json.load(fd)
			if data['version'] != self.json_version:
				raise Exception('Invalid version.')
			x = 0
			for qt in self.query_types:
				try:
					qt.import_state(data['query_data'][qt.label])
				except:
					failed = True
				if qt.label == data['query_type']:
					self.querytype_combo.setCurrentIndex(x)
				x += 1
			self.restoreState(base64.b64decode(data['window_state'].encode('utf-8')))
			self.restoreGeometry(base64.b64decode(data['window_geometry'].encode('utf-8')))
			of = self.overview_text.document().defaultFont()
			of.setFamily(data['overview_font_family'])
			of.setPointSizeF(data['overview_font_points'])
			self.overview_text.document().setDefaultFont(of)
			self.overview.font_changed()
			opts = {fmdb.extent_type_strings_long[t] for t in data['extent_types']}
			aa = self.extent_type_actions.actions()
			for x in range(0, len(fmdb.extent_types)):
				aa[x].setChecked(x in opts)
			self.fmdb.set_extent_types_to_show(opts)
			self.results_tab.setCurrentIndex(data['results_tab'])
			self.extent_table.header().restoreState(base64.b64decode(data['extent_headers'].encode('utf-8')))
			self.inode_table.header().restoreState(base64.b64decode(data['inode_headers'].encode('utf-8')))
			self.zoom_combo.insertItems(0, data['zoom_levels'])
			for x in range(0, self.zoom_combo.count()):
				if self.zoom_combo.itemText(x) == data['zoom']:
					self.zoom_combo.setCurrentIndex(x)
					break
		except Exception as e:
			failed = True
		if failed:
			try:
				os.unlink(self.histfile)
			except:
				pass
		if self.querytype_combo.currentIndex() == 0:
			self.change_querytype(0)

	## Load data into models

	def load_extents(self, f):
		'''Populate the extent table.'''
		t0 = datetime.datetime.today()
		if isinstance(f, list):
			new_data = f
		else:
			n = 0
			new_data = []
			for x in f:
				new_data.append(x)
				if n > 1000:
					self.mp.pump()
					n = 0
				n += 1
		t1 = datetime.datetime.today()
		self.extent_table.sortByColumn(-1, 0)
		t2 = datetime.datetime.today()
		self.etm.revise(new_data)
		self.actionExportExtents.setEnabled(len(new_data) > 0)
		t3 = datetime.datetime.today()
		for x in range(self.etm.columnCount(None)):
			self.extent_table.resizeColumnToContents(x)
		t4 = datetime.datetime.today()
		self.update_query_summary()
		t5 = datetime.datetime.today()
		fmdb.print_times('load_extents', [t0, t1, t2, t3, t4, t5])

	def load_inodes(self, f):
		'''Populate the inode table.'''
		t0 = datetime.datetime.today()
		if isinstance(f, list):
			new_data = f
		else:
			n = 0
			new_data = []
			for x in f:
				new_data.append(x)
				if n > 1000:
					self.mp.pump()
					n = 0
				n += 1
		t1 = datetime.datetime.today()
		self.inode_table.sortByColumn(-1, 0)
		t2 = datetime.datetime.today()
		self.itm.revise(new_data)
		self.actionExportInodes.setEnabled(len(new_data) > 0)
		t3 = datetime.datetime.today()
		for x in range(self.itm.columnCount(None)):
			self.inode_table.resizeColumnToContents(x)
		t4 = datetime.datetime.today()
		self.update_query_summary()
		t5 = datetime.datetime.today()
		fmdb.print_times('load_stats', [t0, t1, t2, t3, t4, t5])

	## Change the overview highlight after selecting some widgets

	def enter_query(self, fn, text):
		'''Load the query UI elements.'''
		for x in range(0, len(self.query_types)):
			if self.query_types[x].query_fn == fn:
				self.querytype_combo.setCurrentIndex(x)
				self.query_text.setEditText(text)
				return

	def pick_fs_tree(self, n, o):
		'''Handle the selection of a FS tree nodes.'''
		self.ost.stop()
		extent_paths = set()
		query_paths = []
		keymod = int(QtWidgets.QApplication.keyboardModifiers())
		is_meta = (keymod & QtCore.Qt.MetaModifier) != 0
		for m in self.fs_tree.selectedIndexes():
			node = m.internalPointer()
			p = node.path if node.path != '' else self.fs.pathsep
			if node.hasChildren() and not is_meta:
				extent_paths.add(p)
				if ' ' in p:
					p = '"%s*"' % p
				else:
					p = '%s*' % p
			else:
				if ' ' in p:
					p = '"%s"' % p
			query_paths.append(p)
		self.etm.highlight_names(extent_paths)
		self.itm.highlight_names(extent_paths)
		self.enter_query(self.query_paths, ' '.join(query_paths))
		self.run_query()

	def __pick_extents(self):
		'''Tell the overview to highlight the selected extents.'''
		t0 = datetime.datetime.today()
		rows = {m.row() for m in self.extent_table.selectedIndexes()}
		ranges = [(ex.p_off, ex.p_off + ex.length - 1) for ex in self.etm.extents(rows)]
		t1 = datetime.datetime.today()
		self.overview.highlight_ranges(ranges)
		t2 = datetime.datetime.today()
		fmdb.print_times('pick_ex', [t0, t1, t2])

	def pick_extent_table(self, n, o):
		'''Handle the selection of extent table rows.'''
		self.mp.start()
		try:
			self.__pick_extents()
		finally:
			self.mp.stop()

	def __pick_inodes(self):
		'''Tell the overview to highlight the selected inodes' extents.'''
		t0 = datetime.datetime.today()
		rows = [m.row() for m in self.inode_table.selectedIndexes()]
		inodes = {i.ino for i in self.itm.inodes(rows)}
		ranges = [(ex.p_off, ex.p_off + ex.length - 1) for ex in self.etm.inodes_extents(inodes)]
		t1 = datetime.datetime.today()
		self.overview.highlight_ranges(ranges)
		t2 = datetime.datetime.today()
		fmdb.print_times('pick_ex', [t0, t1, t2])

	def pick_inode_table(self, n, o):
		'''Handle the selection of inode table rows.'''
		self.mp.start()
		try:
			self.__pick_inodes()
		finally:
			self.mp.stop()

	def select_overview(self):
		'''Handle the user making a physical block selection in
		   the overview.'''
		cursor = self.overview_text.textCursor()
		start = cursor.selectionStart()
		end = cursor.selectionEnd()
		if start == end:
			return
		if self.old_ostart == start and self.old_oend == end:
			return
		self.old_ostart = start
		self.old_oend = end
		if start + 1 == end:
			self.enter_query(self.query_overview, "%s" % start)
		else:
			self.enter_query(self.query_overview, "%s-%s" % (start, end - 1))
		self.ost.start(500)

	## React to UI changes

	def change_querytype(self, idx):
		'''Handle a change in the query type selector.'''
		if self.old_querytype is not None:
			old_qt = self.query_types[self.old_querytype]
			old_qt.save_query()
		new_qt = self.query_types[idx]
		new_qt.load_query()
		self.old_querytype = idx

	def change_units(self, action):
		'''Handle one of the units menu items.'''
		idx = self.unit_actions.index(action)
		avail_units = [
			fmcli.units_auto,
			fmcli.units_bytes,
			fmcli.units_sectors,
			fmcli.units('B', 'blocks', self.fs.block_size),
			fmcli.units_kib,
			fmcli.units_mib,
			fmcli.units_gib,
			fmcli.units_tib,
		]
		self.etm.change_units(avail_units[idx])
		self.itm.change_units(avail_units[idx])
		for u in self.unit_actions:
			u.setChecked(False)
		self.unit_actions[idx].setChecked(True)

	def change_extent_type(self, action):
		'''Toggle display of an extent type in the overview.'''
		arg = set()
		actions = self.extent_type_actions.actions()
		for x in range(0, len(actions)):
			if actions[x].isChecked():
				arg.add(x)
		self.fmdb.set_extent_types_to_show(arg)
		self.overview.render()

	def change_font(self):
		'''Change the overview font.'''
		y = self.overview_text.document().defaultFont()
		if y.family() == 'Source Code Pro,monospace':
			y.setFamily('monospace')
		(f, x) = QtWidgets.QFontDialog.getFont(y)
		if x:
			y.setFamily(f.family())
			y.setPointSizeF(f.pointSizeF())
			self.overview_text.document().setDefaultFont(y)
			self.overview.font_changed()
		self.save_state()

	def closeEvent(self, ev):
		qt = self.query_types[self.querytype_combo.currentIndex()]
		qt.save_query()
		self.save_state()
		super(fmgui, self).closeEvent(ev)

	def change_zoom(self, idx):
		'''Handle a change in the zoom selector.'''
		s = self.zoom_combo.currentText()
		self.overview.set_zoom(s)

	## Queries

	def run_query(self):
		'''Dispatch a query to populate the extent table.'''
		self.status_label.setText('Working...')
		self.ost.stop()
		self.mp.start()
		idx = self.querytype_combo.currentIndex()
		qt = self.query_types[idx]
		try:
			qt.run_query()
			self.__pick_extents()
			# XXX: should we clear the fs tree and extent selection too?
		finally:
			self.mp.stop()
		self.save_state()
		self.do_summary()

	def update_query_summary(self):
		'''Update the query summary text in the UI.'''
		e = self.etm.extent_count()
		i = self.itm.inode_count()
		s = 'Query Results: %s extents; %s inodes' % (
				fmcli.format_number(fmcli.units_none, e),
				fmcli.format_number(fmcli.units_none, i))
		self.results_dock.setWindowTitle(s)

	def query_overview(self, args):
		'''Query based on ranges of overview cells.'''
		if len(args) == 0:
			self.load_extents([])
			self.load_inodes([])
			return
		ranges = self.parse_number_ranges(args, self.overview.total_length())
		self.fmdb.set_overview_length(self.overview.total_length())
		r = list(self.fmdb.pick_cells(ranges))
		self.load_extents(self.fmdb.query_poff_range(r))
		self.load_inodes(self.fmdb.query_poff_range_inodes(r))

	def query_poff(self, args):
		'''Query based on ranges of physical bytes.'''
		if len(args) == 0:
			self.load_extents([])
			self.load_inodes([])
			return
		ranges = self.parse_size_ranges(args)
		self.load_extents(self.fmdb.query_poff_range(ranges))
		self.load_inodes(self.fmdb.query_poff_range_inodes(ranges))

	def query_loff(self, args):
		'''Query based on ranges of logical bytes.'''
		if len(args) == 0:
			self.load_extents([])
			self.load_inodes([])
			return
		ranges = self.parse_size_ranges(args)
		self.load_extents(self.fmdb.query_loff_range(ranges))
		self.load_inodes(self.fmdb.query_loff_range_inodes(ranges))

	def query_inodes(self, args):
		'''Query based on ranges of inodes.'''
		if len(args) == 0:
			self.load_extents([])
			self.load_inodes([])
			return
		ranges = self.parse_number_ranges(args, self.fs.total_inodes)
		self.load_extents(self.fmdb.query_inums(ranges))
		self.load_inodes(self.fmdb.query_inums_inodes(ranges))

	def query_paths(self, args):
		'''Query based on a list of FS paths.'''
		if len(args) == 0:
			self.load_extents([])
			self.load_inodes([])
			return
		self.load_extents(self.fmdb.query_paths(args))
		self.load_inodes(self.fmdb.query_paths_inodes(args))

	def query_extent_type(self, args):
		'''Query based on the extent type code.'''
		r = [x[2] for x in args if x[1]]
		self.load_extents(self.fmdb.query_extent_types(r))
		self.load_inodes(self.fmdb.query_extent_types_inodes(r))

	def query_extent_flags(self, args):
		'''Query based on the extent flag code.'''
		exact = args[-1][1]
		flags = 0
		for x in args:
			if len(x) > 2 and x[1]:
				flags |= x[2]
		self.load_extents(self.fmdb.query_extent_flags(flags, exact))
		self.load_inodes(self.fmdb.query_extent_flags_inodes(flags, exact))

	def query_lengths(self, args):
		'''Query based on ranges of lengths.'''
		if len(args) == 0:
			self.load_extents([])
			self.load_inodes([])
			return
		ranges = self.parse_size_ranges(args)
		self.load_extents(self.fmdb.query_lengths(ranges))
		self.load_inodes(self.fmdb.query_lengths_inodes(ranges))

	def query_travel_scores(self, args):
		'''Query based on ranges of travel scores.'''
		if len(args) == 0:
			self.load_extents([])
			self.load_inodes([])
			return
		ranges = self.parse_size_ranges(args)
		self.load_extents(self.fmdb.query_travel_scores(ranges))
		self.load_inodes(self.fmdb.query_travel_scores_inodes(ranges))

	def query_nr_extents(self, args):
		'''Query based on ranges of primary extent counts.'''
		if len(args) == 0:
			self.load_extents([])
			self.load_inodes([])
			return
		ranges = self.parse_number_ranges(args, 2**64)
		self.load_extents(self.fmdb.query_nr_extents(ranges))
		self.load_inodes(self.fmdb.query_nr_extents_inodes(ranges))

	def query_sizes(self, args):
		'''Query based on ranges of inode sizes.'''
		if len(args) == 0:
			self.load_extents([])
			self.load_inodes([])
			return
		ranges = self.parse_size_ranges(args)
		self.load_extents(self.fmdb.query_sizes(ranges))
		self.load_inodes(self.fmdb.query_sizes_inodes(ranges))

	def query_mtime(self, args):
		'''Query based on last data change time.'''
		self.load_extents(self.fmdb.query_mtimes([args]))
		self.load_inodes(self.fmdb.query_mtimes_inodes([args]))

	def query_atime(self, args):
		'''Query based on last access time.'''
		self.load_extents(self.fmdb.query_atimes([args]))
		self.load_inodes(self.fmdb.query_atimes_inodes([args]))

	def query_ctime(self, args):
		'''Query based on last metadata change time.'''
		self.load_extents(self.fmdb.query_ctimes([args]))
		self.load_inodes(self.fmdb.query_ctimes_inodes([args]))

	def query_crtime(self, args):
		'''Query based on creation time.'''
		self.load_extents(self.fmdb.query_crtimes([args]))
		self.load_inodes(self.fmdb.query_crtimes_inodes([args]))

	def query_inode_type(self, args):
		'''Query based on the inode type code.'''
		r = [x[2] for x in args if x[1]]
		self.load_extents(self.fmdb.query_inode_types(r))
		self.load_inodes(self.fmdb.query_inode_types_inodes(r))

	## Export query results

	def export_extents(self):
		'''Export extents to a CSV file.'''
		fn = QtWidgets.QFileDialog.getSaveFileName(self, 'Export Extents to CSV', \
				filter = 'Comma Separated Value Tables(*.csv);;All Files(*)')
		if fn == '':
			return
		idx = self.querytype_combo.currentIndex()
		qt = self.query_types[idx]
		self.mp.start()
		try:
			with open(fn, 'w') as fd:
				fd.write('# %s(%s) on %s\n' % (self.fs.path, self.fs.fstype, fmcli.posix_timestamp_str(self.fs.date, True)))
				fd.write('# %s\n' % self.status_label.text())
				fd.write('# Query: %s\n' % qt.summarize())
				fd.write('# Path, Physical Offset, Logical Offset, Length, Flags, Type\n')
				n = 0
				for ext in self.etm.extents(None):
					fd.write('"%s",%d,%s,%d,"%s","%s"\n' % \
						(ext.path if ext.path != '' else self.fs.pathsep, \
						 ext.p_off, '' if ext.l_off is None else ext.l_off, \
						 ext.length, \
						 fmdb.extent_flagstr(ext), \
						 fmdb.extent_typestr(ext)))
					if n > 1000:
						self.mp.pump()
						n = 0
					n += 1
		finally:
			self.mp.stop()

	def export_inodes(self):
		'''Export inodes to a CSV file.'''
		fn = QtWidgets.QFileDialog.getSaveFileName(self, 'Export Inodes to CSV', \
				filter = 'Comma Separated Value Tables(*.csv);;All Files(*)')
		if fn == '':
			return
		idx = self.querytype_combo.currentIndex()
		qt = self.query_types[idx]
		self.mp.start()
		try:
			with open(fn, 'w') as fd:
				fd.write('# %s(%s) on %s\n' % (self.fs.path, self.fs.fstype, fmcli.posix_timestamp_str(self.fs.date, True)))
				fd.write('# %s\n' % self.status_label.text())
				fd.write('# Query: %s\n' % qt.summarize())
				fd.write('# Inode, Number of Extents, Travel Score, Type, Size, Last Access, Creation, Last Metadata Change, Last Data Change, Paths\n')
				n = 0
				for inode in self.itm.inodes(None):
					ts = '' if inode.travel_score is None else '%.02f' % inode.travel_score
					nr = '' if inode.nr_extents is None else '%d' % inode.nr_extents
					iss = '' if inode.size is None else inode.size
					fd.write('%d,%s,%s,"%s",%s,%s,%s,%s,%s,"%s"\n' % \
						(inode.ino, nr, ts, \
						 fmdb.inode_typestr(inode), \
						 iss, \
						 fmcli.posix_timestamp_str(inode.atime), \
						 fmcli.posix_timestamp_str(inode.crtime), \
						 fmcli.posix_timestamp_str(inode.ctime), \
						 fmcli.posix_timestamp_str(inode.mtime), \
						 inode.path))
					if n > 1000:
						self.mp.pump()
						n = 0
					n += 1
		finally:
			self.mp.stop()

	def export_overview(self):
		'''Export overview to a HTML file.'''
		fn = QtWidgets.QFileDialog.getSaveFileName(self, 'Export Overview to HTML', \
				filter = 'Hypertext Markup Language(*.html);;All Files(*)')
		if fn == '':
			return
		idx = self.querytype_combo.currentIndex()
		qt = self.query_types[idx]
		if self.overview.auto_size:
			olen = 8192 * self.overview.zoom
		else:
			olen = self.overview.total_length()
		with open(fn, 'w') as fd:
			fd.write('''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN" "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
<head>
<title>%s (%s) on %s</title>
<style type="text/css">
#overview {
	font-family: monospace;
	border: 1px solid gray;
	word-wrap: break-word;
	padding: 0.4em;
}
h1 {
	margin: 0px;
}
p {
	margin-top: 0px;
	margin-bottom: 0.4em;
}
</style>
</head>
<body>
''' % (self.fs.path, self.fs.fstype, fmcli.posix_timestamp_str(self.fs.date, True)))
			fd.write('<h1>%s</h1>\n<p>%s recorded on %s.</p>\n' % (self.fs.path, self.fs.fstype, fmcli.posix_timestamp_str(self.fs.date, True)))
			fd.write('<p>Stats: %s</p>\n' % self.summary_text(int(olen)))
			fd.write('<p>Query: %s</p>\n' % qt.summarize())
			fd.write('''
<div id="overview">
''')
			fd.write(self.overview.render_html(olen))
			fd.write('''
</div>
</body>
</html>''')

def start_qt():
	'''Initialize QT.'''
	return QtWidgets.QApplication([])

def get_db_fname():
	'''Ask the user for the path to the database.'''
	return QtWidgets.QFileDialog.getOpenFileName(None, 'Open FileMapper Database', filter = 'FileMapper Databases(*.db);;All Files(*)')
