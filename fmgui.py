# QT GUI routines for filemapper
# Copyright(C) 2015 Darrick J. Wong
# Licensed under GPLv2.

import sys
from PyQt4 import QtGui, uic, QtCore
import fmcli
import datetime
import fmdb
import math
from collections import namedtuple

null_model = QtCore.QModelIndex()
bold_font = QtGui.QFont()
bold_font.setBold(True)

class FmQueryType:
	'''Retain state data about the query UI.'''
	def __init__(self, label, query_fn, saved_state):
		self.label = label
		self.query_fn = query_fn
		self.saved_state = saved_state

class ExtentTableModel(QtCore.QAbstractTableModel):
	'''Render and highlight an extent table.'''
	def __init__(self, data, units, rows_to_show=100, parent=None, *args):
		QtCore.QAbstractTableModel.__init__(self, parent, *args)
		self.__data = data
		self.headers = ['Physical Offset', 'Logical Offset', \
				'Length', 'Flags', 'Type', 'Path']
		self.header_map = [
			lambda x: fmcli.format_size(self.units, x.p_off),
			lambda x: fmcli.format_size(self.units, x.l_off),
			lambda x: fmcli.format_size(self.units, x.length),
			lambda x: x.flags_to_str(),
			lambda x: fmcli.typecodes[x.type],
			lambda x: x.path if x.path != '' else '/']
		self.sort_keys = [
			lambda x: x.p_off,
			lambda x: x.l_off,
			lambda x: x.length,
			lambda x: x.flags_to_str(),
			lambda x: fmcli.typecodes[x.type],
			lambda x: x.path,
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
		self.emit(QtCore.SIGNAL("layoutAboutToBeChanged()"))
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
		self.emit(QtCore.SIGNAL("layoutChanged()"))

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
		else:
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
		if rows is None:
			for r in self.__data:
				yield r
			return
		for r in rows:
			yield self.__data[r]

	def sort(self, column, order):
		if column < 0:
			return
		self.__data.sort(key = self.sort_keys[column], reverse = order == 1)
		tl = self.createIndex(0, 0)
		br = self.createIndex(self.rows - 1, len(self.headers) - 1)
		self.dataChanged.emit(tl, br)

class FsTreeNode:
	'''A node in the recorded filesystem.'''
	def __init__(self, path, ino, type, load_fn = None, parent = None):
		if load_fn is None and parent is None:
			raise Exception
		self.path = path
		self.type = type
		self.ino = ino
		self.parent = parent
		if load_fn is not None:
			self.load_fn = load_fn
		else:
			self.load_fn = parent.load_fn

		self.loaded = False
		self.children = None
		self.__row = None
		if self.type != 'd':
			self.loaded = True
			self.children = []

	def load(self):
		'''Query the database for child nodes.'''
		if self.loaded:
			return
		self.loaded = True
		self.children = [FsTreeNode(self.path + '/' + de.name, de.ino, de.type, parent = self) for de in self.load_fn(self.path)]

	def row(self):
		if self.__row is None:
			if self.parent is None:
				self.__row = 0
			else:
				self.__row = self.parent.children.index(self)
		return self.__row

	def hasChildren(self):
		return self.type == 'd'

class FsTreeModel(QtCore.QAbstractItemModel):
	'''Model the filesystem tree recorded in the database.'''
	def __init__(self, root, parent=None, *args):
		QtCore.QAbstractItemModel.__init__(self, parent, *args)
		self.root = root
		self.headers = ['Name'] #, 'Inode']

	def index(self, row, column, parent):
		if not parent.isValid():
			self.root.load()
			return self.createIndex(row, column, self.root.children[row])
		parent = parent.internalPointer()
		parent.load()
		return self.createIndex(row, column, parent.children[row])

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
			return self.root.hasChildren()
		node = parent.internalPointer()
		return node.hasChildren()

	def rowCount(self, parent):
		if not parent.isValid():
			self.root.load()
			return len(self.root.children)
		node = parent.internalPointer()
		node.load()
		return len(node.children)

	def columnCount(self, parent):
		return len(self.headers)

	def data(self, index, role):
		if not index.isValid():
			return None
		node = index.internalPointer()
		if role != QtCore.Qt.DisplayRole:
			return None
		node.load()
		if index.column() == 0:
			r = node.path.rindex('/')
			return node.path[r + 1:]
		else:
			return node.ino

	def headerData(self, col, orientation, role):
		if orientation == QtCore.Qt.Horizontal and \
		   role == QtCore.Qt.DisplayRole:
			return self.headers[col]
		return None

class fmgui(QtGui.QMainWindow):
	'''Manage the GUI widgets and interactions.'''
	def __init__(self, fmdb):
		super(fmgui, self).__init__()
		self.fmdb = fmdb
		uic.loadUi('filemapper.ui', self)
		self.setWindowTitle('%s - QFileMapper' % self.fmdb.fspath)
		self.fs_summary = self.fmdb.query_summary()

		# Set up the units menu
		units = fmcli.units_auto
		self.unit_actions = self.menuUnits.actions()
		ag = QtGui.QActionGroup(self)
		for u in self.unit_actions:
			u.setActionGroup(ag)
		ag.triggered.connect(self.change_units)

		# Set up the overview
		self.overview = OverviewModel(fmdb, self.overview_text)
		self.overview_text.selectionChanged.connect(self.select_overview)
		self.ost = QtCore.QTimer()
		self.ost.timeout.connect(self.run_query)
		self.old_ostart = None
		self.old_oend = None

		# Set up the extent view
		self.etm = ExtentTableModel([], units)
		self.unit_actions[0].setChecked(True)
		self.extent_table.setModel(self.etm)
		self.extent_table.selectionModel().selectionChanged.connect(self.pick_extent_table)

		# Set up the fs tree view
		de = self.fmdb.query_root()
		root = FsTreeNode(de.name, de.ino, de.type, lambda x: self.fmdb.query_ls([x]))

		self.ftm = FsTreeModel(root)
		self.fs_tree.setModel(self.ftm)
		self.fs_tree.selectionModel().selectionChanged.connect(self.pick_fs_tree)

		# Set up the query UI
		self.query_btn.clicked.connect(self.run_query)
		extent_types = [
			['File', True, 'f'],
			['Directory', True, 'd'],
			['Extent Map', True, 'e'],
			['Metadata', True, 'm'],
			['Extended Attribute', True, 'x']
		]
		self.query_types = [
			FmQueryType('Overview Cells', self.query_overview, ''),
			FmQueryType('Physical Offsets', self.query_poff, ''),
			FmQueryType('Inode Numbers', self.query_inodes, ''),
			FmQueryType('Path', self.query_paths, ''),
			FmQueryType('Extent Type', self.query_extent_type, ChecklistModel(extent_types)),
		]
		self.querytype_combo.insertItems(0, [x.label for x in self.query_types])
		self.old_querytype = 0
		self.querytype_combo.setCurrentIndex(self.old_querytype)
		self.querytype_combo.currentIndexChanged.connect(self.change_querytype)
		self.zoom_levels = [
			['100%', 1.0],
			['200%', 2.0],
			['400%', 4.0],
			['800%', 8.0],
		]
		self.zoom_combo.insertItems(0, [x[0] for x in self.zoom_levels])
		self.zoom_combo.currentIndexChanged.connect(self.change_zoom)
		self.query_checklist.hide()
		self.query_text.returnPressed.connect(self.run_query)
		self.toolBar.addWidget(self.query_frame)

		# Set up the status bar
		self.status_label = QtGui.QLabel()
		self.status_bar.addWidget(self.status_label)

	def change_zoom(self, idx):
		'''Handle a change in the zoom selector.'''
		self.overview.set_zoom(self.zoom_levels[idx][1])

	def enter_query(self, fn, text):
		'''Load the query UI elements.'''
		for x in range(0, len(self.query_types)):
			if self.query_types[x].query_fn == fn:
				self.querytype_combo.setCurrentIndex(x)
				self.query_text.setText(text)
				return

	def pick_fs_tree(self, n, o):
		'''Handle the selection of a FS tree nodes.'''
		self.ost.stop()
		nodes = {m.internalPointer() for m in self.fs_tree.selectedIndexes()}
		paths = [n.path for n in nodes]
		keymod = int(QtGui.QApplication.keyboardModifiers())
		if keymod & QtCore.Qt.AltModifier:
			if len(paths) == 0:
				self.etm.highlight_names(None)
			else:
				self.etm.highlight_names(paths)
			p = [n.path + '*' if n.hasChildren() else n.path for n in nodes]
		else:
			self.etm.highlight_names(None)
			p = paths
		self.enter_query(self.query_paths, ' '.join(p))
		self.run_query()

	def pick_extent_table(self, n, o):
		'''Handle the selection of extent table rows.'''
		rows = {m.row() for m in self.extent_table.selectedIndexes()}
		if len(rows) == 0:
			r = None
		else:
			r = rows
		ranges = [(ex.p_off, ex.p_off + ex.length - 1) for ex in self.etm.extents(r)]
		self.overview.highlight_ranges(ranges)

	def start(self):
		'''Load data from the database.'''
		# Don't call show() until you're done overriding widget methods
		self.overview.load()
		self.show()
		self.do_summary()
		return

	def change_querytype(self, idx):
		'''Handle a change in the query type selector.'''
		old_qt = self.query_types[self.old_querytype]
		new_qt = self.query_types[idx]
		if type(old_qt.saved_state) == str:
			old_qt.saved_state = self.query_text.text()
		if type(new_qt.saved_state) == str:
			self.query_checklist.hide()
			self.query_text.show()
			self.query_text.setText(new_qt.saved_state)
		elif type(new_qt.saved_state) == ChecklistModel:
			self.query_text.hide()
			self.query_checklist.show()
			self.query_checklist.setModel(new_qt.saved_state)
		self.old_querytype = idx

	def change_units(self, action):
		'''Handle one of the units menu items.'''
		idx = self.unit_actions.index(action)
		avail_units = [
			fmcli.units_auto,
			fmcli.units_bytes,
			fmcli.units_sectors,
			fmcli.units('B', 'blocks', self.fs_summary.block_size),
			fmcli.units_kib,
			fmcli.units_mib,
			fmcli.units_gib,
			fmcli.units_tib,
		]
		self.etm.change_units(avail_units[idx])
		for u in self.unit_actions:
			u.setChecked(False)
		self.unit_actions[idx].setChecked(True)

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
		self.enter_query(self.query_overview, "%s-%s" % (start, end - 1))
		self.ost.start(500)

	def run_query(self):
		'''Dispatch a query to populate the extent table.'''
		self.ost.stop()
		idx = self.querytype_combo.currentIndex()
		qt = self.query_types[idx]
		if type(qt.saved_state) == str:
			qt.saved_state = self.query_text.text()
			args = fmcli.split_unescape(str(qt.saved_state), ' ', ('"', "'"))
		elif type(qt.saved_state) == ChecklistModel:
			args = qt.saved_state.items()
		#print("QUERY:", self.query_types[idx].label, args)
		self.query_types[idx].query_fn(args)
		self.pick_extent_table(None, None)

	def query_extent_type(self, args):
		'''Query for extents based on the extent type code.'''
		r = [x[2] for x in args if x[1]]
		self.load_extents(self.fmdb.query_extent_types(r))

	def query_overview(self, args):
		'''Query for extents mapped to ranges of overview cells.'''
		ranges = []
		for arg in args:
			if arg == 'all':
				self.load_extents(self.fmdb.query_poff_range([]))
				return
			elif '-' in arg:
				pos = arg.index('-')
				ranges.append((int(arg[:pos]), int(arg[pos+1:])))
			else:
				ranges.append(int(arg))
		r = self.fmdb.pick_cells(ranges)
		self.load_extents(self.fmdb.query_poff_range(r))

	def query_poff(self, args):
		'''Query for extents mapped to ranges of physical bytes.'''
		def n2p(num):
			conv = [
				fmcli.units('%', 'percent', self.fs_summary.total_bytes / 100),
				fmcli.units('B', 'blocks', self.fs_summary.block_size),
				fmcli.units_bytes,
				fmcli.units_sectors,
				fmcli.units_kib,
				fmcli.units_mib,
				fmcli.units_gib,
				fmcli.units_tib,
			]
			for unit in conv:
				if num[-1].lower() == unit.abbrev.lower():
					return int(unit.factor * float(num[:-1]))
			return int(num)

		ranges = []
		for arg in args:
			if arg == 'all':
				self.load_extents(self.fmdb.query_poff_range([]))
				return
			elif '-' in arg:
				pos = arg.index('-')
				ranges.append((n2p(arg[:pos]), n2p(arg[pos+1:])))
			else:
				ranges.append((n2p(arg), n2p(arg)))
		self.load_extents(self.fmdb.query_poff_range(ranges))

	def load_extents(self, f):
		'''Populate the extent table.'''
		if isinstance(f, list):
			new_data = f
		else:
			new_data = [x for x in f]
		self.extent_table.sortByColumn(-1)
		self.etm.revise(new_data)
		for x in range(self.etm.columnCount(None)):
			self.extent_table.resizeColumnToContents(x)
		self.extent_dock.setWindowTitle('Extents (%s)' % fmcli.format_number(fmcli.units_none, len(new_data)))

	def query_inodes(self, args):
		'''Query for extents mapped to ranges of inodes.'''
		ranges = []
		for arg in args:
			if arg == 'all':
				self.load_extents(self.fmdb.query_inodes([]))
				return
			elif '-' in arg:
				pos = arg.index('-')
				ranges.append((int(arg[:pos]), int(arg[pos+1:])))
			else:
				ranges.append((int(arg), int(arg)))
		self.load_extents(self.fmdb.query_inodes(ranges))

	def query_paths(self, args):
		'''Query for extents mapped to a list of FS paths.'''
		if '*' in args:
			self.load_extents(self.fmdb.query_paths([], False))
			return
		self.load_extents(self.fmdb.query_paths(args))

	def do_summary(self):
		'''Load the FS summary into the status line.'''
		s = "%s of %s (%.0f%%) space used; %s of %s (%.0f%%) inodes used; %s extents; %s blocks" % \
			(fmcli.format_size(fmcli.units_auto, self.fs_summary.total_bytes - self.fs_summary.free_bytes), \
			 fmcli.format_size(fmcli.units_auto, self.fs_summary.total_bytes), \
			 100 * (1.0 - (self.fs_summary.free_bytes / self.fs_summary.total_bytes)), \
			 fmcli.format_number(fmcli.units_auto, self.fs_summary.total_inodes - self.fs_summary.free_inodes), \
			 fmcli.format_number(fmcli.units_auto, self.fs_summary.total_inodes), \
			 100 * (1.0 - (self.fs_summary.free_inodes / self.fs_summary.total_inodes)), \
			 fmcli.format_number(fmcli.units_auto, self.fs_summary.extents), \
			 fmcli.format_size(fmcli.units_auto, self.fs_summary.block_size))
		self.status_label.setText(s)

class OverviewModel:
	'''Render the overview into a text field.'''
	def __init__(self, fmdb, ctl, precision = 65536):
		self.fmdb = fmdb
		self.fs_summary = self.fmdb.query_summary()
		self.ctl = ctl
		self.ctl.resizeEvent = self.resize_ctl
		qfm = QtGui.QFontMetrics(self.ctl.currentFont())
		self.overview_font_width = qfm.width('D')
		self.overview_font_height = qfm.height()
		self.length = None
		self.zoom = 1.0
		self.precision = precision
		self.overview_big = None
		self.rst = QtCore.QTimer()
		self.rst.timeout.connect(self.delayed_resize)
		self.range_highlight = None

	@staticmethod
	def overview_to_letter(ov):
		'''Convert an overview block to a letter.'''
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

	def load(self):
		'''Query the DB for the high-res overview data.'''
		olen = min(self.precision, self.fs_summary.total_bytes // self.fs_summary.block_size)
		self.fmdb.set_overview_length(olen)
		self.overview_big = [ov for ov in self.fmdb.query_overview()]

	def set_zoom(self, zoom):
		'''Set the zoom factor for the overview.'''
		self.zoom = zoom
		self.render()

	def set_length(self, length):
		'''Set the overview length, in characters.'''
		self.length = length
		self.render()

	def render(self):
		'''Render the overview into the text view.'''
		def is_highlighted(cell):
			if range_highlight is None:
				return False
			for start, end in range_highlight:
				if cell >= start and cell <= end:
					return True
			return False
		def compress_ranges(ranges):
			if len(ranges) < 50:
				return ranges
			max_num = None
			rset = set()
			for start, end in ranges:
				if max_num is None or max_num < end:
					max_num = end
				for x in range(start, end + 1):
					rset.add(x)
			ret = []
			start = None
			end = None
			for n in range(0, max_num + 1):
				if n in rset:
					if start is None:
						start = n
					end = n
				else:
					if start is not None:
						ret.append((start, end))
						start = None
			if start is not None:
				ret.append((start, end))
			return ret

		if self.overview_big is None:
			return
		olen = int(self.length * self.zoom)
		self.fmdb.set_overview_length(olen)
		if self.range_highlight is None:
			range_highlight = None
		else:
			t = {x for x in self.fmdb.pick_bytes(self.range_highlight)}
			range_highlight = compress_ranges(t)
		o2s = len(self.overview_big) / olen
		ov_str = []
		t0 = datetime.datetime.today()
		for i in range(0, olen):
			x = int(math.floor(i * o2s))
			y = int(math.ceil((i + 1) * o2s))
			ss = self.overview_big[x:y]
			ovs = fmdb.overview_block()
			for s in ss:
				ovs.add(s)
			h = is_highlighted(i)
			if h:
				ov_str.append('<b>')
			ov_str.append(self.overview_to_letter(ovs))
			if h:
				ov_str.append('</b>')
		t1 = datetime.datetime.today()
		#print("render ", t1 - t0)
		cursor = self.ctl.textCursor()
		start = cursor.selectionStart()
		end = cursor.selectionEnd()
		self.ctl.setText(''.join(ov_str))
		# XXX: need to set the selection background?

	def resize_ctl(self, event):
		'''Handle the resizing of the text view control.'''
		QtGui.QTextEdit.resizeEvent(self.ctl, event)
		sz = self.ctl.viewport().size()
		# Cheat with the textedit width/height -- use one less
		# column than we probably could, and force wrapping at
		# that column.
		w = (sz.width() // self.overview_font_width) - 1
		h = (sz.height() // self.overview_font_height) - 1
		self.ctl.setLineWrapColumnOrWidth(w)
		#print("overview; %f x %f = %f" % (w, h, w * h))
		self.length = w * h
		self.rst.start(40)

	def delayed_resize(self):
		self.rst.stop()
		self.render()

	def highlight_ranges(self, ranges):
		'''Highlight a range of physical extents in the overview.'''
		old_highlight = self.range_highlight
		if ranges is None:
			self.range_highlight = None
		else:
			self.range_highlight = {x for x in ranges}
		if old_highlight == self.range_highlight:
			return
		self.render()

class ChecklistModel(QtCore.QAbstractTableModel):
	'''A list model for checkable items.'''
	def __init__(self, items, parent=None, *args):
		QtCore.QAbstractTableModel.__init__(self, parent, *args)
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
		self.rows[row][1] = value
		return True

	def items(self):
		return self.rows
