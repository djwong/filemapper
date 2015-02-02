# QT GUI routines for filemapper
# Copyright(C) 2015 Darrick J. Wong
# Licensed under GPLv2.

import sys
from PyQt4 import QtGui, uic, QtCore
import fmcli
import datetime
import fmdb
import math
import fiemap
import os.path
import json
import base64
from abc import ABCMeta, abstractmethod

null_model = QtCore.QModelIndex()
bold_font = QtGui.QFont()
bold_font.setBold(True)

class ExtentTableModel(QtCore.QAbstractTableModel):
	'''Render and highlight an extent table.'''
	def __init__(self, fs, data, units, rows_to_show=100, parent=None, *args):
		super(ExtentTableModel, self).__init__(parent, *args)
		self.__data = data
		self.headers = ['Physical Offset', 'Logical Offset', \
				'Length', 'Flags', 'Type', 'Path']
		self.header_map = [
			lambda x: fmcli.format_size(self.units, x.p_off),
			lambda x: fmcli.format_size(self.units, x.l_off),
			lambda x: fmcli.format_size(self.units, x.length),
			lambda x: x.flags_to_str(),
			lambda x: fmcli.typecodes[x.type],
			lambda x: x.path if x.path != '' else fs.pathsep]
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

class FsTreeNode(object):
	'''A node in the recorded filesystem.'''
	def __init__(self, path, ino, type, load_fn = None, parent = None, fs = None):
		if load_fn is None and parent is None:
			raise Exception
		if fs is None and parent is None:
			raise Exception
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
		if self.type != 'd':
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
		return self.type == 'd'

class FsTreeModel(QtCore.QAbstractItemModel):
	'''Model the filesystem tree recorded in the database.'''
	def __init__(self, fs, root, parent=None, *args):
		super(FsTreeModel, self).__init__(parent, *args)
		self.root = root
		self.headers = ['Name'] #, 'Inode']
		self.fs = fs

	def index(self, row, column, parent):
		if not parent.isValid():
			return self.createIndex(row, column, self.root)
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
					return '/'
				r = node.path.rindex(self.fs.pathsep)
				return node.path[r + 1:]
			else:
				return node.ino
		elif role == QtCore.Qt.DecorationRole:
			if node.type == 'd':
				return QtGui.QIcon.fromTheme('folder')
			else:
				return QtGui.QIcon.fromTheme('text-x-generic')
		return None

	def headerData(self, col, orientation, role):
		if orientation == QtCore.Qt.Horizontal and \
		   role == QtCore.Qt.DisplayRole:
			return self.headers[col]
		return None

def sort_dentry(dentry):
	if dentry.type == 'd':
		return '0' + dentry.name
	else:
		return '1' + dentry.name

class fmgui(QtGui.QMainWindow):
	'''Manage the GUI widgets and interactions.'''
	def __init__(self, fmdb, histfile=os.path.join(os.path.expanduser('~'), \
						'.config', 'fmgui-history')):
		super(fmgui, self).__init__()
		self.json_version = 1
		self.fmdb = fmdb
		uic.loadUi('filemapper.ui', self)
		self.setWindowTitle('%s - QFileMapper' % self.fmdb.fspath)
		self.fs = self.fmdb.query_summary()
		self.histfile = histfile

		# Set up the units menu
		units = fmcli.units_auto
		self.unit_actions = self.menuUnits.actions()
		ag = QtGui.QActionGroup(self)
		for u in self.unit_actions:
			u.setActionGroup(ag)
		ag.triggered.connect(self.change_units)

		# Set up the overview
		self.overview = OverviewModel(fmdb, self.overview_text)
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
		self.extent_table.sortByColumn(-1)

		# Set up the fs tree view
		de = self.fmdb.query_root()
		root = FsTreeNode(de.name, de.ino, de.type, \
				  lambda x: sorted(self.fmdb.query_ls([x]), key = sort_dentry), \
				  fs = self.fs)

		self.ftm = FsTreeModel(self.fs, root)
		self.fs_tree.setModel(self.ftm)
		self.fs_tree.selectionModel().selectionChanged.connect(self.pick_fs_tree)
		self.fs_tree.setRootIsDecorated(False)

		# Set up the query UI
		# First, the combobox-lineedit widget weirdness
		self.query_text.setAutoCompletion(True)
		self.query_text.setAutoCompletionCaseSensitivity(False)
		self.query_text.setDuplicatesEnabled(False)
		self.xle = XLineEdit(self.query_text)
		self.xle.returnPressed.connect(self.run_query)
		self.query_text.setLineEdit(self.xle)
		self.query_text.activated.connect(self.run_query)

		# Next, the query button
		self.query_btn.clicked.connect(self.run_query)

		# Then the check-list query data
		extent_types = [
			['File', True, 'f'],
			['Directory', True, 'd'],
			['Extent Map', True, 'e'],
			['Metadata', True, 'm'],
			['Extended Attribute', True, 'x'],
			['Symbolic Link', True, 's'],
		]
		extent_flags = [
			['Unknown', False, 'n'],
			['Delayed Allocation', False, 'd'],
			['Encoded', False, 'e'],
			['Encrypted', False, 'E'],
			['Unaligned', False, 'u'],
			['Inline', False, 'i'],
			['Tail-packed', False, 't'],
			['Unwritten', False, 'U'],
			['Merged', False, 'm'],
			['Shared', False, 's'],
			['Exact Match', False],
		]
		def sq(l, q):
			return StringQuery(l, self.query_text, q)
		def cq(l, q, x):
			return ChecklistQuery(l, self.query_checklist, q, x)
		self.query_types = [
			sq('Overview Cells', self.query_overview),
			sq('Physical Offsets', self.query_poff),
			sq('Logical Offsets', self.query_loff),
			sq('Inode Numbers', self.query_inodes),
			sq('Paths', self.query_paths),
			cq('Extent Types', self.query_extent_type, extent_types),
			cq('Extent Flags', self.query_extent_flags, extent_flags),
			sq('Extent Lengths', self.query_lengths),
		]

		# Then the query type selector
		self.querytype_combo.insertItems(0, [x.label for x in self.query_types])
		self.querytype_combo.currentIndexChanged.connect(self.change_querytype)
		self.query_checklist.hide()
		self.query_text.hide()
		self.old_querytype = None

		# Finally move the query UI to the toolbar
		self.toolBar.addWidget(self.query_frame)

		# Set up the zoom control
		self.zoom_levels = [
			['100%', 1.0],
			['200%', 2.0],
			['400%', 4.0],
			['800%', 8.0],
		]
		self.zoom_combo.insertItems(0, [x[0] for x in self.zoom_levels])
		self.zoom_combo.currentIndexChanged.connect(self.change_zoom)

		# Set up the status bar
		self.status_label = QtGui.QLabel()
		self.status_bar.addWidget(self.status_label)

		# Here we go!
		self.load_state()

		self.overview.load()
		self.show()

	def closeEvent(self, ev):
		qt = self.query_types[self.querytype_combo.currentIndex()]
		qt.save_query()
		self.save_state()
		super(fmgui, self).closeEvent(ev)

	def save_state(self):
		'''Save the state of the UI.'''
		data = {
			'version': self.json_version,
			'zoom': self.zoom_levels[self.zoom_combo.currentIndex()][1],
			'query_type': self.query_types[self.querytype_combo.currentIndex()].label,
			'units': self.etm.units.label,
			'window_state': base64.b64encode(self.saveState()).decode('utf-8'),
			'window_geometry': base64.b64encode(self.saveGeometry()).decode('utf-8'),
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
			for zl in self.zoom_levels:
				if zl[1] == data['zoom']:
					self.zoom_combo.setCurrentIndex(x)
					break
				x += 1
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
		except Exception as e:
			failed = True
		if failed:
			try:
				os.unlink(self.histfile)
			except:
				pass
		if self.querytype_combo.currentIndex() == 0:
			self.change_querytype(0)

	def change_zoom(self, idx):
		'''Handle a change in the zoom selector.'''
		self.overview.set_zoom(self.zoom_levels[idx][1])

	def enter_query(self, fn, text):
		'''Load the query UI elements.'''
		for x in range(0, len(self.query_types)):
			if self.query_types[x].query_fn == fn:
				self.querytype_combo.setCurrentIndex(x)
				self.query_text.setEditText(text)
				return

	def pick_fs_tree(self, n, o):
		'''Handle the selection of a FS tree nodes.'''
		def n(n):
			p = n.path
			if p == '':
				p = '/'
			if n.hasChildren():
				return p + '*'
			return p
		self.ost.stop()
		nodes = {m.internalPointer() for m in self.fs_tree.selectedIndexes()}
		paths = [n.path if n.path != '' else '/' for n in nodes]
		keymod = int(QtGui.QApplication.keyboardModifiers())
		if keymod & QtCore.Qt.AltModifier:
			if len(paths) == 0:
				self.etm.highlight_names(None)
			else:
				self.etm.highlight_names(paths)
			p = [n(n) for n in nodes]
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
		if start + 1 == end:
			self.enter_query(self.query_overview, "%s" % start)
		else:
			self.enter_query(self.query_overview, "%s-%s" % (start, end - 1))
		self.ost.start(500)

	def run_query(self):
		'''Dispatch a query to populate the extent table.'''
		self.ost.stop()
		idx = self.querytype_combo.currentIndex()
		qt = self.query_types[idx]
		qt.run_query()
		self.pick_extent_table(None, None)
		# XXX: should we clear the fs tree selection too?
		self.save_state()

	def query_extent_type(self, args):
		'''Query for extents based on the extent type code.'''
		r = [x[2] for x in args if x[1]]
		self.load_extents(self.fmdb.query_extent_types(r))

	def query_extent_flags(self, args):
		'''Query for extents based on the extent flag code.'''
		exact = args[-1][1]
		flags = 0
		for x in args:
			if len(x) > 2 and x[1]:
				flags |= fiemap.extent_str_to_flags(x[2])
		self.load_extents(self.fmdb.query_extent_flags(flags, exact))

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

	def query_loff(self, args):
		'''Query for extents mapped to ranges of logical bytes.'''
		ranges = []
		for arg in args:
			if arg == 'all':
				self.load_extents(self.fmdb.query_loff_range([]))
				return
			elif '-' in arg:
				pos = arg.index('-')
				ranges.append((fmcli.n2p(self.fs, arg[:pos]), fmcli.n2p(self.fs, arg[pos+1:])))
			else:
				ranges.append(n2p(self.fs, arg))
		self.load_extents(self.fmdb.query_loff_range(ranges))

	def query_poff(self, args):
		'''Query for extents mapped to ranges of physical bytes.'''
		ranges = []
		for arg in args:
			if arg == 'all':
				self.load_extents(self.fmdb.query_poff_range([]))
				return
			elif '-' in arg:
				pos = arg.index('-')
				ranges.append((fmcli.n2p(self.fs, arg[:pos]), fmcli.n2p(self.fs, arg[pos+1:])))
			else:
				ranges.append(n2p(self.fs, arg))
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
				ranges.append(int(arg))
		self.load_extents(self.fmdb.query_inodes(ranges))

	def query_lengths(self, args):
		'''Query for extents based on ranges of lengths.'''
		ranges = []
		for arg in args:
			if arg == 'all':
				self.load_extents(self.fmdb.query_lengths([]))
				return
			elif '-' in arg:
				pos = arg.index('-')
				ranges.append((fmcli.n2p(self.fs, arg[:pos]), fmcli.n2p(self.fs, arg[pos+1:])))
			else:
				ranges.append(fmcli.n2p(self.fs, arg))
		self.load_extents(self.fmdb.query_lengths(ranges))

	def query_paths(self, args):
		'''Query for extents mapped to a list of FS paths.'''
		if '*' in args:
			self.load_extents(self.fmdb.query_paths([], False))
			return
		self.load_extents(self.fmdb.query_paths(args))

	def do_summary(self):
		'''Load the FS summary into the status line.'''
		s = "%s of %s (%.0f%%) used; %s of %s (%.0f%%) inodes used; %s extents; %s FS blocks; %s per cell; %.1f%% fragmentation" % \
			(fmcli.format_size(fmcli.units_auto, self.fs.total_bytes - self.fs.free_bytes), \
			 fmcli.format_size(fmcli.units_auto, self.fs.total_bytes), \
			 100 * (1.0 - (float(self.fs.free_bytes) / self.fs.total_bytes)), \
			 fmcli.format_number(fmcli.units_auto, self.fs.total_inodes - self.fs.free_inodes), \
			 fmcli.format_number(fmcli.units_auto, self.fs.total_inodes), \
			 100 * (1.0 - (float(self.fs.free_inodes) / self.fs.total_inodes)), \
			 fmcli.format_number(fmcli.units_auto, self.fs.extents), \
			 fmcli.format_size(fmcli.units_auto, self.fs.block_size), \
			 fmcli.format_size(fmcli.units_auto, float(self.fs.total_bytes) / self.overview.total_length()), \
			 100.0 * self.fs.extents / self.fs.inodes - 100)
		self.status_label.setText(s)

class OverviewModel(QtCore.QObject):
	'''Render the overview into a text field.'''
	rendered = QtCore.pyqtSignal()

	def __init__(self, fmdb, ctl, precision = 65536, parent = None):
		super(OverviewModel, self).__init__(parent)
		self.fmdb = fmdb
		self.fs = self.fmdb.query_summary()
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

	def load(self):
		'''Query the DB for the high-res overview data.'''
		olen = min(self.precision, self.fs.total_bytes // self.fs.block_size)
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

	def total_length(self):
		'''The total length of the overview.'''
		return self.length * self.zoom

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
			for x in ranges:
				if type(x) == int:
					start = end = x
				else:
					start, end = x
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

		t0 = datetime.datetime.today()
		if self.overview_big is None:
			return
		olen = int(self.length * self.zoom)
		self.fmdb.set_overview_length(olen)
		if self.range_highlight is None:
			range_highlight = None
		else:
			t = {x for x in self.fmdb.pick_bytes(self.range_highlight)}
			range_highlight = compress_ranges(t)
		o2s = float(len(self.overview_big)) / olen
		ov_str = []
		t1 = datetime.datetime.today()
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
			ov_str.append(ovs.to_letter())
			if h:
				ov_str.append('</b>')
		t2 = datetime.datetime.today()
		#print("render ", t1 - t0, t2 - t1)
		cursor = self.ctl.textCursor()
		start = cursor.selectionStart()
		end = cursor.selectionEnd()
		self.ctl.setText(''.join(ov_str))
		self.rendered.emit()

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
		if string in self.history:
			self.history.remove(string)
		r = self.ctl.findText(string)
		if r >= 0:
			self.ctl.removeItem(r)
		self.history.append(string)
		self.ctl.addItem(string)
		self.ctl.setCurrentIndex(self.history.index(string))

	def export_state(self):
		return {'edit_string': str(self.edit_string), 'history': self.history}

	def import_state(self, data):
		self.edit_string = data['edit_string']
		self.history = data['history']

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

class XLineEdit(QtGui.QLineEdit):
	'''QLineEdit with clear button, which appears when user enters text.'''
	def __init__(self, parent=None):
		super(XLineEdit, self).__init__(parent)
		self.layout = QtGui.QHBoxLayout(self)
		self.image = QtGui.QLabel(self)
		self.image.setCursor(QtCore.Qt.ArrowCursor)
		self.image.setFocusPolicy(QtCore.Qt.NoFocus)
		self.image.setStyleSheet("border: none;")
		pixmap = QtGui.QIcon.fromTheme('locationbar-erase').pixmap(16, 16)
		self.image.setPixmap(pixmap)
		self.image.setSizePolicy(
			QtGui.QSizePolicy.Expanding,
			QtGui.QSizePolicy.Expanding)
		self.image.adjustSize()
		self.image.setScaledContents(True)
		self.layout.addWidget(
			self.image, alignment=QtCore.Qt.AlignRight)
		self.textChanged.connect(self.changed)
		self.image.hide()
		self.image.mouseReleaseEvent = self.clear_mouse_release

	def clear_mouse_release(self, ev):
		QtGui.QLabel.mouseReleaseEvent(self.image, ev)
		if ev.button() == QtCore.Qt.LeftButton:
			self.clear()

	def changed(self, text):
		if len(text) > 0:
			self.image.show()
		else:
			self.image.hide()
