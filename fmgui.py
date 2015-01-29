# QT GUI routines for filemapper
# Copyright(C) 2015 Darrick J. Wong
# Licensed under GPLv2.

import sys
from PyQt4 import QtGui, uic, QtCore
import fmcli
import datetime
import fmdb
import math

null_model = QtCore.QModelIndex()
bold_font = QtGui.QFont()
bold_font.setBold(True)

class ExtentTableModel(QtCore.QAbstractTableModel):
	def __init__(self, data, units, hm, rows_to_show=50, parent=None, *args):
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
		self.units = units
		self.rows_to_show = rows_to_show
		self.rows = min(rows_to_show, len(data))
		self.highlight = hm

	def change_units(self, new_units):
		self.units = new_units
		tl = self.createIndex(0, 0)
		br = self.createIndex(len(self.__data) - 1, 2)
		self.dataChanged.emit(tl, br)

	def revise(self, new_data):
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
		if not index.isValid():
			return None
		i = index.row()
		j = index.column()
		row = self.__data[i]
		if role == QtCore.Qt.DisplayRole:
			return self.header_map[j](row)
		elif role == QtCore.Qt.FontRole:
			if self.highlight.is_name_highlighted(row.path):
				return bold_font
			return None
		else:
			return None

	def headerData(self, col, orientation, role):
		if orientation == QtCore.Qt.Horizontal and \
		   role == QtCore.Qt.DisplayRole:
			return self.headers[col]
		return None

class HighlightModel:
	def __init__(self):
		self.extents = None
		self.paths = None

	def set_highlight(self, extents = None, paths = None):
		self.extents = extents
		self.paths = paths
		# FIXME: actually whack the models?

	def is_name_highlighted(self, name):
		if self.paths is None:
			return False
		return name in self.paths

	def is_range_highlighted(self, start, end):
		return False

class FsTreeNode:
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
		if not index.isValid():
			return null_model
		node = index.internalPointer()
		if node.parent is None:
			return null_model
		# Create an index for the parent, from the perspective
		# of the *grandparent* node...
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
	def __init__(self, fmdb):
		super(fmgui, self).__init__()
		self.fmdb = fmdb
		uic.loadUi('filemapper.ui', self)
		self.setWindowTitle('%s - QFileMapper' % self.fmdb.fspath)
		self.highlight = HighlightModel()
		self.fs_summary = self.fmdb.query_summary()
		self.overview = OverviewModel(fmdb, self.overview_text)

		# Set up the units menu
		units = fmcli.units_auto
		self.unit_actions = self.menuUnits.actions()
		ag = QtGui.QActionGroup(self)
		for u in self.unit_actions:
			u.setActionGroup(ag)
		ag.triggered.connect(self.change_units)

		# Set up the overview
		self.overview_text.selectionChanged.connect(self.select_overview)
		self.ost = QtCore.QTimer()
		self.ost.timeout.connect(self.run_query)
		self.old_ostart = None
		self.old_oend = None

		# Set up the views
		self.etm = ExtentTableModel([], units, self.highlight)
		self.unit_actions[0].setChecked(True)
		self.extent_table.setModel(self.etm)

		de = self.fmdb.query_root()
		root = FsTreeNode(de.name, de.ino, de.type, lambda x: self.fmdb.query_ls([x]))

		self.ftm = FsTreeModel(root)
		self.fs_tree.setModel(self.ftm)
		self.fs_tree.selectionModel().selectionChanged.connect(self.pick_fs_tree)

		# Set up the query UI
		self.query_btn.clicked.connect(self.run_query)
		self.query_types = [
			['Overview Cells', self.query_overview, ''],
			['Physical Offsets', self.query_poff, ''],
			['Inode Numbers', self.query_inodes, ''],
			['Path', self.query_paths, ''],
		]
		self.querytype_combo.insertItems(0, [x[0] for x in self.query_types])
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
		self.toolBar.addWidget(self.query_frame)

		# Set up the status bar
		self.status_label = QtGui.QLabel()
		self.status_bar.addWidget(self.status_label)

	def change_zoom(self, idx):
		self.overview.set_zoom(self.zoom_levels[idx][1])

	def enter_query(self, fn, text):
		for x in range(0, len(self.query_types)):
			if self.query_types[x][1] == fn:
				self.querytype_combo.setCurrentIndex(x)
				self.query_text.setText(text)
				return

	def pick_fs_tree(self, n, o):
		self.ost.stop()
		nodes = [m.internalPointer() for m in n.indexes()]
		paths = [n.path for n in nodes]
		self.highlight.set_highlight(None, paths)
		if QtGui.QApplication.keyboardModifiers() == QtCore.Qt.ShiftModifier:
			p = [n.path + '*' if n.hasChildren() else n.path for n in nodes]
		else:
			p = paths
		self.enter_query(self.query_paths, ' '.join(p))
		self.run_query()

	def start(self):
		# Don't call show() until you're done overriding widget methods
		self.overview.load()
		self.show()
		self.do_summary()
		return

	def change_querytype(self, idx):
		self.query_types[self.old_querytype][2] = self.query_text.text()
		self.query_text.setText(self.query_types[idx][2])
		self.old_querytype = idx

	def change_units(self, action):
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
		self.ost.stop()
		idx = self.querytype_combo.currentIndex()
		args = fmcli.split_unescape(str(self.query_text.text()), ' ', ('"', "'"))
		#print("QUERY:", self.query_types[idx][0], args)
		fn = self.query_types[idx][1]
		fn(args)

	def query_overview(self, args):
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
		if isinstance(f, list):
			new_data = f
		else:
			new_data = [x for x in f]
		self.etm.revise(new_data)
		for x in range(self.etm.columnCount(None)):
			self.extent_table.resizeColumnToContents(x)
		self.extent_dock.setWindowTitle('Extents (%s)' % fmcli.format_number(fmcli.units_none, len(new_data)))

	def query_inodes(self, args):
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
		if '*' in args:
			self.load_extents(self.fmdb.query_paths([], False))
			return
		self.load_extents(self.fmdb.query_paths(args))

	def do_summary(self):
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

	@staticmethod
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

	def load(self):
		olen = min(self.precision, self.fs_summary.total_bytes // self.fs_summary.block_size)
		self.fmdb.set_overview_length(olen)
		self.overview_big = [ov for ov in self.fmdb.query_overview()]

	def set_zoom(self, zoom):
		self.zoom = zoom
		self.render()

	def set_length(self, length):
		self.length = length
		self.render()

	def render(self):
		if self.overview_big is None:
			return
		olen = int(self.length * self.zoom)
		self.fmdb.set_overview_length(olen)
		o2s = len(self.overview_big) / olen
		ov_str = ['.' for x in range(0, olen)]
		t0 = datetime.datetime.today()
		for i in range(0, olen):
			x = int(math.floor(i * o2s))
			y = int(math.ceil((i + 1) * o2s))
			ss = self.overview_big[x:y]
			ovs = fmdb.overview_block()
			for s in ss:
				ovs.add(s)
			ov_str[i] = self.overview_to_letter(ovs)
		t1 = datetime.datetime.today()
		#print("render ", t1 - t0)
		self.ctl.setText(''.join(ov_str))

	def resize_ctl(self, event):
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

if __name__ == '__main__':
	app = QtGui.QApplication(sys.argv)
	window = fmgui(None)
	sys.exit(app.exec_())
