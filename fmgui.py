import sip
sip.setapi('QVariant', 1)

import sys
from PyQt4 import QtGui, uic, QtCore
import fmcli

null_variant = QtCore.QVariant()
null_model = QtCore.QModelIndex()

class ExtentTableModel(QtCore.QAbstractTableModel):
	def __init__(self, data, units, rows_to_show=50, parent=None, *args):
		QtCore.QAbstractTableModel.__init__(self, parent, *args)
		self.__data = data
		self.headers = ['Physical Offset', 'Logical Offset', \
				'Length', 'Flags', 'Type', 'Path']
		self.header_map = [
			lambda x: fmcli.format_number(self.units, x.p_off),
			lambda x: fmcli.format_number(self.units, x.l_off),
			lambda x: fmcli.format_number(self.units, x.length),
			lambda x: x.flags,
			lambda x: fmcli.typecodes[x.type],
			lambda x: x.path]
		self.units = units
		self.rows_to_show = rows_to_show
		self.rows = min(rows_to_show, len(data))

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
		#self.emit(QtCore.SIGNAL("layoutAboutToBeChanged()"))
		self.beginInsertRows(parent, self.rows, self.rows + nlen)
		self.rows += nlen
		self.endInsertRows()
		#self.emit(QtCore.SIGNAL("layoutChanged()"))

	def rowCount(self, parent):
		if not parent.isValid():
			return self.rows
		return 0

	def columnCount(self, parent):
		return len(self.headers)

	def data(self, index, role):
		if not index.isValid():
			return null_variant
		elif role != QtCore.Qt.DisplayRole:
			return null_variant
		i = index.row()
		j = index.column()
		return QtCore.QVariant(self.header_map[j](self.__data[i]))

	def headerData(self, col, orientation, role):
		if orientation == QtCore.Qt.Horizontal and \
		   role == QtCore.Qt.DisplayRole:
			return self.headers[col]
		return null_variant

class FsTreeNode:
	def __init__(self, path = None, type = None, load_fn = None, parent = None):
		if path is not None and type is None:
			raise InvalidArgument("BLSDHDGSS")
		if path is None:
			self.path = ''
			self.type = 'd'
		else:
			self.path = path
			self.type = type
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
		self.children = [FsTreeNode(self.path + '/' + de.name, de.type, parent = self) for de in self.load_fn([self.path])]

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
		return 1

	def data(self, index, role):
		if not index.isValid():
			return None
		node = index.internalPointer()
		if role == QtCore.Qt.DisplayRole:
			node.load()
			r = node.path.rindex('/')
			return node.path[r + 1:]
		return None

	def headerData(self, section, orientation, role):
		if orientation == QtCore.Qt.Horizontal and \
		   role == QtCore.Qt.DisplayRole and section == 0:
			return 'Name'
		return None

class fmgui(QtGui.QMainWindow):
	def __init__(self, fmdb):
		super(fmgui, self).__init__()
		self.fmdb = fmdb
		uic.loadUi('filemapper.ui', self)
		self.setWindowTitle('%s - QFileMapper' % self.fmdb.fspath)
		self.show()

		# Set up the units menu
		units = fmcli.units_bytes
		self.unit_actions = self.menuUnits.actions()
		ag = QtGui.QActionGroup(self)
		for u in self.unit_actions:
			u.setActionGroup(ag)
		ag.triggered.connect(self.change_units)

		# Set up the overview
		self.overview_text.selectionChanged.connect(self.select_overview)

		# Set up the views
		self.etm = ExtentTableModel([], units)
		self.unit_actions[0].setChecked(True)
		self.extent_table.setModel(self.etm)

		root = FsTreeNode('', 'd', self.fmdb.query_ls)

		self.ftm = FsTreeModel(root) #QtGui.QFileSystemModel()
		#self.ftm.setRootPath('/')
		self.fs_tree.setModel(self.ftm)

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

	def start(self):
		#self.load_fstree()
		#self.do_overview()
		return

	def change_querytype(self, idx):
		self.query_types[self.old_querytype][2] = self.query_text.text()
		self.query_text.setText(self.query_types[idx][2])
		self.old_querytype = idx

	def change_units(self, action):
		idx = self.unit_actions.index(action)
		res = self.fmdb.query_summary()
		units = [
			fmcli.units_bytes,
			fmcli.units_sectors,
			fmcli.units('B', 'blocks', lambda x: x // (res.block_size), None),
			fmcli.units_kib,
			fmcli.units_mib,
			fmcli.units_gib,
			fmcli.units_tib,
		]
		self.etm.change_units(units[idx])
		for u in self.unit_actions:
			u.setChecked(False)
		self.unit_actions[idx].setChecked(True)

	def select_overview(self):
		cursor = self.overview_text.textCursor()
		start = cursor.selectionStart()
		end = cursor.selectionEnd()
		if start == end:
			return
		self.querytype_combo.setCurrentIndex(0)
		self.query_text.setText("%s-%s" % (start, end - 1))

	def run_query(self):
		idx = self.querytype_combo.currentIndex()
		args = fmcli.split_unescape(str(self.query_text.text()), ' ', ('"', "'"))
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
				fmcli.units('%', 'percent', None, lambda x: x * res_total_bytes / 100),
				fmcli.units('B', 'blocks', None, lambda x: x * res.block_size),
				fmcli.units_bytes,
				fmcli.units_sectors,
				fmcli.units_kib,
				fmcli.units_mib,
				fmcli.units_gib,
				fmcli.units_tib,
			]
			for unit in conv:
				if num[-1].lower() == unit.abbrev.lower():
					return int(unit.in_fn(float(num[:-1])))
			return int(num)

		ranges = []
		res = self.fmdb.query_summary()
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
		arg = [x.replace('*', '%') for x in args]
		self.load_extents(self.fmdb.query_paths(arg))

	def do_overview(self):
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
		x = [overview_to_letter(ov) for ov in self.fmdb.query_overview()]
		self.overview_text.setText(''.join(x))

	def load_fstree(self):
		self.ftm.revise([pi for pi in self.fmdb.query_ls([])])

if __name__ == '__main__':
	app = QtGui.QApplication(sys.argv)
	window = fmgui(None)
	sys.exit(app.exec_())
