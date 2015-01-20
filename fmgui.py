

import sys
from PyQt4 import QtGui, uic, QtCore

class ExtentTableModel(QtCore.QAbstractTableModel):
	def __init__(self, parent=None, *args):
		QtCore.QAbstractTableModel.__init__(self, parent, *args)
		self.arraydata = [['A:', '0', '0', '100', 'moo?', 'urk']]
		self.headerdata = ['path', 'p_off', 'l_off', 'length', 'flags', 'type']

	def rowCount(self, parent):
		return len(self.arraydata)

	def columnCount(self, parent):
		return len(self.arraydata[0])

	def itemData(self, index, role):
		if not index.isValid():
			return '' #QtCore.QVariant()
		elif role != QtCore.Qt.DisplayRole:
			return '' #QtCore.QVariant()
		return self.arraydata[index.row()][index.column()] #QtCore.QVariant(self.arraydata[index.row()][index.column()])

	def headerData(self, col, orientation, role):
		if orientation == QtCore.Qt.Horizontal and role == QtCore.Qt.DisplayRole:
			return self.headerdata[col] #QtCore.QVariant(self.headerdata[col])
		return '' #QtCore.QVariant()

class fmgui(QtGui.QMainWindow):
	def __init__(self, fmdb):
		super(fmgui, self).__init__()
		self.fmdb = fmdb
		uic.loadUi('filemapper.ui', self)
		self.show()
		self.do_overview()
		#self.extent_table.setModel(ExtentTableModel())
		self.extent_table.setRowCount(0)
		self.extent_table.setColumnCount(6)
		self.extent_table.setHorizontalHeaderLabels(['Path', 'Physical Offset', 'Logical Offset', 'Length', 'Flags', 'Type'])
		self.extent_table.resizeColumnsToContents()
		self.query_btn.clicked.connect(self.run_query)
		self.query_types = [
			['Overview Cells', self.query_overview],
			['Physical Offsets', self.query_poff],
			['Inode Numbers', self.query_inodes],
			['Path', self.query_paths],
		]
		self.querytype_combo.insertItems(0, [x[0] for x in self.query_types])

		# set up first query
		self.query_text.setText('651571531776')
		self.querytype_combo.setCurrentIndex(1)

	def run_query(self):
		idx = self.querytype_combo.currentIndex()
		fn = self.query_types[idx][1]
		fn()

	def load_extent(self, row, ext):
		typecodes = {
			'f': 'file',
			'd': 'directory',
			'e': 'file map',
			'm': 'metadata',
			'x': 'extended attribute',
		}
		self.extent_table.setItem(row, 0, QtGui.QTableWidgetItem(ext.path))
		self.extent_table.setItem(row, 1, QtGui.QTableWidgetItem('%d' % ext.p_off))
		self.extent_table.setItem(row, 2, QtGui.QTableWidgetItem('%d' % ext.l_off))
		self.extent_table.setItem(row, 3, QtGui.QTableWidgetItem('%d' % ext.length))
		self.extent_table.setItem(row, 4, QtGui.QTableWidgetItem('0x%x' % ext.flags))
		self.extent_table.setItem(row, 5, QtGui.QTableWidgetItem(typecodes[ext.type]))

	def query_overview(self):
		pass

	def query_poff(self):
		row = 0
		ls = [x for x in self.fmdb.query_poff_range([])]
		self.extent_table.setRowCount(len(ls))
		for x in ls:
			self.load_extent(row, x)
			row += 1
		self.extent_table.setRowCount(row)
		self.extent_table.resizeColumnsToContents()

	def query_inodes(self):
		pass

	def query_paths(self):
		pass

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

if __name__ == '__main__':
	app = QtGui.QApplication(sys.argv)
	window = fmgui(None)
	sys.exit(app.exec_())
