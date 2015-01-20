import sys
from PyQt4 import QtGui, uic

class fmgui(QtGui.QMainWindow):
	def __init__(self, fmdb):
		super(fmgui, self).__init__()
		self.fmdb = fmdb
		uic.loadUi('filemapper.ui', self)
		self.show()
		self.do_overview()

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
