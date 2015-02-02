CFLAGS=-Wall -O3 -g
LDLIBS=-lsqlite3 -lext2fs -lcom_err
VERSION=0.5

prefix = /usr
exec_prefix = ${prefix}
bindir = ${exec_prefix}/bin
libdir = ${exec_prefix}/lib
fmdir = ${libdir}/filemapper

all: e2mapper

clean:;
	rm -rf e2mapper *.pyc __pycache__

install: all
	mkdir -p $(DESTDIR)$(bindir)
	install -s e2mapper $(DESTDIR)$(bindir)
	install -d $(DESTDIR)$(fmdir)
	install fiemap.py filemapper.py fmcli.py fmdb.py fmgui.py $(DESTDIR)$(fmdir)
	install filemapper.png filemapper.ui $(DESTDIR)$(fmdir)

dist:
	@if test "`git describe`" != "$(VERSION)" ; then \
		echo 'Update VERSION in the Makefile before running "make dist".' ; \
		exit 1 ; \
	fi
	git archive --format=tar --prefix=filemapper-$(VERSION)/ HEAD^{tree} | xz -9 > ../filemapper_$(VERSION).orig.tar.xz
