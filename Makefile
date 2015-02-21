CFLAGS=-Wall -O3 -g
VERSION=0.6

prefix = /usr
exec_prefix = ${prefix}
bindir = ${exec_prefix}/bin
libdir = ${exec_prefix}/lib
fmlibdir = ${libdir}/filemapper
mandir = ${exec_prefix}/man
man1dir = ${mandir}/man1
appdir = ${exec_prefix}/share/applications

all: e2mapper filemapper e2mapper.1.gz filemapper.1.gz filemapper.desktop ntfsmapper ntfsmapper.1.gz

%.1.gz: %.1
	gzip -9 < $< > $@

e2mapper: filemapper.o e2mapper.o
	$(CC) -o $@ $^ -lsqlite3 -lcom_err -lext2fs

filemapper.c: filemapper.h
e2mapper.c: filemapper.h

ntfsmapper: filemapper.o ntfsmapper.o
	$(CC) -o $@ $^ -lsqlite3 -lntfs-3g

ntfsmapper.c: filemapper.h

fatmapper: filemapper.o fatmapper.o dosfs.o
	$(CC) -o $@ $^ -lsqlite3

fatmapper.c: dosfs.h filemapper.h

clean:;
	rm -rf e2mapper *.pyc __pycache__ filemapper e2mapper.1.gz filemapper.1.gz filemapper.desktop *.o ntfsmapper fatmapper

filemapper: filemapper.in
	sed -e "s|%libdir%|${fmlibdir}|g" < $< > $@

filemapper.desktop: filemapper.desktop.in
	sed -e "s|%libdir%|${fmlibdir}|g" < $< > $@

install: all
	install -d $(DESTDIR)$(bindir)
	install -s e2mapper ntfsmapper $(DESTDIR)$(bindir)
	install filemapper $(DESTDIR)$(bindir)
	install -d $(DESTDIR)$(fmlibdir)
	install -m 0644 fiemap.py filemapper.py fmcli.py fmdb.py fmgui.py $(DESTDIR)$(fmlibdir)
	install -m 0644 filemapper.png filemapper.ui $(DESTDIR)$(fmlibdir)
	install -d $(DESTDIR)$(man1dir)
	install -m 0644 e2mapper.1.gz filemapper.1.gz ntfsmapper.1.gz $(DESTDIR)$(man1dir)
	install -d $(DESTDIR)$(appdir)
	install -m 0644 filemapper.desktop $(DESTDIR)$(appdir)

dist:
	@if test "`git describe`" != "$(VERSION)" ; then \
		echo 'Update VERSION in the Makefile before running "make dist".' ; \
		exit 1 ; \
	fi
	git archive --format=tar --prefix=filemapper-$(VERSION)/ HEAD^{tree} | xz -9 > ../filemapper_$(VERSION).orig.tar.xz
