CFLAGS=-Wall -O3 -g
VERSION=0.7.3

prefix = /usr
exec_prefix = ${prefix}
bindir = ${exec_prefix}/bin
libdir = ${exec_prefix}/lib
fmlibdir = ${libdir}/filemapper
mandir = ${exec_prefix}/man
man1dir = ${mandir}/man1
appdir = ${exec_prefix}/share/applications
XFSPROGS ?= Please_set_XFSPROGS_to_the_XFS_source_directory
DOSFSTOOLS ?= Please_set_DOSFSTOOLS_to_the_DOS_source_directory
DOSFS_HEADERS=$(DOSFSTOOLS)/src/fsck.fat.h $(DOSFSTOOLS)/src/file.h $(DOSFSTOOLS)/src/fat.h $(DOSFSTOOLS)/src/lfn.h $(DOSFSTOOLS)/src/charconv.h $(DOSFSTOOLS)/src/boot.h $(DOSFSTOOLS)/src/common.h $(DOSFSTOOLS)/src/io.h

ifeq ("$(notdir $(wildcard $(XFSPROGS)/libxfs/.libs/libxfs.a))", "libxfs.a")
xfsmapper=xfsmapper
endif
ifeq ("$(notdir $(wildcard $(DOSFSTOOLS)/fat.o))", "fat.o")
fatmapper=fatmapper
endif

progs=filemapper e2mapper ntfsmapper $(xfsmapper) $(fatmapper)
manpages=$(patsubst %,%.1.gz,$(progs))

all: $(progs) $(manpages) filemapper.desktop

%.1.gz: %.1
	gzip -9 < $< > $@

filemapper.c: filemapper.h

xfsmapper: filemapper.o xfsmapper.o $(XFSPROGS)/libxfs/.libs/libxfs.a
	$(CC) $(CFLAGS) -o $@ $^ $(XFSPROGS)/repair/btree.o -lsqlite3 -lpthread -luuid

xfsmapper.o: xfsmapper.c filemapper.h $(XFSPROGS)/include/libxfs.h $(XFSPROGS)/repair/btree.h $(XFSPROGS)/libxfs/libxfs_api_defs.h
	$(CC) $(CFLAGS) -o $@ -c $< -I$(XFSPROGS)/include/ -I$(XFSPROGS)/libxfs/ -I$(XFSPROGS)/

e2mapper: filemapper.o e2mapper.o
	$(CC) $(CFLAGS) -o $@ $^ -lsqlite3 -lcom_err -lext2fs

e2mapper.c: filemapper.h

ntfsmapper: filemapper.o ntfsmapper.o
	$(CC) $(CFLAGS) -o $@ $^ -lsqlite3 -lntfs-3g

ntfsmapper.c: filemapper.h

libfat.a: $(DOSFSTOOLS)/boot.o $(DOSFSTOOLS)/charconv.o $(DOSFSTOOLS)/common.o $(DOSFSTOOLS)/fat.o $(DOSFSTOOLS)/file.o $(DOSFSTOOLS)/io.o $(DOSFSTOOLS)/lfn.o
	$(AR) cr libfat.a $^

fatmapper: filemapper.o fatmapper.o fatcheck.o libfat.a
	$(CC) $(CFLAGS) -o $@ $^ -lsqlite3

fatcheck.c: $(DOSFSTOOLS)/src/check.c $(DOSFS_HEADERS)
	sed -e 's/static void add_file/void add_file/g' < $< > $@

fatcheck.o: fatcheck.c
	$(CC) $(CFLAGS) -o $@ -c $< -I$(DOSFSTOOLS)/src/

fatmapper.o: fatmapper.c filemapper.h $(DOSFS_HEADERS)
	$(CC) $(CFLAGS) -o $@ -c $< -I$(DOSFSTOOLS)/src/

clean:;
	rm -rf $(progs) $(manpages) xfsmapper xfsmapper.1.gz fatmapper fatmapper.1.gz libfat.a fatcheck.c *.pyc __pycache__ filemapper.desktop *.o

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
	-test -e fatmapper && install -s fatmapper $(DESTDIR)$(bindir)
	-test -e fatmapper && install -m 0644 fatmapper.1.gz $(DESTDIR)$(man1dir)
	-test -e xfsmapper && install -s xfsmapper $(DESTDIR)$(bindir)
	-test -e xfsmapper && install -m 0644 xfsmapper.1.gz $(DESTDIR)$(man1dir)

dist:
	@if test "`git describe`" != "$(VERSION)" ; then \
		echo 'Update VERSION in the Makefile before running "make dist".' ; \
		exit 1 ; \
	fi
	git archive --format=tar --prefix=filemapper-$(VERSION)/ HEAD^{tree} | xz -9 > ../filemapper_$(VERSION).orig.tar.xz
