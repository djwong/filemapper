LTO=
CFLAGS=-Wall -O3 -g $(LTO) -std=gnu11
LDFLAGS=-Wall -O3 -g $(LTO) -std=gnu11
LIB_CFLAGS=-Wall -O3 -g -std=gnu11 -shared -fPIC
VERSION=0.8.0

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
PYINCLUDE ?= -I/usr/include/python3.5m/
COMPDB_LIBS=-lz -llz4

ifeq ("$(notdir $(wildcard $(XFSPROGS)/libxfs/.libs/libxfs.a))", "libxfs.a")
xfsmapper=xfsmapper
endif
ifeq ("$(notdir $(wildcard $(DOSFSTOOLS)/fat.o))", "fat.o")
fatmapper=fatmapper
endif

progs=filemapper e2mapper ntfsmapper shrinkmapper $(xfsmapper) $(fatmapper)
libs=compdb.so
manpages=$(patsubst %,%.1.gz,$(progs))

all: $(progs) $(libs) $(manpages) filemapper.desktop

%.1.gz: %.1
	gzip -9 < $< > $@

compdb.so: compdb.c compdb.h filemapper.h
	$(CC) $(LIB_CFLAGS) -DPYMOD $(PYINCLUDE) -o $@ $< -lsqlite3 $(COMPDB_LIBS)

compdb.o: compdb.h filemapper.h

compress.o: compress.h

filemapper.o: filemapper.h

shrinkmapper: shrinkmapper.o compress.o compdb.o
	$(CC) $(CFLAGS) -o $@ $^ -lsqlite3 -llz4 -lz

shrinkmapper.o: compdb.h filemapper.h

xfsmapper: filemapper.o xfsmapper.o compress.o compdb.o $(XFSPROGS)/libxfs/.libs/libxfs.a
	$(CC) $(CFLAGS) -o $@ $^ $(XFSPROGS)/repair/btree.o -lsqlite3 -lpthread -luuid -lm $(COMPDB_LIBS)

xfsmapper.o: xfsmapper.c filemapper.h $(XFSPROGS)/include/libxfs.h $(XFSPROGS)/repair/btree.h $(XFSPROGS)/libxfs/libxfs_api_defs.h compdb.h
	$(CC) $(CFLAGS) -D_GNU_SOURCE -o $@ -c $< -I$(XFSPROGS)/include/ -I$(XFSPROGS)/libxfs/ -I$(XFSPROGS)/

e2mapper: filemapper.o e2mapper.o compress.o compdb.o
	$(CC) $(CFLAGS) -o $@ $^ -lsqlite3 -lcom_err -lext2fs -lm $(COMPDB_LIBS)

e2mapper.o: e2mapper.c filemapper.h compdb.h

ntfsmapper: filemapper.o ntfsmapper.o compress.o compdb.o
	$(CC) $(CFLAGS) -o $@ $^ -lsqlite3 -lntfs-3g -lm $(COMPDB_LIBS)

ntfsmapper.o: ntfsmapper.c filemapper.h compdb.h

libfat.a: $(DOSFSTOOLS)/boot.o $(DOSFSTOOLS)/charconv.o $(DOSFSTOOLS)/common.o $(DOSFSTOOLS)/fat.o $(DOSFSTOOLS)/file.o $(DOSFSTOOLS)/io.o $(DOSFSTOOLS)/lfn.o
	$(AR) cr libfat.a $^

fatmapper: filemapper.o fatmapper.o fatcheck.o libfat.a compress.o compdb.o
	$(CC) $(CFLAGS) -o $@ $^ -lsqlite3 -lm $(COMPDB_LIBS)

fatcheck.c: $(DOSFSTOOLS)/src/check.c $(DOSFS_HEADERS)
	sed -e 's/static void add_file/void add_file/g' < $< > $@

fatcheck.o: fatcheck.c
	$(CC) $(CFLAGS) -o $@ -c $< -I$(DOSFSTOOLS)/src/

fatmapper.o: fatmapper.c filemapper.h $(DOSFS_HEADERS) compdb.h
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
