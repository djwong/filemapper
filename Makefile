CFLAGS=-Wall -O3 -g
LDLIBS=-lsqlite3 -lext2fs -lcom_err
VERSION=0.5

all: e2mapper

clean:;
	rm -rf e2mapper *.pyc __pycache__

install: all
	mkdir -p $(DESTDIR)$(bindir)
	install e2mapper $(DESTDIR)$(bindir)

dist:
	@if test "`git describe`" != "$(VERSION)" ; then \
		echo 'Update VERSION in the Makefile before running "make dist".' ; \
		exit 1 ; \
	fi
	git archive --format=tar --prefix=filemapper-$(VERSION)/ HEAD^{tree} | xz -9 > filemapper_$(VERSION).orig.tar.xz
