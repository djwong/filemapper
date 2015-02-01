CFLAGS=-Wall -O3 -g
LDLIBS=-lsqlite3 -lext2fs -lcom_err

all: e2mapper filemapper

clean:;
	rm -rf filemapper e2mapper
