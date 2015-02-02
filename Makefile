CFLAGS=-Wall -O3 -g
LDLIBS=-lsqlite3 -lext2fs -lcom_err

all: e2mapper

clean:;
	rm -rf e2mapper *.pyc __pycache__
