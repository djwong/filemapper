/*
 * Compress SQLite databases.
 * Copyright 2016 Darrick J. Wong.
 * Licensed under the GPLv2.
 */
#include <stdlib.h>
#include <stdio.h>
#include <sqlite3.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <arpa/inet.h>
#include <lz4.h>
#include <lz4hc.h>
#include <zlib.h>
#ifdef PYMOD
# include <Python.h>
#endif

#define DEBUG
#ifdef DEBUG
# define dbg_printf		printf
#else
# define dbg_printf(...)	{}
#endif

/* gzip deflate compression */
static inline int
GZIP_compress(
	const char		*source,
	char			*dest,
	int			sourceSize,
	int			maxDestSize)
{
	z_stream		strm = {0};
	char			*endp;
	int			ret;

	ret = deflateInit(&strm, 5);
	if (ret)
		return 0;

	strm.avail_in = sourceSize;
	strm.next_in = (unsigned char *)source;
	strm.next_out = (unsigned char *)dest;
	endp = dest + maxDestSize;
	do {
		strm.avail_out = endp - (char *)strm.next_out;
		ret = deflate(&strm, Z_FINISH);
		if (ret != Z_STREAM_END && ret != Z_OK) {
			deflateEnd(&strm);
			return 0;
		}
		strm.next_out = (unsigned char *)endp - strm.avail_out;
	} while (strm.avail_in && (char *)strm.next_out <= endp);
	deflateEnd(&strm);

	return (char *)strm.next_out - dest;
}

/* gzip inflate */
static inline int
GZIP_decompress(
	const char		*source,
	char			*dest,
	int			compressedSize,
	int			maxDecompressedSize)
{
	z_stream		strm = {0};
	char			*endp;
	int			ret;

	ret = inflateInit(&strm);
	if (ret != Z_OK)
		return -1;

	strm.avail_in = compressedSize;
	strm.next_in = (unsigned char *)source;
	strm.next_out = (unsigned char *)dest;
	endp = dest + maxDecompressedSize;
	do {
		strm.avail_out = endp - (char *)strm.next_out;
		ret = inflate(&strm, Z_NO_FLUSH);
		if (ret != Z_STREAM_END && ret != Z_OK) {
			inflateEnd(&strm);
			return 0;
		}
		strm.next_out = (unsigned char *)endp - strm.avail_out;
	} while (strm.avail_in && (char *)strm.next_out <= endp);
	inflateEnd(&strm);

	if (strm.avail_in)
		return -1;

	return (char *)strm.next_out - dest;
}

/* LZ4 HC mode */
static inline int
LZ4HC_compress(
	const char		*source,
	char			*dest,
	int			sourceSize,
	int			maxDestSize)
{
	return LZ4_compressHC2_limitedOutput(source, dest, sourceSize,
			maxDestSize, 8);
}

struct compressor_type {
	const char		*name;
	int			(*compress)(const char *, char *, int, int);
	int			(*decompress)(const char *, char *, int, int);
};

static struct compressor_type compressors[] = {
	{"GZIP", GZIP_compress,		GZIP_decompress},
	{"LZ4D", LZ4_compress_default,	LZ4_decompress_safe},
	{"LZ4H", LZ4HC_compress,	LZ4_decompress_safe},
	{NULL, NULL, NULL},
};

/* SQLite engine stuff */

#define container_of(ptr, type, member) ({                      \
        const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
	        (type *)( (char *)__mptr - offsetof(type,member) );})

struct compdb_vfs {
	struct sqlite3_vfs		vfs;
	struct sqlite3_vfs		*oldvfs;
	struct compressor_type		*compressor;

};

enum compdb_type {
	DB_UNKNOWN,
	DB_REGULAR,
	DB_COMPRESSED,
};

struct compdb_file {
	struct sqlite3_io_methods	methods;
	struct compdb_vfs		*cvfs;
	int				(*old_read)(sqlite3_file*, void*,
						    int, sqlite3_int64);
	int				(*old_write)(sqlite3_file*, const void*,
						    int, sqlite3_int64);
	unsigned long long		data_start;
	int				pagesize;
	enum compdb_type		db_type;
};

/* Convert a sqlite file into a compdb file */
static inline struct compdb_file *
COMPDB_F(
	sqlite3_file		*file)
{
	return container_of(file->pMethods, struct compdb_file, methods);
}

/*
 * Put this ahead of every compressed page.  btree pages can't have
 * 0xDA as the first byte.
 */
static const uint8_t FAKEVFS_BLOCK_MAGIC[] = {0xDA, 0xAD};
struct compdb_block_head {
	uint8_t			magic[2];
	uint16_t		len;		/* compressed length */
	uint32_t		offset;		/* page number */
};

/* SQLite superblock format. */
#define SQLITE_FILE_HEADER 	"SQLite format 3"
#define COMPDB_FILE_TEMPLATE	"SQLite %s v.3"
static char COMPDB_FILE_HEADER[16];
struct sqlite3_super {
	uint8_t			magic[16];
	uint16_t		pagesize;
	uint8_t			write_format;
	uint8_t			read_format;
	uint8_t			page_reserve;
	uint8_t			max_fraction;
	uint8_t			min_fraction;
	uint8_t			leaf_payload;
	uint32_t		change_counter;
	uint32_t		nr_pages;
	uint32_t		freelist_start;
	uint32_t		freelist_pages;
	uint32_t		schema_coookie;
	uint32_t		schema_format;
	uint32_t		page_cache_size;
	uint32_t		highest_btree_root;
	uint32_t		text_encoding;
	uint32_t		user_version;
	uint32_t		vacuum_mode;
	uint32_t		app_id;
	uint8_t			reserved[20];
	uint32_t		version_valid_for;
	uint32_t		sqlite_version_number;
};

/* Figure out database parameters. */
static int
compdb_sniff(
	struct compdb_file		*ff,
	const struct sqlite3_super	*super,
	int				is_write)
{
	int				is_sqlite;
	int				is_compr;

	dbg_printf("%s(%d)\n", __func__, __LINE__);
	assert(ff->db_type == DB_UNKNOWN);

	/* Is this really a database? */
	is_sqlite = !memcmp(super->magic, SQLITE_FILE_HEADER,
			sizeof(super->magic));
	is_compr = !memcmp(super->magic, COMPDB_FILE_HEADER,
			sizeof(super->magic));
	if ((!is_sqlite && !is_compr) ||
	    super->max_fraction != 64 || super->min_fraction != 32 ||
	    super->leaf_payload != 32 || ntohl(super->schema_format) > 4)
		return SQLITE_NOTADB;

	/* Is this a regular uncompressed database? */
	if (is_sqlite && !is_write) {
		ff->db_type = DB_REGULAR;
		ff->methods.xRead = ff->old_read;
		ff->methods.xWrite = ff->old_write;
		return SQLITE_OK;
	}

	/* Collect some stats. */
	ff->db_type = DB_COMPRESSED;
	ff->methods.iVersion = 1;
	ff->pagesize = ntohs(super->pagesize);
	if (ff->pagesize == 1)
		ff->pagesize = 65536;
	ff->data_start = (ntohl(super->freelist_start) + 1 +
			  ntohl(super->freelist_pages)) * ff->pagesize;

	dbg_printf("%s(%d) pagesz %d dataoff %llu\n", __func__, __LINE__,
			ff->pagesize, ff->data_start);

	return SQLITE_OK;
}

/* Do some sort of read io. */
static int
compdb_read(
	sqlite3_file			*file,
	void				*ptr,
	int				iAmt,
	sqlite3_int64			iOfst)
{
	struct compdb_file		*ff;
	struct compdb_block_head	*bhead;
	char				*buf;
	int				clen;
	int				ret;

	ff = COMPDB_F(file);
	assert(iOfst == 0 || ff->db_type != DB_UNKNOWN);

	ret = ff->old_read(file, ptr, iAmt, iOfst);
	if (ff->db_type == DB_COMPRESSED && iOfst == 0)
		memcpy(ptr, SQLITE_FILE_HEADER, sizeof(SQLITE_FILE_HEADER));
	if (ret)
		return ret;

	/* We don't compress non-btree pages. */
	bhead = ptr;
	if (ff->db_type == DB_REGULAR || iOfst + iAmt <= ff->data_start ||
	    memcmp(bhead->magic, FAKEVFS_BLOCK_MAGIC, sizeof(bhead->magic))) {
		dbg_printf("%s(%d) len=%d off=%llu\n", __func__, __LINE__,
				iAmt, iOfst);
		return SQLITE_OK;
	}

	/* Header sane? */
	assert(ff->db_type == DB_COMPRESSED);
	clen = ntohs(bhead->len);
	if (clen > ff->pagesize - sizeof(*bhead) ||
	    ntohl(bhead->offset) * ff->pagesize != iOfst)
		return SQLITE_CORRUPT;

	/* Decompress and return. */
	buf = malloc(ff->pagesize);
	if (!buf)
		return SQLITE_NOMEM;

	ret = ff->cvfs->compressor->decompress(ptr + sizeof(*bhead), buf, clen,
			ff->pagesize);
	if (ret < 0) {
		free(buf);
		return SQLITE_CORRUPT;
	}

	assert(ret <= ff->pagesize);
	memcpy(ptr, buf, ret);
	memset(ptr + ret, 0, ff->pagesize - ret);
	free(buf);

	dbg_printf("%s(%d) len=%d off=%llu clen=%d\n", __func__, __LINE__,
				ret, iOfst, clen);
	return 0;
}

/* Do some sort of write. */
static int
compdb_write(
	sqlite3_file			*file,
	const void			*ptr,
	int				iAmt,
	sqlite3_int64			iOfst)
{
	struct compdb_file		*ff;
	struct compdb_block_head	*bhead;
	char				*buf;
	sqlite3_int64			isize;
	int				clen;
	int				ret;

	ff = COMPDB_F(file);

	/* If we don't know db geometry, let's try to pull them in here. */
	if (ff->db_type == DB_UNKNOWN) {
		assert(iOfst == 0);
		ret = compdb_sniff(ff, ptr, 1);
		if (ret)
			return ret;
		assert(ff->db_type != DB_UNKNOWN);
	}

	/* We don't compress non-btree pages. */
	if (ff->db_type == DB_REGULAR || iOfst + iAmt <= ff->data_start)
		goto no_compr;

	/* Try to compress data. */
	buf = malloc(ff->pagesize);
	if (!buf)
		return SQLITE_NOMEM;

	ret = ff->cvfs->compressor->compress(ptr, buf + sizeof(*bhead), iAmt,
			ff->pagesize - sizeof(*bhead));
	if (ret == 0) {
		free(buf);
		goto no_compr;
	}
	clen = ret + sizeof(*bhead);

	/* Attach compression header. */
	bhead = (struct compdb_block_head *)buf;
	memcpy(bhead->magic, FAKEVFS_BLOCK_MAGIC, sizeof(bhead->magic));
	bhead->len = htons(ret);
	bhead->offset = htonl(iOfst / ff->pagesize);

	/* Write compressed data. */
	dbg_printf("%s(%d) len=%d off=%llu clen=%d\n", __func__, __LINE__,
			iAmt, iOfst, clen);
	ret = ff->old_write(file, buf, clen, iOfst);
	free(buf);
	if (ret)
		goto no_compr;

	/* "Truncate" to where the end of the block should be? */
	ret = file->pMethods->xFileSize(file, &isize);
	if (ret)
		return ret;
	if (iOfst + iAmt > isize) {
		ret = file->pMethods->xTruncate(file, iOfst + iAmt);
		if (ret)
			return ret;
	}

	return ret;

no_compr:
	/* Compression fails, just write it straight. */
	dbg_printf("%s(%d) len=%d off=%llu\n", __func__, __LINE__,
			iAmt, iOfst);
	ret = ff->old_write(file, ptr, iAmt, iOfst);
	if (ret || iOfst != 0 || ff->db_type == DB_REGULAR)
		return ret;

	/* Make sure we write out the compressed magic. */
	return ff->old_write(file, COMPDB_FILE_HEADER,
			sizeof(COMPDB_FILE_HEADER), 0);
}

/*
 * Open a file.  We only care about main db files; everything else
 * can just pass through to the underlying VFS.
 */
static int
compdb_open(
	sqlite3_vfs		*vfs,
	const char		*zName,
	sqlite3_file		*file,
	int			flags,
	int			*pOutFlags)
{
	struct sqlite3_super	super;
	struct compdb_vfs	*cvfs;
	struct compdb_file	*ff;
	int			ret;

	cvfs = container_of(vfs, struct compdb_vfs, vfs);
	dbg_printf("%s(%d): zName %s flags %xh\n", __func__, __LINE__,
			zName, flags);

	/* Open the underlying file. */
	ret = cvfs->oldvfs->xOpen(cvfs->oldvfs, zName, file, flags, pOutFlags);
	if (ret || !(flags & SQLITE_OPEN_MAIN_DB))
		return ret;

	/* Shim ourselves in. */
	ff = (struct compdb_file *)(((char *)file) + cvfs->oldvfs->szOsFile);
	ff->cvfs = cvfs;
	ff->methods = *(file->pMethods);
	ff->methods.xRead = compdb_read;
	ff->methods.xWrite = compdb_write;
	ff->old_read = file->pMethods->xRead;
	ff->old_write = file->pMethods->xWrite;
	ff->db_type = DB_UNKNOWN;
	file->pMethods = &ff->methods;

	/* Read the header so we know a few things. */
	ret = ff->old_read(file, &super, sizeof(super), 0);
	if (ret == SQLITE_IOERR_SHORT_READ) {
		/*
		 * Empty db, so disable mmap (there's no way to disable
		 * it after the fact) and let's see what gets written out.
		 */
		ff->methods.iVersion = 1;
		return SQLITE_OK;
	} else if (ret) {
		ff->methods.xClose(file);
		return ret;
	}

	ret = compdb_sniff(ff, &super, 0);
	if (ret) {
		ff->methods.xClose(file);
		return ret;
	}

	return SQLITE_OK;
}

/* Create compdb as a compression shim atop some other VFS. */
int
compdb_init(
	const char		*under_vfs,
	const char		*vfs_name,
	const char		*compressor)
{
	sqlite3_vfs		*vfs;
	struct compdb_vfs	*newvfs;
	struct compressor_type	*cengine;
	struct compressor_type	*ct;
	int			ret;

	/* Find the underlying VFS. */
	vfs = sqlite3_vfs_find(under_vfs);
	if (!vfs)
		return ENOENT;

	/* Already registered? */
	if (sqlite3_vfs_find(vfs_name))
		return EEXIST;

	/* Find our compressor */
	if (compressor) {
		cengine = NULL;
		for (ct = compressors; ct->name; ct++)
			if (strcmp(compressor, ct->name) == 0)
				cengine = ct;
		if (!cengine)
			return ENOENT;
	} else
		cengine = &compressors[0];
	snprintf(COMPDB_FILE_HEADER, sizeof(COMPDB_FILE_HEADER),
			COMPDB_FILE_TEMPLATE, cengine->name);

	dbg_printf("%s: Stacking %s ver %d compressor %s\n", vfs_name,
			vfs->zName, vfs->iVersion, cengine->name);

	/* Allocate new VFS structure. */
	newvfs = malloc(sizeof(*newvfs));
	if (!newvfs)
		return ENOMEM;

	newvfs->oldvfs = vfs;
	newvfs->compressor = cengine;
	newvfs->vfs = *vfs;
	newvfs->vfs.zName = strdup(vfs_name);
	newvfs->vfs.xOpen = compdb_open;
	newvfs->vfs.pNext = NULL;
	newvfs->vfs.szOsFile += sizeof(struct compdb_file);

	ret = sqlite3_vfs_register(&newvfs->vfs, under_vfs ? 0 : 1);
	if (ret)
		return EIO;

	return 0;
}

/* Make us a python module! */

#ifdef PYMOD

#define MOD_NAME		"compdb"

/* Register a compressed-VFS */
static PyObject *
compdb_register(
	PyObject	*self,
	PyObject	*args)
{
	char		*under;
	char		*name;
	char		*compr;
	int 		err;

	if (!PyArg_ParseTuple(args, "zsz", &under, &name, &compr))
		return NULL;

	err = compdb_init(under, name, compr);
        if (err)
		PyErr_SetString(PyExc_RuntimeError, strerror(err));

	return Py_BuildValue("i", err);
}

/* Unregister a VFS */
static PyObject *
compdb_unregister(
	PyObject	*self,
	PyObject	*args)
{
	sqlite3_vfs	*vfs;
	char		*name;
	int 		err;

	if (!PyArg_ParseTuple(args, "z", &name))
		return NULL;

	vfs = sqlite3_vfs_find(name);
	if (!vfs) {
		err = ENOENT;
		PyErr_SetString(PyExc_RuntimeError, strerror(err));
		goto out;
	}

	err = sqlite3_vfs_unregister(vfs);
	if (err != SQLITE_OK) {
		err = EIO;
		PyErr_SetString(PyExc_RuntimeError, strerror(err));
	}

out:
	return Py_BuildValue("i", err);
}

static PyMethodDef compdb_methods[] = {
	{"register", compdb_register, METH_VARARGS, NULL},
	{"unregister", compdb_unregister, METH_VARARGS, NULL},
	{NULL, NULL, 0, NULL}
};

static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        MOD_NAME,
        NULL,
        0,
        compdb_methods,
        NULL,
        NULL,
        NULL,
        NULL
};

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC
PyInit_compdb(void)
{
	PyObject	*m;

	m = PyModule_Create(&moduledef);
	if (!m)
		return NULL;

	return m;
}
#else
PyMODINIT_FUNC
init_compdb(void)
{
	PyObject	*m;

	m = Py_InitModule(MOD_NAME, compdb_methods);
	if (!m)
		return;

	return;
}
#endif /* PY_MAJOR_VERSION */
#endif /* PYMOD */
