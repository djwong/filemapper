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
#include "filemapper.h"
#include "compdb.h"
#include "compress.h"

/* SQLite engine stuff */

#define offsetof(TYPE, MEMBER)      ((size_t)&((TYPE *)0)->MEMBER)
#define container_of(ptr, type, member) ({                      \
        const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
	        (type *)( (char *)__mptr - offsetof(type,member) );})

struct compdb_vfs {
	struct sqlite3_vfs		vfs;
	struct sqlite3_vfs		*oldvfs;
	struct compressor_type		*compressor;
	char				compdb_file_header[16];

};

struct compdb_file {
	struct sqlite3_io_methods	methods;
	struct compdb_vfs		*cvfs;
	int				(*old_read)(sqlite3_file*, void*,
						    int, sqlite3_int64);
	int				(*old_write)(sqlite3_file*, const void*,
						    int, sqlite3_int64);
	unsigned int			freestart;
	unsigned int			freelen;
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
	is_compr = !memcmp(super->magic, ff->cvfs->compdb_file_header,
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
	ff->freestart = ntohl(super->freelist_start);
	ff->freelen = ntohl(super->freelist_pages);

	dbg_printf("%s(%d) pagesz %d freepg %u:%u\n", __func__, __LINE__,
			ff->pagesize, ff->freestart, ff->freelen);

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
	unsigned int			page;
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
	page = iOfst / ff->pagesize;
	if (ff->db_type == DB_REGULAR ||
	    (page >= ff->freestart && page < ff->freestart + ff->freelen) ||
	    memcmp(bhead->magic, COMPDB_BLOCK_MAGIC, sizeof(bhead->magic))) {
		dbg_printf("%s(%d) len=%d off=%llu\n", __func__, __LINE__,
				iAmt, iOfst);
		return SQLITE_OK;
	}

	/* Header sane? */
	assert(ff->db_type == DB_COMPRESSED);
	clen = ntohs(bhead->len);
	if (clen > ff->pagesize - sizeof(*bhead) ||
	    (unsigned long long)ntohl(bhead->offset) * ff->pagesize != iOfst) {
		dbg_printf("%s(%d) header corrupt clen=%d boff=%u iofst=%lld\n",
				__func__, __LINE__, clen, ntohl(bhead->offset),
				iOfst);
		return SQLITE_CORRUPT;
	}

	/* Decompress and return. */
	buf = malloc(ff->pagesize);
	if (!buf)
		return SQLITE_NOMEM;

	ret = ff->cvfs->compressor->decompress(ptr + sizeof(*bhead), buf, clen,
			ff->pagesize);
	if (ret < 0) {
		dbg_printf("%s(%d) decompress failed\n", __func__, __LINE__);
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
	unsigned int			page;
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
	page = iOfst / ff->pagesize;
	if (ff->db_type == DB_REGULAR ||
	    (page >= ff->freestart && page < ff->freestart + ff->freelen))
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
	memcpy(bhead->magic, COMPDB_BLOCK_MAGIC, sizeof(bhead->magic));
	bhead->len = htons(ret);
	bhead->offset = htonl(iOfst / ff->pagesize);

	/*
	 * Truncate to where the end of the compressed block should be
	 * so that XFS won't do speculative preallocation.
	 */
	ret = file->pMethods->xFileSize(file, &isize);
	if (ret)
		return ret;
	if (iOfst + clen > isize) {
		ret = file->pMethods->xTruncate(file, iOfst + clen);
		if (ret)
			return ret;
	}

	/* Write compressed data. */
	dbg_printf("%s(%d) len=%d off=%llu clen=%d\n", __func__, __LINE__,
			iAmt, iOfst, clen);
	ret = ff->old_write(file, buf, clen, iOfst);
	free(buf);
	if (ret)
		goto no_compr;

	/*
	 * Truncate to the end of the block to avoid short reads.
	 */
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
	return ff->old_write(file, ff->cvfs->compdb_file_header,
			sizeof(ff->cvfs->compdb_file_header), 0);
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
compdb_register(
	const char		*under_vfs,
	const char		*vfs_name,
	const char		*compressor)
{
	sqlite3_vfs		*vfs;
	struct compdb_vfs	*newvfs;
	struct compressor_type	*cengine;
	int			ret;

	/* Find the underlying VFS. */
	vfs = sqlite3_vfs_find(under_vfs);
	if (!vfs)
		return ENOENT;

	/* Already registered? */
	if (sqlite3_vfs_find(vfs_name))
		return EEXIST;

	/* Find our compressor */
	cengine = compdb_find_compressor(compressor);
	if (!cengine)
		return ENOENT;

	dbg_printf("%s: Stacking %s ver %d compressor %s\n", vfs_name,
			vfs->zName, vfs->iVersion, cengine->name);

	/* Allocate new VFS structure. */
	newvfs = malloc(sizeof(*newvfs));
	if (!newvfs)
		return ENOMEM;

	newvfs->oldvfs = vfs;
	newvfs->compressor = cengine;
	snprintf(newvfs->compdb_file_header, sizeof(newvfs->compdb_file_header),
			COMPDB_FILE_TEMPLATE, cengine->name);
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
