/*
 * FileMapper declarations for C.
 * Copyright 2015 Darrick J. Wong.
 * Licensed under the GPLv2+.
 */
#ifndef FILEMAPPER_H_
#define FILEMAPPER_H_
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <limits.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdint.h>
#include <iconv.h>
#include <sqlite3.h>

#ifdef DEBUG
# define dbg_printf(f, a...)  do {printf(f, ## a); fflush(stdout); } while (0)
#else
# define dbg_printf(f, a...)
#endif

struct filemapper_t {
	sqlite3 *db;
	int db_err;
	const char *dirpath;
	iconv_t iconv;
};

struct overview_t {
	uint64_t files;
	uint64_t dirs;
	uint64_t mappings;
	uint64_t metadata;
	uint64_t xattrs;
	uint64_t symlinks;
};

#define INO_TYPE_FILE		0
#define INO_TYPE_DIR		1
#define INO_TYPE_METADATA	2
#define INO_TYPE_SYMLINK	3

#define EXT_TYPE_FILE		0
#define EXT_TYPE_DIR		1
#define EXT_TYPE_EXTENT		2
#define EXT_TYPE_METADATA	3
#define EXT_TYPE_XATTR		4
#define EXT_TYPE_SYMLINK	5

/* Extent flags.  Yes, these are the FIEMAP flags. */
#define EXTENT_LAST		0x00000001 /* Last extent in file. */
#define EXTENT_UNKNOWN		0x00000002 /* Data location unknown. */
#define EXTENT_DELALLOC		0x00000004 /* Location still pending.
					    * Sets EXTENT_UNKNOWN. */
#define EXTENT_ENCODED		0x00000008 /* Data can not be read
					    * while fs is unmounted */
#define EXTENT_DATA_ENCRYPTED	0x00000080 /* Data is encrypted by fs.
					    * Sets EXTENT_NO_BYPASS. */
#define EXTENT_NOT_ALIGNED	0x00000100 /* Extent offsets may not be
					    * block aligned. */
#define EXTENT_DATA_INLINE	0x00000200 /* Data mixed with metadata.
					    * Sets EXTENT_NOT_ALIGNED.*/
#define EXTENT_DATA_TAIL	0x00000400 /* Multiple files in block.
					    * Sets EXTENT_NOT_ALIGNED.*/
#define EXTENT_UNWRITTEN	0x00000800 /* Space allocated, but
					    * no data (i.e. zero). */
#define EXTENT_MERGED		0x00001000 /* File does not natively
					    * support extents. Result
					    * merged for efficiency. */
#define EXTENT_SHARED		0x00002000 /* Space shared with other
					    * files. */

#define MAX_EXTENT_LENGTH	(1ULL << 60)

/* Convert a directory pathname */
int icvt(struct filemapper_t *wf, char *in, size_t inl, char *out, size_t outl);

/* Run a bunch of queries */
void run_batch_query(struct filemapper_t *wf, const char *sql);

/* Insert an inode record into the inode and path tables */
void insert_inode(struct filemapper_t *wf, int64_t ino, int type,
		  const char *path, time_t *atime, time_t *crtime,
		  time_t *ctime, time_t *mtime, int64_t *size);

/* Insert a directory entry into the database. */
void insert_dentry(struct filemapper_t *wf, int64_t dir_ino,
		   const char *name, int64_t ino);

/* Insert an extent into the database. */
void insert_extent(struct filemapper_t *wf, int64_t ino, uint64_t physical,
		   uint64_t *logical, uint64_t length, int flags, int type);

void inject_metadata(struct filemapper_t *wf, int64_t parent_ino,
		     const char *path, int64_t ino, const char *name,
		     int type);

/* Store fs statistics in the database */
void collect_fs_stats(struct filemapper_t *wf, char *fs_name,
		     uint32_t blocksize, uint32_t fragsize,
		     uint64_t total_bytes, uint64_t free_bytes,
		     uint64_t total_inodes, uint64_t free_inodes,
		     unsigned int max_name_len, const char *fstype);

/* Mark the database as complete. */
void finalize_fs_stats(struct filemapper_t *wf, char *fs_name);

/* Generate an overview cache. */
void cache_overview(struct filemapper_t *wf, uint64_t length);

/* Prepare database to receive new data. */
void prepare_db(struct filemapper_t *wf);

/* Index database. */
void index_db(struct filemapper_t *wf);

/* Calculate inode statistics */
void calc_inode_stats(struct filemapper_t *wf);

/* Simple bitmap functions */
int fm_test_bit(const uint8_t *bmap, const uint64_t bit);
void fm_set_bit(uint8_t *bmap, const uint64_t bit, const int new_value);

#endif /* ifdef FM_H_ */
