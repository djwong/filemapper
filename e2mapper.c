/*
 * Generate filemapper databases from ext* filesystems.
 * Copyright 2015 Darrick J. Wong.
 * Licensed under the GPLv2.
 */
#include <stdio.h>
#include <time.h>
#include <limits.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <sqlite3.h>
#include <ext2fs/ext2fs.h>

#undef DEBUG

#ifdef DEBUG
# define dbg_printf(f, a...)  do {printf(f, ## a); fflush(stdout); } while (0)
#else
# define dbg_printf(f, a...)
#endif

static char *dbschema = "PRAGMA cache_size = 65536;\
PRAGMA page_size = 4096;\
DROP VIEW IF EXISTS dentry_t;\
DROP VIEW IF EXISTS path_extent_v;\
DROP TABLE IF EXISTS dentry_t;\
DROP TABLE IF EXISTS extent_t;\
DROP TABLE IF EXISTS inode_t;\
DROP TABLE IF EXISTS path_t;\
DROP TABLE IF EXISTS dir_t;\
DROP TABLE IF EXISTS fs_t;\
CREATE TABLE fs_t(path TEXT PRIMARY KEY NOT NULL, block_size INTEGER NOT NULL, frag_size INTEGER NOT NULL, total_bytes INTEGER NOT NULL, free_bytes INTEGER NOT NULL, avail_bytes INTEGER NOT NULL, total_inodes INTEGER NOT NULL, free_inodes INTEGER NOT NULL, avail_inodes INTEGER NOT NULL, max_len INTEGER NOT NULL, timestamp TEXT NOT NULL, finished INTEGER NOT NULL, path_separator TEXT NOT NULL);\
CREATE TABLE inode_t(ino INTEGER PRIMARY KEY UNIQUE NOT NULL, type TEXT NOT NULL CHECK (type in ('f', 'd', 'm', 's')));\
CREATE TABLE dir_t(dir_ino INTEGER REFERENCES inode_t(ino) NOT NULL, name TEXT NOT NULL, name_ino INTEGER REFERENCES inode_t(ino) NOT NULL);\
CREATE TABLE path_t(path TEXT PRIMARY KEY UNIQUE NOT NULL, ino INTEGER REFERENCES inode_t(ino));\
CREATE TABLE extent_t(ino INTEGER REFERENCES inode_t(ino), p_off INTEGER NOT NULL, l_off INTEGER NOT NULL, flags INTEGER NOT NULL, length INTEGER NOT NULL, type TEXT NOT NULL CHECK (type in ('f', 'd', 'e', 'm', 'x', 's')), p_end INTEGER NOT NULL);\
CREATE VIEW path_extent_v AS SELECT path_t.path, extent_t.p_off, extent_t.l_off, extent_t.length, extent_t.flags, extent_t.type, extent_t.p_end, extent_t.ino FROM extent_t, path_t WHERE extent_t.ino = path_t.ino;\
CREATE VIEW dentry_t AS SELECT dir_t.dir_ino, dir_t.name, dir_t.name_ino, inode_t.type FROM dir_t, inode_t WHERE dir_t.name_ino = inode_t.ino;";

static char *dbindex = "CREATE INDEX inode_i ON inode_t(ino);\
CREATE INDEX path_ino_i ON path_t(ino);\
CREATE INDEX path_path_i ON path_t(path);\
CREATE INDEX dir_ino_i ON dir_t(dir_ino);\
CREATE INDEX extent_poff_i ON extent_t(p_off, p_end);\
CREATE INDEX extent_loff_i ON extent_t(l_off, length);\
CREATE INDEX extent_ino_i ON extent_t(ino);";

struct walk_fs_t {
	sqlite3 *db;
	ext2_filsys fs;
	errcode_t err;
	int db_err;
	const char *dirpath;
	ext2fs_inode_bitmap iseen;
};

static char *type_codes[] = {
	[EXT2_FT_DIR] = "d",
	[EXT2_FT_REG_FILE] = "f",
	[EXT2_FT_SYMLINK] = "s",
};

#ifndef EXT4_INLINE_DATA_FL
# define EXT4_INLINE_DATA_FL	0x10000000 /* Inode has inline data */
#endif

#define EXT2_XT_METADATA	(EXT2_FT_MAX + 16)
#define EXT2_XT_EXTENT		(EXT2_FT_MAX + 17)
#define EXT2_XT_XATTR		(EXT2_FT_MAX + 18)
static char *extent_codes[] = {
	[EXT2_FT_REG_FILE] = "f",
	[EXT2_FT_DIR] = "d",
	[EXT2_XT_EXTENT] = "e",
	[EXT2_XT_METADATA] = "m",
	[EXT2_XT_XATTR] = "x",
	[EXT2_FT_SYMLINK] = "s",
};

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

#define INO_METADATA_DIR	(-1)
#define STR_METADATA_DIR	"$metadata"
#define INO_SB_FILE		(-2)
#define STR_SB_FILE		"superblocks"
#define INO_GDT_FILE		(-3)
#define STR_GDT_FILE		"group_descriptors"
#define INO_BBITMAP_FILE	(-4)
#define STR_BBITMAP_FILE	"block_bitmaps"
#define INO_IBITMAP_FILE	(-5)
#define STR_IBITMAP_FILE	"inode_bitmaps"
#define INO_ITABLE_FILE		(-6)
#define STR_ITABLE_FILE		"inodes"
/* This must come last */
#define INO_GROUPS_DIR		(-7)
#define STR_GROUPS_DIR		"groups"

/* Run a bunch of queries */
static int run_batch_query(sqlite3 *db, const char *sql)
{
	sqlite3_stmt *stmt = NULL;
	const char *tail, *p;
	int err, err2 = 0;

	p = sql;
	err = sqlite3_prepare_v2(db, p, -1, &stmt, &tail);
	while (err == 0 && stmt) {
		err = sqlite3_step(stmt);
		if (err != SQLITE_DONE)
			break;
		err = sqlite3_finalize(stmt);
		stmt = NULL;
		if (err)
			break;
		p = tail;
		err = sqlite3_prepare_v2(db, p, -1, &stmt, &tail);
	}
	if (stmt)
		err2 = sqlite3_finalize(stmt);
	if (!err && err2)
		err = err2;

	if (err)
		dbg_printf("err=%d p=%s\n", err, p);

	return err;
}

/* Store fs statistics in the database */
static int collect_fs_stats(sqlite3 *db, ext2_filsys fs)
{
	const char *sql = "INSERT INTO fs_t VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?);";
	char stime[256];
	sqlite3_uint64 x;
	sqlite3_stmt *stmt;
	time_t t;
	struct tm *tmp;
	int err, err2, col = 1;

	err = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
	if (err)
		return err;
	err = sqlite3_bind_text(stmt, col++, fs->device_name, -1,
				SQLITE_STATIC);
	if (err)
		goto out;
	err = sqlite3_bind_int(stmt, col++, fs->blocksize);
	if (err)
		goto out;
	err = sqlite3_bind_int(stmt, col++, fs->fragsize);
	if (err)
		goto out;
	x = ext2fs_blocks_count(fs->super) * fs->blocksize;
	err = sqlite3_bind_int64(stmt, col++, x);
	if (err)
		goto out;
	x = ext2fs_free_blocks_count(fs->super) * fs->blocksize;
	err = sqlite3_bind_int64(stmt, col++, x);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, x);
	if (err)
		goto out;
	x = fs->super->s_inodes_count;
	err = sqlite3_bind_int64(stmt, col++, x);
	if (err)
		goto out;
	x = fs->super->s_free_inodes_count;
	err = sqlite3_bind_int64(stmt, col++, x);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, x);
	if (err)
		goto out;
	err = sqlite3_bind_int(stmt, col++, EXT2_NAME_LEN);
	if (err)
		goto out;
	t = time(NULL);
	tmp = gmtime(&t);
	/* 2015-01-23 01:14:00.792473 */
	strftime(stime, 256, "%F %T", tmp);
	err = sqlite3_bind_text(stmt, col++, stime, -1, SQLITE_STATIC);
	if (err)
		goto out;
	err = sqlite3_bind_text(stmt, col++, "/", -1, SQLITE_STATIC);
	if (err)
		goto out;
	err = sqlite3_step(stmt);
	if (err && err != SQLITE_DONE)
		goto out;
	err = 0;
out:
	err2 = sqlite3_finalize(stmt);
	if (!err && err2)
		err = err2;
	return err;
}

/* Mark the database as complete. */
static int finalize_fs_stats(sqlite3 *db, ext2_filsys fs)
{
	const char *sql = "UPDATE fs_t SET finished = 1 WHERE path = ?;";
	sqlite3_stmt *stmt;
	int err, err2, col = 1;

	err = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
	if (err)
		return err;
	err = sqlite3_bind_text(stmt, col++, fs->device_name, -1,
				SQLITE_STATIC);
	if (err)
		goto out;
	err = sqlite3_step(stmt);
	if (err && err != SQLITE_DONE)
		goto out;
	err = 0;
out:
	err2 = sqlite3_finalize(stmt);
	if (!err && err2)
		err = err2;
	return err;
}

/* Insert an inode record into the inode and path tables */
static int insert_inode(struct walk_fs_t *wf, int64_t ino, int type,
			const char *path)
{
	const char *ino_sql = "INSERT OR REPLACE INTO inode_t VALUES(?, ?);";
	const char *path_sql = "INSERT INTO path_t VALUES(?, ?);";
	sqlite3_stmt *stmt = NULL;
	int err, err2, col = 1;

	dbg_printf("%s: ino=%"PRId64" type=%s path=%s\n", __func__, ino,
		   type_codes[type], path);

	/* Update the inode table */
	err = sqlite3_prepare_v2(wf->db, ino_sql, -1, &stmt, NULL);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, ino);
	if (err)
		goto out;
	err = sqlite3_bind_text(stmt, col++, type_codes[type], -1,
				SQLITE_STATIC);
	if (err)
		goto out;
	err = sqlite3_step(stmt);
	if (err && err != SQLITE_DONE)
		goto out;
	err = sqlite3_finalize(stmt);
	if (err)
		goto out;
	stmt = NULL;

	/* Update the path table */
	col = 1;
	err = sqlite3_prepare_v2(wf->db, path_sql, -1, &stmt, NULL);
	if (err)
		goto out;
	err = sqlite3_bind_text(stmt, col++, path, -1, SQLITE_STATIC);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, ino);
	if (err)
		goto out;
	err = sqlite3_step(stmt);
	if (err && err != SQLITE_DONE)
		goto out;
	err = 0;
out:
	err2 = sqlite3_finalize(stmt);
	if (!err && err2)
		err = err2;
	return err;
}

/* Insert a directory entry into the database. */
static int insert_dentry(struct walk_fs_t *wf, int64_t dir_ino,
			 const char *name, int64_t ino)
{
	const char *dentry_sql = "INSERT INTO dir_t VALUES(?, ?, ?);";
	sqlite3_stmt *stmt = NULL;
	int err, err2, col = 1;

	dbg_printf("%s: dir=%"PRId64" name=%s ino=%"PRId64"\n", __func__,
		   dir_ino, name, ino);

	/* Update the dentry table */
	err = sqlite3_prepare_v2(wf->db, dentry_sql, -1, &stmt, NULL);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, dir_ino);
	if (err)
		goto out;
	err = sqlite3_bind_text(stmt, col++, name, -1, SQLITE_STATIC);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, ino);
	if (err)
		goto out;
	err = sqlite3_step(stmt);
	if (err && err != SQLITE_DONE)
		goto out;
	err = 0;
out:
	err2 = sqlite3_finalize(stmt);
	if (!err && err2)
		err = err2;
	return err;
}

/* Insert an extent into the database. */
static int insert_extent(struct walk_fs_t *wf, int64_t ino, uint64_t physical,
			 uint64_t logical, uint64_t length, int flags, int type)
{
	const char *extent_sql = "INSERT INTO extent_t VALUES(?, ?, ?, ?, ?, ?, ?);";
	sqlite3_stmt *stmt = NULL;
	int err, err2, col = 1;

	dbg_printf("%s: ino=%"PRId64" phys=%"PRIu64" logical=%"PRIu64" len=%"PRIu64" flags=0x%x type=%s\n", __func__,
		   ino, physical, logical, length, flags, extent_codes[type]);

	/* Update the dentry table */
	err = sqlite3_prepare_v2(wf->db, extent_sql, -1, &stmt, NULL);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, ino);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, physical);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, logical);
	if (err)
		goto out;
	err = sqlite3_bind_int(stmt, col++, flags);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, length);
	if (err)
		goto out;
	err = sqlite3_bind_text(stmt, col++, extent_codes[type], -1, SQLITE_STATIC);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, physical + length - 1);
	if (err)
		goto out;
	err = sqlite3_step(stmt);
	if (err && err != SQLITE_DONE)
		goto out;
	err = 0;
out:
	err2 = sqlite3_finalize(stmt);
	if (!err && err2)
		err = err2;
	return err;
}

/* Figure out the physical offset of an inode. */
static uint64_t inode_offset(ext2_filsys fs, ext2_ino_t ino)
{
	blk64_t block, block_nr;
	unsigned long offset;
	dgrp_t group;

	group = (ino - 1) / EXT2_INODES_PER_GROUP(fs->super);
	offset = ((ino - 1) % EXT2_INODES_PER_GROUP(fs->super)) *
		EXT2_INODE_SIZE(fs->super);
	block = offset >> EXT2_BLOCK_SIZE_BITS(fs->super);
	block_nr = ext2fs_inode_table_loc(fs, group) + block;
	offset &= (EXT2_BLOCK_SIZE(fs->super) - 1);

	return (block_nr * fs->blocksize) + offset;
}

/* Walk a file's extents for extents */
static errcode_t walk_extents(struct walk_fs_t *wf, ext2_ino_t ino)
{
	ext2_filsys		fs = wf->fs;
	ext2_extent_handle_t	handle;
	struct ext2fs_extent	extent;
	errcode_t		retval;
	blk64_t			last_pblk, last_lblk, last_sz;
	int			last_flags;

	retval = ext2fs_extent_open(fs, ino, &handle);
	if (retval)
		return retval;

	retval = ext2fs_extent_get(handle, EXT2_EXTENT_ROOT, &extent);
	if (retval)
		goto out;

	last_sz = 0;
	do {
		if (extent.e_flags & EXT2_EXTENT_FLAGS_SECOND_VISIT)
			goto next;

		/* Internal node */
		if (!(extent.e_flags & EXT2_EXTENT_FLAGS_LEAF)) {
			dbg_printf("ino=%d free=%llu bf=%llu\n", list->ino,
					extent.e_pblk, list->blocks_freed + 1);
			wf->db_err = insert_extent(wf, ino,
						   extent.e_pblk * fs->blocksize,
						   extent.e_lblk * fs->blocksize,
						   0,
						   0,
						   EXT2_XT_METADATA);
			if (wf->db_err)
				goto out;
			goto next;
		}

		/* Can we attach it to the previous extent? */
		if (list->count) {
			struct ext2fs_extent *last = list->extents +
						     list->count - 1;
			blk64_t end = last->e_len + extent.e_len;

			if (last->e_pblk + last->e_len == extent.e_pblk &&
			    last->e_lblk + last->e_len == extent.e_lblk &&
			    (last->e_flags & EXT2_EXTENT_FLAGS_UNINIT) ==
			    (extent.e_flags & EXT2_EXTENT_FLAGS_UNINIT) &&
			    end < (1ULL << 32)) {
				last->e_len += extent.e_len;
#ifdef DEBUG
				printf("R: ino=%d len=%u\n", list->ino,
						last->e_len);
#endif
				goto next;
			}
		}

		/* Do we need to expand? */
		if (list->count == list->size) {
			unsigned int new_size = (list->size + NUM_EXTENTS) *
						sizeof(struct ext2fs_extent);
			retval = ext2fs_resize_mem(0, new_size, &list->extents);
			if (retval)
				goto out;
			list->size += NUM_EXTENTS;
		}

		/* Add a new extent */
		memcpy(list->extents + list->count, &extent, sizeof(extent));
#ifdef DEBUG
		printf("R: ino=%d pblk=%llu lblk=%llu len=%u\n", list->ino,
				extent.e_pblk, extent.e_lblk, extent.e_len);
#endif
		list->count++;
next:
		retval = ext2fs_extent_get(handle, EXT2_EXTENT_NEXT, &extent);
	} while (retval == 0);

out:
	/* Ok if we run off the end */
	if (retval == EXT2_ET_EXTENT_NO_NEXT)
		retval = 0;
	ext2fs_extent_free(handle);
	return retval;
}

/* Walk a file's mappings for extents */
static errcode_t walk_file_mappings(struct walk_fs_t *wf, ext2_ino_t ino,
				    int type)
{
	struct ext2_inode_large *inode;
	struct ext2_inode *inod;
	uint32_t *ea_magic;
	blk64_t b;
	uint64_t ino_offset, inode_end, ino_sz;
	errcode_t err;

	if (ext2fs_fast_test_inode_bitmap2(wf->iseen, ino))
		return 0;

	/* Read the inode */
	ino_sz = EXT2_INODE_SIZE(wf->fs->super);
	err = ext2fs_get_memzero(ino_sz, &inode);
	if (err)
		return err;
	inod = (struct ext2_inode *)inode;
	err = ext2fs_read_inode_full(wf->fs, ino, inod, ino_sz);
	if (err)
		goto out;

	/* Where is this inode in the FS? */
	ino_offset = inode_offset(wf->fs, ino);
	inode_end = inode->i_extra_isize;
	wf->db_err = insert_extent(wf, ino, ino_offset, 0,
				   ino_sz,
				   EXTENT_SHARED | EXTENT_NOT_ALIGNED,
				   EXT2_XT_METADATA);
	if (wf->db_err)
		goto out;

	/* inline xattr? */
	ea_magic = (uint32_t *)(((char *)inode) + inode_end);
	if (ext2fs_le32_to_cpu(ea_magic) == EXT2_EXT_ATTR_MAGIC) {
		wf->db_err = insert_extent(wf, ino,
					   ino_offset + inode_end,
					   0,
					   ino_sz - inode_end,
					   EXTENT_SHARED | EXTENT_NOT_ALIGNED,
					   EXT2_XT_XATTR);
		if (wf->db_err)
			goto out;
	}

	/* external xattr? */
	b = ext2fs_file_acl_block(wf->fs, inod);
	if (b) {
		wf->db_err = insert_extent(wf, ino, b * wf->fs->blocksize,
					   0, wf->fs->blocksize, 0,
					   EXT2_XT_XATTR);
		if (wf->db_err)
			goto out;
	}

	/* inline data file or symlink? */
	if (inode->i_flags & EXT4_INLINE_DATA_FL ||
	    type == EXT2_FT_SYMLINK) {
		size_t sz = EXT2_I_SIZE(inode);
		wf->db_err = insert_extent(wf, ino,
					   ino_offset + offsetof(struct ext2_inode, i_block),
					   0, sz > 60 ? 60 : sz,
					   EXTENT_SHARED | EXTENT_DATA_INLINE | EXTENT_NOT_ALIGNED,
					   type);
		if (wf->db_err)
			goto out;
	}

	/* extent file */
	if (inode->i_flags & EXT4_EXTENT_FL) {
	}

out:
	ext2fs_free_mem(&inode);
	ext2fs_fast_mark_inode_bitmap2(wf->iseen, ino);
	return err;
}

/* Handle a directory entry */
static int walk_fs_helper(ext2_ino_t dir, int entry,
			  struct ext2_dir_entry *dirent, int offset,
			  int blocksize, char *buf, void *priv_data)
{
	char path[PATH_MAX + 1];
	char name[EXT2_NAME_LEN + 1];
	const char *old_dirpath;
	int type;
	struct ext2_dir_entry_2 *de2 = (struct ext2_dir_entry_2 *)dirent;
	struct walk_fs_t *wf = priv_data;
	struct ext2_inode inode;

	if (!strcmp(dirent->name, ".") || !strcmp(dirent->name, ".."))
		return 0;
	memcpy(name, dirent->name, dirent->name_len & 0xFF);
	name[dirent->name_len & 0xFF] = 0;

	dbg_printf("dir=%d name=%s/%s ino=%d type=%d\n", dir, wf->dirpath, name,
		   dirent->inode, de2->file_type);

	if (de2->file_type != 0) {
		switch(de2->file_type) {
		case EXT2_FT_REG_FILE:
		case EXT2_FT_DIR:
		case EXT2_FT_SYMLINK:
			type = de2->file_type;
			break;
		default:
			return 0;
		}
	} else {
		wf->err = ext2fs_read_inode(wf->fs, dirent->inode, &inode);
		if (wf->err)
			return DIRENT_ABORT;
		if (S_ISREG(inode.i_mode))
			type = EXT2_FT_REG_FILE;
		else if (S_ISDIR(inode.i_mode))
			type = EXT2_FT_DIR;
		else if (S_ISLNK(inode.i_mode))
			type = EXT2_FT_SYMLINK;
		else
			return 0;
	}

	snprintf(path, PATH_MAX, "%s/%s", wf->dirpath, name);
	wf->db_err = insert_inode(wf, dirent->inode, type, path);
	if (wf->db_err)
		return DIRENT_ABORT;
	wf->db_err = insert_dentry(wf, dir, name, dirent->inode);
	if (wf->db_err)
		return DIRENT_ABORT;

	if (type == EXT2_FT_DIR) {
		errcode_t err;
		old_dirpath = wf->dirpath;
		wf->dirpath = path;
		err = ext2fs_dir_iterate2(wf->fs, dirent->inode, 0, NULL,
					  walk_fs_helper, wf);
		if (!wf->err)
			wf->err = err;

		wf->dirpath = old_dirpath;
	}
	if (wf->err)
		return DIRENT_ABORT;
	if (wf->db_err)
		return DIRENT_ABORT;

	return 0;
}

/* Walk the whole FS, looking for inodes to analyze. */
static errcode_t walk_fs(sqlite3 *db, ext2_filsys fs, int *db_err)
{
	struct walk_fs_t wf;
	errcode_t err;

	memset(&wf, 0, sizeof(wf));
	wf.db = db;
	wf.fs = fs;
	wf.dirpath = "";

	wf.err = ext2fs_allocate_inode_bitmap(fs, "visited inodes", &wf.iseen);
	if (wf.err)
		goto out;

	wf.db_err = insert_inode(&wf, EXT2_ROOT_INO, EXT2_FT_DIR, wf.dirpath);
	if (wf.db_err)
		goto out;

	err = walk_file_mappings(&wf, EXT2_ROOT_INO, EXT2_FT_DIR);
	if (!wf.err)
		wf.err = err;
	if (wf.err || wf.db_err)
		goto out;

	err = ext2fs_dir_iterate2(fs, EXT2_ROOT_INO, 0, NULL, walk_fs_helper,
				  &wf);
	if (!wf.err)
		wf.err = err;
out:
	ext2fs_free_inode_bitmap(wf.iseen);
	*db_err = wf.db_err;
	return wf.err;
}

static void inject_metadata(struct walk_fs_t *wf, int64_t parent_ino,
			    const char *path, int64_t ino, const char *name,
			    int type)
{
	char __path[PATH_MAX + 1];

	snprintf(__path, PATH_MAX, "%s/%s", path, name);
	wf->dirpath = path;
	wf->db_err = insert_inode(wf, ino, type, __path);
	if (wf->db_err)
		return;
	wf->db_err = insert_dentry(wf, parent_ino, name, ino);
	if (wf->db_err)
		return;
}
#define INJECT_METADATA(parent_ino, path, ino, name, type) \
	do { \
		inject_metadata(&wf, parent_ino, path, ino, name, type); \
		if (wf.db_err) \
			goto out; \
	} while(0);

#define INJECT_ROOT_METADATA(suffix, type) \
	INJECT_METADATA(INO_METADATA_DIR, "/" STR_METADATA_DIR, INO_##suffix, STR_##suffix, type)

#define INJECT_GROUP(ino, path, type) \
	INJECT_METADATA(INO_GROUPS_DIR, "/" STR_METADATA_DIR "/" STR_GROUPS_DIR, (ino), (path), (type))

/* Insert extents for a file, given a bitmap */
static errcode_t walk_bitmap(struct walk_fs_t *wf, int64_t ino,
			     ext2fs_block_bitmap bm)
{
	blk64_t start, end, out = 0, loff = 0;
	errcode_t err;

	start = 0;
	end = ext2fs_blocks_count(wf->fs->super) - 1;

	err = ext2fs_find_first_set_block_bitmap2(bm, start, end, &out);
	while (err == 0) {
		start = out;
		err = ext2fs_find_first_zero_block_bitmap2(bm, start, end, &out);
		if (err == ENOENT) {
			out = end;
			err = 0;
		} else if (err)
			break;

		wf->db_err = insert_extent(wf, ino, start * wf->fs->blocksize,
					   loff,
					   (out - start) * wf->fs->blocksize,
					   EXTENT_SHARED,
					   EXT2_XT_METADATA);

		if (wf->db_err)
			break;
		start = out;
		loff += (out - start) * wf->fs->blocksize;
		err = ext2fs_find_first_set_block_bitmap2(bm, start, end, &out);
	}

	if (err == ENOENT)
		err = 0;

	return err;
}

/* Invent a FS tree for metadata. */
static errcode_t walk_metadata(sqlite3 *db, ext2_filsys fs, int *db_err)
{
	struct walk_fs_t wf;
	dgrp_t group;
	int64_t ino, group_ino;
	blk64_t s, o, n;
	blk_t u;
	char path[PATH_MAX + 1];
	ext2fs_block_bitmap sb_bmap, sb_gdt, sb_bbitmap, sb_ibitmap, sb_itable;

	memset(&wf, 0, sizeof(wf));
	wf.db = db;
	wf.fs = fs;

	INJECT_METADATA(EXT2_ROOT_INO, "", INO_METADATA_DIR, \
			STR_METADATA_DIR, EXT2_FT_DIR);
	INJECT_ROOT_METADATA(SB_FILE, EXT2_FT_REG_FILE);
	INJECT_ROOT_METADATA(GDT_FILE, EXT2_FT_REG_FILE);
	INJECT_ROOT_METADATA(BBITMAP_FILE, EXT2_FT_REG_FILE);
	INJECT_ROOT_METADATA(IBITMAP_FILE, EXT2_FT_REG_FILE);
	INJECT_ROOT_METADATA(ITABLE_FILE, EXT2_FT_REG_FILE);
	INJECT_ROOT_METADATA(GROUPS_DIR, EXT2_FT_DIR);

	wf.err = ext2fs_allocate_block_bitmap(fs, "superblock", &sb_bmap);
	if (wf.err)
		goto out;

	wf.err = ext2fs_allocate_block_bitmap(fs, "group descriptors", &sb_gdt);
	if (wf.err)
		goto out;

	wf.err = ext2fs_allocate_block_bitmap(fs, "block bitmaps", &sb_bbitmap);
	if (wf.err)
		goto out;

	wf.err = ext2fs_allocate_block_bitmap(fs, "inode bitmaps", &sb_ibitmap);
	if (wf.err)
		goto out;

	wf.err = ext2fs_allocate_block_bitmap(fs, "inode tables", &sb_itable);
	if (wf.err)
		goto out;

	ino = INO_GROUPS_DIR - 1;
	for (group = 0; group < fs->group_desc_count; group++) {
		snprintf(path, PATH_MAX, "%d", group);
		group_ino = ino;
		ino--;
		INJECT_GROUP(group_ino, path, EXT2_FT_DIR);
		wf.err = ext2fs_super_and_bgd_loc2(fs, group, &s, &o, &n, &u);
		if (wf.err)
			goto out;
		snprintf(path, PATH_MAX, "/%s/%s/%d", STR_METADATA_DIR,
			 STR_GROUPS_DIR, group);

		/* Record the superblock */
		if (s || group == 0) {
			ext2fs_fast_mark_block_bitmap2(sb_bmap, s);
			INJECT_METADATA(group_ino, path, ino, "superblock",
					EXT2_FT_REG_FILE);
			wf.db_err = insert_extent(&wf, ino, s * fs->blocksize,
						  0, fs->blocksize,
						  EXTENT_SHARED,
						  EXT2_XT_METADATA);
			if (wf.db_err)
				goto out;
			ino--;
			u--;
		}

		/* Record old style group descriptors */
		if (o) {
			ext2fs_fast_mark_block_bitmap_range2(sb_gdt, o, u);
			INJECT_METADATA(group_ino, path, ino, "descriptor",
					EXT2_FT_REG_FILE);
			wf.db_err = insert_extent(&wf, ino, o * fs->blocksize,
						  0, u * fs->blocksize,
						  EXTENT_SHARED,
						  EXT2_XT_METADATA);
			if (wf.db_err)
				goto out;
			ino--;
		}

		/* Record new style group descriptors */
		if (n) {
			ext2fs_fast_mark_block_bitmap_range2(sb_gdt, n, u);
			INJECT_METADATA(group_ino, path, ino, "descriptor",
					EXT2_FT_REG_FILE);
			wf.db_err = insert_extent(&wf, ino, n * fs->blocksize,
						  0, u * fs->blocksize,
						  EXTENT_SHARED,
						  EXT2_XT_METADATA);
			if (wf.db_err)
				goto out;
			ino--;
		}

		/* Record block bitmap */
		s = ext2fs_block_bitmap_loc(fs, group);
		ext2fs_fast_mark_block_bitmap2(sb_bbitmap, s);
		INJECT_METADATA(group_ino, path, ino, "block_bitmap",
				EXT2_FT_REG_FILE);
		wf.db_err = insert_extent(&wf, ino, s * fs->blocksize,
					  0, fs->blocksize,
					  EXTENT_SHARED,
					  EXT2_XT_METADATA);
		if (wf.db_err)
			goto out;
		ino--;

		/* Record inode bitmap */
		s = ext2fs_inode_bitmap_loc(fs, group);
		ext2fs_fast_mark_block_bitmap2(sb_ibitmap, s);
		INJECT_METADATA(group_ino, path, ino, "inode_bitmap",
				EXT2_FT_REG_FILE);
		wf.db_err = insert_extent(&wf, ino, s * fs->blocksize,
					  0, fs->blocksize,
					  EXTENT_SHARED,
					  EXT2_XT_METADATA);
		if (wf.db_err)
			goto out;
		ino--;

		/* Record inode table */
		s = ext2fs_inode_table_loc(fs, group);
		ext2fs_fast_mark_block_bitmap_range2(sb_itable, s,
				fs->inode_blocks_per_group);
		INJECT_METADATA(group_ino, path, ino, "inodes",
				EXT2_FT_REG_FILE);
		wf.db_err = insert_extent(&wf, ino, s * fs->blocksize,
					  0, fs->inode_blocks_per_group * fs->blocksize,
					  EXTENT_SHARED,
					  EXT2_XT_METADATA);
		if (wf.db_err)
			goto out;
		ino--;
	}

	/* Emit extents for the overall files */
	wf.err = walk_bitmap(&wf, INO_SB_FILE, sb_bmap);
	if (wf.err || wf.db_err)
		goto out;
	wf.err = walk_bitmap(&wf, INO_GDT_FILE, sb_gdt);
	if (wf.err || wf.db_err)
		goto out;
	wf.err = walk_bitmap(&wf, INO_BBITMAP_FILE, sb_bbitmap);
	if (wf.err || wf.db_err)
		goto out;
	wf.err = walk_bitmap(&wf, INO_IBITMAP_FILE, sb_ibitmap);
	if (wf.err || wf.db_err)
		goto out;
	wf.err = walk_bitmap(&wf, INO_ITABLE_FILE, sb_itable);
	if (wf.err || wf.db_err)
		goto out;

out:
	ext2fs_free_block_bitmap(sb_itable);
	ext2fs_free_block_bitmap(sb_ibitmap);
	ext2fs_free_block_bitmap(sb_bbitmap);
	ext2fs_free_block_bitmap(sb_gdt);
	ext2fs_free_block_bitmap(sb_bmap);
	*db_err = wf.db_err;
	return wf.err;
}

int main(int argc, char *argv[])
{
	const char *dbfile;
	const char *fsdev;
	sqlite3 *db = NULL;
	ext2_filsys fs = NULL;
	int db_err = 0;
	errcode_t err, err2;

	if (argc != 3) {
		printf("Usage: %s dbfile fsdevice\n", argv[0]);
		return 0;
	}

	/* Open things */
	dbfile = argv[1];
	fsdev = argv[2];

	err = ext2fs_open2(fsdev, NULL, EXT2_FLAG_64BITS | EXT2_FLAG_SKIP_MMP,
			   0, 0, unix_io_manager, &fs);
	if (err) {
		com_err(fsdev, err, "while opening filesystem.");
		goto out;
	}
	fs->default_bitmap_type = EXT2FS_BMAP64_RBTREE;

	err = sqlite3_open(dbfile, &db);
	if (err) {
		com_err(dbfile, 0, "%s while opening database",
			sqlite3_errstr(err));
		goto out;
	}

	/* Prepare and clean out database. */
	err = run_batch_query(db, dbschema);
	if (err) {
		com_err(dbfile, 0, "%s while preparing database",
			sqlite3_errstr(err));
		goto out;
	}
	err = collect_fs_stats(db, fs);
	if (err) {
		com_err(dbfile, 0, "%s while storing fs stats",
			sqlite3_errstr(err));
		goto out;
	}

	/* Walk the filesystem */
	err = walk_fs(db, fs, &db_err);
	if (err) {
		com_err(fsdev, err, "while walking filesystem");
		goto out;
	}
	if (db_err) {
		com_err(dbfile, 0, "%d %s while analyzing filesystem", db_err,
			sqlite3_errstr(db_err));
		goto out;
	}

	/* Walk the metadata */
	err = walk_metadata(db, fs, &db_err);
	if (err) {
		com_err(fsdev, err, "while walking metadata");
		goto out;
	}
	if (db_err) {
		com_err(dbfile, 0, "%d %s while analyzing metadata", db_err,
			sqlite3_errstr(db_err));
		goto out;
	}

	/* Generate indexes and finalize. */
	err = run_batch_query(db, dbindex);
	if (err) {
		com_err(dbfile, 0, "%s while indexing database",
			sqlite3_errstr(err));
		goto out;
	}

	err = finalize_fs_stats(db, fs);
	if (err) {
		com_err(dbfile, 0, "%s while finalizing database",
			sqlite3_errstr(err));
		goto out;
	}
out:
	err = sqlite3_close(db);
	if (err)
		com_err(dbfile, 0, "%s while closing database",
			sqlite3_errstr(err));

	err2 = fs ? ext2fs_close_free(&fs) : 0;
	if (err2)
		com_err(fsdev, err2, "while closing filesystem.");

	if (!err && err2)
		err = err2;

	return err;
}
