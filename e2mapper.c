/*
 * Generate filemapper databases from ext* filesystems.
 * Copyright 2015 Darrick J. Wong.
 * Licensed under the GPLv2.
 */
#include <stdio.h>
#include <time.h>
#include <limits.h>
#include <sys/stat.h>
#include <sqlite3.h>
#include <ext2fs/ext2fs.h>

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

#define DEBUG

#ifdef DEBUG
# define dbg_printf(f, a...)  do {printf(f, ## a); fflush(stdout); } while (0)
#else
# define dbg_printf(f, a...)
#endif

struct walk_fs_t {
	sqlite3 *db;
	ext2_filsys fs;
	errcode_t err;
	int db_err;
	char *dirpath;
};

#define EXT2_FT_METADATA	(EXT2_FT_MAX)
static char *type_codes[] = {
	[EXT2_FT_DIR] = "d",
	[EXT2_FT_REG_FILE] = "f",
	[EXT2_FT_SYMLINK] = "s",
	[EXT2_FT_METADATA] = "m",
};

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
static int insert_inode(struct walk_fs_t *wf, ext2_ino_t ino, int type,
			const char *path)
{
	const char *ino_sql = "INSERT OR REPLACE INTO inode_t VALUES(?, ?);";
	const char *path_sql = "INSERT INTO path_t VALUES(?, ?);";
	sqlite3_stmt *stmt = NULL;
	int err, err2, col = 1;

	dbg_printf("%s: ino=%d type=%s path=%s\n", __func__, ino,
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
static int insert_dentry(struct walk_fs_t *wf, ext2_ino_t dir_ino,
			 const char *name, ext2_ino_t ino)
{
	const char *dentry_sql = "INSERT INTO dir_t VALUES(?, ?, ?);";
	sqlite3_stmt *stmt = NULL;
	int err, err2, col = 1;

	dbg_printf("%s: dir=%d name=%s ino=%d\n", __func__, dir_ino, name, ino);

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

/* Handle a directory entry */
static int walk_fs_helper(ext2_ino_t dir, int entry,
			  struct ext2_dir_entry *dirent, int offset,
			  int blocksize, char *buf, void *priv_data)
{
	char path[PATH_MAX + 1];
	char name[EXT2_NAME_LEN + 1];
	char *old_dirpath;
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

	wf.db_err = insert_inode(&wf, EXT2_ROOT_INO, EXT2_FT_DIR, wf.dirpath);
	if (wf.db_err)
		goto out;

	err = ext2fs_dir_iterate2(fs, EXT2_ROOT_INO, 0, NULL, walk_fs_helper,
				  &wf);
	if (!wf.err)
		wf.err = err;
out:
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
