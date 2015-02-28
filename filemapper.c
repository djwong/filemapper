/*
 * FileMapper definitions for C.
 * Copyright 2015 Darrick J. Wong.
 * Licensed under the GPLv2+.
 */
#undef DEBUG
#undef PROGRESS_REPORT
#include <inttypes.h>
#include "filemapper.h"

static char *opschema = "\
PRAGMA cache_size = 4096;\
PRAGMA mmap_size = 1073741824;\
PRAGMA journal_mode = MEMORY;\
PRAGMA synchronous = OFF;\
PRAGMA locking_mode = EXCLUSIVE;\
PRAGMA case_sensitive_like = ON;\
";

static char *dbschema = "PRAGMA page_size = 65536;\
PRAGMA application_id = 61270;\
PRAGMA journal_mode = MEMORY;\
DROP VIEW IF EXISTS dentry_t;\
DROP VIEW IF EXISTS path_extent_v;\
DROP VIEW IF EXISTS path_inode_v;\
DROP TABLE IF EXISTS overview_t;\
DROP TABLE IF EXISTS dentry_t;\
DROP TABLE IF EXISTS extent_t;\
DROP TABLE IF EXISTS inode_t;\
DROP TABLE IF EXISTS path_t;\
DROP TABLE IF EXISTS dir_t;\
DROP TABLE IF EXISTS fs_t;\
CREATE TABLE fs_t(path TEXT PRIMARY KEY NOT NULL, block_size INTEGER NOT NULL, frag_size INTEGER NOT NULL, total_bytes INTEGER NOT NULL, free_bytes INTEGER NOT NULL, avail_bytes INTEGER NOT NULL, total_inodes INTEGER NOT NULL, free_inodes INTEGER NOT NULL, avail_inodes INTEGER NOT NULL, max_len INTEGER NOT NULL, timestamp TEXT NOT NULL, finished INTEGER NOT NULL, path_separator TEXT NOT NULL);\
CREATE TABLE inode_type_t(id INTEGER PRIMARY KEY UNIQUE, code TEXT NOT NULL);\
INSERT INTO inode_type_t VALUES (0, 'f');\
INSERT INTO inode_type_t VALUES (1, 'd');\
INSERT INTO inode_type_t VALUES (2, 'm');\
INSERT INTO inode_type_t VALUES (3, 's');\
CREATE TABLE inode_t(ino INTEGER PRIMARY KEY UNIQUE NOT NULL, type INTEGER NOT NULL, nr_extents INTEGER, travel_score REAL, atime INTEGER, crtime INTEGER, ctime INTEGER, mtime INTEGER, size INTEGER, FOREIGN KEY(type) REFERENCES inode_type_t(id));\
CREATE TABLE dir_t(dir_ino INTEGER NOT NULL, name TEXT NOT NULL, name_ino INTEGER NOT NULL, FOREIGN KEY(dir_ino) REFERENCES inode_t(ino), FOREIGN KEY(name_ino) REFERENCES inode_t(ino));\
CREATE TABLE path_t(path TEXT PRIMARY KEY UNIQUE NOT NULL, ino INTEGER NOT NULL, FOREIGN KEY(ino) REFERENCES inode_t(ino));\
CREATE TABLE extent_type_t (id INTEGER PRIMARY KEY UNIQUE, code TEXT NOT NULL);\
INSERT INTO extent_type_t VALUES (0, 'f');\
INSERT INTO extent_type_t VALUES (1, 'd');\
INSERT INTO extent_type_t VALUES (2, 'e');\
INSERT INTO extent_type_t VALUES (3, 'm');\
INSERT INTO extent_type_t VALUES (4, 'x');\
INSERT INTO extent_type_t VALUES (5, 's');\
CREATE TABLE extent_t(ino INTEGER NOT NULL, p_off INTEGER NOT NULL, l_off INTEGER NOT NULL, flags INTEGER NOT NULL, length INTEGER NOT NULL, type INTEGER NOT NULL, p_end INTEGER NOT NULL, FOREIGN KEY(ino) REFERENCES inode_t(ino), FOREIGN KEY(type) REFERENCES extent_type_t(id));\
CREATE TABLE overview_t(length INTEGER NOT NULL, cell_no INTEGER NOT NULL, files INTEGER NOT NULL, dirs INTEGER NOT NULL, mappings INTEGER NOT NULL, metadata INTEGER NOT NULL, xattrs INTEGER NOT NULL, symlinks INTEGER NOT NULL, CONSTRAINT pk_overview PRIMARY KEY (length, cell_no));\
CREATE VIEW path_extent_v AS SELECT path_t.path, extent_t.p_off, extent_t.l_off, extent_t.length, extent_t.flags, extent_t.type, extent_t.p_end, extent_t.ino FROM extent_t, path_t WHERE extent_t.ino = path_t.ino;\
CREATE VIEW path_inode_v AS SELECT path_t.path, inode_t.ino, inode_t.type, inode_t.nr_extents, inode_t.travel_score, inode_t.atime, inode_t.crtime, inode_t.ctime, inode_t.mtime, inode_t.size FROM path_t, inode_t WHERE inode_t.ino = path_t.ino;\
CREATE VIEW dentry_t AS SELECT dir_t.dir_ino, dir_t.name, dir_t.name_ino, inode_t.type FROM dir_t, inode_t WHERE dir_t.name_ino = inode_t.ino;";

static char *dbindex = "CREATE INDEX inode_i ON inode_t(ino);\
CREATE INDEX path_ino_i ON path_t(ino);\
CREATE INDEX path_path_i ON path_t(path);\
CREATE INDEX dir_ino_i ON dir_t(dir_ino);\
CREATE INDEX dir_nino_i ON dir_t(name_ino);\
CREATE INDEX extent_poff_i ON extent_t(p_off, p_end);\
CREATE INDEX extent_loff_i ON extent_t(l_off, length);\
CREATE INDEX extent_ino_i ON extent_t(ino);\
CREATE INDEX overview_cell_i ON overview_t(length, cell_no);\
CREATE INDEX inode_ino_i ON inode_t(ino);\
CREATE INDEX extent_type_i ON extent_t(type);\
PRAGMA foreign_key_check;";

static int primary_extent_type_for_inode[] = {
	[INO_TYPE_FILE]		= EXT_TYPE_FILE,
	[INO_TYPE_DIR]		= EXT_TYPE_DIR,
	[INO_TYPE_METADATA]	= EXT_TYPE_METADATA,
	[INO_TYPE_SYMLINK]	= EXT_TYPE_SYMLINK,
};

/* Convert a directory pathname */
int icvt(struct filemapper_t *wf, char *in, size_t inl, char *out, size_t outl)
{
	size_t x;

	while (inl) {
		x = iconv(wf->iconv, &in, &inl, &out, &outl);
		if (x == -1) {
			if (errno == EILSEQ || errno == EINVAL) {
				if (outl < 3)
					return -1;
				*out = 0xEF;
				out++;
				*out = 0xBF;
				out++;
				*out = 0xBD;
				out++;
				outl += 3;
				in++;
				inl--;
			} else {
				return -1;
			}
		}
	}

	if (outl < 1) {
		errno = EFBIG;
		return -1;
	}
	*out = 0;
	return 0;
}

/* Run a bunch of queries */
void run_batch_query(struct filemapper_t *wf, const char *sql)
{
	sqlite3 *db = wf->db;
	sqlite3_stmt *stmt = NULL;
	const char *tail, *p;
	int err, err2 = 0;

	p = sql;
	err = sqlite3_prepare_v2(db, p, -1, &stmt, &tail);
	while (err == 0 && stmt) {
		do {
			err = sqlite3_step(stmt);
		} while (err == SQLITE_ROW);
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
		dbg_printf("err=%d p=%s\n", err, tail);

	wf->db_err = err;
}

/* Insert an inode record into the inode and path tables */
void insert_inode(struct filemapper_t *wf, int64_t ino, int type,
		  const char *path, time_t *atime, time_t *crtime,
		  time_t *ctime, time_t *mtime, ssize_t *size)
{
	const char *ino_sql = "INSERT OR REPLACE INTO inode_t VALUES(?, ?, NULL, NULL, ?, ?, ?, ?, ?);";
	const char *path_sql = "INSERT INTO path_t VALUES(?, ?);";
	sqlite3_stmt *stmt = NULL;
	int err, err2, col = 1;

#ifdef PROGRESS_REPORT
	{static int i = 0;
	 if (!(i++ % 23))
		printf("%s: ino=%"PRId64" type=%d path=%s                   \r",
		       __func__, ino, type, path);
	}
#else
	dbg_printf("%s: ino=%"PRId64" type=%d path=%s\n", __func__, ino,
		   type, path);
#endif

	/* Update the inode table */
	err = sqlite3_prepare_v2(wf->db, ino_sql, -1, &stmt, NULL);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, ino);
	if (err)
		goto out;
	err = sqlite3_bind_int(stmt, col++, type);
	if (err)
		goto out;
	if (atime)
		err = sqlite3_bind_int64(stmt, col++, *atime);
	else
		err = sqlite3_bind_null(stmt, col++);
	if (err)
		goto out;
	if (crtime)
		err = sqlite3_bind_int64(stmt, col++, *crtime);
	else
		err = sqlite3_bind_null(stmt, col++);
	if (err)
		goto out;
	if (ctime)
		err = sqlite3_bind_int64(stmt, col++, *ctime);
	else
		err = sqlite3_bind_null(stmt, col++);
	if (err)
		goto out;
	if (mtime)
		err = sqlite3_bind_int64(stmt, col++, *mtime);
	else
		err = sqlite3_bind_null(stmt, col++);
	if (err)
		goto out;
	if (size)
		err = sqlite3_bind_int64(stmt, col++, *size);
	else
		err = sqlite3_bind_null(stmt, col++);
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
	wf->db_err = err;
}

/* Insert a directory entry into the database. */
void insert_dentry(struct filemapper_t *wf, int64_t dir_ino,
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
	wf->db_err = err;
}

/* Insert an extent into the database. */
void insert_extent(struct filemapper_t *wf, int64_t ino, uint64_t physical,
		   uint64_t logical, uint64_t length, int flags, int type)
{
	const char *extent_sql = "INSERT INTO extent_t VALUES(?, ?, ?, ?, ?, ?, ?);";
	sqlite3_stmt *stmt = NULL;
	int err, err2, col = 1;

	dbg_printf("%s: ino=%"PRId64" phys=%"PRIu64" logical=%"PRIu64" len=%"PRIu64" flags=0x%x type=%d\n", __func__,
		   ino, physical, logical, length, flags, type);

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
	err = sqlite3_bind_int(stmt, col++, type);
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
	wf->db_err = err;
}

void inject_metadata(struct filemapper_t *wf, int64_t parent_ino,
		     const char *path, int64_t ino, const char *name,
		     int type)
{
	char __path[PATH_MAX + 1];

	snprintf(__path, PATH_MAX, "%s/%s", path, name);
	wf->dirpath = path;
	insert_inode(wf, ino, type, __path, NULL, NULL, NULL, NULL, NULL);
	if (wf->db_err)
		return;
	insert_dentry(wf, parent_ino, name, ino);
	if (wf->db_err)
		return;
}

/* Store fs statistics in the database */
void collect_fs_stats(struct filemapper_t *wf, char *fs_name,
		      uint32_t blocksize, uint32_t fragsize,
		      uint64_t total_bytes, uint64_t free_bytes,
		      uint64_t total_inodes, uint64_t free_inodes,
		      unsigned int max_name_len)
{
	const char *sql = "INSERT INTO fs_t VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?);";
	char p[PATH_MAX + 1];
	char stime[256];
	sqlite3_stmt *stmt;
	time_t t;
	struct tm *tmp;
	int err, err2, col = 1;

	err = sqlite3_prepare_v2(wf->db, sql, -1, &stmt, NULL);
	if (err)
		goto out;
	err = icvt(wf, fs_name, strlen(fs_name), p, PATH_MAX);
	if (err)
		goto out;
	err = sqlite3_bind_text(stmt, col++, p, -1, SQLITE_STATIC);
	if (err)
		goto out;
	err = sqlite3_bind_int(stmt, col++, blocksize);
	if (err)
		goto out;
	err = sqlite3_bind_int(stmt, col++, fragsize);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, total_bytes);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, free_bytes);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, free_bytes);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, total_inodes);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, free_inodes);
	if (err)
		goto out;
	err = sqlite3_bind_int64(stmt, col++, free_inodes);
	if (err)
		goto out;
	err = sqlite3_bind_int(stmt, col++, max_name_len);
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
	wf->db_err = err;
}

/* Mark the database as complete. */
void finalize_fs_stats(struct filemapper_t *wf, char *fs_name)
{
	const char *sql = "UPDATE fs_t SET finished = 1 WHERE path = ?;";
	char p[PATH_MAX + 1];
	sqlite3_stmt *stmt;
	int64_t total_bytes, max_pend;
	int err, err2, col = 1;

	err = sqlite3_prepare_v2(wf->db, sql, -1, &stmt, NULL);
	if (err)
		goto out;
	err = icvt(wf, fs_name, strlen(fs_name), p, PATH_MAX);
	if (err)
		goto out;
	err = sqlite3_bind_text(stmt, col++, p, -1, SQLITE_STATIC);
	if (err)
		goto out;
	err = sqlite3_step(stmt);
	if (err && err != SQLITE_DONE)
		goto out;
	err = sqlite3_finalize(stmt);
	stmt = NULL;
	if (err)
		goto out;

	/* Make sure the extents don't "overflow" the end of the FS. */
	sql = "SELECT MAX(p_end) FROM extent_t";
	err = sqlite3_prepare_v2(wf->db, sql, -1, &stmt, NULL);
	if (err)
		goto out;
	err = sqlite3_step(stmt);
	if (err && err != SQLITE_ROW)
		goto out;
	max_pend = sqlite3_column_int64(stmt, 0);
	err = sqlite3_step(stmt);
	if (err && err != SQLITE_DONE)
		goto out;
	err = sqlite3_finalize(stmt);
	stmt = NULL;
	if (err)
		goto out;

	sql = "SELECT total_bytes FROM fs_t";
	err = sqlite3_prepare_v2(wf->db, sql, -1, &stmt, NULL);
	if (err)
		goto out;
	err = sqlite3_step(stmt);
	if (err && err != SQLITE_ROW)
		goto out;
	total_bytes = sqlite3_column_int64(stmt, 0);
	err = sqlite3_step(stmt);
	if (err && err != SQLITE_DONE)
		goto out;
	err = sqlite3_finalize(stmt);
	stmt = NULL;
	if (err)
		goto out;

	if (total_bytes <= max_pend) {
		sql = "UPDATE fs_t SET total_bytes = ? WHERE path = ?";
		err = sqlite3_prepare_v2(wf->db, sql, -1, &stmt, NULL);
		if (err)
			goto out;
		err = sqlite3_bind_int64(stmt, 1, max_pend + 1);
		if (err)
			goto out;
		err = sqlite3_bind_text(stmt, col++, p, -1, SQLITE_STATIC);
		if (err)
			goto out;
		err = sqlite3_step(stmt);
		if (err && err != SQLITE_DONE)
			goto out;
		err = sqlite3_finalize(stmt);
		stmt = NULL;
		if (err)
			goto out;
	}
out:
	err2 = (stmt ? sqlite3_finalize(stmt) : 0);
	if (!err && err2)
		err = err2;
	wf->db_err = err;
}

/* Generate an overview cache. */
void cache_overview(struct filemapper_t *wf, uint64_t total_bytes,
		    uint64_t length)
{
	sqlite3 *db = wf->db;
	uint64_t start_cell, end_cell, i;
	uint64_t bytes_per_cell, e_p_off, e_p_end;
	int e_type;
	sqlite3_stmt *stmt = NULL;
	struct overview_t *overview;
	int err, err2;

	/* Allocate memory */
	overview = calloc(length, sizeof(*overview));
	if (overview == NULL) {
		err = SQLITE_NOMEM;
		goto out;
	}

	bytes_per_cell = total_bytes / length;

	/* Aggregate the extents */
	err = sqlite3_prepare_v2(db, "SELECT p_off, p_end, type FROM extent_t;",
				 -1, &stmt, NULL);
	if (err)
		goto out;
	err = sqlite3_step(stmt);
	while (err == SQLITE_ROW) {
		e_p_off = sqlite3_column_int64(stmt, 0);
		e_p_end = sqlite3_column_int64(stmt, 1);
		e_type = sqlite3_column_int(stmt, 2);
		start_cell = e_p_off / bytes_per_cell;
		end_cell = e_p_end / bytes_per_cell;
		switch (e_type) {
		case EXT_TYPE_FILE:
			for (i = start_cell; i <= end_cell; i++)
				overview[i].files++;
			break;
		case EXT_TYPE_DIR:
			for (i = start_cell; i <= end_cell; i++)
				overview[i].dirs++;
			break;
		case EXT_TYPE_EXTENT:
			for (i = start_cell; i <= end_cell; i++)
				overview[i].mappings++;
			break;
		case EXT_TYPE_METADATA:
			for (i = start_cell; i <= end_cell; i++)
				overview[i].metadata++;
			break;
		case EXT_TYPE_XATTR:
			for (i = start_cell; i <= end_cell; i++)
				overview[i].xattrs++;
			break;
		case EXT_TYPE_SYMLINK:
			for (i = start_cell; i <= end_cell; i++)
				overview[i].symlinks++;
			break;
		}
		err = sqlite3_step(stmt);
	}
	if (err && err != SQLITE_DONE)
		goto out;
	err = sqlite3_finalize(stmt);
	if (err)
		goto out;
	stmt = NULL;

	/* Now spit it back to the database */
	err = sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO overview_t VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
				 -1, &stmt, NULL);
	if (err)
		goto out;
	for (i = 0; i < length; i++) {
		err = sqlite3_bind_int64(stmt, 1, length);
		if (err)
			goto out;
		err = sqlite3_bind_int64(stmt, 2, i);
		if (err)
			goto out;
		err = sqlite3_bind_int64(stmt, 3, overview[i].files);
		if (err)
			goto out;
		err = sqlite3_bind_int64(stmt, 4, overview[i].dirs);
		if (err)
			goto out;
		err = sqlite3_bind_int64(stmt, 5, overview[i].mappings);
		if (err)
			goto out;
		err = sqlite3_bind_int64(stmt, 6, overview[i].metadata);
		if (err)
			goto out;
		err = sqlite3_bind_int64(stmt, 7, overview[i].xattrs);
		if (err)
			goto out;
		err = sqlite3_bind_int64(stmt, 8, overview[i].symlinks);
		if (err)
			goto out;
		err = sqlite3_step(stmt);
		if (err && err != SQLITE_DONE)
			goto out;
		err = sqlite3_reset(stmt);
		if (err)
			goto out;
	}
out:
	err2 = sqlite3_finalize(stmt);
	if (!err)
		err = err2;
	free(overview);
	wf->db_err = err;
}

/* Prepare database to receive new data */
void prepare_db(struct filemapper_t *wf)
{
	run_batch_query(wf, opschema);
	if (wf->db_err)
		return;
	run_batch_query(wf, dbschema);
	if (wf->db_err)
		return;
}

/* Index database data */
void index_db(struct filemapper_t *wf)
{
	run_batch_query(wf, dbindex);
}

/* Calculate the number of extents and travel score data */
void calc_inode_stats(struct filemapper_t *wf)
{
	sqlite3 *db = wf->db;
	sqlite3_stmt *ino_stmt = NULL, *upd_stmt = NULL;
	int64_t extents, p_dist, l_dist, last_poff, last_loff;
	int64_t p_off, l_off, length;
	int64_t last_ino, ino;
	int etype, itype, has_ino;
	int err, err2;

	/* For each inode... */
	err = sqlite3_prepare_v2(db, "SELECT extent_t.ino, inode_t.type AS itype, extent_t.type AS etype, p_off, l_off, length FROM extent_t INNER JOIN inode_t WHERE extent_t.ino = inode_t.ino AND inode_t.ino IN (SELECT ino FROM inode_t WHERE travel_score IS NULL OR nr_extents IS NULL) ORDER BY extent_t.ino, l_off;",
				 -1, &ino_stmt, NULL);
	if (err)
		goto out;

	/* Update inode table... */
	err = sqlite3_prepare_v2(db, "UPDATE inode_t SET nr_extents = ?, travel_score = ? WHERE ino = ?;",
				 -1, &upd_stmt, NULL);
	if (err)
		goto out;

	/* For each inode... */
	extents = p_dist = l_dist = 0;
	last_poff = last_loff = 0;
	last_ino = 0;
	has_ino = 0;
	err = sqlite3_step(ino_stmt);
	while (err == SQLITE_ROW) {
		ino = sqlite3_column_int64(ino_stmt, 0);
		itype = sqlite3_column_int(ino_stmt, 1);
		etype = sqlite3_column_int(ino_stmt, 2);
		p_off = sqlite3_column_int64(ino_stmt, 3);
		l_off = sqlite3_column_int64(ino_stmt, 4);
		length = sqlite3_column_int64(ino_stmt, 5);
		dbg_printf("%s: ino=%"PRId64" itype=%d etype=%d poff=%"PRId64
			   " loff=%"PRId64" len=%"PRId64"\n",
			   __func__, ino, itype, etype, p_off, l_off, length);

		if (etype != primary_extent_type_for_inode[itype])
			goto next;

		if (!has_ino || ino != last_ino) {
			if (has_ino) {
				err = sqlite3_reset(upd_stmt);
				if (err)
					goto out;
				err = sqlite3_bind_int64(upd_stmt, 1, extents);
				if (err)
					goto out;
				err = sqlite3_bind_double(upd_stmt, 2, (double)p_dist / l_dist);
				if (err)
					goto out;
				err = sqlite3_bind_int64(upd_stmt, 3, last_ino);
				if (err)
					goto out;
				err = sqlite3_step(upd_stmt);
				if (err && err != SQLITE_DONE)
					goto out;
			}
			extents = p_dist = l_dist = 0;
			last_poff = last_loff = 0;
			has_ino = 1;
			last_ino = ino;
		}

		if (extents) {
			p_dist += abs(p_off - last_poff);
			l_dist += l_off - last_loff;
		}
		extents++;
		p_dist += length;
		l_dist += length;
		last_poff = p_off + length - 1;
		last_loff = l_off + length - 1;

next:
		err = sqlite3_step(ino_stmt);
	}
	if (err && err != SQLITE_DONE)
		goto out;
	err = 0;
out:
	err2 = sqlite3_finalize(upd_stmt);
	if (err2 && !err)
		err = err2;
	err2 = sqlite3_finalize(ino_stmt);
	if (err2 && !err)
		err = err2;
	wf->db_err = err;
}

/* Simple bitmap functions */
int fm_test_bit(const uint8_t *bmap, const uint64_t bit)
{
	return (bmap[bit >> 3] >> (bit & 7)) & 1;
}

void fm_set_bit(uint8_t *bmap, const uint64_t bit, const int new_value)
{
	if (new_value)
		bmap[bit >> 3] |= (1 << (bit & 7));
	else
		bmap[bit >> 3] &= ~(1 << (bit & 7));
}
