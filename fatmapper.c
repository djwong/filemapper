/*
 * Generate filemapper databases from fat filesystems.
 * Copyright 2015 Darrick J. Wong.
 * Licensed under the GPLv2+.
 */
#define _XOPEN_SOURCE		600
#define _FILE_OFFSET_BITS       64
#define _LARGEFILE64_SOURCE     1
#define _GNU_SOURCE		1

#include <endian.h>
#include <stdio.h>
#include <time.h>
#include <limits.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iconv.h>
#include <sqlite3.h>

#include <fsck.fat.h>
#include <file.h>
#include <fat.h>
#include <lfn.h>
#include <charconv.h>
#include <boot.h>
#include <common.h>
#include <io.h>
#undef DEBUG
#include "filemapper.h"

/* from check.h */
void add_file(DOS_FS * fs, DOS_FILE *** chain, DOS_FILE * parent,
	      loff_t offset, FDSC ** cp);

struct fatmap_t {
	struct filemapper_t base;

	DOS_FS *fs;
	int err;
	uint64_t ino, dir_ino;	/* fake inode numbers */
};
#define wf_db		base.db
#define wf_db_err	base.db_err
#define wf_dirpath	base.dirpath
#define wf_iconv	base.iconv

/* misc. dosfs stuff */
#define FAT_MAX_NAME_LEN	255
int interactive = 0, rw = 0, list = 0, test = 0, verbose = 0, write_immed = 0;
int atari_format = 0, boot_only = 0;
unsigned n_files = 0;
void *mem_queue = NULL;
static DOS_FILE *root;

#define ROOT_DIR_INO	1
#define INO_METADATA_DIR	(-1)
#define STR_METADATA_DIR	"$metadata"
#define INO_SB_FILE		(-2)
#define STR_SB_FILE		"superblock"
#define INO_PRIMARY_FAT_FILE	(-3)
#define STR_PRIMARY_FAT_FILE	"primary_fat"
#define INO_BACKUP_FAT_FILE	(-4)
#define STR_BACKUP_FAT_FILE	"backup_fat"
#define INO_FREESP_FILE		(-5)
#define STR_FREESP_FILE		"freespace"

#define FSTART(p,fs) \
  ((uint32_t)le16toh(p->dir_ent.start) | \
   (fs->fat_bits == 32 ? le16toh(p->dir_ent.starthi) << 16 : 0))

typedef int (*walk_file_fn)(DOS_FS *fs, DOS_FILE *file, FDSC **cp, void *private);

/* Walk a file's mappings for extents */
static void walk_file_mappings(struct fatmap_t *wf, DOS_FILE *file)
{
	DOS_FS		*fs = wf->fs;
	uint32_t	curr, lcurr;
	uint64_t	pclus, lclus, len;
	unsigned long long max_extent = MAX_EXTENT_LENGTH / fs->cluster_size;
	int		type;
	uint64_t	loff;

	if (file->dir_ent.attr & ATTR_DIR)
		type = EXT_TYPE_DIR;
	else if (file->dir_ent.attr & ATTR_VOLUME)
		type = EXT_TYPE_METADATA;
	else
		type = EXT_TYPE_FILE;

	len = 0;
	for (curr = FSTART(file, fs) ? FSTART(file, fs) : -1, lcurr = 0;
	     curr != -1;
	     curr = next_cluster(fs, curr), lcurr++) {
		if (len) {
			/* Lengthen extent */
			if (pclus + len == curr && len + 1 <= max_extent) {
				len++;
				dbg_printf("R: ino=%llu len=%u\n", wf->ino, len);
				continue;
			}

			/* Insert the extent */
			dbg_printf("R: ino=%llu pblk=%llu lblk=%llu len=%u\n",
				   wf->ino, cluster_start(fs, pclus),
				   lclus * fs->cluster_size,
				   len * fs->cluster_size);
			loff = lclus * fs->cluster_size;
			insert_extent(&wf->base, wf->ino,
				      cluster_start(fs, pclus),
				      &loff,
				      len * fs->cluster_size,
				      0, type);
			if (wf->wf_db_err)
				goto out;
		}

		/* Set up the next extent */
		pclus = curr;
		lclus = lcurr;
		len = 1;
	}

	if (len) {
		/* Insert the extent */
		dbg_printf("R: ino=%llu pblk=%llu lblk=%llu len=%u\n",
			   wf->ino, cluster_start(fs, pclus),
			   lclus * fs->cluster_size, len * fs->cluster_size);
		loff = lclus * fs->cluster_size;
		insert_extent(&wf->base, wf->ino,
			      cluster_start(fs, pclus),
			      &loff,
			      len * fs->cluster_size,
			      0, type);
		if (wf->wf_db_err)
			goto out;
	}
out:
	return;
}

/* Load directory entries from a directory */
static int scan_dir(DOS_FS * fs, DOS_FILE * this, FDSC ** cp)
{
	DOS_FILE **chain;
	int i;
	uint32_t clu_num;

	chain = &this->first;
	i = 0;
	clu_num = FSTART(this, fs);
	lfn_reset();
	while (clu_num > 0 && clu_num != -1) {
		add_file(fs, &chain, this,
			cluster_start(fs, clu_num) + (i % fs->cluster_size), cp);
		i += sizeof(DIR_ENT);
		if (!(i % fs->cluster_size))
			if ((clu_num = next_cluster(fs, clu_num)) == 0 ||
			    clu_num == -1)
				break;
	}
	lfn_reset();

	return 0;
}

/* Walk each directory entry */
static int walk_dir(DOS_FS *fs, DOS_FILE *start, FDSC **cp, walk_file_fn fn,
		    void *private)
{
	while (start) {
		if (!strncmp((const char *)(start->dir_ent.name), MSDOS_DOT,
			     MSDOS_NAME) ||
		    !strncmp((const char *)(start->dir_ent.name), MSDOS_DOTDOT,
			     MSDOS_NAME)) {
			start = start->next;
			continue;
		}
		if (fn(fs, start, cp, private))
			return 1;
		start = start->next;
	}
	return 0;
}

static time_t decode_time(uint16_t date, uint16_t time)
{
	struct tm ret;

	memset(&ret, 0, sizeof(ret));
	date = le16toh(date);
	time = le16toh(time);

	ret.tm_year = ((date >> 9) & 0x7F) + 80;
	ret.tm_mon = ((date >> 5) & 0xF) - 1;
	ret.tm_mday = (date & 0x1F);
	ret.tm_hour = ((time >> 11) & 0x1F);
	ret.tm_min = ((time >> 5) & 0x3F);
	ret.tm_sec = (time & 0x1F);

	return mktime(&ret);
}

/* Handle a directory entry */
static int walk_fs_helper(DOS_FS *fs, DOS_FILE *file, FDSC **cp, void *priv_data)
{
	char path[PATH_MAX + 1];
	char name[FAT_MAX_NAME_LEN + 1];
	int type;
	uint64_t ino;
	struct fatmap_t *wf = priv_data;
	time_t atime, crtime, mtime;
	ssize_t size;

	/* Ignore volume labels */
	if (file->dir_ent.attr & 0x8)
		return 0;

	if (file->lfn)
		snprintf(name, FAT_MAX_NAME_LEN, "%s", file->lfn);
	else
		snprintf(name, FAT_MAX_NAME_LEN, "%s",
			 file_name(file->dir_ent.name));

	if (!strcmp(name, ".") || !strcmp(name, ".."))
		return 0;

	if (file->dir_ent.attr & ATTR_DIR)
		type = INO_TYPE_DIR;
	else if (file->dir_ent.attr & ATTR_VOLUME)
		type = INO_TYPE_METADATA;
	else
		type = INO_TYPE_FILE;
	ino = wf->ino;

	dbg_printf("dir=%"PRIu64" name=%s/%s attr=0x%x ino=%"PRIu64" type=%d\n",
		   wf->dir_ino, wf->wf_dirpath, name, file->dir_ent.attr,
		   ino, type);

	atime = decode_time(file->dir_ent.adate, 0);
	crtime = decode_time(file->dir_ent.cdate, file->dir_ent.ctime);
	mtime = decode_time(file->dir_ent.date, file->dir_ent.time);
	size = le32toh(file->dir_ent.size);

	snprintf(path, PATH_MAX, "%s/%s", wf->wf_dirpath, name);
	insert_inode(&wf->base, ino, type, path, &atime, &crtime, NULL, &mtime,
		     &size);
	if (wf->wf_db_err)
		goto err;

	insert_dentry(&wf->base, wf->dir_ino, name, ino);
	if (wf->wf_db_err)
		goto err;

	walk_file_mappings(wf, file);
	if (wf->err || wf->wf_db_err)
		goto err;

	wf->ino++;
	if (type == INO_TYPE_DIR) {
		FDSC **n;
		const char *old_dirpath;
		uint64_t old_dir_ino;

		old_dir_ino = wf->dir_ino;
		old_dirpath = wf->wf_dirpath;
		wf->wf_dirpath = path;
		wf->dir_ino = ino;
		n = file_cd(cp, (char *)file->dir_ent.name);
		wf->err = scan_dir(fs, file, n);
		if (wf->err)
			goto err;

		walk_dir(fs, file->first, n, walk_fs_helper, wf);
		if (wf->err || wf->wf_db_err)
			goto err;

		wf->wf_dirpath = old_dirpath;
		wf->dir_ino = old_dir_ino;
	}
	if (wf->err || wf->wf_db_err)
		goto err;

	return 0;
err:
	return -1;
}

/* Walk the whole FS, looking for inodes to analyze. */
static void walk_fs(struct fatmap_t *wf)
{
	DOS_FS *fs = wf->fs;
	DOS_FILE **chain;
	FDSC **n;
	int i;

	wf->wf_dirpath = "";
	wf->ino = wf->dir_ino = ROOT_DIR_INO;

	/* Create a fake root-root inode that points to the root dir */
	root = NULL;
	chain = &root;
	lfn_reset();
	if (fs->root_cluster) {
		add_file(fs, &chain, NULL, 0, &fp_root);
	} else {
	for (i = 0; i < fs->root_entries; i++)
		add_file(fs, &chain, NULL, fs->root_start + i * sizeof(DIR_ENT),
			&fp_root);
	}
	lfn_reset();

	/* Walk the root dir */
	walk_file_mappings(wf, root);
	if (wf->err || wf->wf_db_err)
		goto out;

	/* Now inject the root inode */
	insert_inode(&wf->base, wf->ino, INO_TYPE_DIR, wf->wf_dirpath, NULL,
		     NULL, NULL, NULL, NULL);
	if (wf->wf_db_err)
		goto out;
	wf->ino++;

	/* Now walk it */
	n = file_cd(&fp_root, (char *)root->dir_ent.name);
	wf->err = scan_dir(fs, root, n);
	if (wf->err)
		goto out;

	walk_dir(fs, root->first, n, walk_fs_helper, wf);
	if (wf->err || wf->wf_db_err)
		goto out;
out:
	return;
}

#define INVALID_CLUSTER		(~0U)
static void walk_freesp(struct fatmap_t *wf)
{
	DOS_FS *fs = wf->fs;
	FAT_ENTRY ent;
	uint32_t cluster;
	uint32_t freestart = INVALID_CLUSTER;

	for (cluster = 0; cluster < fs->clusters; cluster++) {
		get_fat(&ent, fs->fat, cluster, fs);
		if (ent.value == 0 && freestart == INVALID_CLUSTER)
			freestart = cluster;
		else if (ent.value && freestart != INVALID_CLUSTER) {
			insert_extent(&wf->base, INO_FREESP_FILE,
				      freestart * fs->cluster_size, NULL,
				      (cluster - freestart) * fs->cluster_size,
				      0, EXT_TYPE_FREESP);
			if (wf->wf_db_err)
				goto out;
			freestart = INVALID_CLUSTER;
		}
	}
	if (freestart != INVALID_CLUSTER) {
		insert_extent(&wf->base, INO_FREESP_FILE,
			      freestart * fs->cluster_size, NULL,
			      (cluster - freestart) * fs->cluster_size,
			      0, EXT_TYPE_FREESP);
		if (wf->wf_db_err)
			goto out;
	}
out:
	return;
}

#define INJECT_METADATA(parent_ino, path, ino, name, type) \
	do { \
		inject_metadata(&wf->base, (parent_ino), (path), (ino), (name), (type)); \
		if (wf->wf_db_err) \
			goto out; \
	} while(0);

#define INJECT_ROOT_METADATA(suffix, type) \
	INJECT_METADATA(INO_METADATA_DIR, "/" STR_METADATA_DIR, INO_##suffix, STR_##suffix, type)

/* Walk the metadata */
static void walk_metadata(struct fatmap_t *wf)
{
	DOS_FS *fs = wf->fs;

	INJECT_METADATA(ROOT_DIR_INO, "", INO_METADATA_DIR, \
			STR_METADATA_DIR, INO_TYPE_DIR);
	INJECT_ROOT_METADATA(SB_FILE, INO_TYPE_METADATA);
	insert_extent(&wf->base, INO_SB_FILE, 0, NULL,
		      fs->cluster_size, 0, EXT_TYPE_METADATA);
	if (wf->wf_db_err)
		goto out;
	INJECT_ROOT_METADATA(PRIMARY_FAT_FILE, INO_TYPE_METADATA);
	insert_extent(&wf->base, INO_PRIMARY_FAT_FILE, fs->fat_start, NULL,
		      fs->fat_size, 0, EXT_TYPE_METADATA);
	if (wf->wf_db_err)
		goto out;
	INJECT_ROOT_METADATA(BACKUP_FAT_FILE, INO_TYPE_METADATA);
	insert_extent(&wf->base, INO_BACKUP_FAT_FILE,
		      fs->fat_start + fs->fat_size, NULL,
		      fs->fat_size, 0, EXT_TYPE_METADATA);
	if (wf->wf_db_err)
		goto out;
	INJECT_ROOT_METADATA(FREESP_FILE, INO_TYPE_FREESP);
	walk_freesp(wf);
	if (wf->wf_db_err)
		goto out;
out:
	return;
}

#define CHECK_ERROR(msg) \
do { \
	if (wf.err) { \
		pdie("%s %s", strerror(errno), (msg)); \
		goto out; \
	} \
	if (wf.wf_db_err) { \
		die("%s %s", sqlite3_errstr(wf.wf_db_err), (msg)); \
		goto out; \
	} \
} while (0);

int main(int argc, char *argv[])
{
	const char *dbfile;
	const char *fsdev;
	char *errm;
	struct fatmap_t wf;
	sqlite3 *db = NULL;
	DOS_FS fsb, *fs = &fsb;
	int db_err = 0;
	uint64_t total_bytes;
	int err = 0, err2;

	if (argc != 3) {
		printf("Usage: %s dbfile fsdevice\n", argv[0]);
		return 0;
	}

	set_dos_codepage(-1);

	/* Open things */
	memset(&fsb, 0, sizeof(fsb));
	memset(&wf, 0, sizeof(wf));
	dbfile = argv[1];
	fsdev = argv[2];

	db_err = truncate(dbfile, 0);
	if (db_err && errno != ENOENT) {
		perror(dbfile);
		goto out;
	}

	fs_open((char *)fsdev, rw);
	read_boot(fs);
	read_fat(fs);

	err = sqlite3_open_v2(dbfile, &db,
			      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
			      "unix-excl");
	if (err) {
		die("%s while opening database",
			sqlite3_errstr(err));
		goto out;
	}

	wf.wf_iconv = iconv_open("UTF-8", "UTF-8");
	wf.fs = fs;
	wf.wf_db = db;
	wf.ino = ROOT_DIR_INO;

	/* Prepare and clean out database. */
	prepare_db(&wf.base);
	CHECK_ERROR("while preparing database");
	wf.wf_db_err = sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &errm);
	if (errm) {
		pdie("%s while starting transaction", errm);
		free(errm);
		goto out;
	}
	CHECK_ERROR("while starting fs analysis database transaction");

	total_bytes = (uint64_t)fs->clusters * fs->cluster_size;

	collect_fs_stats(&wf.base, (char *)fsdev, fs->cluster_size,
			fs->cluster_size, total_bytes,
			(uint64_t)fs->free_clusters * fs->cluster_size,
			0, 0, FAT_MAX_NAME_LEN, "FAT");
	CHECK_ERROR("while storing fs stats");

	/* Walk the filesystem */
	walk_fs(&wf);
	CHECK_ERROR("while analyzing filesystem");

	walk_metadata(&wf);
	CHECK_ERROR("while walking metadata");

	/* Generate indexes and finalize. */
	index_db(&wf.base);
	CHECK_ERROR("while indexing database");
	finalize_fs_stats(&wf.base, (char *)fsdev);
	CHECK_ERROR("while finalizing database");
	calc_inode_stats(&wf.base);
	CHECK_ERROR("while calculating inode statistics");

	wf.wf_db_err = sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &errm);
	if (errm) {
		fprintf(stderr, "%s %s", errm, "while ending transaction");
		free(errm);
		goto out;
	}
	CHECK_ERROR("while flushing fs analysis database transaction");

	wf.wf_db_err = sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &errm);
	if (errm) {
		fprintf(stderr, "%s %s", errm, "while starting transaction");
		free(errm);
		goto out;
	}
	CHECK_ERROR("while starting overview cache database transaction");

	/* Cache overviews. */
	cache_overview(&wf.base, 2048);
	CHECK_ERROR("while caching CLI overview");
	cache_overview(&wf.base, 65536);
	CHECK_ERROR("while caching GUI overview");
	wf.wf_db_err = sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &errm);
	if (errm) {
		pdie("%s while ending transaction", errm);
		free(errm);
		goto out;
	}
	CHECK_ERROR("while flushing overview cache database transaction");

out:
	if (wf.wf_iconv)
		iconv_close(wf.wf_iconv);

	err2 = sqlite3_close(db);
	if (err2)
		die("%s while closing database",
				sqlite3_errstr(err2));
	if (!err && err2)
		err = err2;

	return err;
}
