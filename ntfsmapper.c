/*
 * Generate filemapper databases from ntfs filesystems.
 * Copyright 2015 Darrick J. Wong.
 * Licensed under the GPLv2.
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
#include <ntfs-3g/types.h>
#include <ntfs-3g/volume.h>
#include <ntfs-3g/dir.h>
#include <ntfs-3g/bitmap.h>
#undef DEBUG
#include "filemapper.h"

struct ntfsmap_t {
	struct filemapper_t base;

	ntfs_volume *fs;
	int err;
	u64 dir_ino;
	u64 total_inodes;
	u8 *ino_bmap;
};
#define wf_db		base.db
#define wf_db_err	base.db_err
#define wf_dirpath	base.dirpath
#define wf_iconv	base.iconv

/* These bits are more or less copied from ntfsprogs. */

static const char *invalid_ntfs_msg =
"The device '%s' doesn't have a valid NTFS.\n"
"Maybe you selected the wrong device? Or the whole disk instead of a\n"
"partition (e.g. /dev/hda, not /dev/hda1)? Or the other way around?\n";

static const char *corrupt_volume_msg =
"NTFS is inconsistent. Run chkdsk /f on Windows then reboot it TWICE!\n"
"The usage of the /f parameter is very IMPORTANT! No modification was\n"
"made to NTFS by this software.\n";

static const char *hibernated_volume_msg =
"The NTFS partition is hibernated. Please resume Windows and turned it \n"
"off properly, so mounting could be done safely.\n";

static const char *unclean_journal_msg =
"Access is denied because the NTFS journal file is unclean. Choices are:\n"
" A) Shutdown Windows properly.\n"
" B) Click the 'Safely Remove Hardware' icon in the Windows taskbar\n"
"    notification area before disconnecting the device.\n"
" C) Use 'Eject' from Windows Explorer to safely remove the device.\n"
" D) If you ran chkdsk previously then boot Windows again which will\n"
"    automatically initialize the journal.\n"
" E) Submit 'force' option (WARNING: This solution it not recommended).\n"
" F) ntfsmount: Mount the volume read-only by using the 'ro' mount option.\n";

static const char *opened_volume_msg =
"Access is denied because the NTFS volume is already exclusively opened.\n"
"The volume may be already mounted, or another software may use it which\n"
"could be identified for example by the help of the 'fuser' command.\n";

static const char *fakeraid_msg =
"You seem to have a SoftRAID/FakeRAID hardware and must use an activated,\n"
"different device under /dev/mapper, (e.g. /dev/mapper/nvidia_eahaabcc1)\n"
"to mount NTFS. Please see the 'dmraid' documentation for help.\n";

/* Map NTFS attributes to extent types */
static int extent_codes(ntfs_inode *inode, int attr_type)
{
	if (MREF(inode->mft_no) < FILE_first_user)
		return EXT_TYPE_METADATA;

	switch (attr_type) {
	case AT_EA:
	case AT_EA_INFORMATION:
	case AT_ATTRIBUTE_LIST:
		return EXT_TYPE_XATTR;
	case AT_DATA:
		return EXT_TYPE_FILE;
	case AT_INDEX_ALLOCATION:
		return EXT_TYPE_DIR;
	case AT_BITMAP:
	case AT_REPARSE_POINT:
	case AT_LOGGED_UTILITY_STREAM:
		return EXT_TYPE_METADATA;
	default:
		abort();
	}
}

/* Walk a file's mappings for extents */
static void walk_file_mappings(struct ntfsmap_t *wf, ntfs_inode *inode)
{
	ntfs_attr_search_ctx *ctx;
	runlist *runs = NULL, *r;
	unsigned long long p_block, l_block, e_len;
	unsigned long long max_extent = MAX_EXTENT_LENGTH / wf->fs->cluster_size;
	uint64_t loff;

	if (ntfs_bit_get(wf->ino_bmap, inode->mft_no))
		return;

	ctx = ntfs_attr_get_search_ctx(inode, NULL);
	if (!ctx) {
		wf->err = errno;
		return;
	}

	while (!ntfs_attr_lookup(AT_UNUSED, NULL, 0, 0, 0, NULL, 0, ctx)) {
		if (!ctx->attr->non_resident)
			continue;
		runs = ntfs_mapping_pairs_decompress(wf->fs, ctx->attr, NULL);
		if (!runs) {
			wf->err = errno;
			goto out;
		}
		p_block = l_block = e_len = 0;
		for (r = runs; r->length > 0; r++) {
			if (r->lcn < 0)
				continue;
			if (e_len > 0) {
				if (p_block + e_len == r->lcn &&
				    l_block + e_len == r->vcn &&
				    e_len + r->length <= max_extent) {
					e_len += r->length;
					dbg_printf("R: ino=%d len=%u\n",
						   inode->mft_no, e_len);
					continue;
				}

				dbg_printf("R: ino=%"PRIu64" type=0x%x vcn=%"PRIu64" lcn=%"PRIu64" len=%"PRIu64"\n",
					inode->mft_no, ctx->attr->type,
					p_block, l_block, e_len);
				loff = l_block * wf->fs->cluster_size;
				insert_extent(&wf->base, inode->mft_no,
					      p_block * wf->fs->cluster_size,
					      &loff,
					      e_len * wf->fs->cluster_size,
					      0,
					      extent_codes(inode, ctx->attr->type));
				if (wf->wf_db_err)
					goto out;
			}
			p_block = r->lcn;
			l_block = r->vcn;
			e_len = r->length;
		}

		if (e_len > 0) {
			dbg_printf("R: ino=%"PRIu64" type=0x%x vcn=%"PRIu64" lcn=%"PRIu64" len=%"PRIu64"\n",
				inode->mft_no, ctx->attr->type,
				p_block, l_block, e_len);
			loff = l_block * wf->fs->cluster_size;
			insert_extent(&wf->base, inode->mft_no,
				      p_block * wf->fs->cluster_size,
				      &loff,
				      e_len * wf->fs->cluster_size,
				      0,
				      extent_codes(inode, ctx->attr->type));
			if (wf->wf_db_err)
				goto out;
		}

		free(runs);
		runs = NULL;
	}

out:
	free(runs);
	ntfs_bit_set(wf->ino_bmap, inode->mft_no, 1);
	ntfs_attr_put_search_ctx(ctx);
	return;
}

/* Handle a directory entry */
static int walk_fs_helper(void *priv_data, const ntfschar * de_name,
			  const int de_name_len, const int name_type,
			  const s64 pos, const MFT_REF mref,
			  const unsigned dt_type)
{
	char path[PATH_MAX + 1];
	char name[NTFS_MAX_NAME_LEN + 1], *p;
	int type;
	struct ntfsmap_t *wf = priv_data;
	ntfs_inode *ni = NULL;
	time_t atime, crtime, ctime, mtime;
	struct timespec ts;

	/* Skip the 8.3 names */
	if ((name_type & FILE_NAME_WIN32_AND_DOS) == FILE_NAME_DOS)
		return 0;

	p = name;
	if (de_name) {
		if (ntfs_ucstombs(de_name, de_name_len, &p, NTFS_MAX_NAME_LEN) < 0) {
			wf->err = errno;
			ntfs_log_error("Cannot represent filename in locale.");
			return -1;
		}
	} else {
		name[0] = 0;
	}

	if (!strcmp(name, ".") || !strcmp(name, ".."))
		return 0;

	ni = ntfs_inode_open(wf->fs, mref);
	if (!ni) {
		wf->err = errno;
		return -1;
	}

	if (de_name && MREF(mref) < FILE_first_user) {
		type = INO_TYPE_METADATA;
		goto have_type;
	}

	switch (dt_type) {
	case NTFS_DT_REG:
		type = INO_TYPE_FILE;
		break;
	case NTFS_DT_DIR:
		type = INO_TYPE_DIR;
		break;
	case NTFS_DT_LNK:
		type = INO_TYPE_SYMLINK;
		break;
	default:
		ntfs_inode_close(ni);
		return 0;
	}

have_type:
	dbg_printf("dir=%"PRIu64" name=%s/%s nametype=0x%x ino=%"PRIu64" type=%d\n",
		   wf->dir_ino, wf->wf_dirpath, name, name_type, ni->mft_no, type);

	ts = ntfs2timespec(ni->last_access_time);
	atime = ts.tv_sec;
	ts = ntfs2timespec(ni->creation_time);
	crtime = ts.tv_sec;
	ts = ntfs2timespec(ni->last_mft_change_time);
	ctime = ts.tv_sec;
	ts = ntfs2timespec(ni->last_data_change_time);
	mtime = ts.tv_sec;

	if (de_name)
		snprintf(path, PATH_MAX, "%s/%s", wf->wf_dirpath, name);
	else
		path[0] = 0;
	insert_inode(&wf->base, ni->mft_no, type, path, &atime, &crtime, &ctime,
		     &mtime, &ni->data_size);
	if (wf->wf_db_err)
		goto err;
	if (de_name)
		insert_dentry(&wf->base, wf->dir_ino, name, ni->mft_no);
	if (wf->wf_db_err)
		goto err;

	walk_file_mappings(wf, ni);
	if (wf->err || wf->wf_db_err)
		goto err;

	if (type == INO_TYPE_DIR) {
		const char *old_dirpath;
		u64 old_dir_ino;
		s64 pos = 0;
		int err;

		old_dir_ino = wf->dir_ino;
		old_dirpath = wf->wf_dirpath;
		wf->wf_dirpath = path;
		wf->dir_ino = ni->mft_no;
		err = ntfs_readdir(ni, &pos, wf, walk_fs_helper);
		if (!wf->err)
			wf->err = err;
		wf->wf_dirpath = old_dirpath;
		wf->dir_ino = old_dir_ino;
	}
	if (wf->err || wf->wf_db_err)
		goto err;

	ntfs_inode_close(ni);
	return 0;
err:
	ntfs_inode_close(ni);
	return -1;
}

/* Walk the whole FS, looking for inodes to analyze. */
static void walk_fs(struct ntfsmap_t *wf)
{
	wf->wf_dirpath = "";
	wf->ino_bmap = calloc(1, wf->total_inodes / 8);
	if (!wf->ino_bmap) {
		wf->err = ENOMEM;
		return;
	}

	walk_fs_helper(wf, NULL, 0, FILE_NAME_WIN32, 0, FILE_root, NTFS_DT_DIR);

	free(wf->ino_bmap);
	wf->ino_bmap = NULL;
}

#define CHECK_ERROR(msg) \
do { \
	if (wf.err) { \
		ntfs_log_error("%s %s", strerror(errno), (msg)); \
		goto out; \
	} \
	if (wf.wf_db_err) { \
		ntfs_log_error("%s %s", sqlite3_errstr(wf.wf_db_err), (msg)); \
		goto out; \
	} \
} while (0);

int main(int argc, char *argv[])
{
	const char *dbfile;
	const char *fsdev;
	char *errm;
	struct ntfsmap_t wf;
	sqlite3 *db = NULL;
	ntfs_volume *fs = NULL;
	int db_err = 0;
	uint64_t total_bytes, size;
	int err = 0, err2, delta_bits;

	if (argc != 3) {
		printf("Usage: %s dbfile fsdevice\n", argv[0]);
		return 0;
	}

	/* Open things */
	memset(&wf, 0, sizeof(wf));
	dbfile = argv[1];
	fsdev = argv[2];

	db_err = truncate(dbfile, 0);
	if (db_err && errno != ENOENT) {
		perror(dbfile);
		goto out;
	}

	ntfs_log_set_handler(ntfs_log_handler_stderr);

	fs = ntfs_mount(fsdev, NTFS_MNT_RDONLY | NTFS_MNT_FORENSIC);
	if (!fs) {
		ntfs_log_perror("Failed to mount '%s'", fsdev);
		if (errno == EINVAL)
			ntfs_log_error(invalid_ntfs_msg, fsdev);
		else if (errno == EIO)
			ntfs_log_error("%s", corrupt_volume_msg);
		else if (errno == EPERM)
			ntfs_log_error("%s", hibernated_volume_msg);
		else if (errno == EOPNOTSUPP)
			ntfs_log_error("%s", unclean_journal_msg);
		else if (errno == EBUSY)
			ntfs_log_error("%s", opened_volume_msg);
		else if (errno == ENXIO)
			ntfs_log_error("%s", fakeraid_msg);
		goto out;
	}

	err = sqlite3_open_v2(dbfile, &db,
			      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
			      "unix-excl");
	if (err) {
		ntfs_log_error("%s while opening database",
			sqlite3_errstr(err));
		goto out;
	}

	ntfs_set_char_encoding("utf8");
	wf.wf_iconv = iconv_open("UTF-8", "UTF-8");
	wf.fs = fs;
	wf.wf_db = db;

	/* Prepare and clean out database. */
	prepare_db(&wf.base);
	CHECK_ERROR("while preparing database");
	wf.wf_db_err = sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &errm);
	if (errm) {
		ntfs_log_error("%s while starting transaction", errm);
		free(errm);
		goto out;
	}
	CHECK_ERROR("while starting fs analysis database transaction");

	ntfs_volume_get_free_space(fs);
	total_bytes = fs->nr_clusters * fs->cluster_size;
		
	/* Free inodes on the free space */
	size = fs->free_clusters;
	delta_bits = fs->cluster_size_bits - fs->mft_record_size_bits;
	if (delta_bits >= 0)
		size <<= delta_bits;
	else
		size >>= -delta_bits;

	/* Number of inodes at this point in time. */
	wf.total_inodes = (fs->mftbmp_na->allocated_size << 3) + size;
	
	/* Free inodes available for all and for non-privileged processes. */
	size += fs->free_mft_records;
	if (size < 0)
		size = 0;

	collect_fs_stats(&wf.base, fs->dev->d_name, fs->cluster_size,
			fs->cluster_size, total_bytes,
			fs->free_clusters * fs->cluster_size,
			wf.total_inodes, size, NTFS_MAX_NAME_LEN, "NTFS");
	CHECK_ERROR("while storing fs stats");

	/* Walk the filesystem */
	walk_fs(&wf);
	CHECK_ERROR("while analyzing filesystem");

	/* Generate indexes and finalize. */
	index_db(&wf.base);
	CHECK_ERROR("while indexing database");
	finalize_fs_stats(&wf.base, fs->dev->d_name);
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
		ntfs_log_error("%s while ending transaction", errm);
		free(errm);
		goto out;
	}
	CHECK_ERROR("while flushing overview cache database transaction");

out:
	if (wf.wf_iconv)
		iconv_close(wf.wf_iconv);

	err2 = sqlite3_close(db);
	if (err2)
		ntfs_log_error("%s while closing database",
				sqlite3_errstr(err2));
	if (!err && err2)
		err = err2;

	err2 = ntfs_umount(fs, FALSE);
	if (err2)
		ntfs_log_error("%s %s", strerror(err2),
				"while closing filesystem.\n");

	if (!err && err2)
		err = err2;

	return err;
}
