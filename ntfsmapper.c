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

#ifndef MS_RECOVER
#define MS_RECOVER	0x10000000
#endif

#ifndef MS_FORENSIC
#define MS_FORENSIC	0x04000000 /* No modification during mount */
#endif

#ifndef MS_RDONLY
#define MS_RDONLY	1
#endif

struct ntfsmap_t {
	struct filemapper_t base;

	ntfs_volume *fs;
	int err;
	u64 dir_ino;
	u64 total_inodes;
	u8 *ino_bmap;
#if 0
	ext2_filsys fs;
	errcode_t err;
	ext2fs_inode_bitmap iseen;
	ext2_ino_t ino;
	struct ext2fs_extent last;
	int type;
#endif
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


#if 0
static int type_codes[] = {
	[EXT2_FT_REG_FILE]	= 0,
	[EXT2_FT_DIR]		= 1,
	[EXT2_FT_SYMLINK]	= 3,
};

#ifndef EXT4_INLINE_DATA_FL
# define EXT4_INLINE_DATA_FL	0x10000000 /* Inode has inline data */
#endif

#define EXT2_XT_METADATA	(EXT2_FT_MAX + 16)
#define EXT2_XT_EXTENT		(EXT2_FT_MAX + 17)
#define EXT2_XT_XATTR		(EXT2_FT_MAX + 18)

static int extent_codes[] = {
	[EXT2_FT_REG_FILE]	= EXT_TYPE_FILE,
	[EXT2_FT_DIR]		= EXT_TYPE_DIR,
	[EXT2_XT_EXTENT]	= EXT_TYPE_EXTENT,
	[EXT2_XT_METADATA]	= EXT_TYPE_METADATA,
	[EXT2_XT_XATTR]		= EXT_TYPE_XATTR,
	[EXT2_FT_SYMLINK]	= EXT_TYPE_SYMLINK,
};

/* Fake inodes for FS metadata */
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
#define INO_HIDDEN_DIR		(-7)
#define STR_HIDDEN_DIR		"hidden_files"
/* This must come last */
#define INO_GROUPS_DIR		(-8)
#define STR_GROUPS_DIR		"groups"

/* Hidden inode paths */
#define INO_BADBLOCKS_FILE	EXT2_BAD_INO
#define STR_BADBLOCKS_FILE	"badblocks"
#define INO_USR_QUOTA_FILE	EXT4_USR_QUOTA_INO
#define STR_USR_QUOTA_FILE	"user_quota"
#define INO_GRP_QUOTA_FILE	EXT4_GRP_QUOTA_INO
#define STR_GRP_QUOTA_FILE	"group_quota"
#define INO_BOOTLOADER_FILE	EXT2_BOOT_LOADER_INO
#define STR_BOOTLOADER_FILE	"bootloader"
#define INO_UNDELETE_DIR	EXT2_UNDEL_DIR_INO
#define STR_UNDELETE_DIR	"undelete"
#define INO_RESIZE_FILE		EXT2_RESIZE_INO
#define STR_RESIZE_FILE		"resize"
#define INO_JOURNAL_FILE	EXT2_JOURNAL_INO
#define STR_JOURNAL_FILE	"journal"
#define INO_EXCLUDE_FILE	EXT2_EXCLUDE_INO
#define STR_EXCLUDE_FILE	"exclude"
#define INO_REPLICA_FILE	EXT4_REPLICA_INO
#define STR_REPLICA_FILE	"replica"

struct hidden_file {
	ext2_ino_t ino;
	const char *name;
	int type;
};

#define H(name, type) {INO_##name, STR_##name, EXT2_FT_##type}
static struct hidden_file hidden_inodes[] = {
	H(BADBLOCKS_FILE, REG_FILE),
	H(USR_QUOTA_FILE, REG_FILE),
	H(GRP_QUOTA_FILE, REG_FILE),
	H(BOOTLOADER_FILE, REG_FILE),
	H(UNDELETE_DIR, DIR),
	H(RESIZE_FILE, REG_FILE),
	H(JOURNAL_FILE, REG_FILE),
	H(EXCLUDE_FILE, REG_FILE),
	H(REPLICA_FILE, REG_FILE),
	{},
};
#undef H

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

/* Help find extents... */
static int find_blocks(ext2_filsys fs, blk64_t *blocknr, e2_blkcnt_t blockcnt,
		       blk64_t ref_blk, int ref_offset, void *priv_data)
{
	struct ntfsmap_t *wf = priv_data;

	/* Internal node? */
	if (blockcnt < 0) {
		dbg_printf("ino=%d free=%llu\n", wf->ino, *blocknr);
		wf->wf_db_err = insert_extent(&wf->base, wf->ino,
					   *blocknr * fs->blocksize,
					   0, fs->blocksize, 0,
					   extent_codes[EXT2_XT_EXTENT]);
		if (wf->wf_db_err)
			goto out;
		return 0;
	}

	/* Can we attach it to the previous extent? */
	if (wf->last.e_len) {
		blk64_t end = wf->last.e_len + 1;

		if (wf->last.e_pblk + wf->last.e_len == *blocknr &&
		    end < (1ULL << 32)) {
			wf->last.e_len++;
			dbg_printf("R: ino=%d len=%u\n", wf->ino,
				   wf->last.e_len);
			return 0;
		}

		/* Insert the extent */
		dbg_printf("R: ino=%d pblk=%llu lblk=%llu len=%u\n", wf->ino,
			   *blocknr, blockcnt, 1);
		wf->wf_db_err = insert_extent(&wf->base, wf->ino,
				   wf->last.e_pblk * fs->blocksize,
				   wf->last.e_lblk * fs->blocksize,
				   wf->last.e_len * fs->blocksize,
				   0,
				   extent_codes[wf->type]);
		if (wf->wf_db_err)
			goto out;
	}

	/* Set up the next extent */
	wf->last.e_pblk = *blocknr;
	wf->last.e_lblk = blockcnt;
	wf->last.e_len = 1;
	dbg_printf("R: ino=%d pblk=%llu lblk=%llu len=%u\n", wf->ino, *blocknr,
		   blockcnt, 1);

out:
	if (wf->wf_db_err)
		return BLOCK_ABORT;
	return 0;
}

/* Walk a file's extents for extents */
static void walk_extents(struct ntfsmap_t *wf, ext2_ino_t ino, int type)
{
	ext2_filsys		fs = wf->fs;
	ext2_extent_handle_t	handle;
	struct ext2fs_extent	extent, last;
	int			flags;

	memset(&last, 0, sizeof(last));
	wf->err = ext2fs_extent_open(fs, ino, &handle);
	if (wf->err)
		return;

	wf->err = ext2fs_extent_get(handle, EXT2_EXTENT_ROOT, &extent);
	if (wf->err)
		goto out;

	do {
		if (extent.e_flags & EXT2_EXTENT_FLAGS_SECOND_VISIT)
			goto next;

		/* Internal node */
		if (!(extent.e_flags & EXT2_EXTENT_FLAGS_LEAF)) {
			dbg_printf("ino=%d free=%llu\n", wf->ino,
				   extent.e_pblk);
			wf->wf_db_err = insert_extent(&wf->base, ino,
						   extent.e_pblk * fs->blocksize,
						   extent.e_lblk * fs->blocksize,
						   fs->blocksize,
						   0,
						   extent_codes[EXT2_XT_EXTENT]);
			if (wf->wf_db_err)
				goto out;
			goto next;
		}

		/* Can we attach it to the previous extent? */
		if (last.e_len) {
			blk64_t end = last.e_len + extent.e_len;
			if (last.e_pblk + last.e_len == extent.e_pblk &&
			    last.e_lblk + last.e_len == extent.e_lblk &&
			    (last.e_flags & EXT2_EXTENT_FLAGS_UNINIT) ==
			    (extent.e_flags & EXT2_EXTENT_FLAGS_UNINIT) &&
			    end < (1ULL << 32)) {
				last.e_len += extent.e_len;
				dbg_printf("R: ino=%d len=%u\n", ino,
					   last.e_len);
				goto next;
			}

			/* Insert the extent */
			dbg_printf("R: ino=%d pblk=%llu lblk=%llu len=%u\n", ino,
				   extent.e_pblk, extent.e_lblk, extent.e_len);
			flags = 0;
			if (last.e_flags & EXT2_EXTENT_FLAGS_UNINIT)
				flags |= EXTENT_UNWRITTEN;
			wf->wf_db_err = insert_extent(&wf->base, ino,
						   last.e_pblk * fs->blocksize,
						   last.e_lblk * fs->blocksize,
						   last.e_len * fs->blocksize,
						   flags,
						   extent_codes[type]);
			if (wf->wf_db_err)
				goto out;
		}

		/* Start recording extents */
		last = extent;
next:
		wf->err = ext2fs_extent_get(handle, EXT2_EXTENT_NEXT, &extent);
	} while (wf->err == 0);

	/* Ok if we run off the end */
	if (wf->err == EXT2_ET_EXTENT_NO_NEXT)
		wf->err = 0;
	if (wf->err)
		goto out;

	/* Insert the last extent */
	if (last.e_len) {
		dbg_printf("R: ino=%d pblk=%llu lblk=%llu len=%u\n", ino,
			   last.e_pblk, last.e_lblk, last.e_len);
		flags = 0;
		if (last.e_flags & EXT2_EXTENT_FLAGS_UNINIT)
			flags |= EXTENT_UNWRITTEN;
		wf->wf_db_err = insert_extent(&wf->base, ino,
					   last.e_pblk * fs->blocksize,
					   last.e_lblk * fs->blocksize,
					   last.e_len * fs->blocksize,
					   flags,
					   extent_codes[type]);
		if (wf->wf_db_err)
			goto out;
	}

out:
	ext2fs_extent_free(handle);
	return;
}
#endif

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
	runlist *runs = NULL;
	int i;

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
		for (i = 0; runs[i].length > 0; i++) {
			if (runs[i].lcn < 0)
				continue;
			insert_extent(&wf->base, inode->mft_no,
				      runs[i].lcn * wf->fs->cluster_size,
				      runs[i].vcn * wf->fs->cluster_size,
				      runs[i].length * wf->fs->cluster_size,
				      0,
				      extent_codes(inode, ctx->attr->type));
			if (wf->wf_db_err)
				goto out;
			dbg_printf("ino=%"PRIu64" type=0x%x vcn=%"PRIu64" lcn=%"PRIu64" len=%"PRIu64"\n",
				inode->mft_no, ctx->attr->type, runs[i].vcn,
				runs[i].lcn, runs[i].length);
		}
		free(runs);
		runs = NULL;
	}

out:
	free(runs);
	ntfs_bit_set(wf->ino_bmap, inode->mft_no, 1);
	ntfs_attr_put_search_ctx(ctx);
	return;
#if 0
	/* Where is this inode in the FS? */
	ino_offset = inode_offset(wf->fs, ino);
	inode_end = inode->i_extra_isize;
	wf->wf_db_err = insert_extent(&wf->base, ino, ino_offset, 0,
				   EXT2_INODE_SIZE(wf->fs->super),
				   EXTENT_SHARED | EXTENT_NOT_ALIGNED,
				   extent_codes[EXT2_XT_METADATA]);
	if (wf->wf_db_err)
		goto out;

	/* inline xattr? */
	ea_magic = (uint32_t *)(((char *)inode) + inode_end);
	if (ext2fs_le32_to_cpu(ea_magic) == EXT2_EXT_ATTR_MAGIC) {
		wf->wf_db_err = insert_extent(&wf->base, ino,
					   ino_offset + inode_end,
					   0,
					   EXT2_INODE_SIZE(wf->fs->super) - inode_end,
					   EXTENT_SHARED | EXTENT_NOT_ALIGNED,
					   extent_codes[EXT2_XT_XATTR]);
		if (wf->wf_db_err)
			goto out;
	}

	/* external xattr? */
	b = ext2fs_file_acl_block(wf->fs, inod);
	if (b) {
		wf->wf_db_err = insert_extent(&wf->base, ino, b * wf->fs->blocksize,
					   0, wf->fs->blocksize, 0,
					   extent_codes[EXT2_XT_XATTR]);
		if (wf->wf_db_err)
			goto out;
	}

	if (inode->i_flags & EXT4_INLINE_DATA_FL ||
	    type == EXT2_FT_SYMLINK) {
		/* inline data file or symlink? */
		size_t sz = EXT2_I_SIZE(inode);
		wf->wf_db_err = insert_extent(&wf->base, ino,
				   ino_offset + offsetof(struct ext2_inode, i_block),
				   0, sz > ib_sz ? ib_sz : sz,
				   EXTENT_SHARED | EXTENT_DATA_INLINE | EXTENT_NOT_ALIGNED,
				   extent_codes[type]);
		if (wf->wf_db_err)
			goto out;

		/* inline data in xattr? */
		if (sz <= ib_sz)
			goto out;
		wf->wf_db_err = insert_extent(&wf->base, ino,
				   ino_offset + inode_end,
				   0,
				   EXT2_INODE_SIZE(wf->fs->super) - inode_end,
				   EXTENT_SHARED | EXTENT_DATA_INLINE | EXTENT_NOT_ALIGNED,
				   extent_codes[type]);
		if (wf->wf_db_err)
			goto out;
	} else if (inode->i_flags & EXT4_EXTENTS_FL) {
		/* extent file */
		walk_extents(wf, ino, type);
	} else {
		errcode_t err;

		wf->last.e_len = 0;
		wf->ino = ino;
		wf->type = type;
		err = ext2fs_block_iterate3(wf->fs, ino, BLOCK_FLAG_READ_ONLY,
					    0, find_blocks, wf);
		if (!wf->err)
			wf->err = err;
	}

out:
	ext2fs_free_mem(&inode);
	ext2fs_fast_mark_inode_bitmap2(wf->iseen, ino);
	return;
#endif
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

	/* Skip the 8.3 names */
	if ((name_type & FILE_NAME_WIN32_AND_DOS) == FILE_NAME_DOS)
		return 0;

	p = name;
	if (ntfs_ucstombs(de_name, de_name_len, &p, NTFS_MAX_NAME_LEN) < 0) {
		wf->err = errno;
		ntfs_log_error("Cannot represent filename in locale.");
		return -1;
	}

	if (!strcmp(name, ".") || !strcmp(name, ".."))
		return 0;

	ni = ntfs_inode_open(wf->fs, mref);
	if (!ni) {
		wf->err = errno;
		return -1;
	}

	if (MREF(mref) < FILE_first_user) {
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

	snprintf(path, PATH_MAX, "%s/%s", wf->wf_dirpath, name);
	insert_inode(&wf->base, ni->mft_no, type, path);
	if (wf->wf_db_err)
		goto err;
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
	ntfs_volume *fs = wf->fs;
	ntfs_inode *ni;
	s64 pos = 0;
	int err;

	wf->wf_dirpath = "";
	wf->ino_bmap = calloc(1, wf->total_inodes / 8);
	if (!wf->ino_bmap) {
		wf->err = ENOMEM;
		return;
	}

	ni = ntfs_pathname_to_inode(fs, NULL, "/");
	if (!ni) {
		wf->err = ENOENT;
		goto out;
	}
	wf->dir_ino = ni->mft_no;

	insert_inode(&wf->base, ni->mft_no, INO_TYPE_DIR, wf->wf_dirpath);
	if (wf->wf_db_err)
		goto out;

	walk_file_mappings(wf, ni);
	if (wf->err || wf->wf_db_err)
		goto out;

	err = ntfs_readdir(ni, &pos, wf, walk_fs_helper);
	if (!wf->err)
		wf->err = err;
	if (wf->err || wf->wf_db_err)
		goto out;
out:
	free(wf->ino_bmap);
	wf->ino_bmap = NULL;
	ntfs_inode_close(ni);
}

#if 0
#define INJECT_METADATA(parent_ino, path, ino, name, type) \
	do { \
		inject_metadata(&wf->base, (parent_ino), (path), (ino), (name), type_codes[(type)]); \
		if (wf->wf_db_err) \
			goto out; \
	} while(0);

#define INJECT_ROOT_METADATA(suffix, type) \
	INJECT_METADATA(INO_METADATA_DIR, "/" STR_METADATA_DIR, INO_##suffix, STR_##suffix, type)

#define INJECT_GROUP(ino, path, type) \
	INJECT_METADATA(INO_GROUPS_DIR, "/" STR_METADATA_DIR "/" STR_GROUPS_DIR, (ino), (path), (type))

/* Insert extents for a file, given a bitmap */
static void walk_bitmap(struct ntfsmap_t *wf, int64_t ino, ext2fs_block_bitmap bm)
{
	blk64_t start, end, out = 0, loff = 0;

	start = 0;
	end = ext2fs_blocks_count(wf->fs->super) - 1;

	wf->err = ext2fs_find_first_set_block_bitmap2(bm, start, end, &out);
	while (wf->err == 0) {
		start = out;
		wf->err = ext2fs_find_first_zero_block_bitmap2(bm, start,
							       end, &out);
		if (wf->err == ENOENT) {
			out = end;
			wf->err = 0;
		} else if (wf->err)
			break;

		wf->wf_db_err = insert_extent(&wf->base, ino,
					   start * wf->fs->blocksize,
					   loff,
					   (out - start) * wf->fs->blocksize,
					   EXTENT_SHARED,
					   extent_codes[EXT2_XT_METADATA]);

		if (wf->wf_db_err)
			break;
		start = out;
		loff += (out - start) * wf->fs->blocksize;
		wf->err = ext2fs_find_first_set_block_bitmap2(bm, start,
				end, &out);
	}

	if (wf->err == ENOENT)
		wf->err = 0;
}

/* Invent a FS tree for metadata. */
static void walk_metadata(struct ntfsmap_t *wf)
{
	ext2_filsys fs = wf->fs;
	dgrp_t group;
	int64_t ino, group_ino;
	blk64_t s, o, n, first_data_block;
	blk_t u;
	struct ext2_inode inode;
	char path[PATH_MAX + 1];
	uint32_t zero_buf[EXT2_N_BLOCKS];
	ext2fs_block_bitmap sb_bmap, sb_gdt, sb_bbitmap, sb_ibitmap, sb_itable;
	struct hidden_file *hf;
	int w;

	INJECT_METADATA(EXT2_ROOT_INO, "", INO_METADATA_DIR, \
			STR_METADATA_DIR, EXT2_FT_DIR);
	INJECT_ROOT_METADATA(SB_FILE, EXT2_FT_REG_FILE);
	INJECT_ROOT_METADATA(GDT_FILE, EXT2_FT_REG_FILE);
	INJECT_ROOT_METADATA(BBITMAP_FILE, EXT2_FT_REG_FILE);
	INJECT_ROOT_METADATA(IBITMAP_FILE, EXT2_FT_REG_FILE);
	INJECT_ROOT_METADATA(ITABLE_FILE, EXT2_FT_REG_FILE);
	INJECT_ROOT_METADATA(GROUPS_DIR, EXT2_FT_DIR);
	INJECT_ROOT_METADATA(HIDDEN_DIR, EXT2_FT_DIR);

	first_data_block = fs->super->s_first_data_block;
	fs->super->s_first_data_block = 0;
	wf->err = ext2fs_allocate_block_bitmap(fs, "superblock", &sb_bmap);
	if (wf->err)
		goto out;

	wf->err = ext2fs_allocate_block_bitmap(fs, "group descriptors",
			&sb_gdt);
	if (wf->err)
		goto out;

	wf->err = ext2fs_allocate_block_bitmap(fs, "block bitmaps",
			&sb_bbitmap);
	if (wf->err)
		goto out;

	wf->err = ext2fs_allocate_block_bitmap(fs, "inode bitmaps",
			&sb_ibitmap);
	if (wf->err)
		goto out;

	wf->err = ext2fs_allocate_block_bitmap(fs, "inode tables",
			&sb_itable);
	if (wf->err)
		goto out;
	fs->super->s_first_data_block = first_data_block;

	ino = INO_GROUPS_DIR - 1;
	snprintf(path, PATH_MAX, "%d", fs->group_desc_count);
	w = strlen(path);
	for (group = 0; group < fs->group_desc_count; group++) {
		snprintf(path, PATH_MAX, "%0*d", w, group);
		group_ino = ino;
		ino--;
		INJECT_GROUP(group_ino, path, EXT2_FT_DIR);
		wf->err = ext2fs_super_and_bgd_loc2(fs, group, &s, &o, &n, &u);
		if (wf->err)
			goto out;
		snprintf(path, PATH_MAX, "/%s/%s/%0*d", STR_METADATA_DIR,
			 STR_GROUPS_DIR, w, group);

		/* Record the superblock */
		if (s || group == 0) {
			ext2fs_fast_mark_block_bitmap2(sb_bmap, s);
			INJECT_METADATA(group_ino, path, ino, "superblock",
					EXT2_FT_REG_FILE);
			wf->wf_db_err = insert_extent(&wf->base, ino,
						s * fs->blocksize,
						0, fs->blocksize,
						EXTENT_SHARED,
						extent_codes[EXT2_XT_METADATA]);
			if (wf->wf_db_err)
				goto out;
			ino--;
			u--;
		}

		/* Record old style group descriptors */
		if (o) {
			ext2fs_fast_mark_block_bitmap_range2(sb_gdt, o, u);
			INJECT_METADATA(group_ino, path, ino, "descriptor",
					EXT2_FT_REG_FILE);
			wf->wf_db_err = insert_extent(&wf->base, ino,
						o * fs->blocksize,
						0, u * fs->blocksize,
						EXTENT_SHARED,
						extent_codes[EXT2_XT_METADATA]);
			if (wf->wf_db_err)
				goto out;
			ino--;
		}

		/* Record new style group descriptors */
		if (n) {
			ext2fs_fast_mark_block_bitmap_range2(sb_gdt, n, u);
			INJECT_METADATA(group_ino, path, ino, "descriptor",
					EXT2_FT_REG_FILE);
			wf->wf_db_err = insert_extent(&wf->base, ino,
						n * fs->blocksize,
						0, u * fs->blocksize,
						EXTENT_SHARED,
						extent_codes[EXT2_XT_METADATA]);
			if (wf->wf_db_err)
				goto out;
			ino--;
		}

		/* Record block bitmap */
		s = ext2fs_block_bitmap_loc(fs, group);
		ext2fs_fast_mark_block_bitmap2(sb_bbitmap, s);
		INJECT_METADATA(group_ino, path, ino, "block_bitmap",
				EXT2_FT_REG_FILE);
		wf->wf_db_err = insert_extent(&wf->base, ino, s * fs->blocksize,
					0, fs->blocksize,
					EXTENT_SHARED,
					extent_codes[EXT2_XT_METADATA]);
		if (wf->wf_db_err)
			goto out;
		ino--;

		/* Record inode bitmap */
		s = ext2fs_inode_bitmap_loc(fs, group);
		ext2fs_fast_mark_block_bitmap2(sb_ibitmap, s);
		INJECT_METADATA(group_ino, path, ino, "inode_bitmap",
				EXT2_FT_REG_FILE);
		wf->wf_db_err = insert_extent(&wf->base, ino, s * fs->blocksize,
					0, fs->blocksize,
					EXTENT_SHARED,
					extent_codes[EXT2_XT_METADATA]);
		if (wf->wf_db_err)
			goto out;
		ino--;

		/* Record inode table */
		s = ext2fs_inode_table_loc(fs, group);
		ext2fs_fast_mark_block_bitmap_range2(sb_itable, s,
				fs->inode_blocks_per_group);
		INJECT_METADATA(group_ino, path, ino, "inodes",
				EXT2_FT_REG_FILE);
		wf->wf_db_err = insert_extent(&wf->base, ino, s * fs->blocksize,
					0,
					fs->inode_blocks_per_group * fs->blocksize,
					EXTENT_SHARED,
					extent_codes[EXT2_XT_METADATA]);
		if (wf->wf_db_err)
			goto out;
		ino--;
	}

	/* Emit extents for the overall files */
	walk_bitmap(wf, INO_SB_FILE, sb_bmap);
	if (wf->err || wf->wf_db_err)
		goto out;
	walk_bitmap(wf, INO_GDT_FILE, sb_gdt);
	if (wf->err || wf->wf_db_err)
		goto out;
	walk_bitmap(wf, INO_BBITMAP_FILE, sb_bbitmap);
	if (wf->err || wf->wf_db_err)
		goto out;
	walk_bitmap(wf, INO_IBITMAP_FILE, sb_ibitmap);
	if (wf->err || wf->wf_db_err)
		goto out;
	walk_bitmap(wf, INO_ITABLE_FILE, sb_itable);
	if (wf->err || wf->wf_db_err)
		goto out;

	/* Now go for the hidden files */
	memset(zero_buf, 0, sizeof(zero_buf));
	snprintf(path, PATH_MAX, "/%s/%s", STR_METADATA_DIR, STR_HIDDEN_DIR);
	for (hf = hidden_inodes; hf->ino != 0; hf++) {
		wf->err = ext2fs_read_inode(wf->fs, hf->ino, &inode);
		if (wf->err)
			goto out;
		if (!memcmp(zero_buf, inode.i_block, sizeof(zero_buf)))
			continue;
		INJECT_METADATA(INO_HIDDEN_DIR, path, hf->ino, hf->name,
				hf->type);

		walk_file_mappings(wf, hf->ino, hf->type);
		if (wf->err || wf->wf_db_err)
			goto out;

		if (hf->type == EXT2_FT_DIR) {
			errcode_t err;

			err = ext2fs_dir_iterate2(fs, hf->ino, 0, NULL,
						  walk_fs_helper, wf);
			if (!wf->err)
				wf->err = err;
			if (wf->err || wf->wf_db_err)
				goto out;
		}
	}
out:
	ext2fs_free_block_bitmap(sb_itable);
	ext2fs_free_block_bitmap(sb_ibitmap);
	ext2fs_free_block_bitmap(sb_bbitmap);
	ext2fs_free_block_bitmap(sb_gdt);
	ext2fs_free_block_bitmap(sb_bmap);
	return;
}
#endif

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

	memset(&wf, 0, sizeof(wf));
	ntfs_set_char_encoding("utf8");
	wf.wf_iconv = iconv_open("UTF-8", "UTF-8");
	wf.fs = fs;
	wf.wf_db = db;

	/* Prepare and clean out database. */
	prepare_db(&wf.base);
	CHECK_ERROR("while preparing database");
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
			wf.total_inodes, size, NTFS_MAX_NAME_LEN);
	CHECK_ERROR("while storing fs stats");

	/* Walk the filesystem */
	walk_fs(&wf);
	CHECK_ERROR("while analyzing filesystem");

#if 0
	/* Walk the metadata */
	walk_metadata(&wf);
	CHECK_ERROR("while analyzing metadata");
#endif

	/* Generate indexes and finalize. */
	index_db(&wf.base);
	CHECK_ERROR("while indexing database");
	finalize_fs_stats(&wf.base, fs->dev->d_name);
	CHECK_ERROR("while finalizing database");

	/* Cache overviews. */
	cache_overview(&wf.base, total_bytes, 2048);
	CHECK_ERROR("while caching CLI overview");
	cache_overview(&wf.base, total_bytes, 65536);
	CHECK_ERROR("while caching GUI overview");
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
