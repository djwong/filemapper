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
#include <unistd.h>
#include <iconv.h>
#include <sqlite3.h>
#include <ext2fs/ext2fs.h>
#undef DEBUG
#include "filemapper.h"

struct e2map_t {
	struct filemapper_t base;

	ext2_filsys fs;
	errcode_t err;
	ext2fs_inode_bitmap iseen;
	ext2_ino_t ino;
	struct ext2fs_extent last;
	int type;
};
#define wf_db		base.db
#define wf_db_err	base.db_err
#define wf_dirpath	base.dirpath
#define wf_iconv	base.iconv

#define EXT2_XT_METADATA	(EXT2_FT_MAX + 16)
#define EXT2_XT_EXTENT		(EXT2_FT_MAX + 17)
#define EXT2_XT_XATTR		(EXT2_FT_MAX + 18)

static int type_codes[] = {
	[EXT2_FT_REG_FILE]	= INO_TYPE_FILE,
	[EXT2_FT_DIR]		= INO_TYPE_DIR,
	[EXT2_FT_SYMLINK]	= INO_TYPE_SYMLINK,
	[EXT2_XT_METADATA]	= INO_TYPE_METADATA,
};

#ifndef EXT4_INLINE_DATA_FL
# define EXT4_INLINE_DATA_FL	0x10000000 /* Inode has inline data */
#endif

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

#define H(name, type) {INO_##name, STR_##name, EXT2_##type}
static struct hidden_file hidden_inodes[] = {
	H(BADBLOCKS_FILE, XT_METADATA),
	H(USR_QUOTA_FILE, XT_METADATA),
	H(GRP_QUOTA_FILE, XT_METADATA),
	H(BOOTLOADER_FILE, XT_METADATA),
	H(UNDELETE_DIR, FT_DIR),
	H(RESIZE_FILE, XT_METADATA),
	H(JOURNAL_FILE, XT_METADATA),
	H(EXCLUDE_FILE, XT_METADATA),
	H(REPLICA_FILE, XT_METADATA),
	{},
};
#undef H

/* Time handling stuff */
#define EXT4_EPOCH_BITS 2
#define EXT4_EPOCH_MASK ((1 << EXT4_EPOCH_BITS) - 1)
#define EXT4_NSEC_MASK  (~0UL << EXT4_EPOCH_BITS)

/*
 * Extended fields will fit into an inode if the filesystem was formatted
 * with large inodes (-I 256 or larger) and there are not currently any EAs
 * consuming all of the available space. For new inodes we always reserve
 * enough space for the kernel's known extended fields, but for inodes
 * created with an old kernel this might not have been the case. None of
 * the extended inode fields is critical for correct filesystem operation.
 * This macro checks if a certain field fits in the inode. Note that
 * inode-size = GOOD_OLD_INODE_SIZE + i_extra_isize
 */
#define EXT4_FITS_IN_INODE(ext4_inode, field)		\
	((offsetof(typeof(*ext4_inode), field) +	\
	  sizeof((ext4_inode)->field))			\
	<= (EXT2_GOOD_OLD_INODE_SIZE +			\
	    (ext4_inode)->i_extra_isize))		\

static inline __u32 ext4_encode_extra_time(const struct timespec *time)
{
	__u32 extra = sizeof(time->tv_sec) > 4 ?
			((time->tv_sec - (__s32)time->tv_sec) >> 32) &
			EXT4_EPOCH_MASK : 0;
	return extra | (time->tv_nsec << EXT4_EPOCH_BITS);
}

static inline void ext4_decode_extra_time(struct timespec *time, __u32 extra)
{
	if (sizeof(time->tv_sec) > 4 && (extra & EXT4_EPOCH_MASK)) {
		__u64 extra_bits = extra & EXT4_EPOCH_MASK;
		/*
		 * Prior to kernel 3.14?, we had a broken decode function,
		 * wherein we effectively did this:
		 * if (extra_bits == 3)
		 *     extra_bits = 0;
		 */
		time->tv_sec += extra_bits << 32;
	}
	time->tv_nsec = ((extra) & EXT4_NSEC_MASK) >> EXT4_EPOCH_BITS;
}

#define EXT4_INODE_GET_XTIME(xtime, timespec, raw_inode)		       \
do {									       \
	(timespec)->tv_sec = (signed)((raw_inode)->xtime);		       \
	if (EXT4_FITS_IN_INODE(raw_inode, xtime ## _extra))		       \
		ext4_decode_extra_time((timespec),			       \
				       (raw_inode)->xtime ## _extra);	       \
	else								       \
		(timespec)->tv_nsec = 0;				       \
} while (0)

#define EXT4_EINODE_GET_XTIME(xtime, timespec, raw_inode)		       \
do {									       \
	if (EXT4_FITS_IN_INODE(raw_inode, xtime))			       \
		(timespec)->tv_sec =					       \
			(signed)((raw_inode)->xtime);			       \
	if (EXT4_FITS_IN_INODE(raw_inode, xtime ## _extra))		       \
		ext4_decode_extra_time((timespec),			       \
				       (raw_inode)->xtime ## _extra);	       \
	else								       \
		(timespec)->tv_nsec = 0;				       \
} while (0)

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
	struct e2map_t *wf = priv_data;
	unsigned long long max_extent = MAX_EXTENT_LENGTH / fs->blocksize;
	uint64_t loff;

	/* Internal node? */
	if (blockcnt < 0) {
		if (wf->last.e_len)
			loff = (wf->last.e_lblk + wf->last.e_len) *
				fs->blocksize;
		else
			loff = 0;
		dbg_printf("R: ino=%d pblk=%llu\n", wf->ino, *blocknr);
		insert_extent(&wf->base, wf->ino, *blocknr * fs->blocksize,
			      &loff, fs->blocksize, 0,
			      extent_codes[EXT2_XT_EXTENT]);
		if (wf->wf_db_err)
			goto out;
		return 0;
	}

	/* Can we attach it to the previous extent? */
	if (wf->last.e_len) {
		if (wf->last.e_pblk + wf->last.e_len == *blocknr &&
		    wf->last.e_len + 1 <= max_extent) {
			wf->last.e_len++;
			dbg_printf("R: ino=%d len=%u\n", wf->ino,
				   wf->last.e_len);
			return 0;
		}

		/* Insert the extent */
		dbg_printf("R: ino=%d pblk=%llu lblk=%llu len=%u\n", wf->ino,
			   wf->last.e_pblk, wf->last.e_lblk, wf->last.e_len);
		loff = wf->last.e_lblk * fs->blocksize;
		insert_extent(&wf->base, wf->ino,
			      wf->last.e_pblk * fs->blocksize,
			      &loff,
			      wf->last.e_len * fs->blocksize,
			      0, extent_codes[wf->type]);
		if (wf->wf_db_err)
			goto out;
	}

	/* Set up the next extent */
	wf->last.e_pblk = *blocknr;
	wf->last.e_lblk = blockcnt;
	wf->last.e_len = 1;

out:
	if (wf->wf_db_err)
		return BLOCK_ABORT;
	return 0;
}

/* Walk a file's extents for extents */
static void walk_extents(struct e2map_t *wf, ext2_ino_t ino, int type)
{
	ext2_filsys		fs = wf->fs;
	ext2_extent_handle_t	handle;
	struct ext2fs_extent	extent, last;
	int			flags;
	unsigned long long	max_extent = MAX_EXTENT_LENGTH / fs->blocksize;
	uint64_t		loff;

	memset(&last, 0, sizeof(last));
	wf->err = ext2fs_extent_open(fs, ino, &handle);
	if (wf->err)
		return;

	wf->err = ext2fs_extent_get(handle, EXT2_EXTENT_ROOT, &extent);
	if (wf->err) {
		if (wf->err == EXT2_ET_EXTENT_NO_NEXT)
			wf->err = 0;
		goto out;
	}

	do {
		if (extent.e_flags & EXT2_EXTENT_FLAGS_SECOND_VISIT)
			goto next;

		/* Internal node */
		if (!(extent.e_flags & EXT2_EXTENT_FLAGS_LEAF)) {
			dbg_printf("ino=%d lblk=%llu\n", wf->ino,
				   extent.e_pblk);
			loff = extent.e_lblk * fs->blocksize;
			insert_extent(&wf->base, ino,
				      extent.e_pblk * fs->blocksize,
				      &loff,
				      fs->blocksize,
				      0, extent_codes[EXT2_XT_EXTENT]);
			if (wf->wf_db_err)
				goto out;
			goto next;
		}

		/* Can we attach it to the previous extent? */
		if (last.e_len) {
			if (last.e_pblk + last.e_len == extent.e_pblk &&
			    last.e_lblk + last.e_len == extent.e_lblk &&
			    (last.e_flags & EXT2_EXTENT_FLAGS_UNINIT) ==
			    (extent.e_flags & EXT2_EXTENT_FLAGS_UNINIT) &&
			    last.e_len + extent.e_len <= max_extent) {
				last.e_len += extent.e_len;
				dbg_printf("R: ino=%d len=%u\n", ino,
					   last.e_len);
				goto next;
			}

			/* Insert the extent */
			dbg_printf("R: ino=%d pblk=%llu lblk=%llu len=%u\n", ino,
				   last.e_pblk, last.e_lblk, last.e_len);
			flags = 0;
			if (last.e_flags & EXT2_EXTENT_FLAGS_UNINIT)
				flags |= EXTENT_UNWRITTEN;
			loff = last.e_lblk * fs->blocksize;
			insert_extent(&wf->base, ino,
				      last.e_pblk * fs->blocksize,
				      &loff,
				      last.e_len * fs->blocksize,
				      flags, extent_codes[type]);
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
		loff = last.e_lblk * fs->blocksize;
		insert_extent(&wf->base, ino, last.e_pblk * fs->blocksize,
			      &loff,
			      last.e_len * fs->blocksize,
			      flags, extent_codes[type]);
		if (wf->wf_db_err)
			goto out;
	}

out:
	ext2fs_extent_free(handle);
	return;
}

/* Walk a file's mappings for extents */
static void walk_file_mappings(struct e2map_t *wf, ext2_ino_t ino,
			       int type)
{
	struct ext2_inode_large	*inode;
	struct ext2_inode	*inod;
	uint32_t		*ea_magic;
	blk64_t			b;
	uint64_t		ino_offset, inode_end, ino_sz, loff;
	uint64_t		ib_sz = sizeof(uint32_t) * EXT2_N_BLOCKS;

	if (ext2fs_fast_test_inode_bitmap2(wf->iseen, ino))
		return;

	/* Read the inode */
	ino_sz = EXT2_INODE_SIZE(wf->fs->super);
	if (ino_sz < sizeof(struct ext2_inode_large))
		ino_sz = sizeof(struct ext2_inode_large);
	wf->err = ext2fs_get_memzero(ino_sz, &inode);
	if (wf->err)
		return;
	inod = (struct ext2_inode *)inode;
	wf->err = ext2fs_read_inode_full(wf->fs, ino, inod, ino_sz);
	if (wf->err)
		goto out;

	/* Where is this inode in the FS? */
	ino_offset = inode_offset(wf->fs, ino);
	inode_end = inode->i_extra_isize;
	insert_extent(&wf->base, ino, ino_offset, 0,
		      EXT2_INODE_SIZE(wf->fs->super),
		      EXTENT_SHARED | EXTENT_NOT_ALIGNED,
		      extent_codes[EXT2_XT_METADATA]);
	if (wf->wf_db_err)
		goto out;

	/* inline xattr? */
	ea_magic = (uint32_t *)(((char *)inode) + inode_end);
	if (ext2fs_le32_to_cpu(ea_magic) == EXT2_EXT_ATTR_MAGIC) {
		insert_extent(&wf->base, ino, ino_offset + inode_end,
			      NULL, EXT2_INODE_SIZE(wf->fs->super) - inode_end,
			      EXTENT_SHARED | EXTENT_NOT_ALIGNED,
			      extent_codes[EXT2_XT_XATTR]);
		if (wf->wf_db_err)
			goto out;
	}

	/* external xattr? */
	b = ext2fs_file_acl_block(wf->fs, inod);
	if (b) {
		insert_extent(&wf->base, ino, b * wf->fs->blocksize,
			      NULL, wf->fs->blocksize, 0,
			      extent_codes[EXT2_XT_XATTR]);
		if (wf->wf_db_err)
			goto out;
	}

	if (inode->i_flags & EXT4_INLINE_DATA_FL ||
	    type == EXT2_FT_SYMLINK) {
		/* inline data file or symlink? */
		size_t sz = EXT2_I_SIZE(inode);
		loff = 0;
		insert_extent(&wf->base, ino,
			      ino_offset + offsetof(struct ext2_inode, i_block),
			      &loff, sz > ib_sz ? ib_sz : sz,
			      EXTENT_SHARED | EXTENT_DATA_INLINE | EXTENT_NOT_ALIGNED,
			      extent_codes[type]);
		if (wf->wf_db_err)
			goto out;

		/* inline data in xattr? */
		if (sz <= ib_sz)
			goto out;
		insert_extent(&wf->base, ino, ino_offset + inode_end,
			      &ib_sz, EXT2_INODE_SIZE(wf->fs->super) - inode_end,
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
		if (wf->last.e_len > 0) {
			dbg_printf("R: ino=%d pblk=%llu lblk=%llu len=%u\n",
				   wf->ino, wf->last.e_pblk, wf->last.e_lblk,
				   wf->last.e_len);
			loff = wf->last.e_lblk * wf->fs->blocksize;
			insert_extent(&wf->base, wf->ino,
				      wf->last.e_pblk * wf->fs->blocksize,
				      &loff,
				      wf->last.e_len * wf->fs->blocksize,
				      0, extent_codes[wf->type]);
			if (wf->wf_db_err)
				goto out;
		}
	}

out:
	ext2fs_free_mem(&inode);
	ext2fs_fast_mark_inode_bitmap2(wf->iseen, ino);
	return;
}

/* Handle a directory entry */
static int walk_fs_helper(ext2_ino_t dir, int entry,
			  struct ext2_dir_entry *dirent, int offset,
			  int blocksize, char *buf, void *priv_data)
{
	char path[PATH_MAX + 1];
	char name[EXT2_NAME_LEN + 1];
	const char *old_dirpath;
	int type, sz;
	struct ext2_dir_entry_2 *de2 = (struct ext2_dir_entry_2 *)dirent;
	struct e2map_t *wf = priv_data;
	struct ext2_inode_large inode;
	struct timespec tv;
	time_t atime, crtime, ctime, mtime;
	time_t *pcrtime;
	ssize_t size;

	if (!strcmp(dirent->name, ".") || !strcmp(dirent->name, ".."))
		return 0;

	sz = icvt(&wf->base, dirent->name, dirent->name_len & 0xFF, name,
		  EXT2_NAME_LEN);
	if (sz < 0)
		return DIRENT_ABORT;
	dbg_printf("dir=%d name=%s/%s ino=%d type=%d\n", dir, wf->wf_dirpath, name,
		   dirent->inode, de2->file_type);

	memset(&inode, 0, sizeof(inode));
	wf->err = ext2fs_read_inode_full(wf->fs, dirent->inode,
					 (struct ext2_inode *)&inode,
					 sizeof(inode));
	if (wf->err)
		return DIRENT_ABORT;

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
		if (S_ISREG(inode.i_mode))
			type = EXT2_FT_REG_FILE;
		else if (S_ISDIR(inode.i_mode))
			type = EXT2_FT_DIR;
		else if (S_ISLNK(inode.i_mode))
			type = EXT2_FT_SYMLINK;
		else
			return 0;
	}

	EXT4_INODE_GET_XTIME(i_atime, &tv, &inode);
	atime = tv.tv_sec;
	EXT4_INODE_GET_XTIME(i_mtime, &tv, &inode);
	mtime = tv.tv_sec;
	EXT4_INODE_GET_XTIME(i_ctime, &tv, &inode);
	ctime = tv.tv_sec;
	EXT4_EINODE_GET_XTIME(i_crtime, &tv, &inode);
	crtime = tv.tv_sec;
	pcrtime = (EXT4_FITS_IN_INODE(&inode, i_crtime) ? &crtime : NULL);
	size = EXT2_I_SIZE(&inode);

	if (dir)
		snprintf(path, PATH_MAX, "%s/%s", wf->wf_dirpath, name);
	else
		path[0] = 0;
	insert_inode(&wf->base, dirent->inode, type_codes[type], path, &atime,
		     pcrtime, &ctime, &mtime, &size);
	if (wf->wf_db_err)
		return DIRENT_ABORT;
	if (dir)
		insert_dentry(&wf->base, dir, name, dirent->inode);
	if (wf->wf_db_err)
		return DIRENT_ABORT;

	walk_file_mappings(wf, dirent->inode, type);
	if (wf->err || wf->wf_db_err)
		return DIRENT_ABORT;

	if (type == EXT2_FT_DIR) {
		errcode_t err;

		old_dirpath = wf->wf_dirpath;
		wf->wf_dirpath = path;
		err = ext2fs_dir_iterate2(wf->fs, dirent->inode, 0, NULL,
					  walk_fs_helper, wf);
		if (!wf->err)
			wf->err = err;
		wf->wf_dirpath = old_dirpath;
	}
	if (wf->err || wf->wf_db_err)
		return DIRENT_ABORT;

	return 0;
}

/* Walk the whole FS, looking for inodes to analyze. */
static void walk_fs(struct e2map_t *wf)
{
	ext2_filsys fs = wf->fs;
	struct ext2_dir_entry dirent;

	wf->wf_dirpath = "";

	dirent.inode = EXT2_ROOT_INO;
	ext2fs_set_rec_len(fs, EXT2_DIR_REC_LEN(1), &dirent);
	dirent.name_len = (EXT2_FT_DIR << 8) | 1;
	dirent.name[0] = '/';
	dirent.name[1] = 0;
	walk_fs_helper(0, 0, &dirent, 0, 0, NULL, wf);
}

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
static void walk_bitmap(struct e2map_t *wf, int64_t ino, ext2fs_block_bitmap bm)
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

		insert_extent(&wf->base, ino, start * wf->fs->blocksize,
			      NULL, (out - start) * wf->fs->blocksize,
			      EXTENT_SHARED, extent_codes[EXT2_XT_METADATA]);

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
static void walk_metadata(struct e2map_t *wf)
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
	INJECT_ROOT_METADATA(SB_FILE, EXT2_XT_METADATA);
	INJECT_ROOT_METADATA(GDT_FILE, EXT2_XT_METADATA);
	INJECT_ROOT_METADATA(BBITMAP_FILE, EXT2_XT_METADATA);
	INJECT_ROOT_METADATA(IBITMAP_FILE, EXT2_XT_METADATA);
	INJECT_ROOT_METADATA(ITABLE_FILE, EXT2_XT_METADATA);
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
					EXT2_XT_METADATA);
			insert_extent(&wf->base, ino, s * fs->blocksize,
				      NULL, fs->blocksize, EXTENT_SHARED,
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
					EXT2_XT_METADATA);
			insert_extent(&wf->base, ino, o * fs->blocksize,
				      NULL, u * fs->blocksize, EXTENT_SHARED,
				      extent_codes[EXT2_XT_METADATA]);
			if (wf->wf_db_err)
				goto out;
			ino--;
		}

		/* Record new style group descriptors */
		if (n) {
			ext2fs_fast_mark_block_bitmap_range2(sb_gdt, n, u);
			INJECT_METADATA(group_ino, path, ino, "descriptor",
					EXT2_XT_METADATA);
			insert_extent(&wf->base, ino, n * fs->blocksize,
				      NULL, u * fs->blocksize, EXTENT_SHARED,
				      extent_codes[EXT2_XT_METADATA]);
			if (wf->wf_db_err)
				goto out;
			ino--;
		}

		/* Record block bitmap */
		s = ext2fs_block_bitmap_loc(fs, group);
		ext2fs_fast_mark_block_bitmap2(sb_bbitmap, s);
		INJECT_METADATA(group_ino, path, ino, "block_bitmap",
				EXT2_XT_METADATA);
		insert_extent(&wf->base, ino, s * fs->blocksize, NULL,
			      fs->blocksize, EXTENT_SHARED,
			      extent_codes[EXT2_XT_METADATA]);
		if (wf->wf_db_err)
			goto out;
		ino--;

		/* Record inode bitmap */
		s = ext2fs_inode_bitmap_loc(fs, group);
		ext2fs_fast_mark_block_bitmap2(sb_ibitmap, s);
		INJECT_METADATA(group_ino, path, ino, "inode_bitmap",
				EXT2_XT_METADATA);
		insert_extent(&wf->base, ino, s * fs->blocksize, NULL,
			      fs->blocksize, EXTENT_SHARED,
			      extent_codes[EXT2_XT_METADATA]);
		if (wf->wf_db_err)
			goto out;
		ino--;

		/* Record inode table */
		s = ext2fs_inode_table_loc(fs, group);
		ext2fs_fast_mark_block_bitmap_range2(sb_itable, s,
				fs->inode_blocks_per_group);
		INJECT_METADATA(group_ino, path, ino, "inodes",
				EXT2_XT_METADATA);
		insert_extent(&wf->base, ino, s * fs->blocksize, NULL,
			      fs->inode_blocks_per_group * fs->blocksize,
			      EXTENT_SHARED, extent_codes[EXT2_XT_METADATA]);
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

#define CHECK_ERROR(msg) \
do { \
	if (wf.err) { \
		com_err(fsdev, wf.err, (msg)); \
		goto out; \
	} \
	if (wf.wf_db_err) { \
		com_err(dbfile, 0, "%s %s", sqlite3_errstr(wf.wf_db_err), (msg)); \
		goto out; \
	} \
} while (0);

int main(int argc, char *argv[])
{
	const char *dbfile;
	const char *fsdev;
	char *errm;
	struct e2map_t wf;
	sqlite3 *db = NULL;
	ext2_filsys fs = NULL;
	int db_err = 0;
	errcode_t err = 0, err2;
	uint64_t total_bytes;

	if (argc != 3) {
		printf("Usage: %s dbfile fsdevice\n", argv[0]);
		return 0;
	}

	add_error_table(&et_ext2_error_table);

	/* Open things */
	memset(&wf, 0, sizeof(wf));
	dbfile = argv[1];
	fsdev = argv[2];

	db_err = truncate(dbfile, 0);
	if (db_err && errno != ENOENT) {
		com_err(fsdev, errno, "while truncating database.");
		goto out;
	}

	err = ext2fs_open2(fsdev, NULL, EXT2_FLAG_64BITS | EXT2_FLAG_SKIP_MMP,
			   0, 0, unix_io_manager, &fs);
	if (err) {
		com_err(fsdev, err, "while opening filesystem.");
		goto out;
	}
	fs->default_bitmap_type = EXT2FS_BMAP64_RBTREE;

	err = sqlite3_open_v2(dbfile, &db,
			      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
			      "unix-excl");
	if (err) {
		com_err(dbfile, 0, "%s while opening database",
			sqlite3_errstr(err));
		goto out;
	}

	wf.wf_iconv = iconv_open("UTF-8", "UTF-8");
	wf.wf_db = db;
	wf.fs = fs;

	/* Prepare and clean out database. */
	prepare_db(&wf.base);
	CHECK_ERROR("while preparing database");
	wf.wf_db_err = sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &errm);
	if (errm) {
		com_err(dbfile, 0, "%s while starting transaction", errm);
		free(errm);
		goto out;
	}
	if (wf.wf_db_err) {
		com_err(dbfile, 0, "%s while starting transaction",
				sqlite3_errstr(wf.wf_db_err));
		goto out;
	}
	total_bytes = ext2fs_blocks_count(fs->super) * fs->blocksize;
	collect_fs_stats(&wf.base, fs->device_name, fs->blocksize,
			 fs->fragsize, total_bytes,
			 ext2fs_free_blocks_count(fs->super) * fs->blocksize,
			 fs->super->s_inodes_count,
			 fs->super->s_free_inodes_count,
			 EXT2_NAME_LEN, "ext4");
	CHECK_ERROR("while storing fs stats");

	/* Walk the filesystem */
	wf.err = ext2fs_allocate_inode_bitmap(fs, "visited inodes", &wf.iseen);
	CHECK_ERROR("while allocating scanned inode bitmap");
	walk_fs(&wf);
	CHECK_ERROR("while analyzing filesystem");

	/* Walk the metadata */
	walk_metadata(&wf);
	CHECK_ERROR("while analyzing metadata");

	/* Generate indexes and finalize. */
	index_db(&wf.base);
	CHECK_ERROR("while indexing database");
	finalize_fs_stats(&wf.base, fs->device_name);
	CHECK_ERROR("while finalizing database");
	calc_inode_stats(&wf.base);
	CHECK_ERROR("while calculating inode statistics");

	/* Cache overviews. */
	cache_overview(&wf.base, total_bytes, 2048);
	CHECK_ERROR("while caching CLI overview");
	cache_overview(&wf.base, total_bytes, 65536);
	CHECK_ERROR("while caching GUI overview");
	wf.wf_db_err = sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &errm);
	if (errm) {
		com_err(dbfile, 0, "%s while ending transaction", errm);
		free(errm);
		goto out;
	}
	if (wf.wf_db_err) {
		com_err(dbfile, 0, "%s while ending transaction",
				sqlite3_errstr(wf.wf_db_err));
		goto out;
	}
out:
	if (wf.iseen)
		ext2fs_free_inode_bitmap(wf.iseen);
	if (wf.wf_iconv)
		iconv_close(wf.wf_iconv);

	err2 = sqlite3_close(db);
	if (err2)
		com_err(dbfile, 0, "%s while closing database",
			sqlite3_errstr(err2));
	if (!err && err2)
		err = err2;

	err2 = fs ? ext2fs_close_free(&fs) : 0;
	if (err2)
		com_err(fsdev, err2, "while closing filesystem.");

	if (!err && err2)
		err = err2;

	return err;
}
