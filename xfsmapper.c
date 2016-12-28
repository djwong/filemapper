/*
 * Generate filemapper databases from ntfs filesystems.
 * Copyright 2016 Darrick J. Wong.
 * Licensed under the GPLv2.
 */
#undef DEBUG
#include <libxfs.h>
#include <repair/btree.h>
#include <signal.h>
#include <libgen.h>
#include "filemapper.h"
#include "compdbvfs.h"

#define XFS_FSBLOCK_TO_BYTES(fs, fsblock) \
		(XFS_FSB_TO_DADDR((fs), (fsblock)) << BBSHIFT)

#ifdef XFS_ALLOC_SET_ASIDE
#define xfs_alloc_set_aside XFS_ALLOC_SET_ASIDE
#endif

struct xfs_extent_t
{
	unsigned long long	p_off;
	unsigned long long	l_off;
	unsigned long long	len;
	xfs_exntst_t		state;
	int			unaligned:1;
	int			inlinedata:1;
	int			extentmap:1;
};

struct big_bmap;

struct xfsmap {
	struct filemapper_t	base;

	struct xfs_mount	*fs;
	int			err;
	struct big_bmap		*ino_bmap;
	struct big_bmap		*bbmap;
	xfs_agnumber_t		agno;
	struct xfs_extent_t	last_ext;
	int			itype;
};
#define wf_db			base.db
#define wf_db_err		base.db_err
#define wf_dirpath		base.dirpath
#define wf_iconv		base.iconv

#define XFS_NAME_LEN		255

#define XFS_DIR3_XT_METADATA	(XFS_DIR3_FT_MAX + 16)
#define XFS_DIR3_XT_EXTENT	(XFS_DIR3_FT_MAX + 17)
#define XFS_DIR3_XT_XATTR	(XFS_DIR3_FT_MAX + 18)
#define XFS_DIR3_XT_FREESP	(XFS_DIR3_FT_MAX + 19)

static int type_codes[] = {
	[XFS_DIR3_FT_REG_FILE]	= INO_TYPE_FILE,
	[XFS_DIR3_FT_DIR]	= INO_TYPE_DIR,
	[XFS_DIR3_FT_SYMLINK]	= INO_TYPE_SYMLINK,
	[XFS_DIR3_XT_METADATA]	= INO_TYPE_METADATA,
	[XFS_DIR3_XT_FREESP]	= INO_TYPE_FREESP,
};

static int extent_codes[] = {
	[XFS_DIR3_FT_REG_FILE]	= EXT_TYPE_FILE,
	[XFS_DIR3_FT_DIR]	= EXT_TYPE_DIR,
	[XFS_DIR3_FT_SYMLINK]	= EXT_TYPE_SYMLINK,
	[XFS_DIR3_XT_METADATA]	= EXT_TYPE_METADATA,
	[XFS_DIR3_XT_EXTENT]	= EXT_TYPE_EXTENT,
	[XFS_DIR3_XT_XATTR]	= EXT_TYPE_XATTR,
	[XFS_DIR3_XT_FREESP]	= EXT_TYPE_FREESP,
};

typedef int (*dentry_walk_fn)(xfs_ino_t dir, const char *dname,
			      size_t dname_len, xfs_ino_t ino, int file_type,
			      void *priv_data);

typedef int (*extent_walk_fn)(int64_t ino, struct xfs_extent_t *extent,
			      void *priv_data);

#ifdef STRICT_PUTBUF
void xfsmapper_putbuf(struct xfs_buf *bp)
{
	free(bp->b_addr);
	bp->b_addr = NULL;
	free(bp->b_map);
	bp->b_map = NULL;
	libxfs_putbuf(bp);
}
#else
# define xfsmapper_putbuf	libxfs_putbuf
#endif

/* FS-wide bitmap */

#define	BBMAP_UNUSED	0
#define BBMAP_INUSE	1
#define BBMAP_BAD	2
static int big_bmap_states[] = {0xDEAD, 0xBEEF, 0xBAAD};
struct big_bmap {
	struct xfs_mount	*fs;
	struct btree_root	**bmap;
	xfs_agblock_t		*bmap_sizes;
	xfs_agnumber_t		sz;
	int			multiplier;
};

static int big_block_bmap_init(struct xfs_mount *fs, struct big_bmap **bbmap);
static int big_inode_bmap_init(struct xfs_mount *fs, struct big_bmap **bbmap);
static void big_bmap_destroy(struct big_bmap *bbmap);
static void big_bmap_set(struct big_bmap *bbmap, xfs_agnumber_t agno,
			 xfs_agblock_t offset, xfs_extlen_t blen, int state);
static int big_bmap_test(struct big_bmap *bbmap, xfs_agnumber_t agno,
			 xfs_agblock_t offset);
#ifdef DEBUG
static void big_bmap_dump(struct big_bmap *bbmap, xfs_agnumber_t agno);
#endif

/* AG data */

struct xfs_ag {
	struct xfs_buf		*agf;
	struct xfs_buf		*agi;
	struct xfs_buf		*agfl;
};

/* buffer ops */
struct xfs_metadata_btree_ops {
	const struct xfs_buf_ops	*buf_ops;
	void 				*(*ptraddr)(struct xfs_mount *mp,
						struct xfs_btree_block *block);
};

#define XFS_METADATA_BTREE_OPS3(name, macro) \
static void *xfs_##name##_ptr( \
	struct xfs_mount	*mp, \
	struct xfs_btree_block	*block) \
{ \
	return XFS_##macro##_PTR_ADDR((block), 1, xfs_##name##_maxrecs((mp), (mp)->m_sb.sb_blocksize, 0)); \
} \
static struct xfs_metadata_btree_ops xfs_##name##_metadata_ops = { \
	.buf_ops = &xfs_##name##_buf_ops, \
	.ptraddr = xfs_##name##_ptr, \
};

#define XFS_METADATA_BTREE_OPS4(name, macro) \
static void *xfs_##name##_ptr( \
	struct xfs_mount	*mp, \
	struct xfs_btree_block	*block) \
{ \
	return XFS_##macro##_PTR_ADDR((mp), (block), 1, xfs_##name##_maxrecs((mp), (mp)->m_sb.sb_blocksize, 0)); \
} \
static struct xfs_metadata_btree_ops xfs_##name##_metadata_ops = { \
	.buf_ops = &xfs_##name##_buf_ops, \
	.ptraddr = xfs_##name##_ptr, \
};

#ifdef XFS_RMAP_CRC_MAGIC
XFS_METADATA_BTREE_OPS3(rmapbt, RMAP);
#endif
#ifdef XFS_REFC_CRC_MAGIC
XFS_METADATA_BTREE_OPS3(refcountbt, REFCOUNT);
#endif
XFS_METADATA_BTREE_OPS4(allocbt, ALLOC);
XFS_METADATA_BTREE_OPS4(inobt, INOBT);

/* Free AG information */
static void
free_ags(
	struct xfs_mount	*fs,
	struct xfs_ag		*ags)
{
	xfs_agnumber_t		agno;

	for (agno = 0; agno < fs->m_sb.sb_agcount; agno++) {
		if (ags[agno].agf)
			xfsmapper_putbuf(ags[agno].agf);
		if (ags[agno].agi)
			xfsmapper_putbuf(ags[agno].agi);
		if (ags[agno].agfl)
			xfsmapper_putbuf(ags[agno].agfl);
	}
	free(ags);
}

/* Read in AG metadata blocks */
static int
read_ags(
	struct xfs_mount	*fs,
	struct xfs_ag		**pags)
{
	struct xfs_ag		*ags;
	xfs_agnumber_t		agno;
	int			err;

	ags = calloc(fs->m_sb.sb_agcount, sizeof(struct xfs_ag));
	if (!ags)
		return ENOMEM;

	for (agno = 0; agno < fs->m_sb.sb_agcount; agno++) {
		err = xfs_read_agi(fs, NULL, agno, &ags[agno].agi);
		if (err)
			goto fail;
		err = ags[agno].agi->b_error;
		if (err)
			goto fail;
		err = xfs_read_agf(fs, NULL, agno, 0, &ags[agno].agf);
		if (err)
			goto fail;
		err = ags[agno].agf->b_error;
		if (err)
			goto fail;
		err = xfs_alloc_read_agfl(fs, NULL, agno, &ags[agno].agfl);
		if (err)
			goto fail;
		err = ags[agno].agfl->b_error;
		if (err)
			goto fail;
	}

	*pags = ags;
	return 0;
fail:
	free_ags(fs, ags);
	return err;
}

/* Fake inodes for metadata */

#define INO_METADATA_DIR	(-1)
#define STR_METADATA_DIR	"$metadata"
#define INO_SB_FILE		(-2)
#define STR_SB_FILE		"superblocks"
#define INO_BNOBT_FILE		(-3)
#define STR_BNOBT_FILE		"bnobt"
#define INO_CNTBT_FILE		(-4)
#define STR_CNTBT_FILE		"cntbt"
#define INO_INOBT_FILE		(-5)
#define STR_INOBT_FILE		"inobt"
#define INO_FINOBT_FILE		(-6)
#define STR_FINOBT_FILE		"finobt"
#define INO_RMAPBT_FILE		(-7)
#define STR_RMAPBT_FILE		"rmapbt"
#define INO_REFCOUNTBT_FILE	(-8)
#define STR_REFCOUNTBT_FILE	"refcountbt"
#define INO_FL_FILE		(-9)
#define STR_FL_FILE		"freelist"
#define INO_JOURNAL_FILE	(-10)
#define STR_JOURNAL_FILE	"journal"
#define INO_ITABLE_FILE		(-11)
#define STR_ITABLE_FILE		"inodes"
#define INO_HIDDEN_DIR		(-12)
#define STR_HIDDEN_DIR		"hidden_files"
#define INO_FREESP_FILE		(-13)
#define STR_FREESP_FILE		"freespace"
/* This must come last */
#define INO_GROUPS_DIR		(-14)
#define STR_GROUPS_DIR		"groups"

/* Hidden inode paths */
#define STR_USR_QUOTA_FILE	"user_quota"
#define STR_GRP_QUOTA_FILE	"group_quota"
#define STR_PROJ_QUOTA_FILE	"project_quota"
#define STR_RT_BMAP_FILE	"realtime_bitmap"
#define STR_RT_SUMMARY_FILE	"realtime_summary"

struct hidden_file {
	xfs_ino_t ino;
	const char *name;
	int type;
};

/* Walk a directory */

/* Iterate directory entries in an inline directory */
static int
iterate_inline_dir(
	struct xfs_inode		*ip,
	dentry_walk_fn			fn,
	void				*priv_data)
{
	xfs_dir2_sf_entry_t		*sfep;
	xfs_dir2_sf_hdr_t		*sfp;
	char				namebuf[XFS_NAME_LEN + 1];
	int				i;
	xfs_ino_t			ino;
	uint8_t				filetype;
	const struct xfs_dir_ops	*dops = xfs_dir_get_ops(ip->i_mount, ip);

	ASSERT(ip->i_df.if_flags & XFS_IFINLINE);
	/*
	 * Give up if the directory is way too short.
	 */
	if (ip->i_d.di_size < offsetof(xfs_dir2_sf_hdr_t, parent))
		return EFSCORRUPTED;

	ASSERT(ip->i_df.if_bytes == ip->i_d.di_size);
	ASSERT(ip->i_df.if_u1.if_data != NULL);

	sfp = (xfs_dir2_sf_hdr_t *)ip->i_df.if_u1.if_data;

	ASSERT(ip->i_d.di_size >= xfs_dir2_sf_hdr_size(sfp->i8count));

	sfep = xfs_dir2_sf_firstentry(sfp);
	for (i = 0; i < sfp->count; i++) {
		memcpy(namebuf, sfep->name, sfep->namelen);
		namebuf[sfep->namelen] = 0;
		ino = dops->sf_get_ino(sfp, sfep);
		filetype = dops->sf_get_ftype(sfep);
		if (fn(ip->i_ino, namebuf, sfep->namelen, ino, filetype,
				priv_data))
			break;
		sfep = dops->sf_nextentry(sfp, sfep);
	}
	return 0;
}

/* Iterate directory entries in a directory data block */
static int
iterate_dirblock(
	struct xfs_inode		*ip,
	struct xfs_buf			*bp,
	dentry_walk_fn			fn,
	void				*priv_data)
{
	char				namebuf[XFS_NAME_LEN + 1];
	xfs_dir2_data_hdr_t		*hdr;
	char				*start;
	char				*ptr, *endptr;
	xfs_dir2_data_entry_t		*dep;
	xfs_dir2_data_unused_t		*dup;
	xfs_dir2_block_tail_t		*btp = NULL;
	xfs_ino_t			ino;
	uint8_t				filetype;
	const struct xfs_dir_ops	*dops;

	dops = xfs_dir_get_ops(ip->i_mount, ip);
	hdr = bp->b_addr;
	ptr = start = (char *)dops->data_unused_p(hdr);
	switch (hdr->magic) {
	case cpu_to_be32(XFS_DIR2_BLOCK_MAGIC):
	case cpu_to_be32(XFS_DIR3_BLOCK_MAGIC):
		btp = xfs_dir2_block_tail_p(ip->i_mount->m_dir_geo, hdr);
		endptr = (char *)xfs_dir2_block_leaf_p(btp);
		if (endptr <= ptr || endptr > (char *)btp)
			endptr = (char *)hdr + ip->i_mount->m_dir_geo->blksize;
		break;
	case cpu_to_be32(XFS_DIR3_DATA_MAGIC):
	case cpu_to_be32(XFS_DIR2_DATA_MAGIC):
		endptr = (char *)hdr + ip->i_mount->m_dir_geo->blksize;
		break;
	default:
		printf("Bad directory magic %x\n", be32_to_cpu(hdr->magic));
		return EFSCORRUPTED;
	}

	while (ptr < endptr) {
		dup = (xfs_dir2_data_unused_t *)ptr;
		if (be16_to_cpu(dup->freetag) == XFS_DIR2_DATA_FREE_TAG) {
			ptr += be16_to_cpu(dup->length);
			continue;
		}
		dep = (xfs_dir2_data_entry_t *)dup;

		memcpy(namebuf, dep->name, dep->namelen);
		namebuf[dep->namelen] = 0;
		ino = be64_to_cpu(dep->inumber);
		filetype = dops->data_get_ftype(dep);
		dbg_printf("fn dino=%ld name='%s' (%d), ino=%ld ft=%d ptr=%ld entsz=%d\n",
				ip->i_ino, namebuf, dep->namelen, ino, filetype,
				ptr - (char *)hdr,
				dops->data_entsize(dep->namelen));
		if (fn(ip->i_ino, namebuf, dep->namelen, ino, filetype,
		       priv_data))
			break;
		ptr += dops->data_entsize(dep->namelen);
	}

	return 0;
}

/* Directory block verification routines for libxfs */
void xfs_verifier_error(struct xfs_buf *bp);

static void
xfsmapper_dir3_data_read_verify(
	struct xfs_buf			*bp)
{
	struct xfs_dir2_data_hdr	*hdr = bp->b_addr;

	switch (hdr->magic) {
	case cpu_to_be32(XFS_DIR2_BLOCK_MAGIC):
	case cpu_to_be32(XFS_DIR3_BLOCK_MAGIC):
		xfs_dir3_block_buf_ops.verify_read(bp);
		return;
	case cpu_to_be32(XFS_DIR2_DATA_MAGIC):
	case cpu_to_be32(XFS_DIR3_DATA_MAGIC):
		xfs_dir3_data_buf_ops.verify_read(bp);
		return;
	default:
		xfs_buf_ioerror(bp, EFSCORRUPTED);
		xfs_verifier_error(bp);
		break;
	}
}

static void
fail_write_verify(
	struct xfs_buf		*bp)
{
	abort();
}

const struct xfs_buf_ops xfsmapper_dir3_data_buf_ops = {
	.verify_read = xfsmapper_dir3_data_read_verify,
	.verify_write = fail_write_verify,
};

/* Iterate the directory entries in a given directory inode */
int iterate_directory(
	struct xfs_inode	*ip,
	dentry_walk_fn		fn,
	void			*priv_data)
{
	int			error;
	int			idx;
	struct xfs_ifork		*ifp;
	xfs_fileoff_t		off;
	xfs_extnum_t		nextents;
	int			i;
	xfs_fsblock_t		poff;
	int			dblen;
	struct xfs_bmbt_rec_host	*ep;
	xfs_filblks_t		blen;
	struct xfs_buf		*bp;

	ASSERT(XFS_IFORK_FORMAT(ip, XFS_DATA_FORK) == XFS_DINODE_FMT_BTREE ||
	       XFS_IFORK_FORMAT(ip, XFS_DATA_FORK) == XFS_DINODE_FMT_EXTENTS ||
	       XFS_IFORK_FORMAT(ip, XFS_DATA_FORK) == XFS_DINODE_FMT_LOCAL);
	if (XFS_IFORK_FORMAT(ip, XFS_DATA_FORK) == XFS_DINODE_FMT_LOCAL)
		return iterate_inline_dir(ip, fn, priv_data);

	ifp = XFS_IFORK_PTR(ip, XFS_DATA_FORK);
	if (!(ifp->if_flags & XFS_IFEXTENTS) &&
	    (error = xfs_iread_extents(NULL, ip, XFS_DATA_FORK)))
		return error;
	nextents = ifp->if_bytes / (uint)sizeof(xfs_bmbt_rec_t);
	for (idx = 0; idx < nextents; idx++) {
		ep = xfs_iext_get_ext(ifp, idx);
		blen = xfs_bmbt_get_blockcount(ep);
		off = xfs_bmbt_get_startoff(ep);
		poff = xfs_bmbt_get_startblock(ep);
		dblen = 1 << ip->i_mount->m_sb.sb_dirblklog;
		dbg_printf("EXT: poff=%ld loff=%ld len=%ld dblen=%d\n",
				poff, off, blen, dblen);

		for (i = 0; i < blen; i += dblen, off += dblen, poff += dblen) {
			/* directory entries are never higher than 32GB */
			if (off >= ip->i_mount->m_dir_geo->leafblk)
				return 0;

			bp = libxfs_readbuf(ip->i_mount->m_ddev_targp,
					XFS_FSB_TO_DADDR(ip->i_mount, poff),
					XFS_FSB_TO_BB(ip->i_mount, dblen),
					0, &xfsmapper_dir3_data_buf_ops);
			if (!bp)
				return ENOMEM;
			error = bp->b_error;
			if (error) {
				xfsmapper_putbuf(bp);
				return error;
			}

			error = iterate_dirblock(ip, bp, fn, priv_data);
			xfsmapper_putbuf(bp);
			if (error)
				return error;
		}
	}

	return 0;
}

/* Walk a btree */

/*
 * Walk the nodes of an extent btree
 */
#define XFS_BMAP_BROOT_KEY_ADDR(mp, bb, i) \
	XFS_BMBT_KEY_ADDR(mp, bb, i)

/* Walk the internal nodes of a bmap btree */
int
walk_bmap_btree_nodes(
	struct xfs_inode	*ip,
	int			whichfork,
	extent_walk_fn		fn,
	void			*priv_data)
{
	struct xfs_extent_t	xext;
	struct xfs_btree_block	*block;
	struct xfs_mount	*fs;
	__be64			*pp;
	xfs_bmbt_key_t		*kp;
	struct xfs_buf		*bp;
	struct xfs_ifork		*ifp;
	xfs_fsblock_t		bno;
	xfs_fsblock_t		next_level_bno;
	xfs_fileoff_t		kno;
	xfs_extnum_t		j;
	int			level;
	int			num_recs;
	int			error;

	memset(&xext, 0, sizeof(xext));
	bno = NULLFSBLOCK;
	fs = ip->i_mount;
	ifp = XFS_IFORK_PTR(ip, whichfork);
	block = ifp->if_broot;
	/*
	 * Root level must use BMAP_BROOT_PTR_ADDR macro to get ptr out.
	 */
	level = xfs_btree_get_level(block);
	if (level == 0)
		return 0;
	kp = XFS_BMAP_BROOT_KEY_ADDR(fs, block, 1);
	pp = XFS_BMAP_BROOT_PTR_ADDR(fs, block, 1, ifp->if_broot_bytes);
	next_level_bno = be64_to_cpu(*pp);
	if (!XFS_FSB_SANITY_CHECK(fs, next_level_bno))
		return EFSCORRUPTED;
	bp = NULL;
	do {
		/* process all the blocks in this level */
		do {
			/* process all the key/ptrs in this block */
			num_recs = xfs_btree_get_numrecs(block);
			for (j = 0; j < num_recs; j++, pp++, kp++) {
				kno = be64_to_cpu(kp->br_startoff);
				bno = be64_to_cpu(*pp);
				if (!XFS_FSB_SANITY_CHECK(fs, bno))
					goto err;
				xext.p_off = XFS_FSBLOCK_TO_BYTES(fs, bno);
				xext.l_off = XFS_FSB_TO_B(fs, kno);
				xext.len = XFS_FSB_TO_B(fs, 1);
				xext.state = XFS_EXT_NORM;
				xext.extentmap = 1;
				if (fn(ip->i_ino, &xext, priv_data)) {
					error = 0;
					goto err;
				}
			}

			/* now go to the right sibling */
			bno = be64_to_cpu(block->bb_u.l.bb_rightsib);
			if (bno == NULLFSBLOCK)
				break;
			else if (!XFS_FSB_SANITY_CHECK(fs, bno))
				goto err;
			if (bp)
				xfsmapper_putbuf(bp);
			error = xfs_btree_read_bufl(fs, NULL, bno, 0, &bp,
					XFS_BMAP_BTREE_REF, &xfs_bmbt_buf_ops);
			if (error)
				return error;
			error = EFSCORRUPTED;
			if (bp->b_error)
				goto err;
			block = XFS_BUF_TO_BLOCK(bp);
			kp = XFS_BMBT_KEY_ADDR(fs, block, 1);
			pp = XFS_BMBT_PTR_ADDR(fs, block, 1,
					fs->m_bmap_dmxr[1]);
		} while (1);

		/* now go down the tree */
		level--;
		if (level == 0)
			break;
		if (bp)
			xfsmapper_putbuf(bp);
		error = xfs_btree_read_bufl(fs, NULL, next_level_bno, 0, &bp,
				XFS_BMAP_BTREE_REF, &xfs_bmbt_buf_ops);
		if (error)
			return error;
		error = EFSCORRUPTED;
		if (bp->b_error)
			goto err;
		block = XFS_BUF_TO_BLOCK(bp);
		kp = XFS_BMBT_KEY_ADDR(fs, block, 1);
		pp = XFS_BMBT_PTR_ADDR(fs, block, 1, fs->m_bmap_dmxr[1]);
		next_level_bno = be64_to_cpu(*pp);
		if (!XFS_FSB_SANITY_CHECK(fs, next_level_bno))
			goto err;
	} while (1);
	if (bp)
		xfsmapper_putbuf(bp);
	return 0;
err:
	xfsmapper_putbuf(bp);
	return error;
}

/* Walk extents */

/* Insert an extent immediately. */
int
insert_xfs_extent(
	int64_t			ino,
	struct xfs_extent_t	*xext,
	void			*priv_data)
{
	struct xfsmap		*wf = priv_data;
	int			type;
	int			flags;
	uint64_t		loff;

	flags = 0;
	if (xext->inlinedata)
		flags |= EXTENT_DATA_INLINE;
	if (xext->unaligned)
		flags |= EXTENT_NOT_ALIGNED;
	type = extent_codes[wf->itype];
	if (xext->extentmap)
		type = EXT_TYPE_EXTENT;
	if (xext->state == XFS_EXT_UNWRITTEN)
		flags |= EXTENT_UNWRITTEN;
	loff = xext->l_off;
	insert_extent(&wf->base, ino, xext->p_off, &loff,
			xext->len, flags, type);
	if (wf->wf_db_err)
		return -1;
	return 0;
}

/* Record a metadata extent */
static int
insert_meta_extent(
	int64_t			ino,
	struct xfs_extent_t	*xext,
	void			*priv_data)
{
	struct xfsmap		*wf = priv_data;
	xfs_agblock_t		agbno;
	xfs_extlen_t		blen;
	xfs_daddr_t		daddr = xext->p_off >> BBSHIFT;

	agbno = xfs_daddr_to_agbno(wf->fs, daddr);
	blen = XFS_B_TO_FSB(wf->fs, xext->len);
	big_bmap_set(wf->bbmap, wf->agno, agbno, blen, 1);
	return 0;
}

/* Collect extents, combining adjacent ones */
static int
walk_extent_helper(
	int64_t			ino,
	struct xfs_extent_t	*extent,
	void			*priv_data)
{
	struct xfsmap		*wf = priv_data;
	struct xfs_extent_t	*last = &wf->last_ext;

	if (last->len) {
		if (last->p_off + last->len == extent->p_off &&
		    last->l_off + last->len == extent->l_off &&
		    last->state == extent->state &&
		    last->len + extent->len <= MAX_EXTENT_LENGTH) {
			last->len += extent->len;
			dbg_printf("R: ino=%ld len=%llu\n", ino,
					last->len);
			return 0;
		}

		/* Insert the extent */
		dbg_printf("R: ino=%ld pblk=%llu lblk=%llu len=%llu\n",
				ino, last->p_off, last->l_off, last->len);
		insert_xfs_extent(ino, last, wf);
		if (wf->err || wf->wf_db_err)
			return -1;
	}

	/* Start recording extents */
	*last = *extent;
	return 0;
}

/* Calculate an inode's byte position on disk. */
static unsigned long long
inode_poff(
	struct xfs_inode	*ip)
{
	return XFS_FSBLOCK_TO_BYTES(ip->i_mount,
			XFS_DADDR_TO_FSB(ip->i_mount, ip->i_imap.im_blkno)) +
			ip->i_imap.im_boffset;
}

/* Iterate the extents attached to an inode's fork */
int
iterate_fork_mappings(
	struct xfs_inode		*ip,
	int				fork,
	extent_walk_fn			fn,
	void				*priv_data)
{
	struct xfs_bmbt_irec		ext;
	struct xfs_extent_t		xext;
	struct xfs_ifork		*ifp;
	struct xfs_bmbt_rec_host	*ep;
	xfs_extnum_t			nextents;
	int				idx;
	int				err;

	switch (fork) {
	case XFS_DATA_FORK:
		break;
	case XFS_ATTR_FORK:
		if (ip->i_d.di_forkoff == 0)
			return 0;
		break;
	default:
		printf("Unknown fork %d\n", fork);
		return EFSCORRUPTED;
	}

	memset(&xext, 0, sizeof(xext));
	ifp = XFS_IFORK_PTR(ip, XFS_DATA_FORK);
	switch (XFS_IFORK_FORMAT(ip, fork)) {
	case XFS_DINODE_FMT_LOCAL:
		xext.p_off = inode_poff(ip) +
			     xfs_dinode_size(ip->i_d.di_version) +
			     (fork == XFS_ATTR_FORK ? ip->i_d.di_forkoff << 3 : 0);
		if (fork == XFS_DATA_FORK) {
			xext.len = ip->i_d.di_size;
			xext.inlinedata = 1;
		} else
			xext.len = ip->i_mount->m_sb.sb_inodesize - ip->i_d.di_forkoff;
		xext.state = XFS_EXT_NORM;
		xext.unaligned = 1;
		fn(ip->i_ino, &xext, priv_data);
		break;
	case XFS_DINODE_FMT_BTREE:
		if (!(ifp->if_flags & XFS_IFEXTENTS)) {
			err = xfs_iread_extents(NULL, ip, fork);
			if (err)
				return err;
		}
		/* read leaves... */
		err = walk_bmap_btree_nodes(ip, fork, insert_xfs_extent,
					    priv_data);
		if (err)
			return err;
		/* drop through */
	case XFS_DINODE_FMT_EXTENTS:
		nextents = ifp->if_bytes / (uint)sizeof(xfs_bmbt_rec_t);
		for (idx = 0; idx < nextents; idx++) {
			ep = xfs_iext_get_ext(ifp, idx);

			xfs_bmbt_get_all(ep, &ext);
			xext.p_off = XFS_FSBLOCK_TO_BYTES(ip->i_mount,
					ext.br_startblock);
			xext.l_off = XFS_FSB_TO_B(ip->i_mount, ext.br_startoff);
			xext.len = XFS_FSB_TO_B(ip->i_mount, ext.br_blockcount);
			if (fn(ip->i_ino, &xext, priv_data))
				break;
		}
		break;
	default:
		printf("Unknown fork format %d\n", XFS_IFORK_FORMAT(ip, fork));
		return EFSCORRUPTED;
	}

	return 0;
}

/* Walk the internal leaf nodes of an inode's bmap btrees */
#define WALK_FORK(wf, inode, type, fork) \
	do { \
		int err; \
		struct xfs_extent_t *last = &(wf)->last_ext; \
\
		last->len = 0; \
		if ((fork) == XFS_ATTR_FORK) \
			(wf)->itype = XFS_DIR3_XT_XATTR; \
		else \
			(wf)->itype = (type); \
		err = iterate_fork_mappings((inode), (fork), walk_extent_helper, (wf)); \
		if (!(wf)->err) \
			(wf)->err = err; \
		if ((wf)->err || (wf)->wf_db_err) \
			return; \
\
		if (last->len == 0) \
			break; \
		/* Insert the extent */ \
		dbg_printf("R: ino=%ld pblk=%llu lblk=%llu len=%llu\n", \
			   (inode)->i_ino, last->p_off, last->l_off, \
			   last->len); \
		insert_xfs_extent((inode)->i_ino, last, (wf)); \
		if ((wf)->err || (wf)->wf_db_err) \
			return; \
	} while (0);
static void
walk_file_mappings(
	struct xfsmap		*wf,
	struct xfs_inode	*ip,
	int			type)
{
	unsigned long long	ioff;
	xfs_agnumber_t		agno;
	xfs_agino_t		agino;

	agno = XFS_INO_TO_AGNO(ip->i_mount, ip->i_ino);
	agino = XFS_INO_TO_AGINO(ip->i_mount, ip->i_ino);
	if (big_bmap_test(wf->ino_bmap, agno, agino))
		return;
	big_bmap_set(wf->ino_bmap, agno, agino, 1, 1);

	ioff = inode_poff(ip);
	insert_extent(&wf->base, ip->i_ino, ioff, NULL,
			ip->i_mount->m_sb.sb_inodesize, 0, EXT_TYPE_METADATA);
	if (wf->wf_db_err)
		return;

	/* Walk the inode forks */
	WALK_FORK(wf, ip, type, XFS_DATA_FORK);
	WALK_FORK(wf, ip, type, XFS_ATTR_FORK);
}
#undef WALK_FORK

/* Handle a directory entry */
static int
walk_fs_helper(
	xfs_ino_t		dir,
	const char		*dname,
	size_t			dname_len,
	xfs_ino_t		ino,
	int			file_type,
	void			*priv_data)
{
	char			path[PATH_MAX + 1];
	char			name[XFS_NAME_LEN + 1];
	const char		*old_dirpath;
	int			type, sz;
	struct xfsmap		*wf = priv_data;
	struct xfs_inode	*inode = NULL;
	time_t			atime, crtime, ctime, mtime;
	time_t			*pcrtime = NULL;
	ssize_t			size;
	int			err;

	if (!strcmp(dname, ".") || !strcmp(dname, ".."))
		return 0;

	sz = icvt(&wf->base, (char *)dname, dname_len, name, XFS_NAME_LEN);
	if (sz < 0)
		return -1;
	dbg_printf("dir=%ld name=%s/%s ino=%ld type=%d\n", dir, wf->wf_dirpath,
			name, ino, file_type);

	wf->err = libxfs_iget(wf->fs, NULL, ino, 0, &inode);
	if (wf->err)
		return -1;

	if (file_type != XFS_DIR3_FT_UNKNOWN) {
		switch(file_type) {
		case XFS_DIR3_FT_REG_FILE:
		case XFS_DIR3_FT_DIR:
		case XFS_DIR3_FT_SYMLINK:
			type = file_type;
			break;
		default:
			goto out;
		}
	} else {
		if (S_ISREG(VFS_I(inode)->i_mode))
			type = XFS_DIR3_FT_REG_FILE;
		else if (S_ISDIR(VFS_I(inode)->i_mode))
			type = XFS_DIR3_FT_DIR;
		else if (S_ISLNK(VFS_I(inode)->i_mode))
			type = XFS_DIR3_FT_SYMLINK;
		else
			goto out;
	}

	atime = VFS_I(inode)->i_atime.tv_sec;
	mtime = VFS_I(inode)->i_mtime.tv_sec;
	ctime = VFS_I(inode)->i_ctime.tv_sec;
	if (inode->i_d.di_version >= 3) {
		crtime = inode->i_d.di_crtime.t_sec;
		pcrtime = &crtime;
	}
	size = inode->i_d.di_size;

	if (dir)
		snprintf(path, PATH_MAX, "%s/%s", wf->wf_dirpath, name);
	else
		path[0] = 0;
	insert_inode(&wf->base, ino, type_codes[type], path, &atime,
			pcrtime, &ctime, &mtime, &size);
	if (wf->wf_db_err)
		goto out;
	if (dir)
		insert_dentry(&wf->base, dir, name, ino);
	if (wf->wf_db_err)
		goto out;

	walk_file_mappings(wf, inode, type);
	if (wf->err || wf->wf_db_err)
		goto out;

	if (type == XFS_DIR3_FT_DIR) {
		old_dirpath = wf->wf_dirpath;
		wf->wf_dirpath = path;
		err = iterate_directory(inode, walk_fs_helper, wf);
		if (!wf->err)
			wf->err = err;
		wf->wf_dirpath = old_dirpath;
	}
	if (wf->err || wf->wf_db_err)
		goto out;

out:
	IRELE(inode);
	return (wf->err || wf->wf_db_err) ? -1 : 0;
}

/* Walk the whole FS, looking for inodes to analyze. */
static void
walk_fs(
	struct xfsmap		*wf)
{
	wf->wf_dirpath = "";
	walk_fs_helper(0, "", 0, wf->fs->m_sb.sb_rootino, XFS_DIR3_FT_DIR, wf);
}

/* Handle in-core bitmaps */
static void
update_bmap(
	struct btree_root	*bmap,
	xfs_agblock_t		offset,
	xfs_extlen_t		blen,
	void			*new_state)
{
	int			*cur_state;
	int			*next_state;
	int			*prev_state;
	unsigned long		end = offset + blen;
	unsigned long		cur_key;
	unsigned long		next_key;

	cur_state = btree_find(bmap, offset, &cur_key);
	if (!cur_state)
		return;

	if (offset == cur_key) {
		/* if the start is the same as the "item" extent */
		if (cur_state == new_state)
			return;

		/*
		 * Note: this may be NULL if we are updating the map for
		 * the superblock.
		 */
		prev_state = btree_peek_prev(bmap, NULL);

		next_state = btree_peek_next(bmap, &next_key);
		if (next_key > end) {
			/* different end */
			if (new_state == prev_state) {
				/* #1: prev has same state, move offset up */
				btree_update_key(bmap, offset, end);
				return;
			}

			/* #4: insert new extent after, update current value */
			btree_update_value(bmap, offset, new_state);
			btree_insert(bmap, end, cur_state);
			return;
		}

		/* same end (and same start) */
		if (new_state == next_state) {
			/* next has same state */
			if (new_state == prev_state) {
				/* #3: merge prev & next */
				btree_delete(bmap, offset);
				btree_delete(bmap, end);
				return;
			}

			/* #8: merge next */
			btree_update_value(bmap, offset, new_state);
			btree_delete(bmap, end);
			return;
		}

		/* same start, same end, next has different state */
		if (new_state == prev_state) {
			/* #5: prev has same state */
			btree_delete(bmap, offset);
			return;
		}

		/* #6: update value only */
		btree_update_value(bmap, offset, new_state);
		return;
	}

	/* different start, offset is in the middle of "cur" */
	prev_state = btree_peek_prev(bmap, NULL);
	ASSERT(prev_state != NULL);
	if (prev_state == new_state)
		return;

	if (end == cur_key) {
		/* end is at the same point as the current extent */
		if (new_state == cur_state) {
			/* #7: move next extent down */
			btree_update_key(bmap, end, offset);
			return;
		}

		/* #9: different start, same end, add new extent */
		btree_insert(bmap, offset, new_state);
		return;
	}

	/* #2: insert an extent into the middle of another extent */
	btree_insert(bmap, offset, new_state);
	btree_insert(bmap, end, prev_state);
}

/* Given an AG and an AG block number, find the extent associated with them */
int
get_bmap_ext(
	struct btree_root	*bmap,
	xfs_agblock_t		agbno,
	xfs_agblock_t		maxbno,
	xfs_extlen_t		*blen)
{
	int			*statep;
	unsigned long		key;

	statep = btree_find(bmap, agbno, &key);
	if (!statep)
		return -1;

	if (key == agbno) {
		if (blen) {
			if (!btree_peek_next(bmap, &key))
				return -1;
			*blen = MIN(maxbno, key) - agbno;
		}
		return *statep;
	}

	statep = btree_peek_prev(bmap, NULL);
	if (!statep)
		return -1;
	if (blen)
		*blen = MIN(maxbno, key) - agbno;

	return *statep;
}

/* Create a big bitmap to cover the whole FS */
static int
big_bmap_init(
	struct xfs_mount	*fs,
	struct big_bmap		**bbmap,
	int			multiplier)
{
	struct big_bmap		*u;
	xfs_agnumber_t		agno;
	xfs_agblock_t		ag_size;

	u = calloc(1, sizeof(struct big_bmap));
	if (!u)
		return ENOMEM;

	u->fs = fs;
	u->sz = fs->m_sb.sb_agcount;
	u->multiplier = multiplier;
	u->bmap = calloc(u->sz, sizeof(struct btree_root *));
	if (!u->bmap) {
		free(u);
		return ENOMEM;
	}
	u->bmap_sizes = calloc(u->sz, sizeof(xfs_agblock_t));
	if (!u->bmap_sizes) {
		free(u->bmap);
		free(u);
		return ENOMEM;
	}

	ag_size = fs->m_sb.sb_agblocks;
	for (agno = 0; agno < u->sz; agno++) {
		if (agno == u->sz - 1)
			ag_size = (xfs_extlen_t)(fs->m_sb.sb_dblocks -
				   (xfs_rfsblock_t)fs->m_sb.sb_agblocks * agno);
		u->bmap_sizes[agno] = ag_size * multiplier;
		btree_init(&u->bmap[agno]);
		btree_clear(u->bmap[agno]);
		btree_insert(u->bmap[agno], 0,
				&big_bmap_states[BBMAP_UNUSED]);
		btree_insert(u->bmap[agno], ag_size * multiplier,
				&big_bmap_states[BBMAP_BAD]);
	}

	*bbmap = u;
	return 0;
}

static int
big_block_bmap_init(
	struct xfs_mount	*fs,
	struct big_bmap		**bbmap)
{
	return big_bmap_init(fs, bbmap, 1);
}

static int
big_inode_bmap_init(
	struct xfs_mount	*fs,
	struct big_bmap		**bbmap)
{
	int			m;

	m = fs->m_sb.sb_blocksize / fs->m_sb.sb_inodesize;
	return big_bmap_init(fs, bbmap, m);
}

/* Free a big block bitmap */
static void
big_bmap_destroy(
	struct big_bmap		*bbmap)
{
	xfs_agnumber_t		agno;

	for (agno = 0; agno < bbmap->sz; agno++)
		btree_destroy(bbmap->bmap[agno]);
	free(bbmap->bmap);
	free(bbmap);
}

/* Set the state of a range of big bitmap blocks */
static void
big_bmap_set(
	struct big_bmap		*bbmap,
	xfs_agnumber_t		agno,
	xfs_agblock_t		offset,
	xfs_extlen_t		blen,
	int			state)
{
	assert(blen);
	dbg_printf("%s: agno=%d offset=%d blen=%d state=%d\n", __func__,
			agno, offset, blen, state);
	update_bmap(bbmap->bmap[agno], offset, blen, &big_bmap_states[state]);
}

/* Test the status of a block in a big bitmap */
static int
big_bmap_test(
	struct big_bmap		*bbmap,
	xfs_agnumber_t		agno,
	xfs_agblock_t		offset)
{
	unsigned long		key = offset;
	struct btree_root	*bmap;
	int			*val;

	bmap = bbmap->bmap[agno];

	/* btree_find retrieves either the the exact key or the next highest */
	val = btree_find(bmap, offset, &key);
	assert(val != NULL);
	if (offset == key)
		return val == &big_bmap_states[BBMAP_INUSE];
	val = btree_peek_prev(bmap, NULL);
	assert(val != NULL);
	return val == &big_bmap_states[BBMAP_INUSE];
}

#ifdef DEBUG
/* Dump a big bitmap */
static void
big_bmap_dump(
	struct big_bmap		*bbmap,
	xfs_agnumber_t		agno)
{
	unsigned long		key;
	xfs_agblock_t		ag_size;
	xfs_extlen_t		len;
	int			*val;
	struct btree_root	*bmap;

	ag_size = bbmap->fs->m_sb.sb_agblocks;
	if (agno == bbmap->sz - 1)
		ag_size = (xfs_extlen_t)(bbmap->fs->m_sb.sb_dblocks -
			   (xfs_rfsblock_t)bbmap->fs->m_sb.sb_agblocks * agno);
	ag_size *= bbmap->multiplier;
	key = 0;
	bmap = bbmap->bmap[agno];
	val = btree_find(bmap, key, &key);
	while (val != NULL && key != ag_size * bbmap->multiplier) {
		get_bmap_ext(bmap, key, ag_size, &len);
		printf("%s: agno=%d key=%lu len=%lu val=%x\n",
			   __func__, agno, key, (unsigned long)len, *val);
		val = btree_lookup_next(bmap, &key);
	}
}
#endif

/* Walk an AG's bitmap, attaching extents for the in use blocks to the inode */
static void
__walk_ag_bitmap(
	struct xfsmap		*wf,
	xfs_ino_t		ino,
	struct big_bmap		*bbmap,
	xfs_agnumber_t		agno,
	int			dtype)
{
	struct xfs_mount	*fs = wf->fs;
	unsigned long		key;
	xfs_agblock_t		ag_size;
	xfs_fsblock_t		s;
	xfs_extlen_t		len;
	int			*val;
	struct btree_root	*bmap;

	ag_size = fs->m_sb.sb_agblocks;
	if (agno == bbmap->sz - 1)
		ag_size = (xfs_extlen_t)(fs->m_sb.sb_dblocks -
			   (xfs_rfsblock_t)fs->m_sb.sb_agblocks * agno);
	ag_size *= bbmap->multiplier;
	key = 0;
	bmap = bbmap->bmap[agno];
	val = btree_find(bmap, key, &key);
	while (val != NULL && key != ag_size) {
		get_bmap_ext(bmap, key, ag_size, &len);
		if (val != &big_bmap_states[BBMAP_INUSE]) {
			val = btree_lookup_next(bmap, &key);
			continue;
		}

		dbg_printf("%s: ino=%ld agno=%d key=%lu len=%lu val=%x\n",
			   __func__, ino, agno, key, (unsigned long)len, *val);
		s = XFS_AGB_TO_FSB(fs, agno, key);
		insert_extent(&wf->base, ino, XFS_FSBLOCK_TO_BYTES(fs, s), NULL,
				XFS_FSB_TO_B(fs, len), EXTENT_SHARED,
				extent_codes[dtype]);
		if (wf->err || wf->wf_db_err)
			break;
		val = btree_lookup_next(bmap, &key);
	}
}

static void
walk_ag_bitmap(
	struct xfsmap		*wf,
	xfs_ino_t		ino,
	struct big_bmap		*bbmap,
	xfs_agnumber_t		agno)
{
	__walk_ag_bitmap(wf, ino, bbmap, agno, XFS_DIR3_XT_METADATA);
}

static void
walk_ag_freesp_bitmap(
	struct xfsmap		*wf,
	xfs_ino_t		ino,
	struct big_bmap		*bbmap,
	xfs_agnumber_t		agno)
{
	__walk_ag_bitmap(wf, ino, bbmap, agno, XFS_DIR3_XT_FREESP);
}

/* Walk a big bitmap, attaching in use block as extents of the inode */
static void
__walk_bitmap(
	struct xfsmap	*wf,
	xfs_ino_t	ino,
	struct big_bmap	*bbmap,
	int		dtype)
{
	xfs_agnumber_t	agno;

	for (agno = 0; agno < bbmap->sz; agno++) {
		__walk_ag_bitmap(wf, ino, bbmap, agno, dtype);
		if (wf->err || wf->wf_db_err)
			break;
	}
}

static void
walk_bitmap(
	struct xfsmap	*wf,
	xfs_ino_t	ino,
	struct big_bmap	*bbmap)
{
	__walk_bitmap(wf, ino, bbmap, XFS_DIR3_XT_METADATA);
}

static void
walk_freesp_bitmap(
	struct xfsmap	*wf,
	xfs_ino_t	ino,
	struct big_bmap	*bbmap)
{
	__walk_bitmap(wf, ino, bbmap, XFS_DIR3_XT_FREESP);
}

/* Walk the internal nodes of a AG btree */
static int
walk_ag_btree_nodes(
	struct xfs_mount	*fs,
	int64_t			ino,
	xfs_agnumber_t		agno,
	xfs_agblock_t		rootbno,
	int			refval,
	const struct xfs_metadata_btree_ops		*ops,
	extent_walk_fn		fn,
	void			*priv_data,
	xfs_agblock_t		*left_node_agbno)
{
	struct xfs_extent_t	xext;
	struct xfs_btree_block	*block;
	struct xfs_buf		*bp;
	xfs_agblock_t		bno;
	xfs_agblock_t		next_level_bno;
	xfs_fsblock_t		fsbno;
	xfs_fsblock_t		next_level_fsbno;
	xfs_extnum_t		j;
	__be32			*pp;
	int			level;
	int			num_recs;
	int			error;

	if (left_node_agbno)
		*left_node_agbno = NULLAGBLOCK;
	memset(&xext, 0, sizeof(xext));
	bp = NULL;
	bno = rootbno;

	/* Look out for obviously incorrect tree roots */
	if (rootbno == 0 || rootbno == NULLAGBLOCK)
		return EFSCORRUPTED;

	/* Read the tree root */
	fsbno = XFS_AGB_TO_FSB(fs, agno, bno);
	if (!XFS_FSB_SANITY_CHECK(fs, fsbno))
		return EFSCORRUPTED;
	error = xfs_btree_read_bufl(fs, NULL, fsbno, 0, &bp, refval,
			ops->buf_ops);
	if (error)
		return error;
	error = EFSCORRUPTED;
	if (bp->b_error)
		goto err;
	block = XFS_BUF_TO_BLOCK(bp);
	num_recs = xfs_btree_get_numrecs(block);
	level = xfs_btree_get_level(block);

	/* Create an extent for the root */
	xext.p_off = XFS_FSBLOCK_TO_BYTES(fs, fsbno);
	xext.l_off = 0;
	xext.len = fs->m_sb.sb_blocksize;
	xext.state = XFS_EXT_NORM;
	if (fn(ino, &xext, priv_data)) {
		error = 0;
		goto err;
	}
	if (level == 0) {
		if (left_node_agbno)
			*left_node_agbno = bno;
		error = 0;
		goto err;
	}

	/* Prepare to iterate */
	pp = ops->ptraddr(fs, block);
	next_level_bno = be32_to_cpu(*pp);
	next_level_fsbno = XFS_AGB_TO_FSB(fs, agno, next_level_bno);
	if (!XFS_FSB_SANITY_CHECK(fs, next_level_fsbno))
		goto err;
	do {
		/* process all the blocks in this level */
		do {
			/* process all the key/ptrs in this block */
			for (j = 0; j < num_recs; j++, pp++) {
				bno = be32_to_cpu(*pp);
				fsbno = XFS_AGB_TO_FSB(fs, agno, bno);
				if (!XFS_FSB_SANITY_CHECK(fs, fsbno))
					goto err;
				xext.p_off = XFS_FSBLOCK_TO_BYTES(fs, fsbno);
				xext.l_off = 0;
				xext.len = XFS_FSB_TO_B(fs, 1);
				xext.state = XFS_EXT_NORM;
				if (fn(ino, &xext, priv_data)) {
					error = 0;
					goto err;
				}
			}

			/* now go to the right sibling */
			bno = be32_to_cpu(block->bb_u.s.bb_rightsib);
			fsbno = XFS_AGB_TO_FSB(fs, agno, bno);
			if (bno == NULLAGBLOCK)
				break;
			else if (!XFS_FSB_SANITY_CHECK(fs, fsbno))
				goto err;
			if (bp)
				xfsmapper_putbuf(bp);
			error = xfs_btree_read_bufl(fs, NULL, fsbno, 0, &bp,
					refval, ops->buf_ops);
			if (error)
				return error;
			error = EFSCORRUPTED;
			if (bp->b_error)
				goto err;
			block = XFS_BUF_TO_BLOCK(bp);
			num_recs = xfs_btree_get_numrecs(block);
			pp = ops->ptraddr(fs, block);
		} while (1);

		/* now go down the tree */
		level--;
		if (level == 0) {
			if (left_node_agbno)
				*left_node_agbno = next_level_bno;
			break;
		}
		if (bp)
			xfsmapper_putbuf(bp);
		error = xfs_btree_read_bufl(fs, NULL, next_level_fsbno, 0, &bp,
				refval, ops->buf_ops);
		if (error)
			return error;
		error = EFSCORRUPTED;
		if (bp->b_error)
			goto err;
		block = XFS_BUF_TO_BLOCK(bp);
		num_recs = xfs_btree_get_numrecs(block);
		pp = ops->ptraddr(fs, block);
		next_level_bno = be32_to_cpu(*pp);
		next_level_fsbno = XFS_AGB_TO_FSB(fs, agno, next_level_bno);
		if (!XFS_FSB_SANITY_CHECK(fs, next_level_fsbno))
			goto err;
	} while (1);
	if (bp)
		xfsmapper_putbuf(bp);
	return 0;
err:
	xfsmapper_putbuf(bp);
	return error;
}

static int
walk_ag_allocbt_nodes(
	struct xfs_mount	*fs,
	int64_t			ino,
	xfs_agnumber_t		agno,
	xfs_agblock_t		rootbno,
	extent_walk_fn		fn,
	void			*priv_data,
	xfs_agblock_t		*left_node_agbno)
{
	return walk_ag_btree_nodes(fs, ino, agno, rootbno, XFS_ALLOC_BTREE_REF,
			&xfs_allocbt_metadata_ops, fn, priv_data,
			left_node_agbno);
}

static int
walk_ag_inobt_nodes(
	struct xfs_mount	*fs,
	int64_t			ino,
	xfs_agnumber_t		agno,
	xfs_agblock_t		rootbno,
	extent_walk_fn		fn,
	void			*priv_data,
	xfs_agblock_t		*left_node_agbno)
{
	return walk_ag_btree_nodes(fs, ino, agno, rootbno, XFS_INO_BTREE_REF,
			&xfs_inobt_metadata_ops, fn, priv_data,
			left_node_agbno);
}

#ifndef XFS_RMAP_CRC_MAGIC
# define XFS_RMAP_BTREE_REF	1
# define xfs_sb_version_hasrmapbt(mp)	(0)
#else
static int
walk_ag_rmapbt_nodes(
	struct xfs_mount	*fs,
	int64_t			ino,
	xfs_agnumber_t		agno,
	xfs_agblock_t		rootbno,
	extent_walk_fn		fn,
	void			*priv_data)
{
	return walk_ag_btree_nodes(fs, ino, agno, rootbno, XFS_RMAP_BTREE_REF,
			&xfs_rmapbt_metadata_ops, fn, priv_data, NULL);
}
#endif

#ifndef XFS_REFC_CRC_MAGIC
# define XFS_REFC_BTREE_REF	1
# define xfs_sb_version_hasreflink(mp)	(0)
#else
static int
walk_ag_refcountbt_nodes(
	struct xfs_mount	*fs,
	int64_t			ino,
	xfs_agnumber_t		agno,
	xfs_agblock_t		rootbno,
	extent_walk_fn		fn,
	void			*priv_data)
{
	return walk_ag_btree_nodes(fs, ino, agno, rootbno, XFS_REFC_BTREE_REF,
			&xfs_refcountbt_metadata_ops, fn, priv_data, NULL);
}
#endif

/* Walk the bnobt blocks of a AG */
static int
walk_ag_bnobt_records(
	struct xfs_mount	*fs,
	int64_t			ino,
	xfs_agnumber_t		agno,
	xfs_agblock_t		left_node_bno,
	extent_walk_fn		fn,
	void			*priv_data)
{
	struct xfs_extent_t	xext;
	struct xfs_btree_block	*block;
	struct xfs_buf		*bp;
	struct xfs_alloc_rec	*pp;
	xfs_fsblock_t		fsbno;
	xfs_agblock_t		bno;
	xfs_agblock_t		len;
	int			error;
	int			num_recs;
	unsigned int		j;

	memset(&xext, 0, sizeof(xext));
	bp = NULL;
	bno = left_node_bno;

	while (bno != NULLAGBLOCK) {
		/* Read the leaf */
		fsbno = XFS_AGB_TO_FSB(fs, agno, bno);
		if (!XFS_FSB_SANITY_CHECK(fs, fsbno))
			return EFSCORRUPTED;
		error = xfs_btree_read_bufl(fs, NULL, fsbno, 0, &bp,
				XFS_ALLOC_BTREE_REF, &xfs_allocbt_buf_ops);
		if (error)
			return error;
		error = EFSCORRUPTED;
		if (bp->b_error)
			goto err;
		block = XFS_BUF_TO_BLOCK(bp);
		num_recs = xfs_btree_get_numrecs(block);
		ASSERT(xfs_btree_get_level(block) == 0);
		pp = XFS_ALLOC_REC_ADDR(fs, block, 1);

		/* For each record in this leaf... */
		for (j = 0; j < num_recs; j++, pp++) {
			bno = be32_to_cpu(pp->ar_startblock);
			len = be32_to_cpu(pp->ar_blockcount);
			fsbno = XFS_AGB_TO_FSB(fs, agno, bno);
			dbg_printf("ag:%d agino:%u>%u bno:%lu fsbno:%lu len:%llu\n",
					agno, bno, fsbno, len);
			if (!XFS_FSB_SANITY_CHECK(fs, fsbno))
				goto err;
			xext.p_off = XFS_FSBLOCK_TO_BYTES(fs, fsbno);
			xext.l_off = 0;
			xext.len = XFS_FSB_TO_B(fs, len);
			xext.state = XFS_EXT_NORM;
			if (fn(ino, &xext, priv_data)) {
				error = 0;
				goto err;
			}
		}

		/* Go to the right sibling */
		bno = be32_to_cpu(block->bb_u.s.bb_rightsib);
		if (bp)
			xfsmapper_putbuf(bp);
	}
	return 0;
err:
	xfsmapper_putbuf(bp);
	return error;
}

/* Walk the inode blocks of a AG */
static int walk_ag_inode_blocks(
	struct xfs_mount	*fs,
	int64_t			ino,
	xfs_agnumber_t		agno,
	xfs_agblock_t		left_node_bno,
	extent_walk_fn		fn,
	void			*priv_data)
{
	struct xfs_extent_t	xext;
	struct xfs_btree_block	*block;
	struct xfs_buf		*bp;
	struct xfs_inobt_rec	*pp;
	xfs_fsblock_t		fsbno;
	xfs_agblock_t		bno;
	xfs_extnum_t		j;
	xfs_agino_t		agino;
	int			num_recs;
	int			error;

	memset(&xext, 0, sizeof(xext));
	bp = NULL;
	bno = left_node_bno;

	while (bno != NULLAGBLOCK) {
		/* Read the leaf */
		fsbno = XFS_AGB_TO_FSB(fs, agno, bno);
		if (!XFS_FSB_SANITY_CHECK(fs, fsbno))
			return EFSCORRUPTED;
		error = xfs_btree_read_bufl(fs, NULL, fsbno, 0, &bp,
				XFS_INO_BTREE_REF, &xfs_inobt_buf_ops);
		if (error)
			return error;
		error = EFSCORRUPTED;
		if (bp->b_error)
			goto err;
		block = XFS_BUF_TO_BLOCK(bp);
		num_recs = xfs_btree_get_numrecs(block);
		ASSERT(xfs_btree_get_level(block) == 0);
		pp = XFS_INOBT_REC_ADDR(fs, block, 1);

		/* For each record in this leaf... */
		for (j = 0; j < num_recs; j++, pp++) {
			agino = be32_to_cpu(pp->ir_startino);
			bno = XFS_AGINO_TO_AGBNO(fs, agino);
			fsbno = XFS_AGB_TO_FSB(fs, agno, bno);
			dbg_printf("ag:%d agino:%u>%u bno:%lu fsbno:%lu poff:%llu\n",
					agno, agino,
					XFS_AGINO_TO_INO(fs, agno, agino), bno,
					fsbno, XFS_FSBLOCK_TO_BYTES(fs, fsbno));
			if (!XFS_FSB_SANITY_CHECK(fs, fsbno))
				goto err;
			xext.p_off = XFS_FSBLOCK_TO_BYTES(fs, fsbno);
			xext.l_off = 0;
			xext.len = 64 * fs->m_sb.sb_inodesize;
			xext.state = XFS_EXT_NORM;
			if (fn(ino, &xext, priv_data)) {
				error = 0;
				goto err;
			}
		}

		/* Go to the right sibling */
		bno = be32_to_cpu(block->bb_u.s.bb_rightsib);
		if (bp)
			xfsmapper_putbuf(bp);
	}
	return 0;
err:
	xfsmapper_putbuf(bp);
	return error;
}

/* Analyze metadata */

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

/* Invent a FS tree for metadata. */
static void
walk_metadata(
	struct xfsmap		*wf)
{
	struct xfs_mount	*fs = wf->fs;
	struct big_bmap		*bmap_ag, *bmap_agfl, *bmap_freesp, *bmap_bnobt,
				*bmap_cntbt, *bmap_inobt, *bmap_finobt,
				*bmap_itable, *bmap_rmapbt, *bmap_refcountbt;
	struct xfs_ag		*ags = NULL;
	xfs_agf_t		*agf;
	xfs_agi_t		*agi;
	char			path[PATH_MAX + 1];
	uint64_t		loff;
	int64_t			ino, group_ino;
	unsigned long		i, len;
	xfs_fsblock_t		s;
	xfs_agnumber_t		agno;
	xfs_agblock_t		left_inobt_leaf_agbno = 0;
	xfs_agblock_t		left_bnobt_leaf_agbno = 0;
	__be32			*freelist;
	int			w;
	int			err;

	/* Create hidden inodes */
#define H(ino, name, type) {(ino), STR_##name, XFS_DIR3_##type}
	struct hidden_file *hf;
	struct hidden_file hidden_inodes[] = {
		H(fs->m_sb.sb_uquotino, USR_QUOTA_FILE, XT_METADATA),
		H(fs->m_sb.sb_gquotino, GRP_QUOTA_FILE, XT_METADATA),
		H(fs->m_sb.sb_pquotino, PROJ_QUOTA_FILE, XT_METADATA),
		H(fs->m_sb.sb_rbmino, RT_BMAP_FILE, XT_METADATA),
		H(fs->m_sb.sb_rsumino, RT_SUMMARY_FILE, XT_METADATA),
		{},
	};
#undef H

	bmap_ag = bmap_agfl = bmap_freesp = bmap_bnobt = bmap_cntbt =
			bmap_inobt = bmap_finobt = bmap_itable = bmap_rmapbt =
			bmap_refcountbt = NULL;

	wf->err = read_ags(fs, &ags);
	if (wf->err)
		return;

	INJECT_METADATA(fs->m_sb.sb_rootino, "", INO_METADATA_DIR, \
			STR_METADATA_DIR, XFS_DIR3_FT_DIR);
	INJECT_ROOT_METADATA(GROUPS_DIR, XFS_DIR3_FT_DIR);
	INJECT_ROOT_METADATA(HIDDEN_DIR, XFS_DIR3_FT_DIR);
	INJECT_ROOT_METADATA(SB_FILE, XFS_DIR3_XT_METADATA);
	INJECT_ROOT_METADATA(FL_FILE, XFS_DIR3_XT_METADATA);
	INJECT_ROOT_METADATA(BNOBT_FILE, XFS_DIR3_XT_METADATA);
	INJECT_ROOT_METADATA(FREESP_FILE, XFS_DIR3_XT_FREESP);
	INJECT_ROOT_METADATA(CNTBT_FILE, XFS_DIR3_XT_METADATA);
	INJECT_ROOT_METADATA(INOBT_FILE, XFS_DIR3_XT_METADATA);
	if (xfs_sb_version_hasfinobt(&fs->m_sb))
		INJECT_ROOT_METADATA(FINOBT_FILE, XFS_DIR3_XT_METADATA);
	if (xfs_sb_version_hasrmapbt(&fs->m_sb))
		INJECT_ROOT_METADATA(RMAPBT_FILE, XFS_DIR3_XT_METADATA);
	if (xfs_sb_version_hasreflink(&fs->m_sb))
		INJECT_ROOT_METADATA(REFCOUNTBT_FILE, XFS_DIR3_XT_METADATA);
	INJECT_ROOT_METADATA(ITABLE_FILE, XFS_DIR3_XT_METADATA);

	/* Handle the log */
	if (fs->m_sb.sb_logstart) {
		INJECT_ROOT_METADATA(JOURNAL_FILE, XFS_DIR3_FT_REG_FILE);
		loff = 0;
		insert_extent(&wf->base, INO_JOURNAL_FILE,
			      XFS_FSBLOCK_TO_BYTES(fs, fs->m_sb.sb_logstart),
			      &loff, XFS_FSB_TO_B(fs, fs->m_sb.sb_logblocks), 0,
			      extent_codes[XFS_DIR3_FT_REG_FILE]);
		if (wf->wf_db_err)
			goto out;
	}

	/* Bitmaps for aggregate metafiles */
	wf->err = big_block_bmap_init(fs, &bmap_ag);
	if (wf->err)
		goto out;
	wf->err = big_block_bmap_init(fs, &bmap_agfl);
	if (wf->err)
		goto out;
	wf->err = big_block_bmap_init(fs, &bmap_bnobt);
	if (wf->err)
		goto out;
	wf->err = big_block_bmap_init(fs, &bmap_freesp);
	if (wf->err)
		goto out;
	wf->err = big_block_bmap_init(fs, &bmap_cntbt);
	if (wf->err)
		goto out;
	wf->err = big_block_bmap_init(fs, &bmap_inobt);
	if (wf->err)
		goto out;
	wf->err = big_block_bmap_init(fs, &bmap_finobt);
	if (wf->err)
		goto out;
	wf->err = big_block_bmap_init(fs, &bmap_itable);
	if (wf->err)
		goto out;
	wf->err = big_block_bmap_init(fs, &bmap_rmapbt);
	if (wf->err)
		goto out;
	wf->err = big_block_bmap_init(fs, &bmap_refcountbt);
	if (wf->err)
		goto out;

	ino = INO_GROUPS_DIR - 1;
	snprintf(path, PATH_MAX, "%d", fs->m_sb.sb_agcount);
	w = strlen(path);
	wf->itype = XFS_DIR3_XT_METADATA;
	for (agno = 0; agno < fs->m_sb.sb_agcount; agno++) {
		agf = XFS_BUF_TO_AGF(ags[agno].agf);
		agi = XFS_BUF_TO_AGI(ags[agno].agi);
		wf->agno = agno;

		/* set up per-group virtual files */
		snprintf(path, PATH_MAX, "%0*d", w, agno);
		group_ino = ino;
		ino--;
		INJECT_GROUP(group_ino, path, XFS_DIR3_FT_DIR);

		snprintf(path, PATH_MAX, "/%s/%s/%0*d", STR_METADATA_DIR,
				STR_GROUPS_DIR, w, agno);

		/* Record the superblock+agf+agi+agfl blocks */
		s = XFS_AGB_TO_FSB(fs, agno, 0);
		len = 4 * fs->m_sb.sb_sectsize / fs->m_sb.sb_blocksize;
		if (len < 1)
			len = 1;
		big_bmap_set(bmap_ag, agno, 0, len, 1);
		INJECT_METADATA(group_ino, path, ino, STR_SB_FILE,
				XFS_DIR3_XT_METADATA);
		insert_extent(&wf->base, ino, XFS_FSBLOCK_TO_BYTES(fs, s),
				NULL, 4 * fs->m_sb.sb_sectsize, EXTENT_SHARED,
				extent_codes[XFS_DIR3_XT_METADATA]);
		if (wf->wf_db_err)
			goto out;
		ino--;

		/* AG free list */
		freelist = XFS_BUF_TO_AGFL_BNO(fs, ags[agno].agfl);
		for (i = be32_to_cpu(agf->agf_flfirst);
		     i <= be32_to_cpu(agf->agf_fllast); i++)
			big_bmap_set(bmap_agfl, agno,
					be32_to_cpu(freelist[i]), 1, 1);

		INJECT_METADATA(group_ino, path, ino, STR_FL_FILE,
				XFS_DIR3_XT_METADATA);
		walk_ag_bitmap(wf, ino, bmap_agfl, agno);
		if (wf->err || wf->wf_db_err)
			goto out;
		ino--;

		/* bnobt */
		wf->bbmap = bmap_bnobt;
		INJECT_METADATA(group_ino, path, ino, STR_BNOBT_FILE,
				XFS_DIR3_XT_METADATA);
		err = walk_ag_allocbt_nodes(fs, ino, agno,
				be32_to_cpu(agf->agf_roots[XFS_BTNUM_BNO]),
				insert_meta_extent, wf, &left_bnobt_leaf_agbno);
		if (!wf->err)
			wf->err = err;
		if (wf->err)
			goto out;
		walk_ag_bitmap(wf, ino, wf->bbmap, agno);
		if (wf->err || wf->wf_db_err)
			goto out;
		ino--;

		/* free space */
		wf->bbmap = bmap_freesp;
		INJECT_METADATA(group_ino, path, ino, STR_FREESP_FILE,
				XFS_DIR3_XT_FREESP);
		err = walk_ag_bnobt_records(fs, ino, agno,
				left_bnobt_leaf_agbno, insert_meta_extent, wf);
		if (!wf->err)
			wf->err = err;
		if (wf->err)
			goto out;
		walk_ag_freesp_bitmap(wf, ino, wf->bbmap, agno);
		if (wf->err || wf->wf_db_err)
			goto out;
		ino--;

		/* cntbt */
		wf->bbmap = bmap_cntbt;
		INJECT_METADATA(group_ino, path, ino, STR_CNTBT_FILE,
				XFS_DIR3_XT_METADATA);
		err = walk_ag_allocbt_nodes(fs, ino, agno,
				be32_to_cpu(agf->agf_roots[XFS_BTNUM_CNT]),
				insert_meta_extent, wf, NULL);
		if (!wf->err)
			wf->err = err;
		if (wf->err)
			goto out;
		walk_ag_bitmap(wf, ino, wf->bbmap, agno);
		if (wf->err || wf->wf_db_err)
			goto out;
		ino--;

		/* inobt */
		wf->bbmap = bmap_inobt;
		INJECT_METADATA(group_ino, path, ino, STR_INOBT_FILE,
				XFS_DIR3_XT_METADATA);
		err = walk_ag_inobt_nodes(fs, ino, agno,
				be32_to_cpu(agi->agi_root),
				insert_meta_extent, wf, &left_inobt_leaf_agbno);
		if (!wf->err)
			wf->err = err;
		if (wf->err)
			goto out;
		walk_ag_bitmap(wf, ino, wf->bbmap, agno);
		if (wf->err || wf->wf_db_err)
			goto out;
		ino--;

		/* finobt */
		if (!xfs_sb_version_hasfinobt(&fs->m_sb))
			goto no_finobt;
		wf->bbmap = bmap_finobt;
		INJECT_METADATA(group_ino, path, ino, STR_FINOBT_FILE,
				XFS_DIR3_XT_METADATA);
		err = walk_ag_inobt_nodes(fs, ino, agno,
				be32_to_cpu(agi->agi_free_root),
				insert_meta_extent, wf, NULL);
		if (!wf->err)
			wf->err = err;
		if (wf->err)
			goto out;
		walk_ag_bitmap(wf, ino, wf->bbmap, agno);
		if (wf->err || wf->wf_db_err)
			goto out;
		ino--;
no_finobt:

		/* inode blocks */
		wf->bbmap = bmap_itable;
		INJECT_METADATA(group_ino, path, ino, STR_ITABLE_FILE,
				XFS_DIR3_XT_METADATA);
		err = walk_ag_inode_blocks(fs, ino, agno,
				left_inobt_leaf_agbno, insert_meta_extent, wf);
		if (!wf->err)
			wf->err = err;
		if (wf->err)
			goto out;
		walk_ag_bitmap(wf, ino, wf->bbmap, agno);
		if (wf->err || wf->wf_db_err)
			goto out;
		ino--;

		/* rmapbt */
		if (!xfs_sb_version_hasrmapbt(&fs->m_sb))
			goto no_rmapbt;
#ifdef XFS_RMAP_CRC_MAGIC
		wf->bbmap = bmap_rmapbt;
		INJECT_METADATA(group_ino, path, ino, STR_RMAPBT_FILE,
				XFS_DIR3_XT_METADATA);
		err = walk_ag_rmapbt_nodes(fs, ino, agno,
				be32_to_cpu(agf->agf_roots[XFS_BTNUM_RMAP]),
				insert_meta_extent, wf);
		if (!wf->err)
			wf->err = err;
		if (wf->err)
			goto out;
		walk_ag_bitmap(wf, ino, wf->bbmap, agno);
		if (wf->err || wf->wf_db_err)
			goto out;
		ino--;
#endif
no_rmapbt:

		/* refcountbt */
		if (!xfs_sb_version_hasreflink(&fs->m_sb))
			goto no_refcountbt;
#ifdef XFS_REFC_CRC_MAGIC
		wf->bbmap = bmap_refcountbt;
		INJECT_METADATA(group_ino, path, ino, STR_REFCOUNTBT_FILE,
				XFS_DIR3_XT_METADATA);
		err = walk_ag_refcountbt_nodes(fs, ino, agno,
				be32_to_cpu(agf->agf_refcount_root),
				insert_meta_extent, wf);
		if (!wf->err)
			wf->err = err;
		if (wf->err)
			goto out;
		walk_ag_bitmap(wf, ino, wf->bbmap, agno);
		if (wf->err || wf->wf_db_err)
			goto out;
		ino--;
#endif
no_refcountbt:
		;
	}

	/* Emit extents for the overall files */
	walk_bitmap(wf, INO_SB_FILE, bmap_ag);
	if (wf->err || wf->wf_db_err)
		goto out;
	walk_bitmap(wf, INO_FL_FILE, bmap_agfl);
	if (wf->err || wf->wf_db_err)
		goto out;
	walk_bitmap(wf, INO_BNOBT_FILE, bmap_bnobt);
	if (wf->err || wf->wf_db_err)
		goto out;
	walk_freesp_bitmap(wf, INO_FREESP_FILE, bmap_freesp);
	if (wf->err || wf->wf_db_err)
		goto out;
	walk_bitmap(wf, INO_CNTBT_FILE, bmap_cntbt);
	if (wf->err || wf->wf_db_err)
		goto out;
	walk_bitmap(wf, INO_INOBT_FILE, bmap_inobt);
	if (wf->err || wf->wf_db_err)
		goto out;
	if (xfs_sb_version_hasfinobt(&fs->m_sb)) {
		walk_bitmap(wf, INO_FINOBT_FILE, bmap_finobt);
		if (wf->err || wf->wf_db_err)
			goto out;
	}
	walk_bitmap(wf, INO_ITABLE_FILE, bmap_itable);
	if (wf->err || wf->wf_db_err)
		goto out;
	if (xfs_sb_version_hasrmapbt(&fs->m_sb)) {
		walk_bitmap(wf, INO_RMAPBT_FILE, bmap_rmapbt);
		if (wf->err || wf->wf_db_err)
			goto out;
	}
	if (xfs_sb_version_hasreflink(&fs->m_sb)) {
		walk_bitmap(wf, INO_REFCOUNTBT_FILE, bmap_refcountbt);
		if (wf->err || wf->wf_db_err)
			goto out;
	}

	/* Now go for the hidden files */
	snprintf(path, PATH_MAX, "/%s/%s", STR_METADATA_DIR, STR_HIDDEN_DIR);
	for (hf = hidden_inodes; hf->name != NULL; hf++) {
		if (hf->ino == NULLFSINO || hf->ino == 0)
			continue;
		walk_fs_helper(INO_HIDDEN_DIR, hf->name, strlen(hf->name),
				hf->ino, XFS_DIR3_FT_UNKNOWN, wf);
		if (wf->err || wf->wf_db_err)
			goto out;
	}
out:
	if (bmap_refcountbt)
		big_bmap_destroy(bmap_refcountbt);
	if (bmap_rmapbt)
		big_bmap_destroy(bmap_rmapbt);
	if (bmap_itable)
		big_bmap_destroy(bmap_itable);
	if (bmap_finobt)
		big_bmap_destroy(bmap_finobt);
	if (bmap_inobt)
		big_bmap_destroy(bmap_inobt);
	if (bmap_cntbt)
		big_bmap_destroy(bmap_cntbt);
	if (bmap_freesp)
		big_bmap_destroy(bmap_freesp);
	if (bmap_bnobt)
		big_bmap_destroy(bmap_bnobt);
	if (bmap_agfl)
		big_bmap_destroy(bmap_agfl);
	if (bmap_ag)
		big_bmap_destroy(bmap_ag);
	if (ags)
		free_ags(fs, ags);
	return;
}

static void
usage(void)
{
	fprintf(stderr, _(
		"Usage: %s [-ifrV] [-l logdev] dbfile device\n"
		), progname);
	exit(1);
}

#define CHECK_ERROR(msg) \
do { \
	if (wf.err) { \
		fprintf(stderr, "%s %s\n", strerror(wf.err), (msg)); \
		goto out; \
	} \
	if (wf.wf_db_err) { \
		fprintf(stderr, "%s %s\n", sqlite3_errstr(wf.wf_db_err), (msg)); \
		goto out; \
	} \
} while (0);

int
main(
	int			argc,
	char			**argv)
{
	libxfs_init_t		x;
	struct xfs_mount	xmount;
	struct xfsmap		wf;
	struct xfs_mount	*fs;
	char			*fsdev, *dbfile;
	struct xfs_sb		*sbp;
	struct xfs_buf		*bp;
	sqlite3			*db = NULL;
	char			*errm;
	unsigned long long	total_bytes, free_bytes, total_inodes,
				free_inodes, fakeinos;
	int			c;
	int			db_err, err, err2;

	err = 0;
	memset(&x, 0, sizeof(x));
	x.isreadonly = (LIBXFS_ISREADONLY | LIBXFS_ISINACTIVE);
	x.isdirect = 0;
	progname = basename(argv[0]);
	while ((c = getopt(argc, argv, "fl:")) != EOF) {
		switch (c) {
		case 'f':
			x.disfile = 1;
			break;
		case 'l':
			x.logname = optarg;
			break;
		case '?':
			usage();
			/*NOTREACHED*/
		}
	}
	if (optind + 2 != argc) {
		usage();
		/*NOTREACHED*/
	}

	dbfile = argv[optind];
	fsdev = argv[optind + 1];
	if (!x.disfile)
		x.volname = fsdev;
	else
		x.dname = fsdev;

	x.bcache_flags = CACHE_MISCOMPARE_PURGE;
	if (!libxfs_init(&x)) {
		fputs(_("\nfatal error -- couldn't initialize XFS library\n"),
			stderr);
		exit(1);
	}

	/*
	 * Read the superblock.
	 */
	memset(&xmount, 0, sizeof(struct xfs_mount));
	libxfs_buftarg_init(&xmount, x.ddev, x.logdev, x.rtdev);
	bp = libxfs_readbuf(xmount.m_ddev_targp, XFS_SB_DADDR,
			    1 << (XFS_MAX_SECTORSIZE_LOG - BBSHIFT), 0, NULL);

	if (!bp || bp->b_error) {
		fprintf(stderr, _("%s: %s is invalid (cannot read first 512 "
			"bytes)\n"), progname, fsdev);
		exit(1);
	}

	/* copy SB from buffer to in-core, converting architecture as we go */
	libxfs_sb_from_disk(&xmount.m_sb, XFS_BUF_TO_SBP(bp));
	xfsmapper_putbuf(bp);
	libxfs_purgebuf(bp);

	sbp = &xmount.m_sb;
	if (sbp->sb_magicnum != XFS_SB_MAGIC) {
		fprintf(stderr,
_("%s: %s is not a valid XFS filesystem (unexpected SB magic number 0x%08x)\n"),
			progname, fsdev, sbp->sb_magicnum);
	}

	fs = libxfs_mount(&xmount, sbp, x.ddev, x.logdev, x.rtdev, 0);
	if (!fs) {
		fprintf(stderr,
_("%s: device %s unusable (not an XFS filesystem?)\n"),
			progname, fsdev);
		exit(1);
	}

	if (!xfs_sb_good_version(sbp)) {
		fprintf(stderr,
_("%s: unknown version code 0x%x.\n"),
			fsdev, sbp->sb_versionnum);
	}

	if (XFS_SB_VERSION_NUM(sbp) == XFS_SB_VERSION_5 &&
	    xfs_sb_has_compat_feature(sbp, XFS_SB_FEAT_COMPAT_UNKNOWN)) {
		fprintf(stderr,
_("%s: unknown compat feature 0x%x; continuing anyway.\n"),
			fsdev, fs->m_sb.sb_features_compat);
	}

	if (XFS_SB_VERSION_NUM(sbp) == XFS_SB_VERSION_5 &&
	    xfs_sb_has_ro_compat_feature(sbp, XFS_SB_FEAT_RO_COMPAT_UNKNOWN)) {
		fprintf(stderr,
_("%s: unknown rocompat feature 0x%x; continuing anyway.\n"),
			fsdev, fs->m_sb.sb_features_ro_compat);
	}

	if (XFS_SB_VERSION_NUM(sbp) == XFS_SB_VERSION_5 &&
	    xfs_sb_has_incompat_feature(sbp, XFS_SB_FEAT_INCOMPAT_UNKNOWN)) {
		fprintf(stderr,
_("%s: unknown incompat feature 0x%x; continuing anyway.\n"),
			fsdev, fs->m_sb.sb_features_incompat);
	}

	if (XFS_SB_VERSION_NUM(sbp) == XFS_SB_VERSION_5 &&
	    xfs_sb_has_incompat_log_feature(sbp, XFS_SB_FEAT_INCOMPAT_LOG_UNKNOWN)) {
		fprintf(stderr,
_("%s: unknown incompatlog feature 0x%x; continuing anyway.\n"),
			fsdev, fs->m_sb.sb_features_log_incompat);
	}

	/*
	 * xfs_check needs corrected incore superblock values
	 */
	if (sbp->sb_rootino != NULLFSINO &&
	    xfs_sb_version_haslazysbcount(&fs->m_sb)) {
		int error = xfs_initialize_perag_data(fs, sbp->sb_agcount);
		if (error) {
			fprintf(stderr,
_("%s: cannot init perag data (%d). Continuing anyway.\n"),
				progname, error);
		}
	}

	/* Open things */
	memset(&wf, 0, sizeof(wf));
	db_err = truncate(dbfile, 0);
	if (db_err && errno != ENOENT) {
		fprintf(stderr, "%s %s", strerror(errno),
			_("while truncating database"));
		goto out;
	}

	err = compdbvfs_init("unix-excl", "comp-unix-excl", NULL);
	if (err) {
		fprintf(stderr, "%s %s\n", sqlite3_errstr(err),
			_("while setting up compressed db"));
		goto out;
	}

	err = sqlite3_open_v2(dbfile, &db,
			      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
			      "comp-unix-excl");
	if (err) {
		fprintf(stderr, "%s %s\n", sqlite3_errstr(err),
			_("while opening database"));
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
		fprintf(stderr, "%s %s", errm, "while starting transaction");
		free(errm);
		goto out;
	}
	CHECK_ERROR("while starting fs analysis database transaction");

	/*
	 * Use (almost) the same measurements as xfs_super.c.  We count the
	 * log towards total bytes, unlike XFS.
	 */
	total_bytes = fs->m_sb.sb_dblocks * fs->m_sb.sb_blocksize;
	free_bytes = (fs->m_sb.sb_fdblocks - xfs_alloc_set_aside(fs)) * fs->m_sb.sb_blocksize;
	fakeinos = (fs->m_sb.sb_fdblocks - xfs_alloc_set_aside(fs)) << fs->m_sb.sb_inopblog;
	total_inodes = MIN(fs->m_sb.sb_icount + fakeinos, (uint64_t)XFS_MAXINUMBER);
	if (fs->m_maxicount)
		total_inodes = MIN(total_inodes, fs->m_maxicount);
	total_inodes = MAX(total_inodes, fs->m_sb.sb_icount);
	free_inodes = total_inodes - (fs->m_sb.sb_icount - fs->m_sb.sb_ifree);
	collect_fs_stats(&wf.base, fsdev, fs->m_sb.sb_blocksize,
			fs->m_sb.sb_sectsize, total_bytes, free_bytes,
			total_inodes, free_inodes, XFS_NAME_LEN, "XFS");
	CHECK_ERROR("while storing fs stats");

	/* Walk the filesystem */
	wf.err = big_inode_bmap_init(fs, &wf.ino_bmap);
	CHECK_ERROR("while allocating scanned inode bitmap");
	walk_fs(&wf);
	CHECK_ERROR("while analyzing filesystem");

	/* Walk the metadata */
	walk_metadata(&wf);
	CHECK_ERROR("while analyzing metadata");

	/* Generate indexes and finalize. */
	index_db(&wf.base);
	CHECK_ERROR("while indexing database");
	finalize_fs_stats(&wf.base, fsdev);
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
		fprintf(stderr, "%s %s", errm, "while ending transaction");
		free(errm);
		goto out;
	}
	CHECK_ERROR("while flushing overview cache database transaction");

out:
	if (wf.ino_bmap)
		big_bmap_destroy(wf.ino_bmap);
	if (wf.wf_iconv)
		iconv_close(wf.wf_iconv);

	err2 = sqlite3_close(db);
	if (err2)
		fprintf(stderr, "%s %s", sqlite3_errstr(err2),
				"while closing database");
	if (!err && err2)
		err = err2;

	libxfs_umount(fs);
	if (x.ddev)
		libxfs_device_close(x.ddev);
	if (x.logdev && x.logdev != x.ddev)
		libxfs_device_close(x.logdev);
	if (x.rtdev)
		libxfs_device_close(x.rtdev);
	libxfs_destroy();

	return err;
}
