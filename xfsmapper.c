/*
 * Generate filemapper databases from ntfs filesystems.
 * Copyright 2015 Darrick J. Wong.
 * Licensed under the GPLv2.
 */
#undef DEBUG
#include <xfs/libxfs.h>
#include <repair/btree.h>
#include <signal.h>
#include <libgen.h>
#include "filemapper.h"

#define XFS_FSBLOCK_TO_BYTES(fs, fsblock) \
		(XFS_FSB_TO_DADDR((fs), (fsblock)) << BBSHIFT)

struct xfs_extent_t
{
	unsigned long long p_off;
	unsigned long long l_off;
	unsigned long long len;
	xfs_exntst_t	state;
	int		unaligned:1;
	int		inlinedata:1;
	int		extentmap:1;
};

struct big_bmap;

struct xfsmap_t {
	struct filemapper_t base;

	xfs_mount_t	*fs;
	int		err;
	struct big_bmap *ino_bmap;
	struct big_bmap	*bbmap;
	xfs_agnumber_t	agno;
	struct xfs_extent_t	last_ext;
	int		itype;
};
#define wf_db		base.db
#define wf_db_err	base.db_err
#define wf_dirpath	base.dirpath
#define wf_iconv	base.iconv

#define XFS_NAME_LEN	255

#define XFS_DIR3_XT_METADATA	(XFS_DIR3_FT_MAX + 16)
#define XFS_DIR3_XT_EXTENT	(XFS_DIR3_FT_MAX + 17)
#define XFS_DIR3_XT_XATTR	(XFS_DIR3_FT_MAX + 18)

static int type_codes[] = {
	[XFS_DIR3_FT_REG_FILE]	= INO_TYPE_FILE,
	[XFS_DIR3_FT_DIR]	= INO_TYPE_DIR,
	[XFS_DIR3_FT_SYMLINK]	= INO_TYPE_SYMLINK,
	[XFS_DIR3_XT_METADATA]	= INO_TYPE_METADATA,
};

static int extent_codes[] = {
	[XFS_DIR3_FT_REG_FILE]	= EXT_TYPE_FILE,
	[XFS_DIR3_FT_DIR]	= EXT_TYPE_DIR,
	[XFS_DIR3_FT_SYMLINK]	= EXT_TYPE_SYMLINK,
	[XFS_DIR3_XT_METADATA]	= EXT_TYPE_METADATA,
	[XFS_DIR3_XT_EXTENT]	= EXT_TYPE_EXTENT,
	[XFS_DIR3_XT_XATTR]	= EXT_TYPE_XATTR,
};

typedef int (*dentry_walk_fn)(xfs_ino_t dir, const char *dname,
			      size_t dname_len, xfs_ino_t ino, int file_type,
			      void *priv_data);

typedef int (*extent_walk_fn)(int64_t ino, struct xfs_extent_t *extent,
			      void *priv_data);

#ifdef STRICT_PUTBUF
void xfsmapper_putbuf(xfs_buf_t *bp)
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
	xfs_mount_t *fs;
	struct btree_root **bmap;
	xfs_agblock_t *bmap_sizes;
	xfs_agnumber_t sz;
	int multiplier;
};

static int big_block_bmap_init(xfs_mount_t *fs, struct big_bmap **bbmap);
static int big_inode_bmap_init(xfs_mount_t *fs, struct big_bmap **bbmap);
static void big_bmap_destroy(struct big_bmap *bbmap);
static void big_bmap_set(struct big_bmap *bbmap, xfs_agnumber_t agno,
			 xfs_agblock_t offset, xfs_extlen_t blen, int state);
static int big_bmap_test(struct big_bmap *bbmap, xfs_agnumber_t agno,
			 xfs_agblock_t offset);
static void big_bmap_dump(struct big_bmap *bbmap, xfs_agnumber_t agno);

/* AG data */

struct xfs_ag {
	xfs_buf_t *agf;
	xfs_buf_t *agi;
	xfs_buf_t *agfl;
};

/*
 * Read in the allocation group free block array.
 */
static int				/* error */
xfs_alloc_read_agfl(
	xfs_mount_t	*mp,		/* mount point structure */
	xfs_trans_t	*tp,		/* transaction pointer */
	xfs_agnumber_t	agno,		/* allocation group number */
	xfs_buf_t	**bpp)		/* buffer for the ag free block array */
{
	xfs_buf_t	*bp;		/* return value */
	int		error;

	ASSERT(agno != NULLAGNUMBER);
	error = libxfs_trans_read_buf(
			mp, tp, mp->m_ddev_targp,
			XFS_AG_DADDR(mp, agno, XFS_AGFL_DADDR(mp)),
			XFS_FSS_TO_BB(mp, 1), 0, &bp, &xfs_agfl_buf_ops);
	if (error)
		return error;
	ASSERT(!bp->b_error);
	xfs_buf_set_ref(bp, XFS_AGFL_REF);
	*bpp = bp;
	return 0;
}

/* Free AG information */
static void free_ags(xfs_mount_t *fs, struct xfs_ag *ags)
{
	xfs_agnumber_t agno;

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
static int read_ags(xfs_mount_t *fs, struct xfs_ag **pags)
{
	struct xfs_ag *ags;
	xfs_agnumber_t agno;
	int err;

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
#define INO_FL_FILE		(-7)
#define STR_FL_FILE		"freelist"
#define INO_JOURNAL_FILE	(-8)
#define STR_JOURNAL_FILE	"journal"
#define INO_ITABLE_FILE		(-9)
#define STR_ITABLE_FILE		"inodes"
#define INO_HIDDEN_DIR		(-10)
#define STR_HIDDEN_DIR		"hidden_files"
/* This must come last */
#define INO_GROUPS_DIR		(-11)
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
static int iterate_inline_dir(xfs_inode_t *ip, dentry_walk_fn fn,
			      void *priv_data)
{
	xfs_dir2_sf_entry_t	*sfep;		/* shortform directory entry */
	xfs_dir2_sf_hdr_t	*sfp;		/* shortform structure */
	char			namebuf[XFS_NAME_LEN + 1];
	int			i;
	xfs_ino_t		ino;
	uint8_t			filetype;

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
		ino = xfs_dir3_sfe_get_ino(ip->i_mount, sfp, sfep);
		filetype = xfs_dir3_sfe_get_ftype(ip->i_mount, sfp, sfep);
		if (fn(ip->i_ino, namebuf, sfep->namelen, ino, filetype, priv_data))
			break;
		sfep = xfs_dir3_sf_nextentry(ip->i_mount, sfp, sfep);
	}
	return 0;
}

/* Iterate directory entries in a directory data block */
static int iterate_dirblock(xfs_inode_t *ip, xfs_buf_t *bp, dentry_walk_fn fn,
			    void *priv_data)
{
	char			namebuf[XFS_NAME_LEN + 1];
	xfs_dir2_data_hdr_t	*hdr;
	char			*start;
	char			*ptr, *endptr;
	xfs_dir2_data_entry_t	*dep;
	xfs_dir2_data_unused_t	*dup;
	xfs_dir2_block_tail_t	*btp = NULL;
	xfs_ino_t		ino;
	uint8_t			filetype;

	hdr = bp->b_addr;
	ptr = start = (char *)xfs_dir3_data_unused_p(hdr);
	switch (hdr->magic) {
	case cpu_to_be32(XFS_DIR2_BLOCK_MAGIC):
	case cpu_to_be32(XFS_DIR3_BLOCK_MAGIC):
		btp = xfs_dir2_block_tail_p(ip->i_mount, hdr);
		endptr = (char *)xfs_dir2_block_leaf_p(btp);
		if (endptr <= ptr || endptr > (char *)btp)
			endptr = (char *)hdr + ip->i_mount->m_dirblksize;
		break;
	case cpu_to_be32(XFS_DIR3_DATA_MAGIC):
	case cpu_to_be32(XFS_DIR2_DATA_MAGIC):
		endptr = (char *)hdr + ip->i_mount->m_dirblksize;
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
		filetype = xfs_dir3_dirent_get_ftype(ip->i_mount, dep);
		dbg_printf("fn dino=%ld name='%s' (%d), ino=%ld ft=%d ptr=%ld entsz=%d\n",
			   ip->i_ino, namebuf, dep->namelen, ino, filetype,
			   ptr - (char *)hdr,
			   xfs_dir3_data_entsize(ip->i_mount, dep->namelen));
		if (fn(ip->i_ino, namebuf, dep->namelen, ino, filetype,
		       priv_data))
			break;
		ptr += xfs_dir3_data_entsize(ip->i_mount, dep->namelen);
	}

	return 0;
}

/* Directory block verification routines for libxfs */
void xfs_verifier_error(struct xfs_buf *bp);

static void xfsmapper_dir3_data_read_verify(
	struct xfs_buf		*bp)
{
	struct xfs_dir2_data_hdr *hdr = bp->b_addr;

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

static void fail_write_verify(struct xfs_buf	*bp)
{
	abort();
}

const struct xfs_buf_ops xfsmapper_dir3_data_buf_ops = {
	.verify_read = xfsmapper_dir3_data_read_verify,
	.verify_write = fail_write_verify,
};

/* Iterate the directory entries in a given directory inode */
int iterate_directory(xfs_inode_t *ip, dentry_walk_fn fn, void *priv_data)
{
	int		error;			/* error return value */
	int		idx;			/* extent record index */
	xfs_ifork_t	*ifp;			/* inode fork pointer */
	xfs_fileoff_t	off;			/* offset for this block */
	xfs_extnum_t	nextents;		/* number of extent entries */
	int		i;
	xfs_fsblock_t	poff;
	int		dblen;
	xfs_bmbt_rec_host_t	*ep;
	xfs_filblks_t	blen;
	xfs_buf_t	*bp;

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
		dbg_printf("EXT: poff=%ld loff=%ld len=%ld dblen=%d\n", poff, off, blen, dblen);

		for (i = 0; i < blen; i += dblen, off += dblen, poff += dblen) {
			/* directory entries are never higher than 32GB */
			if (off >= ip->i_mount->m_dirleafblk)
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

static int
xfs_bmap_sanity_check(
	struct xfs_mount	*mp,
	struct xfs_buf		*bp,
	int			level)
{
	struct xfs_btree_block  *block = XFS_BUF_TO_BLOCK(bp);

	if (block->bb_magic != cpu_to_be32(XFS_BMAP_CRC_MAGIC) &&
	    block->bb_magic != cpu_to_be32(XFS_BMAP_MAGIC))
		return 0;

	if (be16_to_cpu(block->bb_level) != level ||
	    be16_to_cpu(block->bb_numrecs) == 0 ||
	    be16_to_cpu(block->bb_numrecs) > mp->m_bmap_dmxr[level != 0])
		return 0;

	return 1;
}

/* Walk the internal nodes of a bmap btree */
int walk_bmap_btree_nodes(xfs_inode_t *ip, int whichfork, extent_walk_fn fn,
			  void *priv_data)
{
	struct xfs_btree_block	*block;	/* current btree block */
	xfs_fsblock_t		bno;	/* block # of "block" */
	xfs_fsblock_t		next_level_bno;	/* block # of next level in tree */
	xfs_fileoff_t		kno;	/* file offset of this btree node */
	xfs_buf_t		*bp;	/* buffer for "block" */
	int			error;	/* error return value */
	xfs_extnum_t		j;	/* index into the extents list */
	xfs_ifork_t		*ifp;	/* fork structure */
	int			level;	/* btree level, for checking */
	xfs_mount_t		*fs;	/* file system mount structure */
	__be64			*pp;	/* pointer to block address */
	xfs_bmbt_key_t		*kp;	/* pointer to offset address */
	int			num_recs;
	struct xfs_extent_t	xext;
	/* REFERENCED */

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
			if (!xfs_bmap_sanity_check(fs, bp, level))
				goto err;
			kp = XFS_BMBT_KEY_ADDR(fs, block, 1);
			pp = XFS_BMBT_PTR_ADDR(fs, block, 1, fs->m_bmap_dmxr[1]);
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
		if (!xfs_bmap_sanity_check(fs, bp, level))
			goto err;
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
int insert_xfs_extent(int64_t ino, struct xfs_extent_t *xext, void *priv_data)
{
	struct xfsmap_t *wf = priv_data;
	int type;
	int flags;
	uint64_t loff;

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
static int insert_meta_extent(int64_t ino, struct xfs_extent_t *xext,
			      void *priv_data)
{
	struct xfsmap_t *wf = priv_data;
	xfs_agblock_t agbno;
	xfs_extlen_t blen;
	xfs_daddr_t daddr = xext->p_off >> BBSHIFT;

	agbno = xfs_daddr_to_agbno(wf->fs, daddr);
	blen = XFS_B_TO_FSB(wf->fs, xext->len);
	big_bmap_set(wf->bbmap, wf->agno, agbno, blen, 1);
	return 0;
}

/* Collect extents, combining adjacent ones */
static int walk_extent_helper(int64_t ino, struct xfs_extent_t *extent,
			      void *priv_data)
{
	struct xfsmap_t	*wf = priv_data;
	struct xfs_extent_t *last = &wf->last_ext;

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
static unsigned long long inode_poff(xfs_inode_t *ip)
{
	return XFS_FSBLOCK_TO_BYTES(ip->i_mount,
			XFS_DADDR_TO_FSB(ip->i_mount, ip->i_imap.im_blkno)) +
			ip->i_imap.im_boffset;
}

/* Iterate the extents attached to an inode's fork */
int iterate_fork_mappings(xfs_inode_t *ip, int fork, extent_walk_fn fn,
			  void *priv_data)
{
	int			idx;		/* extent record index */
	xfs_ifork_t		*ifp;		/* inode fork pointer */
	xfs_extnum_t		nextents;	/* number of extent entries */
	xfs_bmbt_rec_host_t	*ep;
	xfs_bmbt_irec_t		ext;
	struct xfs_extent_t	xext;
	int			err;

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
			xext.p_off = XFS_FSBLOCK_TO_BYTES(ip->i_mount, ext.br_startblock);
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
static void walk_file_mappings(struct xfsmap_t *wf, xfs_inode_t *ip, int type)
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
static int walk_fs_helper(xfs_ino_t dir, const char *dname, size_t dname_len,
			  xfs_ino_t ino, int file_type, void *priv_data)
{
	char		path[PATH_MAX + 1];
	char		name[XFS_NAME_LEN + 1];
	const char	*old_dirpath;
	int		type, sz;
	struct xfsmap_t	*wf = priv_data;
	xfs_inode_t	*inode = NULL;
	time_t		atime, crtime, ctime, mtime;
	time_t		*pcrtime = NULL;
	ssize_t		size;
	int		err;

	if (!strcmp(dname, ".") || !strcmp(dname, ".."))
		return 0;

	sz = icvt(&wf->base, (char *)dname, dname_len, name, XFS_NAME_LEN);
	if (sz < 0)
		return -1;
	dbg_printf("dir=%ld name=%s/%s ino=%ld type=%d\n", dir, wf->wf_dirpath, name,
		   ino, file_type);

	wf->err = libxfs_iget(wf->fs, NULL, ino, 0, &inode, 0);
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
		if (S_ISREG(inode->i_d.di_mode))
			type = XFS_DIR3_FT_REG_FILE;
		else if (S_ISDIR(inode->i_d.di_mode))
			type = XFS_DIR3_FT_DIR;
		else if (S_ISLNK(inode->i_d.di_mode))
			type = XFS_DIR3_FT_SYMLINK;
		else
			goto out;
	}

	atime = inode->i_d.di_atime.t_sec;
	mtime = inode->i_d.di_mtime.t_sec;
	ctime = inode->i_d.di_ctime.t_sec;
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
static void walk_fs(struct xfsmap_t *wf)
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
	unsigned long		end = offset + blen;
	int			*cur_state;
	unsigned long		cur_key;
	int			*next_state;
	unsigned long		next_key;
	int			*prev_state;

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
static int big_bmap_init(xfs_mount_t *fs, struct big_bmap **bbmap,
			 int multiplier)
{
	xfs_agnumber_t agno;
	xfs_agblock_t ag_size;
	struct big_bmap *u;

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
				   (xfs_drfsbno_t)fs->m_sb.sb_agblocks * agno);
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

static int big_block_bmap_init(xfs_mount_t *fs, struct big_bmap **bbmap)
{
	return big_bmap_init(fs, bbmap, 1);
}

static int big_inode_bmap_init(xfs_mount_t *fs, struct big_bmap **bbmap)
{
	int m;

	m = fs->m_sb.sb_blocksize / fs->m_sb.sb_inodesize;
	return big_bmap_init(fs, bbmap, m);
}

/* Free a big block bitmap */
static void big_bmap_destroy(struct big_bmap *bbmap)
{
	xfs_agnumber_t agno;

	for (agno = 0; agno < bbmap->sz; agno++)
		btree_destroy(bbmap->bmap[agno]);
	free(bbmap->bmap);
	free(bbmap);
}

/* Set the state of a range of big bitmap blocks */
static void big_bmap_set(struct big_bmap *bbmap, xfs_agnumber_t agno,
			 xfs_agblock_t offset, xfs_extlen_t blen, int state)
{
	assert(blen);
	dbg_printf("%s: agno=%d offset=%d blen=%d state=%d\n", __func__,
		   agno, offset, blen, state);
	update_bmap(bbmap->bmap[agno], offset, blen, &big_bmap_states[state]);
}

/* Test the status of a block in a big bitmap */
static int big_bmap_test(struct big_bmap *bbmap, xfs_agnumber_t agno,
			 xfs_agblock_t offset)
{
	unsigned long key = offset;
	struct btree_root *bmap;
	int *val;

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

/* Dump a big bitmap */
static void big_bmap_dump(struct big_bmap *bbmap, xfs_agnumber_t agno)
{
	unsigned long key;
	xfs_agblock_t ag_size;
	xfs_extlen_t len;
	int *val;
	struct btree_root *bmap;

	ag_size = bbmap->fs->m_sb.sb_agblocks;
	if (agno == bbmap->sz - 1)
		ag_size = (xfs_extlen_t)(bbmap->fs->m_sb.sb_dblocks -
			   (xfs_drfsbno_t)bbmap->fs->m_sb.sb_agblocks * agno);
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

/* Walk an AG's bitmap, attaching extents for the in use blocks to the inode */
static void walk_ag_bitmap(struct xfsmap_t *wf, xfs_ino_t ino,
			   struct big_bmap *bbmap, xfs_agnumber_t agno)
{
	xfs_mount_t		*fs = wf->fs;
	unsigned long		key;
	xfs_agblock_t		ag_size;
	xfs_fsblock_t		s;
	xfs_extlen_t		len;
	int			*val;
	struct btree_root	*bmap;

	ag_size = fs->m_sb.sb_agblocks;
	if (agno == bbmap->sz - 1)
		ag_size = (xfs_extlen_t)(fs->m_sb.sb_dblocks -
			   (xfs_drfsbno_t)fs->m_sb.sb_agblocks * agno);
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
			      extent_codes[XFS_DIR3_XT_METADATA]);
		if (wf->err || wf->wf_db_err)
			break;
		val = btree_lookup_next(bmap, &key);
	}
}

/* Walk a big bitmap, attaching in use block as extents of the inode */
static void walk_bitmap(struct xfsmap_t *wf, xfs_ino_t ino,
			struct big_bmap *bbmap)
{
	xfs_agnumber_t agno;

	for (agno = 0; agno < bbmap->sz; agno++) {
		walk_ag_bitmap(wf, ino, bbmap, agno);
		if (wf->err || wf->wf_db_err)
			break;
	}
}

/* Find a btree block's pointers */
static void *ag_btree_ptr_addr(xfs_mount_t *fs, struct xfs_btree_block *block,
			       int is_inobt)
{
	if (is_inobt)
		return XFS_INOBT_PTR_ADDR(fs, block, 1,
			xfs_inobt_maxrecs(fs, fs->m_sb.sb_blocksize, 0));
	return XFS_ALLOC_PTR_ADDR(fs, block, 1,
			xfs_allocbt_maxrecs(fs, fs->m_sb.sb_blocksize, 0));
}

/* Walk the internal nodes of a AG btree */
static int walk_ag_btree_nodes(xfs_mount_t *fs, int64_t ino,
			       xfs_agnumber_t agno, xfs_agblock_t rootbno,
			       int refval, const struct xfs_buf_ops *ops,
			       extent_walk_fn fn, void *priv_data,
			       xfs_agblock_t *left_node_agbno, int is_inobt)
{
	struct xfs_btree_block	*block;	/* current btree block */
	xfs_agblock_t		bno;	/* block # of "block" */
	xfs_agblock_t		next_level_bno;	/* block # of next level in tree */
	xfs_fsblock_t		fsbno;
	xfs_fsblock_t		next_level_fsbno;
	xfs_buf_t		*bp;	/* buffer for "block" */
	int			error;	/* error return value */
	xfs_extnum_t		j;	/* index into the extents list */
	int			level;	/* btree level, for checking */
	__be32			*pp;	/* pointer to block address */
	int			num_recs;
	struct xfs_extent_t	xext;
	/* REFERENCED */

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
	error = xfs_btree_read_bufl(fs, NULL, fsbno, 0, &bp, refval, ops);
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
	pp = ag_btree_ptr_addr(fs, block, is_inobt);
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
					refval, ops);
			if (error)
				return error;
			error = EFSCORRUPTED;
			if (bp->b_error)
				goto err;
			block = XFS_BUF_TO_BLOCK(bp);
			num_recs = xfs_btree_get_numrecs(block);
			if (!xfs_bmap_sanity_check(fs, bp, level))
				goto err;
			pp = ag_btree_ptr_addr(fs, block, is_inobt);
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
				refval, ops);
		if (error)
			return error;
		error = EFSCORRUPTED;
		if (bp->b_error)
			goto err;
		block = XFS_BUF_TO_BLOCK(bp);
		num_recs = xfs_btree_get_numrecs(block);
		if (!xfs_bmap_sanity_check(fs, bp, level))
			goto err;
		pp = ag_btree_ptr_addr(fs, block, is_inobt);
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

static int walk_ag_allocbt_nodes(xfs_mount_t *fs, int64_t ino,
				 xfs_agnumber_t agno, xfs_agblock_t rootbno,
				 extent_walk_fn fn, void *priv_data)
{
	return walk_ag_btree_nodes(fs, ino, agno, rootbno, XFS_ALLOC_BTREE_REF,
				   &xfs_allocbt_buf_ops, fn, priv_data, NULL, 0);
}

static int walk_ag_inobt_nodes(xfs_mount_t *fs, int64_t ino,
			       xfs_agnumber_t agno, xfs_agblock_t rootbno,
			       extent_walk_fn fn, void *priv_data,
			       xfs_agblock_t *left_node_agbno)
{
	return walk_ag_btree_nodes(fs, ino, agno, rootbno, XFS_INO_BTREE_REF,
				   &xfs_inobt_buf_ops, fn, priv_data,
				   left_node_agbno, 1);
}

/* Walk the inode blocks of a AG */
static int walk_ag_inode_blocks(xfs_mount_t *fs, int64_t ino,
			       xfs_agnumber_t agno, xfs_agblock_t left_node_bno,
			       extent_walk_fn fn, void *priv_data)
{
	struct xfs_btree_block	*block;	/* current btree block */
	xfs_agblock_t		bno;	/* block # of "block" */
	xfs_fsblock_t		fsbno;
	xfs_buf_t		*bp;	/* buffer for "block" */
	int			error;	/* error return value */
	xfs_extnum_t		j;	/* index into the extents list */
	xfs_inobt_rec_t		*pp;	/* pointer to inode records */
	int			num_recs;
	struct xfs_extent_t	xext;
	/* REFERENCED */
	xfs_agino_t agino;

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
static void walk_metadata(struct xfsmap_t *wf)
{
	xfs_mount_t		*fs = wf->fs;
	xfs_agnumber_t		agno;
	int64_t			ino, group_ino;
	xfs_fsblock_t		s;
	char			path[PATH_MAX + 1];
	struct big_bmap		*bmap_ag, *bmap_agfl, *bmap_bnobt, *bmap_cntbt,
				*bmap_inobt, *bmap_finobt, *bmap_itable;
	struct xfs_ag		*ags = NULL;
	xfs_agf_t		*agf;
	xfs_agi_t		*agi;
	int			w;
	__be32			*freelist;
	unsigned long		i, len;
	xfs_agblock_t		left_inobt_leaf_agbno = 0;
	int			err;
	uint64_t		loff;

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

	bmap_ag = bmap_agfl = bmap_bnobt = bmap_cntbt = bmap_inobt =
			bmap_finobt = bmap_itable = NULL;

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
	INJECT_ROOT_METADATA(CNTBT_FILE, XFS_DIR3_XT_METADATA);
	INJECT_ROOT_METADATA(INOBT_FILE, XFS_DIR3_XT_METADATA);
	if (xfs_sb_version_hasfinobt(&fs->m_sb))
		INJECT_ROOT_METADATA(FINOBT_FILE, XFS_DIR3_XT_METADATA);
	INJECT_ROOT_METADATA(ITABLE_FILE, XFS_DIR3_XT_METADATA);

	/* Handle the log */
	if (fs->m_sb.sb_logstart) {
		INJECT_ROOT_METADATA(JOURNAL_FILE, XFS_DIR3_FT_REG_FILE);
		loff = 0;
		insert_extent(&wf->base, INO_JOURNAL_FILE,
			      XFS_FSBLOCK_TO_BYTES(fs, fs->m_sb.sb_logstart), &loff,
			      XFS_FSB_TO_B(fs, fs->m_sb.sb_logblocks), 0,
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
			big_bmap_set(bmap_agfl, agno, be32_to_cpu(freelist[i]), 1, 1);

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
				insert_meta_extent, wf);
		if (!wf->err)
			wf->err = err;
		if (wf->err)
			goto out;
		walk_ag_bitmap(wf, ino, wf->bbmap, agno);
		if (wf->err || wf->wf_db_err)
			goto out;
		ino--;

		/* cntbt */
		wf->bbmap = bmap_cntbt;
		INJECT_METADATA(group_ino, path, ino, STR_CNTBT_FILE,
				XFS_DIR3_XT_METADATA);
		err = walk_ag_allocbt_nodes(fs, ino, agno,
				be32_to_cpu(agf->agf_roots[XFS_BTNUM_CNT]),
				insert_meta_extent, wf);
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
	if (bmap_itable)
		big_bmap_destroy(bmap_itable);
	if (bmap_finobt)
		big_bmap_destroy(bmap_finobt);
	if (bmap_inobt)
		big_bmap_destroy(bmap_inobt);
	if (bmap_cntbt)
		big_bmap_destroy(bmap_cntbt);
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
	int	argc,
	char	**argv)
{
	char		*fsdev, *dbfile;
	libxfs_init_t	x;
	struct xfs_sb	*sbp;
	struct xfs_buf	*bp;
	int		c;
	xfs_mount_t	xmount;
	struct xfsmap_t	wf;
	int		db_err, err, err2;
	sqlite3		*db = NULL;
	char		*errm;
	unsigned long long	total_bytes, free_bytes, total_inodes,
				free_inodes, fakeinos;
	xfs_mount_t	*fs;

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
		fprintf(stderr, _("%s: %s is not a valid XFS filesystem (unexpected SB magic number 0x%08x)\n"),
			progname, fsdev, sbp->sb_magicnum);
	}

	fs = libxfs_mount(&xmount, sbp, x.ddev, x.logdev, x.rtdev, 0);
	if (!fs) {
		fprintf(stderr,
			_("%s: device %s unusable (not an XFS filesystem?)\n"),
			progname, fsdev);
		exit(1);
	}
	//blkbb = 1 << fs->m_blkbb_log;

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
			"while truncating database");
		goto out;
	}

	err = sqlite3_open_v2(dbfile, &db,
			      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
			      "unix-excl");
	if (err) {
		fprintf(stderr, "%s %s", sqlite3_errstr(err),
			"while opening database");
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
	free_bytes = (fs->m_sb.sb_fdblocks - XFS_ALLOC_SET_ASIDE(fs)) * fs->m_sb.sb_blocksize;
	fakeinos = (fs->m_sb.sb_fdblocks - XFS_ALLOC_SET_ASIDE(fs)) << fs->m_sb.sb_inopblog;
	total_inodes = MIN(fs->m_sb.sb_icount + fakeinos, (uint64_t)XFS_MAXINUMBER);
	if (fs->m_maxicount)
		total_inodes = MIN(total_inodes, fs->m_maxicount);
	total_inodes = MAX(total_inodes, fs->m_sb.sb_icount);
	free_inodes = total_inodes - (fs->m_sb.sb_icount - fs->m_sb.sb_ifree);
	collect_fs_stats(&wf.base, fsdev, fs->m_sb.sb_blocksize,
			 fs->m_sb.sb_sectsize, total_bytes, free_bytes,
			 total_inodes, free_inodes,
			 XFS_NAME_LEN, "XFS");
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
	cache_overview(&wf.base, total_bytes, 2048);
	CHECK_ERROR("while caching CLI overview");
	cache_overview(&wf.base, total_bytes, 65536);
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
