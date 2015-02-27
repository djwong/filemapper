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

struct xfsmap_t {
	struct filemapper_t base;

	xfs_mount_t	*fs;
	int		err;
	uint8_t		*ino_bmap;

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

typedef int (*extent_walk_fn)(xfs_inode_t *ip, struct xfs_extent_t *extent,
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
	struct btree_root **bmap;
	xfs_agnumber_t sz;
};

static int big_bmap_init(xfs_mount_t *fs, struct big_bmap **bbmap);
static void big_bmap_destroy(struct big_bmap *bbmap);
static void big_bmap_set(struct big_bmap *bbmap, xfs_agnumber_t group,
			 xfs_agblock_t offset, xfs_extlen_t blen, int state);

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
/* This must come last */
#define INO_GROUPS_DIR		(-9)
#define STR_GROUPS_DIR		"groups"


/* Walk a directory */

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
	ASSERT(0);
}

const struct xfs_buf_ops xfsmapper_dir3_data_buf_ops = {
	.verify_read = xfsmapper_dir3_data_read_verify,
	.verify_write = fail_write_verify,
};

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

int walk_bmap_btree_leaves(xfs_inode_t *ip, int whichfork, extent_walk_fn fn,
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
	xfs_mount_t		*mp;	/* file system mount structure */
	__be64			*pp;	/* pointer to block address */
	xfs_bmbt_key_t		*kp;	/* pointer to offset address */
	int			num_recs;
	struct xfs_extent_t	xext;
	/* REFERENCED */

	memset(&xext, 0, sizeof(xext));
	bno = NULLFSBLOCK;
	mp = ip->i_mount;
	ifp = XFS_IFORK_PTR(ip, whichfork);
	block = ifp->if_broot;
	/*
	 * Root level must use BMAP_BROOT_PTR_ADDR macro to get ptr out.
	 */
	level = be16_to_cpu(block->bb_level);
	kp = XFS_BMAP_BROOT_KEY_ADDR(mp, block, 1);
	pp = XFS_BMAP_BROOT_PTR_ADDR(mp, block, 1, ifp->if_broot_bytes);
	next_level_bno = be64_to_cpu(*pp);
	if (!XFS_FSB_SANITY_CHECK(mp, next_level_bno))
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
				if (!XFS_FSB_SANITY_CHECK(mp, bno))
					goto err;
				xext.p_off = XFS_FSB_TO_B(ip->i_mount, bno);
				xext.l_off = XFS_FSB_TO_B(ip->i_mount, kno);
				xext.len = 0;
				xext.state = XFS_EXT_NORM;
				xext.extentmap = 1;
				if (fn(ip, &xext, priv_data))
					goto err;
			}

			/* now go to the right sibling */
			bno = be64_to_cpu(block->bb_u.l.bb_rightsib);
			if (bno == NULLFSBLOCK)
				break;
			else if (!XFS_FSB_SANITY_CHECK(mp, bno))
				goto err;
			if (bp)
				xfsmapper_putbuf(bp);
			error = xfs_btree_read_bufl(mp, NULL, bno, 0, &bp,
					XFS_BMAP_BTREE_REF, &xfs_bmbt_buf_ops);
			if (error)
				return error;
			if (bp->b_error)
				goto err;
			block = XFS_BUF_TO_BLOCK(bp);
			if (!xfs_bmap_sanity_check(mp, bp, level))
				goto err;
			kp = XFS_BMBT_KEY_ADDR(mp, block, 1);
			pp = XFS_BMBT_PTR_ADDR(mp, block, 1, mp->m_bmap_dmxr[1]);
		} while (1);

		/* now go down the tree */
		level--;
		if (level == 0)
			break;
		if (bp)
			xfsmapper_putbuf(bp);
		error = xfs_btree_read_bufl(mp, NULL, next_level_bno, 0, &bp,
				XFS_BMAP_BTREE_REF, &xfs_bmbt_buf_ops);
		if (error)
			return error;
		if (bp->b_error)
			goto err;
		block = XFS_BUF_TO_BLOCK(bp);
		if (!xfs_bmap_sanity_check(mp, bp, level))
			goto err;
		kp = XFS_BMBT_KEY_ADDR(mp, block, 1);
		pp = XFS_BMBT_PTR_ADDR(mp, block, 1, mp->m_bmap_dmxr[1]);
		next_level_bno = be64_to_cpu(*pp);
		if (!XFS_FSB_SANITY_CHECK(mp, next_level_bno))
			goto err;
	} while (1);
	if (bp)
		xfsmapper_putbuf(bp);
	return 0;
err:
	xfsmapper_putbuf(bp);
	return EFSCORRUPTED;
}

/* Walk fork extents */

static void insert_xfs_extent(xfs_inode_t *ip, struct xfs_extent_t *xext,
			      struct xfsmap_t *wf)
{
	int type;
	int flags;

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
	insert_extent(&wf->base, ip->i_ino, xext->p_off, xext->l_off,
		      xext->len, flags, type);
}

static int walk_extent_helper(xfs_inode_t *ip, struct xfs_extent_t *extent,
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
			dbg_printf("R: ino=%ld len=%llu\n", ip->i_ino,
				   last->len);
			return 0;
		}

		/* Insert the extent */
		dbg_printf("R: ino=%ld pblk=%llu lblk=%llu len=%llu\n",
			   ip->i_ino, last->p_off, last->l_off, last->len);
		insert_xfs_extent(ip, last, wf);
		if (wf->wf_db_err)
			return -1;
	}

	/* Start recording extents */
	*last = *extent;
	return 0;
}

static unsigned long long inode_poff(xfs_inode_t *ip)
{
	return XFS_FSB_TO_B(ip->i_mount,
			XFS_DADDR_TO_FSB(ip->i_mount, ip->i_imap.im_blkno)) +
			ip->i_imap.im_boffset;
}

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
		fn(ip, &xext, priv_data);
		break;
	case XFS_DINODE_FMT_BTREE:
		if (!(ifp->if_flags & XFS_IFEXTENTS)) {
			err = xfs_iread_extents(NULL, ip, fork);
			if (err)
				return err;
		}
		/* read leaves... */
		err = walk_bmap_btree_leaves(ip, fork,
				(extent_walk_fn)insert_xfs_extent, priv_data);
		if (err)
			return err;
		/* drop through */
	case XFS_DINODE_FMT_EXTENTS:
		nextents = ifp->if_bytes / (uint)sizeof(xfs_bmbt_rec_t);
		for (idx = 0; idx < nextents; idx++) {
			ep = xfs_iext_get_ext(ifp, idx);

			xfs_bmbt_get_all(ep, &ext);
			xext.p_off = XFS_FSB_TO_B(ip->i_mount, ext.br_startblock);
			xext.l_off = XFS_FSB_TO_B(ip->i_mount, ext.br_startoff);
			xext.len = XFS_FSB_TO_B(ip->i_mount, ext.br_blockcount);
			if (fn(ip, &xext, priv_data))
				break;
		}
		break;
	default:
		printf("Unknown fork format %d\n", XFS_IFORK_FORMAT(ip, fork));
		return EFSCORRUPTED;
	}

	return 0;
}

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
		insert_xfs_extent((inode), last, (wf)); \
		if (wf->wf_db_err) \
			return; \
	} while (0);

static void walk_file_mappings(struct xfsmap_t *wf, xfs_inode_t *ip, int type)
{
	unsigned long long ioff;

// XXX
#if 0
	if (test_bit(wf->ino_bmap, ip->i_ino))
		return;
	set_bit(wf->ino_bmap, ip->i_ino, 1);
#endif

	ioff = inode_poff(ip);
	insert_extent(&wf->base, ip->i_ino, ioff, 0,
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
	char path[PATH_MAX + 1];
	char name[XFS_NAME_LEN + 1];
	const char *old_dirpath;
	int type, sz;
	struct xfsmap_t *wf = priv_data;
	xfs_inode_t *inode = NULL;
	time_t atime, crtime, ctime, mtime;
	time_t *pcrtime = NULL;
	ssize_t size;
	int err = 0;

	if (!strcmp(dname, ".") || !strcmp(dname, ".."))
		return 0;

	sz = icvt(&wf->base, (char *)dname, dname_len, name, XFS_NAME_LEN);
	if (sz < 0)
		return -1;
	dbg_printf("dir=%ld name=%s/%s ino=%ld type=%d\n", dir, wf->wf_dirpath, name,
		   ino, file_type);

	memset(&inode, 0, sizeof(inode));
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

#if 0
	// XXX speed things up for metadata debugging
	if (type == XFS_DIR3_FT_DIR) {
		old_dirpath = wf->wf_dirpath;
		wf->wf_dirpath = path;
		err = iterate_directory(inode, walk_fs_helper, wf);
		if (!wf->err)
			wf->err = err;
		wf->wf_dirpath = old_dirpath;
	}
#endif
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

static int big_bmap_init(xfs_mount_t *fs, struct big_bmap **bbmap)
{
	xfs_agnumber_t group;
	xfs_agblock_t ag_size;
	struct big_bmap *u;

	u = calloc(1, sizeof(struct big_bmap));
	if (!u)
		return ENOMEM;

	u->sz = fs->m_sb.sb_agcount;
	u->bmap = calloc(fs->m_sb.sb_agcount, sizeof(struct btree_root *));
	if (!u->bmap) {
		free(u);
		return ENOMEM;
	}

	ag_size = fs->m_sb.sb_agblocks;
	for (group = 0; group < u->sz; group++) {
		if (group == u->sz - 1)
			ag_size = (xfs_extlen_t)(fs->m_sb.sb_dblocks -
				   (xfs_drfsbno_t)fs->m_sb.sb_agblocks * group);
		btree_init(&u->bmap[group]);
		btree_clear(u->bmap[group]);
		btree_insert(u->bmap[group], 0,
				&big_bmap_states[BBMAP_UNUSED]);
		btree_insert(u->bmap[group], ag_size,
				&big_bmap_states[BBMAP_BAD]);
	}

	*bbmap = u;
	return 0;
}

static void big_bmap_destroy(struct big_bmap *bbmap)
{
	xfs_agnumber_t group;

	for (group = 0; group < bbmap->sz; group++)
		btree_destroy(bbmap->bmap[group]);
	free(bbmap->bmap);
	free(bbmap);
}

static void big_bmap_set(struct big_bmap *bbmap, xfs_agnumber_t group,
			 xfs_agblock_t offset, xfs_extlen_t blen, int state)
{
	dbg_printf("%s: group=%d offset=%d blen=%d state=%d\n", __func__,
		   group, offset, blen, state);
	update_bmap(bbmap->bmap[group], offset, blen, &big_bmap_states[state]);
}

static void walk_bitmap(struct xfsmap_t *wf, xfs_ino_t ino,
			struct big_bmap *bbmap)
{
	xfs_mount_t *fs = wf->fs;
	xfs_agnumber_t group;
	unsigned long key = 0;
	xfs_extlen_t len;
	int *val;
	struct btree_root *bmap;
	xfs_agblock_t ag_size;
	xfs_fsblock_t s;
	int64_t loff;

	ag_size = fs->m_sb.sb_agblocks;
	loff = 0;
	for (group = 0; group < bbmap->sz; group++) {
		if (group == bbmap->sz - 1)
			ag_size = (xfs_extlen_t)(fs->m_sb.sb_dblocks -
				   (xfs_drfsbno_t)fs->m_sb.sb_agblocks * group);
		key = 0;
		bmap = bbmap->bmap[group];
		val = btree_find(bmap, key, &key);
		while (val != NULL && key != ag_size) {
			get_bmap_ext(bmap, key, ag_size, &len);
			if (val != &big_bmap_states[BBMAP_INUSE]) {
				val = btree_lookup_next(bmap, &key);
				continue;
			}

			dbg_printf("group=%d key=%lu len=%lu val=%x\n", group,
				   key, (unsigned long)len, *val);
			s = XFS_AGB_TO_FSB(fs, group, 0);
			loff += (3 * fs->m_sb.sb_sectsize);
			insert_extent(&wf->base, ino, XFS_FSB_TO_B(fs, s), loff,
				      3 * fs->m_sb.sb_sectsize, EXTENT_SHARED,
				      extent_codes[XFS_DIR3_XT_METADATA]);
			val = btree_lookup_next(bmap, &key);
		}
	}
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

/* Invent a FS tree for metadata. */
static void walk_metadata(struct xfsmap_t *wf)
{
	xfs_mount_t *fs = wf->fs;
	xfs_agnumber_t group;
	int64_t ino, group_ino;
	xfs_fsblock_t s;
	char path[PATH_MAX + 1];
	struct big_bmap *bmap_agi;
	int w;

	bmap_agi = NULL;

	INJECT_METADATA(fs->m_sb.sb_rootino, "", INO_METADATA_DIR, \
			STR_METADATA_DIR, XFS_DIR3_FT_DIR);
	INJECT_ROOT_METADATA(GROUPS_DIR, XFS_DIR3_FT_DIR);
	INJECT_ROOT_METADATA(SB_FILE, XFS_DIR3_XT_METADATA);
#if 0
	INJECT_ROOT_METADATA(GDT_FILE, EXT2_XT_METADATA);
	INJECT_ROOT_METADATA(BBITMAP_FILE, EXT2_XT_METADATA);
	INJECT_ROOT_METADATA(IBITMAP_FILE, EXT2_XT_METADATA);
	INJECT_ROOT_METADATA(ITABLE_FILE, EXT2_XT_METADATA);
	INJECT_ROOT_METADATA(HIDDEN_DIR, EXT2_FT_DIR);
#endif
	/* Handle the log */
	if (fs->m_sb.sb_logstart) {
	INJECT_ROOT_METADATA(JOURNAL_FILE, XFS_DIR3_FT_REG_FILE);
		insert_extent(&wf->base, INO_JOURNAL_FILE,
			      XFS_FSB_TO_B(fs, fs->m_sb.sb_logstart), 0,
			      XFS_FSB_TO_B(fs, fs->m_sb.sb_logblocks), 0,
			      extent_codes[XFS_DIR3_FT_REG_FILE]);
		if (wf->wf_db_err)
			goto out;
	}

	/* Bitmaps for aggregate metafiles */
	wf->err = big_bmap_init(fs, &bmap_agi);
	if (wf->err)
		goto out;
#if 0
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
#endif

	ino = INO_GROUPS_DIR - 1;
	snprintf(path, PATH_MAX, "%d", fs->m_sb.sb_agcount);
	w = strlen(path);
	for (group = 0; group < fs->m_sb.sb_agcount; group++) {
		/* load superblock stuff */
		snprintf(path, PATH_MAX, "%0*d", w, group);
		group_ino = ino;
		ino--;
		INJECT_GROUP(group_ino, path, XFS_DIR3_FT_DIR);

		snprintf(path, PATH_MAX, "/%s/%s/%0*d", STR_METADATA_DIR,
			 STR_GROUPS_DIR, w, group);

		/* Record the superblock+agf+agi+agfl blocks */
		s = XFS_AGB_TO_FSB(fs, group, 0);
		big_bmap_set(bmap_agi, group, 0, 1, 1);
		INJECT_METADATA(group_ino, path, ino, "superblock",
				XFS_DIR3_XT_METADATA);
		insert_extent(&wf->base, ino, XFS_FSB_TO_B(fs, s),
			      0, 3 * fs->m_sb.sb_sectsize, EXTENT_SHARED,
			      extent_codes[XFS_DIR3_XT_METADATA]);
		if (wf->wf_db_err)
			goto out;
		ino--;

#if 0
		/* Record block bitmap */
		s = ext2fs_block_bitmap_loc(fs, group);
		ext2fs_fast_mark_block_bitmap2(sb_bbitmap, s);
		INJECT_METADATA(group_ino, path, ino, "block_bitmap",
				EXT2_XT_METADATA);
		insert_extent(&wf->base, ino, s * fs->blocksize, 0,
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
		insert_extent(&wf->base, ino, s * fs->blocksize, 0,
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
		insert_extent(&wf->base, ino, s * fs->blocksize, 0,
			      fs->inode_blocks_per_group * fs->blocksize,
			      EXTENT_SHARED, extent_codes[EXT2_XT_METADATA]);
		if (wf->wf_db_err)
			goto out;
		ino--;
#endif
	}

	/* Emit extents for the overall files */
	walk_bitmap(wf, INO_SB_FILE, bmap_agi);
	if (wf->err || wf->wf_db_err)
		goto out;
#if 0
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
#endif
out:
#if 0
	ext2fs_free_block_bitmap(sb_itable);
	ext2fs_free_block_bitmap(sb_ibitmap);
	ext2fs_free_block_bitmap(sb_bbitmap);
	ext2fs_free_block_bitmap(sb_gdt);
	ext2fs_free_block_bitmap(sb_bmap);
#endif
	big_bmap_destroy(bmap_agi);
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
		fprintf(stderr, "%s %s", strerror(errno), (msg)); \
		goto out; \
	} \
	if (wf.wf_db_err) { \
		fprintf(stderr, "%s %s", sqlite3_errstr(wf.wf_db_err), (msg)); \
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
	unsigned long long	total_bytes;
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
	 * Read the superblock, but don't validate it - we are a diagnostic
	 * tool and so need to be able to mount busted filesystems.
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
		fprintf(stderr, "%s %s", strerror(errno), "while truncating database");
		goto out;
	}

	err = sqlite3_open_v2(dbfile, &db,
			      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
			      "unix-excl");
	if (err) {
		fprintf(stderr, "%s %s", sqlite3_errstr(err), "while opening database");
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
	if (wf.wf_db_err) {
		fprintf(stderr, "%s %s", sqlite3_errstr(wf.wf_db_err), "while starting transaction");
		goto out;
	}

	total_bytes = fs->m_sb.sb_dblocks * fs->m_sb.sb_blocksize;
	collect_fs_stats(&wf.base, fsdev, fs->m_sb.sb_blocksize,
			 fs->m_sb.sb_sectsize, total_bytes,
			 fs->m_sb.sb_fdblocks * fs->m_sb.sb_blocksize,
			 fs->m_sb.sb_icount,
			 fs->m_sb.sb_ifree,
			 XFS_NAME_LEN);
	CHECK_ERROR("while storing fs stats");

	/* Walk the filesystem */
	wf.ino_bmap = calloc(1, fs->m_sb.sb_icount / 8);
	if (!wf.ino_bmap)
		wf.err = ENOMEM;
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

#if 1
	/* Cache overviews. */
	cache_overview(&wf.base, total_bytes, 2048);
	CHECK_ERROR("while caching CLI overview");
	cache_overview(&wf.base, total_bytes, 65536);
	CHECK_ERROR("while caching GUI overview");
#endif
	wf.wf_db_err = sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &errm);
	if (errm) {
		fprintf(stderr, "%s %s", errm, "while starting transaction");
		free(errm);
		goto out;
	}
	if (wf.wf_db_err) {
		fprintf(stderr, "%s %s", sqlite3_errstr(wf.wf_db_err), "while ending transaction");
		goto out;
	}
out:
	if (wf.ino_bmap)
		free(wf.ino_bmap);
	if (wf.wf_iconv)
		iconv_close(wf.wf_iconv);

	err2 = sqlite3_close(db);
	if (err2)
		fprintf(stderr, "%s %s", sqlite3_errstr(err2), "while closing database");
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
