/*
 * Generate filemapper databases from ntfs filesystems.
 * Copyright 2015 Darrick J. Wong.
 * Licensed under the GPLv2.
 */
#undef DEBUG
#include <xfs/libxfs.h>
#include <signal.h>
#include <libgen.h>
#include "filemapper.h"

struct xfsmap_t {
	struct filemapper_t base;

	xfs_mount_t *fs;
	int err;
	uint8_t *ino_bmap;
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

typedef int (*walk_fn)(xfs_ino_t dir, const char *dname, size_t dname_len,
		       xfs_ino_t ino, int file_type, void *priv_data);

static int iterate_inline_dir(xfs_inode_t *ip, walk_fn fn, void *priv_data)
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
		return -EIO;

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
		if (fn(ip->i_ino, namebuf, sfep->namelen, ino, filetype,
		       priv_data))
			return -1;
		sfep = xfs_dir3_sf_nextentry(ip->i_mount, sfp, sfep);
	}
	return 0;
}

static int iterate_dirblock(xfs_inode_t *ip, xfs_buf_t *bp, walk_fn fn,
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
		if (fn(ip->i_ino, namebuf, dep->namelen, ino, filetype, priv_data)) {
			libxfs_putbuf(bp);
			return -1;
		}
		ptr += xfs_dir3_data_entsize(ip->i_mount, dep->namelen);
	}

	return 0;
}

int iterate_directory(xfs_inode_t *ip, walk_fn fn, void *priv_data)
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
			xfs_buf_t	*bp;

			/* directory entries are never higher than 32GB */
			if (off >= ip->i_mount->m_dirleafblk)
				return 0;

			bp = libxfs_readbuf(ip->i_mount->m_ddev_targp,
					XFS_FSB_TO_DADDR(ip->i_mount, poff),
					XFS_FSB_TO_BB(ip->i_mount, dblen),
					0, NULL);
			if (!bp)
				return -1;

			if (iterate_dirblock(ip, bp, fn, priv_data)) {
				libxfs_putbuf(bp);
				return -1;
			}
			libxfs_putbuf(bp);
		}
	}
	return 0;
}

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
			IRELE(inode);
			return 0;
		}
	} else {
		if (S_ISREG(inode->i_d.di_mode))
			type = XFS_DIR3_FT_REG_FILE;
		else if (S_ISDIR(inode->i_d.di_mode))
			type = XFS_DIR3_FT_DIR;
		else if (S_ISLNK(inode->i_d.di_mode))
			type = XFS_DIR3_FT_SYMLINK;
		else {
			IRELE(inode);
			return 0;
		}
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
	if (wf->wf_db_err) {
		IRELE(inode);
		return -1;
	}
	if (dir)
		insert_dentry(&wf->base, dir, name, ino);
	if (wf->wf_db_err) {
		IRELE(inode);
		return -1;
	}

#if 0
	walk_file_mappings(wf, dirent->inode, type);
	if (wf->err || wf->wf_db_err)
		return DIRENT_ABORT;
#endif

	if (type == XFS_DIR3_FT_DIR) {
		int err;

		old_dirpath = wf->wf_dirpath;
		wf->wf_dirpath = path;
		err = iterate_directory(inode, walk_fs_helper, wf);
		if (!wf->err)
			wf->err = err;
		wf->wf_dirpath = old_dirpath;
	}
	IRELE(inode);
	if (wf->err || wf->wf_db_err)
		return -1;

	return 0;
}

/* Walk the whole FS, looking for inodes to analyze. */
static void walk_fs(struct xfsmap_t *wf)
{
	wf->wf_dirpath = "";
	walk_fs_helper(0, "", 0, wf->fs->m_sb.sb_rootino, XFS_DIR3_FT_DIR, wf);
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
	libxfs_putbuf(bp);
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

#if 0
	/* Walk the metadata */
	walk_metadata(&wf);
	CHECK_ERROR("while analyzing metadata");
#endif

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
#endif
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

	return err;
}
