/*
 * Compress existing SQLite databases.
 * Copyright 2016 Darrick J. Wong.
 * Licensed under the GPLv2.
 */
#define DEBUG
#include <stdlib.h>
#include <stdio.h>
#include <sqlite3.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <lz4.h>
#include <lz4hc.h>
#include <zlib.h>
#include "compdb.h"

#define swap(a, b) \
        do { typeof(a) __tmp = (a); (a) = (b); (b) = __tmp; } while (0)

struct compdb_info {
	enum compdb_type	type;
	int			pagesize;
	unsigned long long	datastart;
	char			in_file_header[16];
	char			out_file_header[16];
};

/* Figure out database parameters. */
static int
sniff(
	const struct sqlite3_super	*super,
	struct compdb_info		*cdb)
{
	int				is_sqlite;
	int				is_compr;

	/* Is this really a database? */
	is_sqlite = !memcmp(super->magic, SQLITE_FILE_HEADER,
			sizeof(super->magic));
	is_compr = !memcmp(super->magic, cdb->in_file_header,
			sizeof(super->magic));
	if ((!is_sqlite && !is_compr) ||
	    super->max_fraction != 64 || super->min_fraction != 32 ||
	    super->leaf_payload != 32 || ntohl(super->schema_format) > 4)
		return EUCLEAN;

	if (is_sqlite)
		cdb->type = DB_REGULAR;
	else if (is_compr)
		cdb->type = DB_COMPRESSED;

	/* Collect some stats. */
	cdb->pagesize = ntohs(super->pagesize);
	if (cdb->pagesize == 1)
		cdb->pagesize = 65536;
	cdb->datastart = (ntohl(super->freelist_start) + 1 +
			   ntohl(super->freelist_pages)) * cdb->pagesize;
	dbg_printf("%s(%d): pagesize=%d datastart=%llu\n", __func__, __LINE__,
			cdb->pagesize, cdb->datastart);
	return 0;
}

int
main(
	int			argc,
	char			*argv[])
{
	struct sqlite3_super	super;
	struct compdb_info	cdb;
	struct stat		sb;
	char			*name;
	void			*bin, *bout;
	void			*outp;
	struct compdb_block_head	*bhead;
	struct compressor_type	*inc;
	struct compressor_type	*outc;
	int			fdin, fdout;
	int			try_compress;
	size_t			outlen;
	size_t			page;
	size_t			nr_pages;
	ssize_t			ret;

	if (argc < 3 || argc > 5) {
		printf("Usage: %s infile outfile [compressor] [compressor]\n", argv[0]);
		return 1;
	}

	/* Open files */
	fdin = open(argv[1], O_RDONLY);
	if (fdin < 0) {
		perror(argv[1]);
		return 2;
	}

	fdout = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, 0644);
	if (fdout < 0) {
		perror(argv[2]);
		return 2;
	}

	ret = fstat(fdin, &sb);
	if (ret) {
		perror(argv[1]);
		return 2;
	}

	/* Select compressor(s) */
	if (argc >= 4 && argv[3][0] != 0)
		name = argv[3];
	else
		name = NULL;

	inc = compdb_find_compressor(name);
	if (!inc) {
		printf("%s: no such compressor?\n", name);
		return 2;
	}
	snprintf(cdb.in_file_header, sizeof(cdb.in_file_header),
			COMPDB_FILE_TEMPLATE, inc->name);

	if (argc == 5 && argv[4][0] != 0)
		name = argv[4];
	else
		name = NULL;

	outc = compdb_find_compressor(name);
	if (!outc) {
		printf("%s: no such compressor?\n", name);
		return 2;
	}
	snprintf(cdb.out_file_header, sizeof(cdb.out_file_header),
			COMPDB_FILE_TEMPLATE, outc->name);
	dbg_printf("recompress %s(%s) -> %s(%s)\n", argv[1], inc->name,
			argv[2], outc->name);

	/* Verify superblock */
	ret = pread(fdin, &super, sizeof(super), 0);
	if (ret < 0) {
		perror(argv[1]);
		return 2;
	} else if (ret < sizeof(super)) {
		printf("%s: Short super read??\n", argv[1]);
		return 2;
	}

	ret = sniff(&super, &cdb);
	if (ret < 0) {
		perror(argv[1]);
		return 2;
	}

	/* Allocate buffers */
	bin = malloc(cdb.pagesize);
	if (!bin) {
		perror("malloc");
		return 2;
	}

	bout = malloc(cdb.pagesize);
	if (!bout) {
		perror("malloc");
		return 2;
	}

	/* Copy pages */
	nr_pages = (sb.st_size + cdb.pagesize - 1) / cdb.pagesize;
	for (page = 0; page < nr_pages; page++) {
		/* Read buffer. */
		dbg_printf("%s(%d) off=%zu len=%u\n", __func__, __LINE__,
				page * cdb.pagesize, cdb.pagesize);
		ret = pread(fdin, bin, cdb.pagesize, page * cdb.pagesize);
		if (ret < 0) {
			perror(argv[1]);
			return 3;
		} else if (ret < cdb.pagesize && page != nr_pages - 1) {
			printf("%s: Short page %zu read?\n", argv[1], page);
			return 3;
		}

		/* Transform buffer. */
		outlen = cdb.pagesize;
		outp = bin;
		bhead = bin;
		try_compress = 0;
		if ((cdb.type == DB_REGULAR || inc != outc) && page == 0) {
			/* Do we need to change the header? */
			memcpy(outp, cdb.out_file_header,
					sizeof(cdb.out_file_header));
		} else if (cdb.type == DB_COMPRESSED &&
			   !memcmp(bhead->magic, COMPDB_BLOCK_MAGIC,
					sizeof(COMPDB_BLOCK_MAGIC)) &&
			   ntohl(bhead->offset) == page) {
			if (inc == outc) {
				/* Compressed page, send it along. */
				outlen = ntohs(bhead->len) + sizeof(*bhead);
			} else {
				/* Changing compression types; decompress. */
				ret = inc->decompress(bin + sizeof(*bhead),
						bout,
						ntohs(bhead->len),
						cdb.pagesize);
				if (ret <= 0) {
					printf("%s: Decompression failed at "
						" page %zu\n", argv[1], page);
					return 3;
				}
				dbg_printf("%s(%d) off=%zu len=%d\n", __func__,
						__LINE__, page * cdb.pagesize,
						ntohs(bhead->len));
				swap(bin, bout);
				try_compress = 1;
			}
		} else if ((page + 1) * cdb.pagesize > cdb.datastart) {
			/* Uncompressed; try to compress. */
			try_compress = 1;
		}

		if (try_compress) {
			/* Try to compress this page? */
			ret = outc->compress(bin, bout + sizeof(*bhead),
					cdb.pagesize,
					cdb.pagesize - sizeof(*bhead));
			if (ret > 0) {
				bhead = bout;
				memcpy(bhead->magic, COMPDB_BLOCK_MAGIC,
						sizeof(bhead->magic));
				bhead->len = htons(ret);
				bhead->offset = htonl(page);
				outp = bout;
				outlen = ret + sizeof(*bhead);
			}
		}

		/*
		 * Truncate to where the end of the compressed block
		 * should be so that XFS won't do speculative preallocation.
		 */
		ret = ftruncate(fdout, (page * cdb.pagesize) + outlen);
		if (ret) {
			perror(argv[2]);
			return 2;
		}

		/* Write to disk. */
		dbg_printf("%s(%d) off=%zu len=%zu\n", __func__, __LINE__,
				page * cdb.pagesize, outlen);
		ret = pwrite(fdout, outp, outlen, page * cdb.pagesize);
		if (ret < 0) {
			perror(argv[2]);
			return 3;
		} else if (ret < outlen) {
			printf("%s: Short page %zu write?\n", argv[2], page);
			return 3;
		}

		/*
		 * Truncate to the end of the block to avoid short reads.
		 */
		if (outlen != cdb.pagesize) {
			ret = ftruncate(fdout, (page + 1) * cdb.pagesize);
			if (ret) {
				perror(argv[2]);
				return 2;
			}
		}
	}

	free(bout);
	free(bin);
	close(fdout);
	close(fdin);

	return 0;
}
