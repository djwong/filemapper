/*
 * Crappy program to render a visual file block map.
 * Copyright (C) 2010 IBM.  All rights reserved.
 * Author: Darrick J. Wong
 * (some parts shamelessly stolen from filefrag.c in e2fsprogs)
 * This program is licensed under the GNU General Public License, version 2.
 */
#define _XOPEN_SOURCE 500
#include <ftw.h>
#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include <search.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <fcntl.h>
#include "fiemap.h"

#define FS_IOC_FIEMAP	_IOWR('f', 11, struct fiemap)

#define BLOCK_UNKNOWN	0x1
#define BLOCK_FILE	0x2
#define BLOCK_DIR	0x4

struct extent_t {
	uint64_t start;
	uint64_t length;
	ino_t inode;
};

struct inode_t {
	ino_t inode;
	char *path;
	char type;
};

struct command_t {
	const char *stem;
	int (*fn) (const char *args);
};

struct map_context_t {
	char *map;
	unsigned int blocks_per_char;
};

struct inode_pair_t {
	ino_t start, end;
};

struct inode_context_t {
	struct inode_pair_t *inodes;
	unsigned int num_inodes;
};

#define ALLOC_SIZE	4096
#define BUF_SIZE	4096
#define PROMPT		"fiemapper> "

static int save_argc;
static char **save_argv;
static struct inode_context_t recursive_file_ctxt;
static unsigned int map_width = 2000;
static struct statvfs fs_stat;
static struct stat fs_root_stat;
static unsigned int blk_shift;
static struct extent_t *extents;
static struct inode_t *inodes;
static size_t num_inodes, max_inodes;
static size_t num_extents, max_extents;

int int_log2(int arg)
{
	int     l = 0;

	arg >>= 1;
	while (arg) {
		l++;
		arg >>= 1;
	}
	return l;
}

int compare_inodes(const void *a, const void *b)
{
	return ((struct inode_t *)a)->inode - ((struct inode_t *)b)->inode;
}

int compare_extents(const void *a, const void *b)
{
	return ((struct extent_t *)a)->start - ((struct extent_t *)b)->start;
}

int add_extent(ino_t inode, struct fiemap_extent *fm_extent)
{
	struct extent_t *extent;

	/* embedded in metadata, skip */
	if (fm_extent->fe_flags & FIEMAP_EXTENT_DATA_INLINE)
		return 0;

	/* resize extent array? */
	if (num_extents == max_extents) {
		extents = realloc(extents, (max_extents + ALLOC_SIZE) * sizeof(*extents));
		if (!extents)
			return -ENOMEM;
		max_extents += ALLOC_SIZE;
	}

	/* stuff extents array */
	extent = &extents[num_extents++];
	extent->inode = inode;
	extent->start = fm_extent->fe_physical >> blk_shift;
	extent->length = fm_extent->fe_length >> blk_shift;

	return 0;
}

int filefrag_fiemap(ino_t inode, const char *path)
{
	int fd;
	char buf[4096] = "";
	struct fiemap *fiemap = (struct fiemap *)buf;
	struct fiemap_extent *fm_ext = &fiemap->fm_extents[0];
	int count = (sizeof(buf) - sizeof(*fiemap)) /
			sizeof(struct fiemap_extent);
	unsigned long flags;
	unsigned int i;
	int last = 0;
	int rc = 0;

	fd = open(path, O_RDONLY);
	if (fd < 0) {
		perror(path);
		return -errno;
	}

	fiemap->fm_length = ~0ULL;
	memset(fiemap, 0, sizeof(struct fiemap));
	flags = FIEMAP_FLAG_SYNC;

	do {
		fiemap->fm_length = ~0ULL;
		fiemap->fm_flags = flags;
		fiemap->fm_extent_count = count;
		rc = ioctl(fd, FS_IOC_FIEMAP, (unsigned long) fiemap);
		if (rc < 0) {
			perror(path);
			goto out;
		}

		/* If 0 extents are returned, then more ioctls are not needed */
		if (fiemap->fm_mapped_extents == 0)
			break;

		for (i = 0; i < fiemap->fm_mapped_extents; i++) {
			rc = add_extent(inode, &fm_ext[i]);
			if (rc)
				goto out;

			if (fm_ext[i].fe_flags & FIEMAP_EXTENT_LAST)
				last = 1;
		}

		fiemap->fm_start = (fm_ext[i-1].fe_logical +
				    fm_ext[i-1].fe_length);
	} while (last == 0);

out:
	close(fd);
	return rc;
}

int process_file(const char *path, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
	struct inode_t key, *inode;

	/* ignore objects that aren't directories or files */
	if (!S_ISREG(sb->st_mode) && !S_ISDIR(sb->st_mode))
		return 0;

	/* have we already examined this inode? */
	key.inode = sb->st_ino;
	inode = lfind(&key, inodes, &num_inodes, sizeof(*inodes), compare_inodes);
	if (inode)
		return 0;

	/* stuff unseen inode in the inode array */
	if (num_inodes == max_inodes) {
		inodes = realloc(inodes, (max_inodes + ALLOC_SIZE) * sizeof(*inodes));
		if (!inodes) {
			perror(path);
			return -ENOMEM;
		}
		max_inodes += ALLOC_SIZE;
	}
	inode = &inodes[num_inodes++];
	inode->inode = sb->st_ino;
	inode->path = strdup(path);
	if (S_ISREG(sb->st_mode))
		inode->type = BLOCK_FILE;
	else if (S_ISDIR(sb->st_mode))
		inode->type = BLOCK_DIR;

	/* now figure out the extent mappings */
	return filefrag_fiemap(inode->inode, inode->path);
}

int walk_tree(const char *path)
{
	return nftw(path, process_file, 128, FTW_MOUNT | FTW_PHYS);
}

int init_data(void)
{
	inodes = realloc(NULL, ALLOC_SIZE * sizeof(*inodes));
	extents = realloc(NULL, ALLOC_SIZE * sizeof(*extents));

	if (!extents || !inodes) {
		fprintf(stderr, "Unable to allocate memory.\n");
		return -ENOMEM;
	}

	max_inodes = max_extents = ALLOC_SIZE;
	num_inodes = num_extents = 0;

	return 0;
}

void dump_inode(struct inode_t *i)
{
	printf("%llu: %s\n", (uint64_t)i->inode, i->path);
}

int dump_inodes_cmd(const char *args)
{
	unsigned int i;
	struct inode_t *inode = inodes;

	for (i = 0; i < num_inodes; i++)
		dump_inode(inode++);

	return 0;
}

void dump_extent(struct extent_t *i)
{
	printf("%llu: %llu -> %llu (%llu)\n", (uint64_t)i->inode, i->start, i->start + i->length - 1, i->length);
}

int dump_extents_cmd(const char *args)
{
	unsigned int i;
	struct extent_t *extent = extents;

	for (i = 0; i < num_extents; i++)
		dump_extent(extent++);

	return 0;
}

void mark_block_in_map(struct map_context_t *ctxt, ino_t inode, uint64_t block)
{
	struct inode_t key, *val;
	uint64_t map_num = block / ctxt->blocks_per_char;

	key.inode = inode;
	val = bsearch(&key, inodes, num_inodes, sizeof(*val), compare_inodes);
	if (!val)
		ctxt->map[map_num] |= BLOCK_UNKNOWN;
	else
		ctxt->map[map_num] |= val->type;
}

int generate_blockmap(unsigned int nr_chars, char **map, int (*block_fn)(struct map_context_t *ctxt, void *data), void *data)
{
	struct map_context_t ctxt;
	int i, ret;

	/* go find the blocks to highlight */
	ctxt.map = malloc(nr_chars + 1);
	if (!ctxt.map)
		return -ENOMEM;
	memset(ctxt.map, 0, nr_chars + 1);

	ctxt.blocks_per_char = fs_stat.f_blocks / nr_chars;
	if (fs_stat.f_blocks % nr_chars)
		ctxt.blocks_per_char++;

	ret = block_fn(&ctxt, data);
	if (ret)
		return ret;

	/* convert flags to printable characters */
	for (i = 0; i < nr_chars; i++) {
		if (ctxt.map[i] == BLOCK_DIR)
			ctxt.map[i] = 'D';
		else if (ctxt.map[i] == BLOCK_FILE)
			ctxt.map[i] = 'F';
		else if (ctxt.map[i] == BLOCK_UNKNOWN)
			ctxt.map[i] = 'U';
		else if (!ctxt.map[i])
			ctxt.map[i] = '.';
		else
			ctxt.map[i] = 'X';
	}

	*map = ctxt.map;
	return 0;
}

int find_all_blocks(struct map_context_t *ctxt, void *data)
{
	struct extent_t *extent = extents;
	struct extent_t *end = extents + num_extents;
	int i;

	while (extent != end) {
		for (i = 0; i < extent->length; i++) {
			mark_block_in_map(ctxt, extent->inode, extent->start + i);
		}
		extent++;
	}

	return 0;
}

int overview_cmd(const char *args)
{
	int ret;
	char *map;

	/* print pretty block map */
	ret = generate_blockmap(map_width, &map, find_all_blocks, NULL);
	if (ret)
		return ret;
	printf("%s\n", map);
	free(map);

	return 0;
}

int find_inode_blocks(struct map_context_t *ctxt, void *data)
{
	struct inode_context_t *ictxt = data;
	struct extent_t *extent = extents;
	struct extent_t *end = extents + num_extents;
	int i;

	if (!ictxt->num_inodes)
		return -ENOENT;

	while (extent != end) {
		int found = 0;
		for (i = 0; i < ictxt->num_inodes; i++) {
			if (extent->inode >= ictxt->inodes[i].start &&
			    extent->inode <= ictxt->inodes[i].end) {
				found = 1;
				break;
			}
		}

		if (!found)
			goto loop_end;

		for (i = 0; i < extent->length; i++) {
			mark_block_in_map(ctxt, extent->inode, extent->start + i);
		}

loop_end:
		extent++;
	}

	return 0;
}

int inode_cmd(const char *args)
{
	struct inode_context_t ctxt;
	int ret = 0;
	char *map, *tok, *tok_str = (char *)args;

	ctxt.inodes = NULL;
	ctxt.num_inodes = 0;

	while ((tok = strtok(tok_str, " "))) {
		char *endptr;
		unsigned long x, y;

		errno = 0;
		x = strtoul(tok, &endptr, 0);
		if (tok[0] == '-' || errno) {
			fprintf(stderr, "%s: Invalid start inode.\n", tok);
			goto loop_end;
		}
		y = x;

		if (*endptr == '-' && *(++endptr) != 0) {
			errno = 0;
			y = strtoul(endptr, NULL, 0);
			if (errno) {
				fprintf(stderr, "%s: Invalid end inode.\n", endptr);
				goto loop_end;
			}
		}

		ctxt.inodes = realloc(ctxt.inodes, (ctxt.num_inodes + 1) * sizeof(*ctxt.inodes));
		if (!ctxt.inodes) {
			ret = -ENOMEM;
			goto err;
		}
		ctxt.inodes[ctxt.num_inodes].start = x;
		ctxt.inodes[ctxt.num_inodes].end = y;
		ctxt.num_inodes++;

loop_end:
		tok_str = NULL;
	}

	/* print pretty block map */
	ret = generate_blockmap(map_width, &map, find_inode_blocks, &ctxt);
	if (ret)
		goto err;

	printf("%s\n", map);
	free(map);

err:
	free(ctxt.inodes);
	return ret;
}

int file_cmd(const char *args)
{
	struct inode_context_t ctxt;
	int ret = 0;
	char *map, *tok, *tok_str = (char *)args;

	ctxt.inodes = NULL;
	ctxt.num_inodes = 0;

	while ((tok = strtok(tok_str, " "))) {
		struct stat buf;

		ret = lstat(tok, &buf);
		if (ret) {
			perror(tok);
			goto loop_end;
		}

		if (buf.st_dev != fs_root_stat.st_dev) {
			fprintf(stderr, "%s: Not on the same filesystem.\n", tok);
			goto loop_end;
		}

		ctxt.inodes = realloc(ctxt.inodes, (ctxt.num_inodes + 1) * sizeof(*ctxt.inodes));
		if (!ctxt.inodes) {
			ret = -ENOMEM;
			goto err;
		}

		ctxt.inodes[ctxt.num_inodes].start = buf.st_ino;
		ctxt.inodes[ctxt.num_inodes].end = buf.st_ino;
		ctxt.num_inodes++;

loop_end:
		tok_str = NULL;
	}

	/* print pretty block map */
	ret = generate_blockmap(map_width, &map, find_inode_blocks, &ctxt);
	if (ret)
		goto err;

	printf("%s\n", map);
	free(map);

err:
	free(ctxt.inodes);
	return ret;
}

int recursive_file_cmd_helper(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftw)
{
	recursive_file_ctxt.inodes = realloc(recursive_file_ctxt.inodes, (recursive_file_ctxt.num_inodes + 1) * sizeof(*recursive_file_ctxt.inodes));
	if (!recursive_file_ctxt.inodes)
		return -ENOMEM;

	recursive_file_ctxt.inodes[recursive_file_ctxt.num_inodes].start = sb->st_ino;
	recursive_file_ctxt.inodes[recursive_file_ctxt.num_inodes].end = sb->st_ino;
	recursive_file_ctxt.num_inodes++;

	return 0;
}

int recursive_file_cmd(const char *args)
{
	int ret = 0;
	char *map, *tok, *tok_str = (char *)args;

	recursive_file_ctxt.inodes = NULL;
	recursive_file_ctxt.num_inodes = 0;

	while ((tok = strtok(tok_str, " "))) {
		struct stat buf;

		ret = lstat(tok, &buf);
		if (ret) {
			perror(tok);
			goto loop_end;
		}

		if (buf.st_dev != fs_root_stat.st_dev) {
			fprintf(stderr, "%s: Not on the same filesystem.\n", tok);
			goto loop_end;
		}

		nftw(tok, recursive_file_cmd_helper, 128, FTW_MOUNT | FTW_PHYS);

loop_end:
		tok_str = NULL;
	}

	/* print pretty block map */
	ret = generate_blockmap(map_width, &map, find_inode_blocks, &recursive_file_ctxt);
	if (ret)
		goto err;

	printf("%s\n", map);
	free(map);

err:
	free(recursive_file_ctxt.inodes);
	return ret;
}

int quit_cmd(const char *args)
{
	exit(0);
}

int width_cmd(const char *args)
{
	long x;

	x = strtol(args, NULL, 0);
	if (errno || x < 1) {
		fprintf(stderr, "%s: Invalid width.\n", args);
		return -EINVAL;
	}

	printf("Width set to %lu.\n", x);
	map_width = x;

	return 0;
}

int help_cmd(const char *args)
{
	int i;

	printf("Command Reference:\n");
	printf("file		Print block usage of specific files.\n");
	printf("help		Displays this help screen.\n");
	printf("inode		Print block usage of specific inodes or ranges of inodes.\n");
	printf("overview	Prints an overview of the filesystem.\n");
	printf("quit		Terminates this program.\n");
	printf("recursive	Print block usage of specific filesystem subtrees.\n");
	printf("width		Changes the width of the overview bar (currently %d).\n", map_width);
	printf("\n");
	printf("In the overview, D=directory, F=file, U=unknown, X=multiple, and .=empty\n");
	printf("Current view:");
	for (i = optind; i < save_argc; i++)
		printf(" %s", save_argv[i]);
	printf("\n");

	return 0;
}

static struct command_t commands[] = {
	/* long command format */
	{"overview", overview_cmd},
	{"quit", quit_cmd},
	{"help", help_cmd},
	{"width", width_cmd},
	{"inode", inode_cmd},
	{"file", file_cmd},
	{"recursive", recursive_file_cmd},

	/* short command format */
	{"o", overview_cmd},
	{"q", quit_cmd},
	{"h", help_cmd},
	{"w", width_cmd},
	{"i", inode_cmd},
	{"di", dump_inodes_cmd},
	{"de", dump_extents_cmd},
	{"f", file_cmd},
	{"r", recursive_file_cmd},

	{NULL, NULL},
};

void print_cmdline_help(const char *progname)
{
	printf("Usage: %s [-w width] path [paths...]\n", progname);
}

int main(int argc, char *argv[])
{
	int opt, i, ret = 0;
	struct statvfs arg_fs_stat;
	char cmd[BUF_SIZE];

	ret = init_data();
	if (ret)
		goto err;

	if (argc < 2 || !strcmp(argv[1], "--help")) {
		print_cmdline_help(argv[0]);
		return ret;
	}

	while ((opt = getopt(argc, argv, "w:")) != -1) {
		switch (opt) {
		case 'w':
			map_width = atoi(optarg);
			break;
		default:
			print_cmdline_help(argv[0]);
			break;
		}
	}

	/* collect fs data to ensure we don't go outside one fs */
	ret = statvfs(argv[optind], &fs_stat);
	if (ret) {
		perror(argv[optind]);
		goto err;
	}
	blk_shift = int_log2(fs_stat.f_frsize);
	if (fs_stat.f_frsize != fs_stat.f_bsize) {
		fprintf(stderr, "Fragment size != block size.  Hrm...\n");
	}

	for (i = optind + 1; i < argc; i++) {
		ret = statvfs(argv[i], &arg_fs_stat);
		if (ret) {
			perror(argv[i]);
			goto err;
		}

		if (memcmp(&arg_fs_stat, &fs_stat, sizeof(fs_stat))) {
			fprintf(stderr, "Error: One filesystem at a time!\n");
			ret = -ENOENT;
			goto err;
		}
	}

	ret = lstat(argv[optind], &fs_root_stat);
	if (ret) {
		perror(argv[optind]);
		goto err;
	}

	/* collect file data */
	for (i = optind; i < argc; i++) {
		ret = walk_tree(argv[i]);
		if (ret)
			goto err;
	}

	/* sort file data for better performance */
	qsort(inodes, num_inodes, sizeof(*inodes), compare_inodes);
	qsort(extents, num_extents, sizeof(*extents), compare_extents);

	save_argc = argc;
	save_argv = argv;
	overview_cmd(NULL);

	/* enter shell mode */
	fprintf(stdout, PROMPT);
	fflush(stdout);
	while (fgets(cmd, BUF_SIZE - 1, stdin)) {
		struct command_t *command = commands;
		char *args = NULL;
		char *space = strchr(cmd, ' ');
		int cmdlen = strlen(cmd);

		if (cmd[cmdlen - 1] == '\n')
			cmd[cmdlen - 1] = 0;

		if (!strcmp(cmd, ""))
			goto loop_end;

		if (space) {
			args = space + 1;
			*space = 0;
		}

		while (command->stem) {
			if (!strcmp(command->stem, cmd)) {
				ret = command->fn(args);
				if (ret)
					fprintf(stderr, "%s: returned %d\n", cmd, ret);
				break;
			}
			command++;
		}
		if (!command->stem)
			fprintf(stderr, "%s: Unknown command.\n", cmd);

loop_end:
		fprintf(stdout, PROMPT);
		fflush(stdout);
	}

	fprintf(stdout, "\n");
err:
	return -ret;
}
