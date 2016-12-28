/*
 * Compression routines.
 * Copyright 2016 Darrick J. Wong.
 * Licensed under the GPLv2.
 */
#ifndef COMPRESS_H
#define COMPRESS_H

struct compressor_type {
	const char		*name;
	int			(*compress)(const char *, char *, int, int);
	int			(*decompress)(const char *, char *, int, int);
};

/* Find compression engine. */
struct compressor_type *compdb_find_compressor(const char *name);

/* List of supported compressors. */
char *compdb_compressors(void);

#endif /* COMPRESS_H */
