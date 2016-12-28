/*
 * Compress SQLite databases.
 * Copyright 2016 Darrick J. Wong.
 * Licensed under the GPLv2.
 */
#include <string.h>
#include <stdlib.h>
#include <lz4.h>
#include <lz4hc.h>
#include <zlib.h>
#include "compress.h"

/* gzip deflate compression */
static inline int
GZIP_compress(
	const char		*source,
	char			*dest,
	int			sourceSize,
	int			maxDestSize)
{
	z_stream		strm = {0};
	char			*endp;
	int			ret;

	ret = deflateInit(&strm, 5);
	if (ret)
		return 0;

	strm.avail_in = sourceSize;
	strm.next_in = (unsigned char *)source;
	strm.next_out = (unsigned char *)dest;
	endp = dest + maxDestSize;
	do {
		strm.avail_out = endp - (char *)strm.next_out;
		ret = deflate(&strm, Z_FINISH);
		if (ret != Z_STREAM_END && ret != Z_OK) {
			deflateEnd(&strm);
			return 0;
		}
		strm.next_out = (unsigned char *)endp - strm.avail_out;
	} while (strm.avail_in && (char *)strm.next_out <= endp);
	deflateEnd(&strm);

	return (char *)strm.next_out - dest;
}

/* gzip inflate */
static inline int
GZIP_decompress(
	const char		*source,
	char			*dest,
	int			compressedSize,
	int			maxDecompressedSize)
{
	z_stream		strm = {0};
	char			*endp;
	int			ret;

	ret = inflateInit(&strm);
	if (ret != Z_OK)
		return -1;

	strm.avail_in = compressedSize;
	strm.next_in = (unsigned char *)source;
	strm.next_out = (unsigned char *)dest;
	endp = dest + maxDecompressedSize;
	do {
		strm.avail_out = endp - (char *)strm.next_out;
		ret = inflate(&strm, Z_NO_FLUSH);
		if (ret != Z_STREAM_END && ret != Z_OK) {
			inflateEnd(&strm);
			return 0;
		}
		strm.next_out = (unsigned char *)endp - strm.avail_out;
	} while (strm.avail_in && (char *)strm.next_out <= endp);
	inflateEnd(&strm);

	if (strm.avail_in)
		return -1;

	return (char *)strm.next_out - dest;
}

/* LZ4 HC mode */
static inline int
LZ4HC_compress(
	const char		*source,
	char			*dest,
	int			sourceSize,
	int			maxDestSize)
{
	return LZ4_compressHC2_limitedOutput(source, dest, sourceSize,
			maxDestSize, 8);
}

static struct compressor_type compressors[] = {
	{"GZIP", GZIP_compress,		GZIP_decompress},
	{"LZ4D", LZ4_compress_default,	LZ4_decompress_safe},
	{"LZ4H", LZ4HC_compress,	LZ4_decompress_safe},
	{NULL, NULL, NULL},
};

/* Return a comma separated list of compressors. */
char *
compdb_compressors(void)
{
	struct compressor_type	*ct;
	char			*s;
	int			len = 1;

	for (ct = compressors; ct->name; ct++)
		len += strlen(ct->name) + 1;
	s = malloc(len);
	if (!s)
		return NULL;
	for (ct = compressors, len = 0; ct->name; ct++) {
		if (ct != compressors) {
			strcpy(s + len, ",");
			len++;
		}
		strcpy(s + len, ct->name);
		len += strlen(ct->name);
	}

	return s;
}

/* Find a compression engine. */
struct compressor_type *
compdb_find_compressor(
	const char		*name)
{
	struct compressor_type	*ct;

	if (!name)
		return &compressors[0];

	for (ct = compressors; ct->name; ct++)
		if (strcmp(name, ct->name) == 0)
			return ct;
	return NULL;
}
