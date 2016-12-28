/*
 * Compress SQLite databases.
 * Copyright 2016 Darrick J. Wong.
 * Licensed under the GPLv2.
 */
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <lz4.h>
#include <lz4hc.h>
#include <zlib.h>
#include <lzma.h>
#include <bzlib.h>
#include "compress.h"

/* bzip2 */

static inline int
BZIP_compress(
	const char		*source,
	char			*dest,
	int			sourceSize,
	int			maxDestSize)
{
	bz_stream		strm = {0};
	char			*endp;
	int			ret;

	ret = BZ2_bzCompressInit(&strm, 1, 0, 30);
	if (ret)
		return 0;

	strm.avail_in = sourceSize;
	strm.next_in = (char *)source;
	strm.next_out = dest;
	endp = dest + maxDestSize;
	do {
		strm.avail_out = endp - (char *)strm.next_out;
		ret = BZ2_bzCompress(&strm, BZ_FINISH);
		if (ret != BZ_STREAM_END && ret != BZ_OK) {
			BZ2_bzCompressEnd(&strm);
			return 0;
		}
		strm.next_out = endp - strm.avail_out;
	} while (strm.avail_in && (char *)strm.next_out <= endp);
	BZ2_bzCompressEnd(&strm);

	return (char *)strm.next_out - dest;
}

static inline int
BZIP_decompress(
	const char		*source,
	char			*dest,
	int			compressedSize,
	int			maxDecompressedSize)
{
	bz_stream		strm = {0};
	char			*endp;
	int			ret;

	ret = BZ2_bzDecompressInit(&strm, 0, 0);
	if (ret != BZ_OK)
		return -1;

	strm.avail_in = compressedSize;
	strm.next_in = (char *)source;
	strm.next_out = dest;
	endp = dest + maxDecompressedSize;
	do {
		strm.avail_out = endp - (char *)strm.next_out;
		ret = BZ2_bzDecompress(&strm);
		if (ret != BZ_STREAM_END && ret != BZ_OK) {
			BZ2_bzDecompressEnd(&strm);
			return 0;
		}
		strm.next_out = endp - strm.avail_out;
	} while (strm.avail_in && (char *)strm.next_out <= endp);
	BZ2_bzDecompressEnd(&strm);

	if (strm.avail_in)
		return -1;

	return (char *)strm.next_out - dest;
}

/* LZMA */

static inline int
LZMA_compress(
	const char		*source,
	char			*dest,
	int			sourceSize,
	int			maxDestSize)
{
	lzma_stream		strm = {0};
	char			*endp;
	int			ret;

	ret = lzma_easy_encoder(&strm, 6, LZMA_CHECK_CRC64);
	if (ret)
		return 0;

	strm.avail_in = sourceSize;
	strm.next_in = (unsigned char *)source;
	strm.next_out = (unsigned char *)dest;
	endp = dest + maxDestSize;
	do {
		strm.avail_out = endp - (char *)strm.next_out;
		ret = lzma_code(&strm, LZMA_FINISH);
		if (ret != LZMA_STREAM_END && ret != LZMA_OK) {
			lzma_end(&strm);
			return 0;
		}
		strm.next_out = (unsigned char *)endp - strm.avail_out;
	} while (strm.avail_in && (char *)strm.next_out <= endp);
	lzma_end(&strm);

	return (char *)strm.next_out - dest;
}

static inline int
LZMA_decompress(
	const char		*source,
	char			*dest,
	int			compressedSize,
	int			maxDecompressedSize)
{
	lzma_stream		strm = {0};
	char			*endp;
	int			ret;

	ret = lzma_stream_decoder(&strm, ULONG_MAX,
			LZMA_TELL_UNSUPPORTED_CHECK | LZMA_CONCATENATED);
	if (ret != LZMA_OK)
		return -1;

	strm.avail_in = compressedSize;
	strm.next_in = (unsigned char *)source;
	strm.next_out = (unsigned char *)dest;
	endp = dest + maxDecompressedSize;
	do {
		strm.avail_out = endp - (char *)strm.next_out;
		ret = lzma_code(&strm, LZMA_FINISH);
		if (ret != LZMA_STREAM_END && ret != LZMA_OK) {
			lzma_end(&strm);
			return 0;
		}
		strm.next_out = (unsigned char *)endp - strm.avail_out;
	} while (strm.avail_in && (char *)strm.next_out <= endp);
	lzma_end(&strm);

	if (strm.avail_in)
		return -1;

	return (char *)strm.next_out - dest;
}

/* gzip deflate */

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

/* Generic stuff */

static struct compressor_type compressors[] = {
	{"GZIP", GZIP_compress,		GZIP_decompress},
	{"LZ4D", LZ4_compress_default,	LZ4_decompress_safe},
	{"LZ4H", LZ4HC_compress,	LZ4_decompress_safe},
	{"LZMA", LZMA_compress,		LZMA_decompress},
	{"BZ2A", BZIP_compress,		BZIP_decompress},
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
