/*
 * Compress SQLite databases.
 * Copyright 2016 Darrick J. Wong.
 * Licensed under the GPLv2.
 */
#ifndef COMPDB_H
#define COMPDB_H

enum compdb_type {
	DB_UNKNOWN,
	DB_REGULAR,
	DB_COMPRESSED,
};

/*
 * Put this ahead of every compressed page.  btree pages can't have
 * 0xDA as the first byte.
 */
static const uint8_t COMPDB_BLOCK_MAGIC[] = {0xDA, 0xAD};
struct compdb_block_head {
	uint8_t			magic[2];
	uint16_t		len;		/* compressed length */
	uint32_t		offset;		/* page number */
};

/* SQLite superblock format. */
#define SQLITE_FILE_HEADER 	"SQLite format 3"
#define COMPDB_FILE_TEMPLATE	"SQLite %s v.3"
struct sqlite3_super {
	uint8_t			magic[16];
	uint16_t		pagesize;
	uint8_t			write_format;
	uint8_t			read_format;
	uint8_t			page_reserve;
	uint8_t			max_fraction;
	uint8_t			min_fraction;
	uint8_t			leaf_payload;
	uint32_t		change_counter;
	uint32_t		nr_pages;
	uint32_t		freelist_start;
	uint32_t		freelist_pages;
	uint32_t		schema_coookie;
	uint32_t		schema_format;
	uint32_t		page_cache_size;
	uint32_t		highest_btree_root;
	uint32_t		text_encoding;
	uint32_t		user_version;
	uint32_t		vacuum_mode;
	uint32_t		app_id;
	uint8_t			reserved[20];
	uint32_t		version_valid_for;
	uint32_t		sqlite_version_number;
};

struct compressor_type {
	const char		*name;
	int			(*compress)(const char *, char *, int, int);
	int			(*decompress)(const char *, char *, int, int);
};

/* Find compression engine. */
struct compressor_type *compdb_find_compressor(const char *name);

/* List of supported compressors. */
char *compdb_compressors(void);

/* Init compressed DB VFS for sqlite3. */
int compdb_register(const char *under_vfs, const char *vfs_name,
		const char *compressor);

#endif /* COMPDB_H */
