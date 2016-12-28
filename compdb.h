/*
 * Compress SQLite databases.
 * Copyright 2016 Darrick J. Wong.
 * Licensed under the GPLv2.
 */
#ifndef COMPDB_H
#define COMPDB_H

/* Init compressed DB VFS for sqlite3. */
int compdb_init(const char *under_vfs, const char *vfs_name,
		const char *compressor);

#endif /* COMPDB_H */
