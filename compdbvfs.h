/*
 * Compress SQLite databases.
 * Copyright 2016 Darrick J. Wong.
 * Licensed under the GPLv2.
 */
#ifndef COMPDBVFS_H
#define COMPDBVFS_H

/* Init compressed DB VFS for sqlite3. */
int compdbvfs_init(const char *under_vfs, const char *vfs_name,
		   const char *compressor);

#endif /* COMPDBVFS_H */
