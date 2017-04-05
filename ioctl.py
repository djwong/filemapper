#!/usr/bin/env python3
# Python wrapper of ioctl support code.
# Copyright (C) 2017 Darrick J. Wong.  All rights reserved.
# Licensed under the GPLv2.

# Internals and plumbing
# From asm-generic/ioctl.h
_IOC_NRBITS = 8
_IOC_TYPEBITS = 8
_IOC_SIZEBITS = 14

_IOC_NRSHIFT = 0
_IOC_TYPESHIFT = _IOC_NRSHIFT + _IOC_NRBITS
_IOC_SIZESHIFT = _IOC_TYPESHIFT + _IOC_TYPEBITS
_IOC_DIRSHIFT = _IOC_SIZESHIFT + _IOC_SIZEBITS

_IOC_TYPECHECK = lambda struct: struct.size

_IOC = lambda dir_, type_, nr, size: \
		(dir_ << _IOC_DIRSHIFT) | (type_ << _IOC_TYPESHIFT) | \
		(nr << _IOC_NRSHIFT) | (size << _IOC_SIZESHIFT)
_IOC_NONE = 0
_IOC_WRITE = 1
_IOC_READ = 2
_IOWR = lambda type_, nr, size: \
		_IOC(_IOC_READ | _IOC_WRITE, type_, nr, _IOC_TYPECHECK(size))
_IO = lambda type_, nr: \
		_IOC(_IOC_NONE, type_, nr, 0)

_UINT32_MAX = (2 ** 32) - 1
_UINT64_MAX = (2 ** 64) - 1
