/*
 * Python module code...
 * Copyright 2016 Darrick J. Wong.
 * Licensed under the GPLv2.
 */
#include <Python.h>
#include "filemapper.h"
#include "compdb.h"
#include "compress.h"

#define MOD_NAME		"compdb"

/* Get a list of supported compression algorithms. */
static PyObject *
compdb_compressors_py(
	PyObject	*self,
	PyObject	*args)
{
	char		*s;
	PyObject	*o;

	s = compdb_compressors();
	o = Py_BuildValue("s", s);
	free(s);

	return o;
}

/* Register a compressed-VFS */
static PyObject *
compdb_register_py(
	PyObject	*self,
	PyObject	*args)
{
	char		*under;
	char		*name;
	char		*compr;
	int 		err;

	if (!PyArg_ParseTuple(args, "zsz", &under, &name, &compr))
		return NULL;

	err = compdb_register(under, name, compr);
        if (err)
		PyErr_SetString(PyExc_RuntimeError, strerror(err));

	return Py_BuildValue("i", err);
}

/* Unregister a VFS */
static PyObject *
compdb_unregister_py(
	PyObject	*self,
	PyObject	*args)
{
	sqlite3_vfs	*vfs;
	char		*name;
	int 		err;

	if (!PyArg_ParseTuple(args, "z", &name))
		return NULL;

	vfs = sqlite3_vfs_find(name);
	if (!vfs) {
		err = ENOENT;
		PyErr_SetString(PyExc_RuntimeError, strerror(err));
		goto out;
	}

	err = sqlite3_vfs_unregister(vfs);
	if (err != SQLITE_OK) {
		err = EIO;
		PyErr_SetString(PyExc_RuntimeError, strerror(err));
	}

out:
	return Py_BuildValue("i", err);
}

static PyMethodDef compdb_methods[] = {
	{"register", compdb_register_py, METH_VARARGS, NULL},
	{"unregister", compdb_unregister_py, METH_VARARGS, NULL},
	{"compressors", compdb_compressors_py, METH_NOARGS, NULL},
	{NULL, NULL, 0, NULL}
};

static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        MOD_NAME,
        NULL,
        0,
        compdb_methods,
        NULL,
        NULL,
        NULL,
        NULL
};

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC
PyInit_compdb(void)
{
	PyObject	*m;

	m = PyModule_Create(&moduledef);
	if (!m)
		return NULL;

	return m;
}
#else
PyMODINIT_FUNC
init_compdb(void)
{
	PyObject	*m;

	m = Py_InitModule(MOD_NAME, compdb_methods);
	if (!m)
		return;

	return;
}
#endif /* PY_MAJOR_VERSION */
