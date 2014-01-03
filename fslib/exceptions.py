# -*- coding: utf-8 -*-
# Copyright (c) 2010-2013 Raphaël Barrois
# This software is distributed under the two-clause BSD license.

"""Common exceptions for fslib."""

from __future__ import absolute_import, unicode_literals

import errno


class FSError(OSError):
    pass


class DeletedObjectError(OSError):
    def __init__(self, path, **kwargs):
        super(DeletedObjectError, self).__init__(
            errno.ENOENT,
            "No such file or directory: %r" % path,
            **kwargs
        )


def OSErrorWrapper(code, message):  # pylint: disable=invalid-name
    def wrapper(path):
        return OSError(code, "%s: %r" % (message, path))
    return wrapper


EACCES = OSErrorWrapper(errno.EACCES, "Permission denied")
EBUSY = OSErrorWrapper(errno.EBUSY, "Device or resource busy")
EEXIST = OSErrorWrapper(errno.EEXIST, "File exists")
EINVAL = OSErrorWrapper(errno.EINVAL, "Invalid argument")
EISDIR = OSErrorWrapper(errno.EISDIR, "Is a directory")
ENOENT = OSErrorWrapper(errno.ENOENT, "No such file or directory")
ENOTDIR = OSErrorWrapper(errno.ENOTDIR, "Not a directory")
ENOTEMPTY = OSErrorWrapper(errno.ENOTEMPTY, "Directory not empty")
EROFS = OSErrorWrapper(errno.EROFS, "Read-only file system")


