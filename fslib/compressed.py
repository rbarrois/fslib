# -*- coding: utf-8 -*-
# Copyright (c) 2010-2013 Raphaël Barrois
# This software is distributed under the two-clause BSD license.

from __future__ import absolute_import, unicode_literals

import os.path

from . import base
from . import exceptions


class TarFS(base.BaseFS):
    def __init__(self, tarf, **kwargs):
        super(TarFS, self).__init__(**kwargs)
        self._tarf = tarf

    def _lstat_from_tarinfo(self, tarinfo):
        return os.stat_result((
            tarinfo.mode,       # st_mode
            0,                  # st_ino
            0,                  # st_dev
            1,                  # st_nlink
            tarinfo.uid,        # st_uid
            tarinfo.gid,        # st_gid
            tarinfo.size,       # st_size
            tarinfo.mtime,      # st_atime
            tarinfo.mtime,      # st_mtime
            tarinfo.mtime,      # st_ctime
        ))

    def _access(self, path, mode):
        parent = os.path.dirname(path)

        if parent:
            # If within a subfolder, ensure the parent folder exists
            try:
                self._tarf.getmember(parent)
            except KeyError:
                return exceptions.ENOENT(path)

        try:
            self._tarf.getmember(parent)
        except KeyError:
            return False

        if mode & os.W_OK:
            return self._tarf.fileobj.writable()

    def _listdir(self, path):
        try:
            self._tarf.getmember(path)
        except KeyError:
            raise exceptions.ENOENT(path)

        return [subpath
            for subpath in self._tarf.getnames()
            if os.path.dirname(subpath) == path
        ]

    def _lstat(self, path):
        try:
            tarinfo = self._tarf.getmember(path)
        except KeyError:
            raise exceptions.ENOENT(path)

        return self._lstat_from_tarinfo(tarinfo)

    def _readlink(self, path):
        try:
            tarinfo = self._tarf.getmember(path)
        except KeyError:
            raise exceptions.ENOENT(path)

        if not tarinfo.islnk():
            raise exceptions.EINVAL(path)

        return tarinfo.linkname

    def _stat(self, path):
        try:
            tarinfo = self._tarf.getmember(path)
        except KeyError:
            raise exceptions.ENOENT(path)

        if tarinfo.islnk():
            return self._stat(tarinfo.linkname)
        return self._lstat_from_tarinfo(tarinfo)
