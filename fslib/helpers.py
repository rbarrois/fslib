# -*- coding: utf-8 -*-
# Copyright (c) 2010-2013 Raphaël Barrois
# This software is distributed under the two-clause BSD license.

from __future__ import absolute_import, unicode_literals

import os


def get_active_umask():
    """Find the currently (OS) umask."""
    umask = os.umask(0)
    os.umask(umask)
    return umask


def is_readonly_open_mode(mode):
    """Whether a 'open()' mode string is read-only."""
    return set(mode) < set('rbt')


def is_parent(dir1, dir2):
    relpath = os.path.relpath(dir2, start=dir1)
    # If dir2 == os.path.join(dir1, x), os.path.relpath(dir2, start=dir1) == x
    return relpath == '' or relpath[:2] != '..'


def normpath(path):
    if not os.path.isabs(path):
        path = os.path.normpath(path)
    return path
