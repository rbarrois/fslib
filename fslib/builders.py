# -*- coding: utf-8 -*-
# Copyright (c) 2010-2013 Raphaël Barrois
# This software is distributed under the two-clause BSD license.

from __future__ import absolute_import, unicode_literals

from . import stacking


def make_memory_fake(**common_flags):
    whiteout_cache = stacking.MemoryWhiteoutCache()
    memory_fs = stacking.MemoryFS(**common_flags)
    whiteout_fs = stacking.WhiteoutFS(
        whiteout_cache=whiteout_cache,
        wrapped=memory_fs,
    )
    return whiteout_fs


