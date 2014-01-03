# -*- coding: utf-8 -*-
# Copyright (c) 2010-2013 Raphaël Barrois
# This software is distributed under the two-clause BSD license.


"""Stacking filesystems."""

from __future__ import absolute_import, unicode_literals

import collections
import contextlib
import dbm
import errno
import io
import os
import stat
import time

from . import base
from . import exceptions
from . import helpers


ROOT = base.ROOT


# {{{ ChrootFS
# ============


class ChrootFS(base.WrappingFS):
    """Simple FS that swaps an external 'root' for another internal one.

    All paths are converted on the fly.
    """
    def __init__(self, external_root=ROOT, internal_root=ROOT, **kwargs):
        super(ChrootFS, self).__init__(**kwargs)
        self.external_root = external_root
        self.internal_root = internal_root

    def convert_path_in(self, path):
        if not helpers.is_parent(self.external_root, path):
            raise exceptions.EACCES(path)
        return os.path.join(self.internal_root, path[len(self.external_root):])

    def convert_path_out(self, path):
        if not helpers.is_parent(self.internal_root, path):
            raise exceptions.EACCES(path)
        return os.path.join(self.external_root, path[len(self.internal_root):])


# }}} /ChrootFS


# {{{ ReadOnlyFS
# ==============

class ReadOnlyFS(base.WrappingFS):

    _FEATURES = (
        base.BaseFS.FEATURE_READONLY,
    )

    # Read
    # ----

    def _access(self, path, mode):
        if mode & os.W_OK:
            return False
        return self.wrapped.access(path, mode)

    # def _listdir: unchanged
    # def _lstat: unchanged
    # def _readlink: unchanged
    # def _stat: unchanged

    # Read/write
    # ----------

    def _open_binary(self, path, mode):
        if not helpers.is_readonly_open_mode(mode):
            raise exceptions.EROFS(path)
        return self.wrapped.open_binary(path, mode)

    def _open_text(self, path, mode, encoding):
        if not helpers.is_readonly_open_mode(mode):
            raise exceptions.EROFS(path)
        return self.wrapped.open_text(path, mode, encoding)

    # Write
    # -----

    def _chmod(self, path, mode):
        raise exceptions.EROFS(path)

    def _chown(self, path, uid, gid):
        raise exceptions.EROFS(path)

    def _mkdir(self, path):
        raise exceptions.EROFS(path)

    def _symlink(self, link_name, target):
        raise exceptions.EROFS(link_name)

    # Delete
    # ------

    def _rmdir(self, path):
        raise exceptions.EROFS(path)

    def _unlink(self, path):
        raise exceptions.EROFS(path)


# }}} /ReadOnlyFS


# {{{ Whiteout
# ============


class BaseWhiteoutCache(object):
    def __contains__(self, key):
        raise NotImplementedError()

    def __delitem__(self, key):
        raise NotImplementedError()

    def add(self, key):
        raise NotImplementedError()

    def close(self):
        pass


class MemoryWhiteoutCache(BaseWhiteoutCache):
    def __init__(self):
        self.storage = set()

    def __contains__(self, key):
        return key in self.storage

    def __delitem__(self, key):
        self.storage.discard(key)

    def add(self, key):
        self.storage.add(key)


class DBMWhiteoutCache(BaseWhiteoutCache):
    def __init__(self, path):
        self.storage = dbm.open(path, 'c')

    @classmethod
    def _norm_key(cls, key):
        return key.encode('utf-8')

    def __contains__(self, key):
        return self._norm_key(key) in self.storage

    def __delitem__(self, key):
        try:
            del self.storage[self._norm_key(key)]
        except KeyError:
            pass

    def add(self, key):
        self.storage[self._norm_key(key)] = 'DELETED'

    def close(self):
        self.storage.close()


class WhiteoutFS(base.WrappingFS):
    """A filesystem backend that holds a "whiteout cache".

    All creations/updates/etc. will be forwarded to the wrapped FS,
    but deletions are handled at this level.
    """

    _FEATURES = (
        base.BaseFS.FEATURE_WHITEOUT,
    )

    def __init__(self, whiteout_cache, **kwargs):
        super(WhiteoutFS, self).__init__(**kwargs)
        self.whiteout_cache = whiteout_cache

    def __del__(self):
        self.whiteout_cache.close()

    def _check_component(self, path, ensure_dir=True):
        """Check whether a given path may be accessed.

        Expects that all *parent* paths have been already checked.

        Raises:
            OSError is the path can't be stat()ed.
        """
        if path in self.whiteout_cache:
            raise exceptions.DeletedObjectError(path)

        if ensure_dir and not self.wrapped.isdir(path) and path != ROOT:
            raise exceptions.ENOTDIR(path)

    def _check_path(self, path):
        """Check whether a path, and all its parents, exists."""
        for part in self.iter_path(path):
            self._check_component(part, ensure_dir=(path != part))

    @contextlib.contextmanager
    def _manage_whiteout(self, path, for_creation):
        """Handle pre-open checks."""
        if for_creation:
            self._check_path(os.path.dirname(path))  # Ensure the parent exists
        else:
            self._check_path(path)  # Ensure the file exists
        yield
        if for_creation:
            del self.whiteout_cache[path]

    # Read
    # ----

    def _access(self, path, mode):
        try:
            self._check_path(path)
        except OSError as e:
            if e.errno in (errno.ENOTDIR, errno.ENOENT, errno.EACCES):
                return False
            else:
                raise

        return self.wrapped.access(path, mode)

    def _listdir(self, path):
        with self._manage_whiteout(path, for_creation=False):
            for item in self.wrapped.listdir(path):
                if item not in self.whiteout_cache:
                    yield item

    def _lstat(self, path):
        with self._manage_whiteout(path, for_creation=False):
            return self.wrapped.lstat(path)

    def _readlink(self, path):
        with self._manage_whiteout(path, for_creation=False):
            return self.wrapped.readlink(path)

    def _stat(self, path):
        with self._manage_whiteout(path, for_creation=False):
            return self.wrapped.stat(path)

    # Read/write
    # ----------

    def _open_binary(self, path, mode):
        for_creation = not (
                helpers.is_readonly_open_mode(mode)
                or self.access(path, os.F_OK))

        with self._manage_whiteout(path, for_creation):
            return self.wrapped.open_binary(path, mode)

    def _open_text(self, path, mode, encoding):
        for_creation = not (
                helpers.is_readonly_open_mode(mode)
                or self.access(path, os.F_OK))

        with self._manage_whiteout(path, for_creation):
            return self.wrapped.open_text(path, mode, encoding)

    # Write
    # -----

    def _chmod(self, path, mode):
        with self._manage_whiteout(path, for_creation=False):
            return self.wrapped.chmod(path, mode)

    def _chown(self, path, uid, gid):
        with self._manage_whiteout(path, for_creation=False):
            return self.wrapped.chown(path, uid, gid)

    def _mkdir(self, path):
        with self._manage_whiteout(path, for_creation=True):
            return self.wrapped.mkdir(path)

    def _symlink(self, link_name, target):
        with self._manage_whiteout(link_name, for_creation=True):
            return self.wrapped.symlink(link_name, target)

    # Delete
    # ------

    def _unlink(self, path):
        self._check_path(path)
        self.whiteout_cache.add(path)

    def _rmdir(self, path):
        contents = any(self.listdir(path))
        if contents:
            raise exceptions.ENOTEMPTY(path)
        self.whiteout_cache.add(path)


# }}} /Whiteout


# {{{ UnionFS
# ===========


_Branch = collections.namedtuple('_Branch', ['fs', 'rank', 'writable'])


_STATUS_DELETED = 'deleted'
_STATUS_UNKNOWN = 'unknown'
_STATUS_INVALID = 'invalid'
_STATUS_NOPERM = 'noperm'
_STATUS_EXISTS = 'exists'

_PStat = collections.namedtuple('_PStat', ['stats', 'status'])


class UnionFS(base.BaseFS):
    _FEATURES = (
        base.BaseFS.FEATURE_WHITEOUT,
    )

    def __init__(self, strict=False, **kwargs):
        super(UnionFS, self).__init__(**kwargs)
        self.strict = strict
        self._branches = {}
        self._sorted_branches = []
        self._write_branches = []
        self._next_branch_ref = 0

    def __repr__(self):
        return '<UnionFS: %r>' % ([b.fs for b in self._sorted_branches],)

    def has_feature(self, feature):
        if feature == self.FEATURE_READONLY:
            return not self._write_branches
        return super(UnionFS, self).has_feature(feature)

    # Branches management
    # -------------------

    def _update_branches_cache(self):
        self._sorted_branches = list(sorted(
            self._branches.values(), key=lambda b: b.rank))
        self._write_branches = [
            branch
            for branch in self._sorted_branches
            if branch.writable
        ]

    def add_branch(self, fs, ref, rank=None, writable=False):
        """Add a branch to the UnionFS.

        Args:
            fs: BaseFS, the actual FS to connect
            ref: str, a referenc for this branch
            rank: int or None, the rank (depth within the stack)
            writable: bool, whether the branch should be writable.
                A writable branch MUST NOT be 'readonly' and MUST support
                WHITEOUT.

        Returns:
            ref, an int
        """
        if ref in self._branches:
            raise ValueError("Reference %r is already in use" % ref)
        if any(b.rank == rank for b in self._sorted_branches):
            raise ValueError("A branch with rank %d already exists" % rank)

        if writable:
            if fs.has_feature(self.FEATURE_READONLY):
                raise ValueError(
                        "Can't add readonly FS %r as writable branch" % fs)
            elif not fs.has_feature(self.FEATURE_WHITEOUT):
                raise ValueError(
                    "Can't add non-whiteout-capable FS %r as writable branch"
                    % fs)

        if rank is None:
            rank = 1 + max(b.rank for b in self._sorted_branches)

        self._branches[ref] = _Branch(
            fs=fs,
            rank=rank,
            writable=writable,
        )
        self._update_branches_cache()

    def remove_branch(self, ref):
        del self._branches[ref]
        self._update_branches_cache()

    # Path management
    # ---------------

    def _get_branch_pstat(self, branch, path):
        status = _STATUS_EXISTS
        stats = None
        try:
            stats = branch.fs.stat(path)
        except exceptions.DeletedObjectError:
            status = _STATUS_DELETED
        except OSError as e:
            if e.errno == errno.ENOENT:
                status = _STATUS_UNKNOWN
            elif e.errno == errno.EACCES:
                status = _STATUS_NOPERM
            elif e.errno == errno.ENOTDIR:
                status = _STATUS_INVALID
            else:
                raise

        return _PStat(stats=stats, status=status)

    def _get_pstat(self, path):
        for branch in self._sorted_branches:
            pstats = self._get_branch_pstat(branch, path)
            if pstats.status in (
                    _STATUS_DELETED,
                    _STATUS_INVALID,
                    _STATUS_NOPERM,
                    _STATUS_EXISTS,
                    ):
                return pstats

            assert pstats.status == _STATUS_UNKNOWN
        raise exceptions.ENOENT(path)

    def _get_read_branch(self, path):
        for branch in self._sorted_branches:
            try:
                stats = branch.fs.stat(path)
            except exceptions.DeletedObjectError:
                # Propagate for proper FEATURE_WHITEOUT behavior
                raise
            except OSError as e:
                if e.errno == errno.ENOENT:
                    # The file or one of its parent doesn't exist in this branch
                    continue
                # Maybe EACCES (can't enter) or ENOTDIR (invalid path) or worse
                raise
            else:
                # Found it!
                return branch, stats
        raise exceptions.ENOENT(path)

    def _copy_stat(self, path, branch, stats):
        try:
            branch.fs.chmod(path, stat.S_IMODE(stats.st_mode))
        except OSError:
            if self.strict:
                raise
        try:
            branch.fs.chown(path, stats.st_uid, stats.st_gid)
        except OSError:
            if self.strict:
                raise

    def _copy_tree(self, path, branch):
        """Create a tree for a given path within a given branch.

        Expects ``self.isdir(path)``.
        """
        for component in self.iter_path(path):
            if branch.fs.access(component, os.F_OK):
                if not branch.fs.isdir(component):
                    raise exceptions.ENOTDIR(component)
            else:
                old_stat = self.stat(component)
                branch.fs.mkdir(component)
                self._copy_stat(component, branch, old_stat)

    def _copy_object(self, path, target_branch, old_branch,
            for_overwrite=False):

        old_stat = old_branch.fs.lstat(path)
        if stat.S_ISDIR(old_stat.st_mode):
            target_branch.fs.mkdir(path)

        elif stat.S_ISLNK(old_stat.st_mode):
            target = old_branch.fs.readlink(path)
            target_branch.fs.symlink(path, target)

        elif stat.S_ISREG(old_stat.st_mode):
            if for_overwrite:
                # About to delete it: no need to change anything
                with target_branch.fs.open_binary(path, 'wb') as f:
                    f.write(b'')
            else:
                with old_branch.fs.open_binary(path, 'rb') as src:
                    with target_branch.fs.open_binary(path, 'wb') as dst:
                        dst.write(src.read())

        else:
            raise exceptions.FSError("Can't copy inode at %r" % path)

        self._copy_stat(path, target_branch, old_stat)

    _EXIST_ANY = 'any'
    _EXIST_YES = 'yes'
    _EXIST_NO = 'no'

    def _copy_on_write(self, target_path, branch,
            expected=_EXIST_ANY, for_overwrite=False):
        # 1. Ensure the parent dir is valid somewhere
        parent = os.path.dirname(target_path)
        if not self.isdir(parent):
            raise exceptions.ENOTDIR(parent)

        if (expected == self._EXIST_YES and
                not self.access(target_path, os.F_OK)):
            raise exceptions.ENOENT(target_path)
        elif expected == self._EXIST_NO and self.access(target_path, os.F_OK):
            raise exceptions.EEXIST(target_path)

        # 2. Copy the tree
        self._copy_tree(parent, branch)

        # 3. If the file already exists, copy it (along with attributes)
        try:
            old_branch, _old_stats = self._get_read_branch(target_path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
        else:
            self._copy_object(
                path=target_path,
                target_branch=branch,
                old_branch=old_branch,
                for_overwrite=for_overwrite,
            )

    def _get_write_branch(self, path, **kwargs):
        # XXX: Could handle multiple writable branches.
        if not self._write_branches:
            raise exceptions.EACCES(path)
        branch = self._write_branches[0]
        self._copy_on_write(path, branch, **kwargs)
        return branch

    # Read
    # ----

    def _access(self, path, mode):
        try:
            branch, _stats = self._get_read_branch(path)
        except OSError as e:
            if e.errno in (errno.ENOTDIR, errno.EACCES, errno.ENOENT):
                return False
            # Worse!
            raise
        return branch.fs.access(path, mode)

    def _get_dir_branches(self, path):
        """Fetch the list of branches where a directory exists."""
        for branch in self._sorted_branches:
            pstat = self._get_branch_pstat(branch, path)
            if pstat.status in (
                    _STATUS_DELETED,
                    _STATUS_INVALID,
                    _STATUS_NOPERM,
                    ):
                # Not readable there, shadows deeper branches.
                break
            elif pstat.status == _STATUS_UNKNOWN:
                continue
            else:
                assert pstat.status == _STATUS_EXISTS
                if (branch.fs.access(path, os.R_OK & os.X_OK)
                        and stat.S_ISDIR(pstat.stats.st_mode)):
                    yield branch
                else:
                    # Not readable there, shadows deeper branches.
                    break

    def _listdir(self, path):
        branches = list(self._get_dir_branches(path))
        if not branches:
            raise exceptions.ENOENT(path)

        seen = set()
        members = set()
        for rank, branch in enumerate(branches):
            for member in branch.fs.listdir(path):
                if member in seen:
                    continue
                seen.add(member)

                valid = True
                member_path = os.path.join(path, member)
                for higher_branch in branches[:rank]:
                    status = self._get_branch_pstat(higher_branch, member_path)
                    if status == _STATUS_DELETED:
                        valid = False
                        break
                    # Can't be _STATUS_EXISTS (would already appear in 'seen'),
                    # Can't be _STATUS_NOPERM (stat() always possible if R_OK & X_OK),
                    # Can't be _STATUS_INVALID (stat() always possible if isdir())
                    assert status == _STATUS_UNKNOWN

                if valid:
                    members.add(member)

        return list(members)

    def _lstat(self, path):
        branch, _stats = self._get_read_branch(path)
        return branch.fs.lstat(path)

    def _readlink(self, path):
        branch, _stats = self._get_read_branch(path)
        return branch.fs.readlink(path)

    def _stat(self, path):
        _branch, stats = self._get_read_branch(path)
        return stats

    # Read/write
    # ----------

    def _open_binary(self, path, mode):
        if helpers.is_readonly_open_mode(mode):
            branch, _stats = self._get_read_branch(path)
        else:
            branch = self._get_write_branch(path, for_overwrite=True)
        return branch.fs.open_binary(path, mode)

    def _open_text(self, path, mode, encoding):
        if helpers.is_readonly_open_mode(mode):
            branch, _stats = self._get_read_branch(path)
        else:
            branch = self._get_write_branch(path, for_overwrite=True)
        return branch.fs.open_text(path, mode, encoding)

    # Write
    # -----

    def _chmod(self, path, mode):
        branch = self._get_write_branch(path, expected=self._EXIST_YES)
        return branch.fs.chmod(path, mode)

    def _chown(self, path, uid, gid):
        branch = self._get_write_branch(path, expected=self._EXIST_YES)
        return branch.fs.chown(path, uid, gid)

    def _mkdir(self, path):
        branch = self._get_write_branch(path, expected=self._EXIST_NO)
        return branch.fs.mkdir(path)

    def _symlink(self, link_name, target):
        branch = self._get_write_branch(link_name, expected=self._EXIST_NO)
        return branch.fs.symlink(link_name, target)

    # Delete
    # ------

    def _rmdir(self, path):
        contents = list(self.listdir(path))
        if contents:
            raise exceptions.ENOTEMPTY(path)

        # No need to check for _EXIST_YES, already done in listdir()
        branch = self._get_write_branch(path)
        return branch.fs.rmdir(path)

    def _unlink(self, path):
        branch = self._get_write_branch(path,
            expected=self._EXIST_YES, for_overwrite=True)
        return branch.fs.unlink(path)


# }}} /UnionFS


# {{{ MemoryFS
# ============


def _has_access(mode, uid, gid, target_mode, target_uid, target_gid):
    if uid == target_uid and (mode & target_mode & stat.S_IRWXU):
        return True
    if gid == target_gid and (mode & target_mode & stat.S_IRWXG):
        return True
    if mode & target_mode & stat.S_IRWXO:
        return True
    return False


class FakeFSObject(object):
    BASE_ST_MOD = 0

    def __init__(self, path, mode, uid, gid):
        self.path = path
        self.mode = mode | self.BASE_ST_MOD
        self.uid = uid
        self.gid = gid
        self._size = 0
        now = time.time()
        self._atime = now
        self._mtime = now
        self._ctime = now

    def access(self, mode):
        if mode == os.F_OK:
            return True

        target_mode = 0
        if mode & os.R_OK:
            target_mode |= (stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        if mode & os.W_OK:
            target_mode |= (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
        if mode & os.X_OK:
            target_mode |= (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        uid = os.getuid()
        gid = os.getgid()
        return _has_access(target_mode, uid, gid, self.mode, self.uid, self.gid)

    def lstat(self):
        return os.stat_result((
            self.mode,          # st_mode
            0,                  # st_ino
            0,                  # st_dev
            1,                  # st_nlink
            self.uid,           # st_uid
            self.gid,           # st_gid
            self._size,         # st_size
            self._atime,        # st_atime
            self._mtime,        # st_mtime
            self._ctime,        # st_ctime
        ))

    def stat(self):
        return self.lstat()

    def readlink(self):
        raise exceptions.EINVAL(self.path)

    def chmod(self, mode):
        if not self.access(os.W_OK):
            raise exceptions.EACCES(self.path)
        self.mode = mode

    def chown(self, uid, gid):
        if not self.access(os.W_OK):
            raise exceptions.EACCES(self.path)
        self.uid = uid
        self.gid = gid

    def rmdir(self, _relative_path):
        raise exceptions.ENOTDIR(self.path)

    is_file = False
    is_dir = False
    is_symlink = False


class BufferWrapper(object):
    def __init__(self, buf):
        self._buf = buf

    def close(self):
        pass

    def __getattr__(self, name):
        return getattr(self._buf, name)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass


class FakeFile(FakeFSObject):

    BASE_ST_MOD = stat.S_IFREG
    is_file = True

    def __init__(self, **kwargs):
        super(FakeFile, self).__init__(**kwargs)
        self.content = io.BytesIO()

    def open_binary(self, mode):
        if not helpers.is_readonly_open_mode(mode) and not self.access(os.W_OK):
            raise exceptions.EACCES(self.path)
        return BufferWrapper(self.content)

    def open_text(self, mode, encoding):
        if not helpers.is_readonly_open_mode(mode) and not self.access(os.W_OK):
            raise exceptions.EACCES(self.path)
        return io.TextIOWrapper(BufferWrapper(self.content), encoding=encoding)



class FakeDir(FakeFSObject):
    """A fake directory.

    Attributes:
        contents (dict(path => FakeFSObject): contained objects
    """
    BASE_ST_MOD = stat.S_IFDIR
    is_dir = True

    def __init__(self, **kwargs):
        super(FakeDir, self).__init__(**kwargs)
        self.contents = {}

    def __contains__(self, path):
        return path in self.contents

    def __iter__(self):
        return iter(self.contents.items())

    def __getitem__(self, path):
        return self.contents[path]

    def make_file(self, relative_path, uid, gid, mode):
        full_path = os.path.join(self.path, relative_path)
        if not self.access(os.W_OK):
            raise exceptions.EACCES(full_path)
        # Handle g+s mode
        if self.mode & stat.S_ISGID:
            gid = self.gid
        new_file = FakeFile(
            path=relative_path,
            mode=mode,
            uid=uid,
            gid=gid,
        )
        self.contents[relative_path] = new_file
        return new_file

    def make_subdir(self, relative_path, uid, gid, mode):
        full_path = os.path.join(self.path, relative_path)
        if not self.access(os.W_OK):
            raise exceptions.EACCES(full_path)
        # Handle g+s mode
        if self.mode & stat.S_ISGID:
            gid = self.gid
        new_dir = FakeDir(
            path=relative_path,
            mode=mode,
            uid=uid,
            gid=gid,
        )
        self.contents[relative_path] = new_dir
        return new_dir

    def make_symlink(self, relative_path, target, uid, gid, mode):
        full_path = os.path.join(self.path, relative_path)
        if not self.access(os.W_OK):
            raise exceptions.EACCES(full_path)
        if self.mode & stat.S_ISGID:
            gid = self.gid
        new_link = FakeSymlink(target,
            mode=mode,
            uid=uid,
            gid=gid,
        )
        self.contents[relative_path] = new_link
        return new_link

    def rmdir(self, relative_path):
        full_path = os.path.join(self.path, relative_path)
        if relative_path not in self:
            raise exceptions.ENOENT(full_path)
        target = self[relative_path]
        if not target.is_dir:
            raise exceptions.ENOTDIR(full_path)
        if not self.access(os.W_OK):
            raise exceptions.EACCES(full_path)
        if target.contents:
            raise exceptions.ENOTEMPTY(full_path)
        del self.contents[relative_path]
        return target

    def unlink(self, relative_path):
        full_path = os.path.join(self.path, relative_path)
        if relative_path not in self:
            raise exceptions.ENOENT(full_path)
        target = self[relative_path]
        if target.is_dir:
            raise exceptions.EISDIR(full_path)
        if not self.access(os.W_OK):
            raise exceptions.EACCES(full_path)
        del self.contents[relative_path]
        return target


class FakeSymlink(FakeFSObject):

    BASE_ST_MOD = stat.S_IFLNK
    is_symlink = True

    def __init__(self, target, **kwargs):
        super(FakeSymlink, self).__init__(**kwargs)
        self.target = target

    def stat(self):
        return self.target.stat()

    def readlink(self):
        return self.target.path


class MemoryFS(base.BaseFS):
    def __init__(self, *args, **kwargs):
        super(MemoryFS, self).__init__(*args, **kwargs)
        self.fake_root = FakeDir(
            path=ROOT,
            mode=self.default_dir_mode,
            uid=self.default_uid,
            gid=self.default_gid,
        )
        self._full_map = {
            ROOT: self.fake_root,
        }

    def _get(self, path, follow_symlinks=True):
        target = self._full_map[path]
        if target.is_symlink and follow_symlinks:
            return self._get(target.target, follow_symlinks=follow_symlinks)
        return target

    def _get_parent(self, path):
        parent_path = os.path.dirname(path)
        try:
            parent = self._get(parent_path)
        except KeyError:
            raise exceptions.ENOENT(path)
        if not parent.is_dir:
            raise exceptions.ENOTDIR(path)
        return parent

    def _get_or_raise(self, path, follow_symlinks=True):
        try:
            return self._get(path, follow_symlinks=follow_symlinks)
        except KeyError:
            raise exceptions.ENOENT(path)

    # Read
    # ----

    def _access(self, path, mode):
        try:
            target = self._get(path)
        except KeyError:
            return False
        return target.access(mode)

    def _listdir(self, path):
        target = self._get_or_raise(path)
        if not target.is_dir:
            raise exceptions.ENOTDIR(path)
        return list(target.contents.keys())

    def _lstat(self, path):
        target = self._get_or_raise(path, follow_symlinks=False)
        return target.lstat()

    def _readlink(self, path):
        target = self._get_or_raise(path, follow_symlinks=False)
        return target.readlink()

    def _stat(self, path):
        target = self._get_or_raise(path)
        return target.stat()

    # Read/write
    # ----------

    def _get_or_create_file(self, path, mode):
        try:
            target = self._get(path)
        except KeyError:
            if helpers.is_readonly_open_mode(mode):
                raise exceptions.ENOENT(path)
            parent = self._get_parent(path)
            target = parent.make_file(os.path.basename(path),
                mode=self.default_file_mode,
                uid=self.default_uid,
                gid=self.default_gid,
            )

            self._full_map[path] = target

        return target

    def _open_binary(self, path, mode):
        target = self._get_or_create_file(path, mode)
        return target.open_binary(mode)

    def _open_text(self, path, mode, encoding):
        target = self._get_or_create_file(path, mode)
        return target.open_text(mode, encoding)

    # Write
    # -----

    def _chmod(self, path, mode):
        target = self._get_or_raise(path)
        return target.chmod(mode)

    def _chown(self, path, uid, gid):
        target = self._get_or_raise(path)
        return target.chown(uid, gid)

    def _symlink(self, link_name, target):
        parent = self._get_parent(link_name)
        path = os.path.basename(link_name)
        if path in parent:
            raise exceptions.EEXIST(link_name)

        new_link = parent.make_symlink(path, target,
            mode=self.default_symlink_mode,
            uid=self.default_uid,
            gid=self.default_gid,
        )
        self._full_map[link_name] = new_link
        return new_link

    def _mkdir(self, path):
        parent = self._get_parent(path)
        new_dir = parent.make_subdir(os.path.basename(path),
            mode=self.default_dir_mode,
            uid=self.default_uid,
            gid=self.default_gid,
        )
        self._full_map[path] = new_dir
        return new_dir

    # Delete
    # ------

    def _rmdir(self, path):
        parent = self._get_parent(path)
        parent.rmdir(os.path.basename(path))
        del self._full_map[path]

    def _unlink(self, path):
        parent = self._get_parent(path)
        parent.unlink(os.path.basename(path))
        del self._full_map[path]


# }}} /MemoryFS


# {{{ MountFS
# ===========


class MountFS(base.BaseFS):
    """A UNIX-like tree of file systems.

    Handles 'mount' points.

    This "filesystem tree" will route all low-level calls to the appropriate
    subfs, converting all paths to be relative to the filesystem's root.

    Example:
        If a file-system whose root is '/bar' is mounted at '/home',
        an action on '/home/bar/baz' will be converted into an action on
        '/bar/baz' on that file-system.
    """
    def __init__(self, **kwargs):
        super(MountFS, self).__init__(**kwargs)
        self.filesystems = {}
        self._sorted_filesystems = []

    def __repr__(self):
        return '<MountFS: %s>' % ', '.join(
            '%s:%r' % item
            for item in sorted(self.filesystems.items())
        )

    # Mounting
    # --------

    def _update_mount_cache(self):
        self._sorted_filesystems = list(reversed(sorted(
            self.filesystems.items(),
            key=lambda item: (len(item[0]), item[0], item[1]),
        )))

    def mount_fs(self, subfs, mount_point):
        """Mount an existing BaseFS instance at the given mount_point.

        The mount_point must already exist as a directory, and be within this
        MountFS' root path.
        """
        mount_point = helpers.normpath(mount_point)

        if not self.filesystems and mount_point != ROOT:
            raise ValueError("First subfs MUST be mounted at root (%s), not at %s" % (ROOT, mount_point))

        if mount_point in self.filesystems:
            raise exceptions.FSError("Can't mount a second FS at %r" % mount_point)

        if self.filesystems and not self.isdir(mount_point):
            raise exceptions.FSError("Can't mount subfs %r at %s: dir doesn't exist." % (subfs, mount_point))

        self.filesystems[mount_point] = subfs
        self._update_mount_cache()

    def umount_fs(self, mount_point):
        if mount_point not in self.filesystems:
            raise exceptions.EINVAL(mount_point)

        for anchor in self.filesystems:
            if helpers.is_parent(mount_point, anchor) and anchor != mount_point:
                raise exceptions.EBUSY(mount_point)

        if mount_point == ROOT:
            raise exceptions.EINVAL(mount_point)

        del self.filesystems[mount_point]
        self._update_mount_cache()

    def _get_subfs(self, path):
        """Find the innermost subfs handling the provided path.

        Returns:
            (mount_point, subfs)
        """
        for subfs_anchor, subfs in self._sorted_filesystems:
            if helpers.is_parent(subfs_anchor, path):
                return subfs_anchor, subfs

        return None, None

    def _map_path(self, path):
        """Map a path to the proper subfs, along with the related path.

        Returns:
            (relative_path, subfs)

        Raises:
            FSError if no subfs handles the provided path.
        """
        anchor, subfs = self._get_subfs(path)
        if anchor is None:
            raise exceptions.FSError("No subfs for path %s" % path)
        relpath = os.path.relpath(path, start=anchor)
        return os.path.join(ROOT, relpath), subfs

    # Read
    # ----

    def _access(self, path, mode):
        relpath, subfs = self._map_path(path)
        return subfs.access(relpath, mode)

    def _listdir(self, path):
        relpath, subfs = self._map_path(path)
        return subfs.listdir(relpath)

    def _lstat(self, path):
        relpath, subfs = self._map_path(path)
        return subfs.lstat(relpath)

    def _readlink(self, path):
        relpath, subfs = self._map_path(path)
        return subfs.readlink(relpath)

    def _stat(self, path):
        relpath, subfs = self._map_path(path)
        return subfs.stat(relpath)

    # Read/write
    # ----------

    def _open_binary(self, path, mode):
        relpath, subfs = self._map_path(path)
        return subfs.open_binary(relpath, mode)

    def _open_text(self, path, mode, encoding):
        relpath, subfs = self._map_path(path)
        return subfs.open_text(relpath, mode, encoding)

    # Write
    # -----

    def _chmod(self, path, mode):
        relpath, subfs = self._map_path(path)
        return subfs.chmod(relpath, mode)

    def _chown(self, path, uid, gid):
        relpath, subfs = self._map_path(path)
        return subfs.chown(relpath, uid, gid)

    def _mkdir(self, path):
        relpath, subfs = self._map_path(path)
        return subfs.mkdir(relpath)

    def _symlink(self, link_name, target):
        relative_link, link_subfs = self._map_path(link_name)
        relative_target, target_subfs = self._map_path(target)
        if target_subfs != link_subfs:
            raise exceptions.FSError("Can't create cross-filesystem symlink from %r to %r."
                    % (link_subfs, target_subfs))
        return link_subfs.symlink(relative_link, relative_target)

    # Delete
    # ------

    def _rmdir(self, path):
        for anchor in self.filesystems:
            if helpers.is_parent(path, anchor):
                raise exceptions.EBUSY(path)
        relpath, subfs = self._map_path(path)
        return subfs.rmdir(relpath)

    def _unlink(self, path):
        relpath, subfs = self._map_path(path)
        return subfs.unlink(relpath)


# }}} /MountFS
