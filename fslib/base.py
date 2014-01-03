# -*- coding: utf-8 -*-
# Copyright (c) 2010-2013 Raphaël Barrois
# This software is distributed under the two-clause BSD license.

from __future__ import absolute_import, unicode_literals

import hashlib
import io
import os
import stat

from . import exceptions
from . import helpers

ROOT = '/'


class FileSystem(object):
    """Abstraction layer around ``import os``.
    """
    def __init__(self, backend, files_encoding='utf-8', **kwargs):
        super(FileSystem, self).__init__(**kwargs)
        self.files_encoding = files_encoding
        self.backend = backend

    # Read
    # ----
    def access(self, path, read=True, write=False):
        """Whether a file can be accessed."""
        mode = os.F_OK
        if read:
            mode |= os.R_OK
        if write:
            mode |= os.W_OK
        return self.backend.access(path, mode)

    def stat(self, path):
        return self.backend.stat(path)

    def file_exists(self, path):
        """Whether the path exists, and is a file."""
        if not self.backend.access(path, os.F_OK):
            return False
        f_stat = self.backend.stat(path)
        return stat.S_ISREG(f_stat.st_mode)

    def dir_exists(self, path):
        if not self.backend.access(path, os.F_OK):
            return False
        f_stat = self.backend.stat(path)
        return stat.S_ISDIR(f_stat.st_mode)

    def symlink_exists(self, path):
        if not self.backend.access(path, os.F_OK):
            return False
        f_stat = self.backend.stat(path)
        return stat.S_ISLNK(f_stat.st_mode)

    def read_one_line(self, path, encoding=None):
        """Read one (stripped) line from a file.

        Typically used to read a password.
        """
        with self.open(path, 'rt', encoding=encoding) as f:
            return f.readline().strip()

    def readlines(self, path, encoding=None):
        """Read all lines from a file.

        Yields lines of the file, stripping the terminating \n.
        """
        with self.open(path, 'rt', encoding=encoding) as f:
            for line in f:
                # Strip final \n
                yield line[:-1]

    def get_hash(self, filename, method=hashlib.md5):
        file_hash = method()
        read_size = 32768
        with self.backend.open_binary(filename, 'rb') as f:
            data = f.read(read_size)
            while data:
                file_hash.update(data)
                data = f.read(read_size)
        return file_hash

    # Read/write
    # ----------

    def open(self, path, mode, encoding=None):
        if 'b' in mode:
            return self.backend.open_binary(path, mode)
        else:
            return self.backend.open_text(path, mode,
                encoding=encoding or self.files_encoding)

    # Write
    # -----

    def mkdir(self, path):
        self.backend.mkdir(path)

    def makedirs(self, path):
        if not self.dir_exists(path):
            self.backend.makedirs(path)

    def chmod(self, path, mode):
        settable_mode = stat.S_IMODE(mode)
        return self.backend.chmod(path, settable_mode)

    def chown(self, path, uid, gid):
        return self.backend.chown(path, uid, gid)

    def symlink(self, link_name, target):
        return self.backend.symlink(
            link_name,
            target,
        )

    def create_symlink(self, link_name, target, relative=False, force=False):
        if relative:
            raise NotImplementedError("Need to implement relative=True.")

        if self.access(link_name, read=False, write=False):
            file_stat = self.stat(link_name)
            if not stat.S_ISLNK(file_stat.st_mode) and not force:
                raise exceptions.EEXIST(link_name)
            elif stat.S_ISDIR(file_stat.st_mode):
                raise exceptions.EISDIR(link_name)
            else:
                self.remove(link_name)

        self.symlink(link_name, target)

    def copy(self, source, destination, copy_mode=True, copy_user=False):
        with self.backend.open_binary(source, 'rb') as src:
            with self.backend.open_binary(destination, 'wb') as dst:
                dst.write(src.read())

        if copy_mode or copy_user:
            stats = self.backend.stat(source)
            if copy_mode:
                self.chmod(destination, stats.st_mode)
            if copy_user:
                self.chown(destination, stats.st_uid, stats.st_gid)

    def writelines(self, path, lines, encoding=None):
        """Write a set of lines to a file.

        A \n will be appended to lines before writing.
        """
        with self.open(path, 'wt', encoding=encoding) as f:
            for line in lines:
                f.write(u"%s\n" % line)

    # Delete
    # ------

    def remove(self, path):
        if self.dir_exists(path):
            return self.backend.rmdir(path)
        else:
            return self.backend.unlink(path)


class BaseFS(object):
    """A filesystem backend.

    Handles low-level directives (access, stat, chmod, etc.).
    """

    def __init__(self, default_umask=None, default_uid=None, default_gid=None,
            **kwargs):
        super(BaseFS, self).__init__(**kwargs)
        if default_umask is None:
            default_umask = helpers.get_active_umask()
        if default_uid is None:
            default_uid = os.getuid()
        if default_gid is None:
            default_gid = os.getgid()

        self.default_umask = default_umask
        self.default_uid = default_uid
        self.default_gid = default_gid

    # umask & co
    # ----------

    @property
    def default_dir_mode(self):
        """Default dir mode, drwxrwxrwx & umask."""
        return (stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO) & self.default_umask

    @property
    def default_file_mode(self):
        """Default file mode, -rw-rw-rw- & umask."""
        return (
            stat.S_IRUSR | stat.S_IWUSR
            | stat.S_IRGRP | stat.S_IWGRP
            | stat.S_IROTH | stat.S_IWOTH
        ) & self.default_umask

    @property
    def default_symlink_mode(self):
        """Default symlink mode, lrwxrwxrwx."""
        return stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO

    # Path utilities
    # --------------

    def convert_path_in(self, path):
        """Normalize a path (unless it is already an absolute path)."""
        return helpers.normpath(path)

    def convert_path_out(self, path):
        return helpers.normpath(path)

    def explode_path(self, path):
        """Convert a (normalized) path to a list of its components.

        Example:

            >>> fs.explode_path('/tmp/x/y')
            ['tmp', 'x', 'y']
        """
        parts = []
        head = path
        while head:
            # /foo/bar/baz => head=/foo/bar, tail=baz
            head, tail = os.path.split(head)
            parts.append(tail)
        return reversed(parts)

    def iter_path(self, path):
        """Convert a (normalized) path to a list of its parents.

        >>> BaseFS(root='/foo/bar').iter_path('/foo/bar/baz/blah')
        ['/foo/bar', '/foo/bar/baz', '/foo/bar/baz/blah']
        """
        parts = []

        head = tail = path
        while tail:
            parts.append(head)
            head, tail = os.path.split(path)
            path = head
        return reversed(parts)

    # Features
    # --------

    _FEATURES = ()

    FEATURE_READONLY = 'readonly'
    FEATURE_WHITEOUT = 'whiteout'

    ALL_FEATURES = (
        FEATURE_READONLY,
        FEATURE_WHITEOUT,
    )

    def has_feature(self, feature):
        return feature in self._FEATURES

    # Read
    # ----

    def access(self, path, mode):
        return self._access(self.convert_path_in(path), mode)

    def _access(self, path, mode):
        """Test whether a path can be accessed in the chosen mode.

        The mode should be provided as a os.*_OK combination.

        Example:

            >>> obj.access('/tmp/blah', os.R_OK | os.W_OK)
            True
        """
        raise NotImplementedError()

    def listdir(self, path):
        return [self.convert_path_out(rpath)
            for rpath in self._listdir(self.convert_path_in(path))]

    def _listdir(self, path):
        raise NotImplementedError()

    def lstat(self, path):
        return self._lstat(self.convert_path_in(path))

    def _lstat(self, path):
        raise NotImplementedError()

    def readlink(self, path):
        return self._readlink(self.convert_path_in(path))

    def _readlink(self, path):
        raise NotImplementedError()

    def stat(self, path):
        return self._stat(self.convert_path_in(path))

    def _stat(self, path):
        """Retrieve the stats for a given path, as a os.stats object."""
        raise NotImplementedError()

    # Read/write
    # ----------

    def open_binary(self, path, mode):
        return self._open_binary(self.convert_path_in(path), mode)

    def _open_binary(self, path, mode):
        """Open a file with a given mode for binary reading/writing."""
        raise NotImplementedError()

    def open_text(self, path, mode, encoding):
        return self._open_text(self.convert_path_in(path), mode, encoding)

    def _open_text(self, path, mode, encoding):
        """Open a file with a given mode for text reading/writing."""
        raise NotImplementedError()

    # Write
    # -----

    def chmod(self, path, mode):
        return self._chmod(self.convert_path_in(path), mode)

    def _chmod(self, path, mode):
        """Update the mode (as a stats.S_* combination) of a file.

        Example:

        >>> obj.chmod('/tmp/blah', stat.S_IRUSR)  # Switch to -r--------
        """
        raise NotImplementedError()

    def chown(self, path, uid, gid):
        return self._chown(self.convert_path_in(path), uid, gid)

    def _chown(self, path, uid, gid):
        """Update the uid:gid of a file."""
        raise NotImplementedError()

    def mkdir(self, path):
        return self._mkdir(self.convert_path_in(path))

    def _mkdir(self, path):
        raise NotImplementedError()

    def symlink(self, link_name, target):
        return self._symlink(
            self.convert_path_in(link_name),
            self.convert_path_in(target),
        )

    def _symlink(self, link_name, target):
        """Create a symbolic link at `link_name` pointing to `target`."""
        raise NotImplementedError()

    # Delete
    # ------

    def rmdir(self, path):
        return self._rmdir(self.convert_path_in(path))

    def _rmdir(self, path):
        """Remove an empty directory."""
        raise NotImplementedError()

    def unlink(self, path):
        return self._unlink(self.convert_path_in(path))

    def _unlink(self, path):
        """Remove a file or symlink."""
        raise NotImplementedError()

    # Helpers
    # -------

    def makedirs(self, path):
        """Create a directory, also creating all parents if needed.

        See ``mkdir -p`` in bash.
        """
        for part in self.iter_path(path):
            if not self.access(part, os.F_OK):
                self.mkdir(part)

    def isdir(self, path):
        stats = self.stat(path)
        return stat.S_ISDIR(stats.st_mode)


class OSFS(BaseFS):
    """Actual filesystem backend."""

    def __init__(self, mapped_root=ROOT, path_encoding='utf-8', **kwargs):
        super(OSFS, self).__init__(**kwargs)
        self.mapped_root = mapped_root
        self.path_encoding = path_encoding

    def __repr__(self):
        return '<OSFS: %r (%s)>' % (self.mapped_root, self.path_encoding)

    def convert_path_in(self, path):
        path = super(OSFS, self).convert_path_in(path[len(ROOT):])
        assert helpers.is_parent(ROOT, path)

        return os.path.join(self.mapped_root, path)

    def convert_path_out(self, path):
        assert helpers.is_parent(self.mapped_root, path)
        relpath = os.path.relpath(path, self.mapped_root)
        return super(OSFS, self).convert_path_out(os.path.join(ROOT, relpath))

    # Read
    # ----

    def _access(self, path, mode):
        return os.access(path.encode(self.path_encoding), mode)

    def _listdir(self, path):
        return [
            item.decode(self.path_encoding)
            for item
            in os.listdir(path.encode(self.path_encoding))
        ]

    def _lstat(self, path):
        return os.lstat(path.encode(self.path_encoding))

    def _readlink(self, path):
        return os.readlink(path.encode(self.path_encoding))

    def _stat(self, path):
        return os.stat(path.encode(self.path_encoding))

    # Read/write
    # ----------

    def _open_binary(self, path, mode):
        if 'b' not in mode:
            mode += 'b'
        return open(path.encode(self.path_encoding), mode)

    def _open_text(self, path, mode, encoding):
        return io.open(path.encode(self.path_encoding), mode, encoding=encoding)

    # Write
    # -----

    def _chmod(self, path, mode):
        return os.chmod(path.encode(self.path_encoding), mode)

    def _chown(self, path, uid, gid):
        return os.chown(path.encode(self.path_encoding), uid, gid)

    def _mkdir(self, path):
        return os.mkdir(path.encode(self.path_encoding))

    def _symlink(self, link_name, target):
        return os.symlink(
            target.encode(self.path_encoding),
            link_name.encode(self.path_encoding),
        )

    # Delete
    # ------

    def _rmdir(self, path):
        return os.rmdir(path.encode(self.path_encoding))

    def _unlink(self, path):
        return os.unlink(path.encode(self.path_encoding))


class WrappingFS(BaseFS):

    def has_feature(self, feature):
        return feature in self._FEATURES or self.wrapped.has_feature(feature)

    def __init__(self, wrapped, **kwargs):
        self.wrapped = wrapped
        super(WrappingFS, self).__init__(**kwargs)

    def __repr__(self):
        return '<%s(%r)>' % (self.__class__.__name__, self.wrapped)

    # Reading
    # -------

    def _access(self, path, mode):
        return self.wrapped.access(path, mode)

    def _listdir(self, path):
        return self.wrapped.listdir(path)

    def _lstat(self, path):
        return self.wrapped.lstat(path)

    def _readlink(self, path):
        return self.wrapped.readlink(path)

    def _stat(self, path):
        return self.wrapped.stat(path)

    # Mixed
    # -----

    def _open_binary(self, path, mode):
        return self.wrapped.open_binary(path, mode)

    def _open_text(self, path, mode, encoding):
        return self.wrapped.open_text(path, mode, encoding)

    # Writing
    # -------

    def _chmod(self, path, mode):
        return self.wrapped.chmod(path, mode)

    def _chown(self, path, uid, gid):
        return self.wrapped.chown(path, uid, gid)

    def _symlink(self, link_name, target):
        return self.wrapped.symlink(link_name, target)

    def _mkdir(self, path):
        return self.wrapped.mkdir(path)

    # Deleting
    # --------

    def _rmdir(self, path):
        return self.wrapped.rmdir(path)

    def _unlink(self, path):
        return self.wrapped.unlink(path)
