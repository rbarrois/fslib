fslib
=====

fslib is a wrapper around Python's ``os.*`` low-level functions, aiming to:

* Provide more pythonic APIs for low-level primitives
* Merge various physical locations into a simple pseudo-filesystem for the application
* Handle UnionFS-like features, including transparent write overlay on top of read-only paths

fslib supports Python 2.7 and 3.3+; it requires only the standard Python library.

Links
-----

* Documentation: (none yet)
* Official repository: https://github.com/rbarrois/fslib/
* Package: https://pypi.python.org/pypi/fslib/
* Issues: https://github.com/rbarrois/fslib/issues/


Download
--------


PyPI: https://pypi.python.org/pypi/fslib/

.. code-block:: sh

    $ pip install fslib

Source: https://github.com/rbarrois/fslib/

.. code-block:: sh

    $ git clone git://github.com/rbarrois/fslib/
    $ python setup.py install


Usage
-----

Replacing ``import os``
"""""""""""""""""""""""

.. code-block:: pycon

    >>> import fslib
    >>>
    >>> fs = fslib.FileSystem(fslib.OSFS('/'))
    >>> fs.dir_exists('/etc/')
    True
    >>> fs.listdir('/')
    ['/etc', '/dev', '/proc']


Forcing read-only
"""""""""""""""""

In order to ensure that user code doesn't change any physical file,
fslib provides a simple wrapper that prevents writing to the filesystem.

.. code-block:: pycon

    >>> import fslib
    >>> import fslib.stacking
    >>>
    >>> fs = fslib.FileSystem(fslib.stacking.ReadOnlyFS(fslib.OSFS('/')))
    >>> fs.read_one_line('/etc/hostname')
    "ithor"
    >>> fs.open('/tmp/x', 'w')
    OSError: [Errno 30] Read-only file system: '/tmp/x'


UnionFS-like mounting
"""""""""""""""""""""

fslib provides a UnionFS-like wrapper as ``fslib.stacking.UnionFS``.
This backend will provide a merged view of several branch,
redirecting all writes to the branch(es) declared ``writable=True``.

.. code-block:: pycon

    >>> import fslib, fslib.builders, fslib.stacking
    >>>
    >>> mem_fs = fslib.builders.make_memory_fake()
    >>> union_fs = fslib.stacking.UnionFS()
    >>> union_fs.add_branch(mem_fs, ref='memory', rank=0, writable=True)
    >>> union_fs.add_branch(fslib.stacking.ReadOnlyFS(fslib.OSFS('/')), ref='os', rank=1)
    >>>
    >>> fs = fslib.FileSystem(backend=union_fs)
    >>> fs.writelines('/tmp/x', ['aa', 'bb'])
    >>>
    >>> open('/tmp/x', 'r')
    IOError: [Errno 2] No such file or directory: '/tmp/x'
    >>> fs.file_exists('/tmp/x')
    True
    >>> fs.readlines('/tmp/x')
    ['aa', 'bb']
    >>> fs.access('/tmp/x', os.F_OK)
    True


Unix-like filesystem tree
"""""""""""""""""""""""""

It is possible to "overlay" physical or virtual file systems to present
a simple, unified structure to the program.

.. code-block:: pycon

    >>> import fslib, fslib.stacking
    >>> mnt = fslib.MountFS()
    >>> mnt.mount_fs('/', fslib.stacking.ReadOnlyFS(fslib.OSFS('/')))
    >>> mnt.mount_fs('/home/xelnor/.myapp', fslib.stacking.MemoryFS())
    >>> mnt.mount_fs('/home/xelnor/.myapp/cache', fslib.OSFS('/tmp/myapp/shared_cache'))

With this setup:

- All reads/writes to ``/home/xelnor/.myapp/cache`` will actually occur within ``/tmp/myapp/shared_cache``
- All reads/writes within ``/home/xelnor/.myapp`` (except for ``/cache``) will occur in memory
- No write will be permitted anywhere else.
