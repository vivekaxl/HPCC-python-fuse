#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno

from fuse import FUSE, FuseOSError, Operations


class Passthrough(Operations):
    def __init__(self, root):
        self.root = root

    # Helpers
    # =======

    def _full_path(self, partial):
        print "_full_path"
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    # Filesystem methods
    # ==================
    # Read Only
    def access(self, path, mode):
        print "access"
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    # !Read Only
    def chmod(self, path, mode):
        print "chmod"
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    # !Read Only
    def chown(self, path, uid, gid):
        print "chown"
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    # Read Only
    def getattr(self, path, fh=None):
        print "getattr: "
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        temp = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
        # print temp
        # print "-- " * 20
        return temp

    # Read Only
    def readdir(self, path, fh):
        print "readdir" + "-"*10
        full_path = self._full_path(path)
        # print "Path: ", path
        # print "Full Path: ", full_path
        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        # print "Path: ", path
        for r in dirents: print r
        print "---" * 10
        for r in dirents:
            yield r

    # Read Only
    def readlink(self, path):
        print "readlink"
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        print "mknod"
        return os.mknod(self._full_path(path), mode, dev)

    # !Read Only
    def rmdir(self, path):
        print "rmdir"
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    # !Read Only
    def mkdir(self, path, mode):
        print "mkdir"
        return os.mkdir(self._full_path(path), mode)

    # Read Only
    def statfs(self, path):
        print "statfs"
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    # !Read Only
    def unlink(self, path):
        print "unlink"
        return os.unlink(self._full_path(path))

    # !Read Only
    def symlink(self, name, target):
        print "symlink"
        return os.symlink(name, self._full_path(target))

    # !Read Only
    def rename(self, old, new):
        print "rename"
        return os.rename(self._full_path(old), self._full_path(new))

    # !Read Only
    def link(self, target, name):
        print "link"
        return os.link(self._full_path(target), self._full_path(name))

    # Read Only
    def utimens(self, path, times=None):
        print "utimens"
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    # Read Only
    def open(self, path, flags):
        print "open"
        full_path = self._full_path(path)
        return os.open(full_path, flags)

    # !Read Only
    def create(self, path, mode, fi=None):
        print "create"
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    # Read Only
    def read(self, path, length, offset, fh):
        print "read"
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    # !Read Only
    def write(self, path, buf, offset, fh):
        print "write"
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    # !Read Only
    def truncate(self, path, length, fh=None):
        print "truncate"
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    # !Read Only
    def flush(self, path, fh):
        print "flush"
        return os.fsync(fh)

    # Read Only
    def release(self, path, fh):
        print "release"
        return os.close(fh)

    # !Read Only
    def fsync(self, path, fdatasync, fh):
        print "fsync"
        return self.flush(path, fh)


def main(mountpoint, root):
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
