#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno
import urllib2
import re
import utility
import ujson
import xmltodict

from fuse import FUSE, FuseOSError, Operations


class Passthrough(Operations):
    def __init__(self, ip, port="8010"):
        self.ip = ip
        self.port = port

    # Helpers
    # =======

    def _get_url(self):
        if self.ip != "":
            url = "http://" + self.ip + ":" + self.port + "/"
        else:
            raise ValueError("Ip address is invalid")
        return url

    def _is_file(self, filename):
        if filename == "": modified_path = ""
        elif filename != "/":
            # import pdb
            # pdb.set_trace()
            modified_path = filename[0] + filename[1:].replace("/", "::")
        else:
            modified_path = ""
        url = self._get_url() + "WsDfu/DFUInfo?ver_=1.31&wsdl"
        result = utility.get_result(url, modified_path)
        if 'DFULogicalFiles' in result.keys():
            return False
        else:
            return True


    # Filesystem methods
    # ==================
    # Read Only
    def access(self, path, mode):
        return True  # Assumption: All the files are readable by the user

    # !Read Only
    def chmod(self, path, mode):
        raise Exception(errno.EPERM)

    # !Read Only
    def chown(self, path, uid, gid):
        raise Exception(errno.EPERM)

    # Read Only
    def getattr(self, path, fh=None):
        def _is_dir(result):
            if 'DFULogicalFiles' in result.keys():
                return True
            else:
                return False

        def _get_ctimed(result):
            all_ctime = [utility.unix_time(element['Modified']) for element in result['DFULogicalFiles'] \
                ['DFULogicalFile'] if element['isDirectory'] is False]
            if len(all_ctime) == 0: return 0  # The root directory sometimes have no files
            return max(all_ctime)

        def _get_ctimef(result, path):
            return utility.unix_time(result['FileDetail']['Modified'])

        def _get_sizef(result):
            return int(result['FileDetail']['Filesize'].replace(',', ''))

        def _get_nlinksf(result):
            return 1  # the n_links for a file is always 1

        def _get_st_modef(result):
            return 33188

        def _get_nlinks():
            """ Counts the number of folders in the folder + 2"""
            return len([element['Directory'] for element in result['DFULogicalFiles'] \
                ['DFULogicalFile'] if element['isDirectory'] is True]) + 2

        def _get_st_mode():
            """ refer to http://stackoverflow.com/questions/35375084/c-unix-how-to-extract-the-bits-from-st-mode """
            return 16877

        if path != "/":
            modified_path = path[0] + path[1:].replace("/", "::")
        else:
            modified_path = ""
        url = self._get_url() + "WsDfu/DFUFileView?ver_=1.31&wsdl"
        result = utility.get_result(url, modified_path)
        if _is_dir(result):
            return_dict = {
                'st_ctime': _get_ctimed(result),
                'st_mtime': _get_ctimed(result),
                'st_nlinks': _get_nlinks(),
                'st_mode': _get_st_mode(),
                # Since it is a folder always return 4096
                'st_size': 4096,
                'st_gid': 1000,
                'st_uid': 1000,
                'st_atime': _get_ctimed(result)
            }
        else:
            url = self._get_url() + "WsDfu/DFUInfo?ver_=1.31&wsdl"
            result = utility.get_result(url, modified_path)
            return_dict = {
                'st_ctime': _get_ctimef(result, modified_path),
                'st_mtime': _get_ctimef(result, modified_path),
                'st_nlinks': _get_nlinksf(result),
                'st_mode': _get_st_modef(result),
                # Since it is a folder always return 4096
                'st_size': _get_sizef(result),
                'st_gid': 1000,
                'st_uid': 1000,
                'st_atime': _get_ctimef(result, modified_path)
            }
        return return_dict



    # Read Only
    def readdir(self, path, fh):
        def _readdir(modified_path):
            url = self.geturl() + "WsDfu/DFUFileView?ver_=1.31&wsdl"
            result = utility.get_result(url, modified_path)
            files = [element['Name'] for element in result['DFULogicalFiles'] \
                ['DFULogicalFile'] if element['isDirectory'] is False]
            folders = [element['Directory'] for element in result['DFULogicalFiles'] \
                ['DFULogicalFile'] if element['isDirectory'] is True]
            return files + folders

        if path != "/":
            modified_path = path[0] + path[1:].replace("/", "::")
        else:
            modified_path = ""
        dirents = ['.', '..']
        # Need to check if path is a dir but
        # I am checking if it a file or not
        if self._is_file(modified_path) is False:
            dirents = _readdir(modified_path)
        # print dirents
        for r in dirents:
            yield r

    # Read Only
    def readlink(self, path):
        raise Exception(errno.EPERM)

    def mknod(self, path, mode, dev):
        raise Exception(errno.EPERM)

    # !Read Only
    def rmdir(self, path):
        raise Exception(errno.EPERM)

    # !Read Only
    def mkdir(self, path, mode):
        raise Exception(errno.EPERM)

    # Read Only
    def statfs(self, path):
        raise Exception(errno.EPERM)

    # !Read Only
    def unlink(self, path):
        raise Exception(errno.EPERM)

    # !Read Only
    def symlink(self, name, target):
        raise Exception(errno.EPERM)

    # !Read Only
    def rename(self, old, new):
        raise Exception(errno.EPERM)

    # !Read Only
    def link(self, target, name):
        raise Exception(errno.EPERM)

    # Read Only
    def utimens(self, path, times=None):
        raise Exception(errno.EPERM)

    # File methods
    # ============

    # Read Only
    def open(self, path, flags):
        raise Exception(errno.EPERM)

    # !Read Only
    def create(self, path, mode, fi=None):
        raise Exception(errno.EPERM)

    # Read Only
    def read(self, path, length, offset, fh):
        raise Exception(errno.EPERM)

    # !Read Only
    def write(self, path, buf, offset, fh):
        raise Exception(errno.EPERM)

    # !Read Only
    def truncate(self, path, length, fh=None):
        raise Exception(errno.EPERM)

    # !Read Only
    def flush(self, path, fh):
        raise Exception(errno.EPERM)

    # Read Only
    def release(self, path, fh):
        raise Exception(errno.EPERM)

    # !Read Only
    def fsync(self, path, fdatasync, fh):
        raise Exception(errno.EPERM)


def main(mountpoint, ip="10.239.227.6"):
    FUSE(Passthrough(ip), mountpoint, nothreads=True, foreground=True)


if __name__ == '__main__':
    # Usage: python passthrough_hpcc.py ip mountpoint
    main(sys.argv[2])
