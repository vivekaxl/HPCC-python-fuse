#!/usr/bin/env python

from __future__ import with_statement

import sys
import errno
import utility
import logging
import os
import signal
from fuse import FUSE, FuseOSError, Operations

TEMP_DIR = "./TEMP"


class Passthrough(Operations):
    def __init__(self, ip, port="8010"):
        self.ip = ip
        self.port = port
        self._cleanup()

    # Helpers
    # =======
    def _cleanup(self):
        print "Clean up"
        os.system("rm -rf " + TEMP_DIR + "/*")

    def _get_url(self):
        if self.ip != "":
            url = "http://" + self.ip + ":" + self.port + "/"
        else:
            raise ValueError("Ip address is invalid")
        return url

    def _is_file(self, filename):
        if filename == "": modified_path = ""
        elif filename != "/":
            modified_path = filename[0] + filename[1:].replace("/", "::")
        else:
            modified_path = ""
        url = self._get_url() + "WsDfu/DFUFileView?ver_=1.31&wsdl"
        result = utility.get_result(url, modified_path)
        if 'DFULogicalFiles' in result.keys(): return False
        else: return True

    def _get_data(self, filename):
        url = self._get_url() +"WsDfu/DFUBrowseData?ver_=1.31&wsdl"
        print "_get_data: ", url
        result = utility.get_data(url, filename)
        return result


    # Filesystem methods
    # ==================
    # Read Only
    def access(self, path, mode):
        return False  # Assumption: All the files are readable by the user

    # !Read Only
    def chmod(self, path, mode):
        raise Exception(errno.EPERM)

    # !Read Only
    def chown(self, path, uid, gid):
        raise Exception(errno.EPERM)

    # Read Only
    def getattr(self, path, fh=None):
        def _is_dir(result):
            if 'DFULogicalFiles' in result.keys(): return True
            else: return False

        def _get_ctimed(result):
            all_ctime = [utility.unix_time(element['Modified']) for element in result['DFULogicalFiles'] \
                ['DFULogicalFile'] if element['isDirectory'] is False]
            if len(all_ctime) == 0: return 0  # The root directory sometimes have no files
            return max(all_ctime)

        def _get_ctimef(result, path):
            if path.split('/')[-1][0] == '.': return -1
            print "_get_ctimef > ", "path: ", path
            if 'FileDetail' not in result.keys(): return
            return utility.unix_time(result['FileDetail']['Modified'])

        def _get_sizef(result):
            if 'FileDetail' not in result.keys(): return
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

        print "getattr: ", path

        if path != "/":
            if path.split('/')[-1][0] == '.': return {
                'st_ctime': -1,
                'st_mtime': -1,
                'st_nlinks': -1,
                'st_mode': -1,
                # Since it is a folder always return 4096
                'st_size': -1,
                'st_gid': -1,
                'st_uid': -1,
                'st_atime': -1
            }
            modified_path = path[1:].replace("/", "::")
        else:
            modified_path = ""

        # logging.debug("getattr: 1. %s", str(modified_path))
        url = self._get_url() + "WsDfu/DFUFileView?ver_=1.31&wsdl"
        # logging.debug("getattr: 2. URL requested : " + str(url))
        result = utility.get_result(url, modified_path)
        if _is_dir(result):
            # logging.debug("getattr: 2.1. It is a directory")
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
            url = self._get_url() + "WsDfu/DFUFileView?ver_=1.31&wsdl"
            result = utility.get_result(url, modified_path)
            files = [element['Name'].split("::")[-1] for element in result['DFULogicalFiles'] \
                ['DFULogicalFile'] if element['isDirectory'] is False]
            folders = [element['Directory'] for element in result['DFULogicalFiles'] \
                ['DFULogicalFile'] if element['isDirectory'] is True]
            # result = [f for f in files.extend(folders) if f.split('/')[0] != '.']
            return files + folders

        if path != "/":
            modified_path = path[1:].replace("/", "::")
        else:
            modified_path = ""
        logging.debug("readdir: 1. %s", modified_path)
        dirents = ['.', '..']
        # Need to check if path is a dir but
        # I am checking if it a file or not
        if self._is_file(modified_path) is False:
            dirents = _readdir(modified_path)
        logging.debug("readdir: 2. files and folders %s", ",".join(dirents))
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
        return_dict = {'f_bsize': 4096, 'f_bavail': 22106710, 'f_favail': 6104275,
                       'f_files': 6365184, 'f_frsize': 4096, 'f_blocks': 24930076,
                       'f_ffree': 6104275, 'f_bfree': 23383842, 'f_namemax': 255,
                       'f_flag': 4097}  # f_flag has been changes to read only and to not use uids. Rest
                        # of the data is junk
        return return_dict

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
        print "Open Path: ", path, flags
        filename = path.split('/')[-1]
        full_path = TEMP_DIR + path
        print ">> " * 10 , full_path
        parent_path = '/'.join(full_path.split('/')[:-1])
        print ">> " * 10, "Parent Path: ", parent_path
        if not os.path.exists(parent_path): os.makedirs(parent_path)
        # get data
        modified_path = path[1:].replace("/", "::")
        data = self._get_data(modified_path)

        open(parent_path + '/' + filename, 'w').write("\n".join(data))
        return os.open(parent_path + '/' + filename, flags)

    # !Read Only
    def create(self, path, mode, fi=None):
        raise Exception(errno.EPERM)

    # Read Only
    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    # !Read Only
    def write(self, path, buf, offset, fh):
        raise Exception(errno.EPERM)

    # !Read Only
    def truncate(self, path, length, fh=None):
        raise Exception(errno.EPERM)

    # # !Read Only
    # def flush(self, path, fh):
    #     raise Exception(errno.EPERM)

    # Read Only
    def release(self, path, fh):
        return os.close(fh)

    # !Read Only
    def fsync(self, path, fdatasync, fh):
        raise Exception(errno.EPERM)

    def __exit__(self, exc_type, exc_value, traceback):
        print "Clean up"
        os.system("rm -rf " + TEMP_DIR + "/*")


def main(mountpoint, ip="10.239.227.6"):
    FUSE(Passthrough(ip), mountpoint, nothreads=True, foreground=True)




if __name__ == '__main__':
    # Usage: python passthrough_hpcc.py ip mountpoint

    main(sys.argv[2], sys.argv[1])
