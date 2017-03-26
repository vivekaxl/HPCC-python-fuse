#!/usr/bin/env python

from __future__ import with_statement

import sys
import utility
import logging
import os
from fuse import FUSE,  Operations
from cache import cache
from read_cache import ReadCache

# Temporary Directory to store the files. This to help in reading
TEMP_DIR = "./.AUX/TEMP"

# Adding logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# create a file handler
handler = logging.FileHandler('HISTORY.log')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)


class Passthrough(Operations):
    def __init__(self, ip, port="8010"):
        self.ip = ip
        self.port = port
        self._cleanup()
        self.cache = cache(ip, logger)
        self.read_cache = ReadCache(logger, ip, port)

    # Helpers
    # =======
    def _cleanup(self):
        """Cleaning up the files stored from the last session"""
        # TODO: Need to add functionality so that the files are deleted at SIGINT
        logger.info("_cleanup: Clean up started")
        os.system("rm -rf " + TEMP_DIR + "/*")
        os.system("rm -rf ./.AUX/*.p")
        os.system("rm -rf ./.AUX/*")
        logger.info("_cleanup: Clean up finished")

    def _get_url(self):
        if self.ip != "":
            url = "http://" + self.ip + ":" + self.port + "/"
            logger.info("_geturl: The url is: " + url)
        else:
            logger.info("_geturl: Ip address is invalid")
            raise ValueError("Ip address is invalid")
        return url

    def _is_file(self, pathname):
        """Check if the pathname passed is a filename of not"""
        if pathname == "": modified_path = ""
        elif pathname != "/":
            modified_path = pathname[0] + pathname[1:].replace("/", "::")
        else:
            modified_path = ""
        url = self._get_url() + "WsDfu/DFUFileView?ver_=1.31&wsdl"
        result = utility.get_result(url, modified_path)

        if 'DFULogicalFiles' in result.keys():
            logger.info("_is_file: True, Pathname: " + pathname)
            return False
        else:
            logger.info("_is_file: False, Pathname: " + pathname)
            return True

    def _get_data(self, filename):
        """ Getting data from a filename"""
        url = self._get_url() + "WsDfu/DFUBrowseData?ver_=1.31&wsdl"
        logger.info("_get_data: {0}".format(url))
        result = utility.get_data(url, filename)
        logger.info("_get_data: Data returned is " + result)
        return result.encode('utf-8').strip()

    # Filesystem methods
    # ==================
    # Read Only
    def access(self, path, mode):
        return False  # Assumption: All the files are readable by the user

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
            logger.info("_get_ctimef > " + "path: " + path)
            if 'FileDetail' not in result.keys(): return -1
            return utility.unix_time(result['FileDetail']['Modified'])

        def _get_sizef(result):
            return 10**10
            # return -1
            # if 'FileDetail' not in result.keys(): return
            # return int(result['FileDetail']['Filesize'].replace(',', ''))

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

        logger.info("getattr: " + str(path))

        cached_entry = self.cache.get_entry(path, 'getattr')
        if cached_entry is not None:
            return cached_entry

        if path != "/":
            if path.split('/')[-1][0] == '.':
                return_dict = {
                    'st_ctime': -1,
                    'st_mtime': -1,
                    'st_nlinks': -1,
                    'st_mode': -1,
                    # Since it is a folder always return 4096
                    'st_size': sys.maxint,
                    # 'st_size': -1,
                    'st_gid': -1,
                    'st_uid': -1,
                    'st_atime': -1
                }
                self.cache.set_entry(path, 'getattr', return_dict)
                return return_dict
            modified_path = path[1:].replace("/", "::")
        else:
            modified_path = ""

        logger.info("getattr: 1. %s", str(modified_path))
        url = self._get_url() + "WsDfu/DFUFileView?ver_=1.31&wsdl"
        logger.info("getattr: 2. URL requested : " + str(url))
        result = utility.get_result(url, modified_path)
        if _is_dir(result):
            logger.info("getattr: 2.1. It is a directory")
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
            logger.info("getattr: 2.1. It is a file" + str(_get_sizef(result)))
            return_dict = {
                'st_ctime': _get_ctimef(result, modified_path),
                'st_mtime': _get_ctimef(result, modified_path),
                'st_nlinks': _get_nlinksf(result),
                'st_mode': _get_st_modef(result),
                'st_size': _get_sizef(result),
                'st_gid': 1000,
                'st_uid': 1000,
                'st_atime': _get_ctimef(result, modified_path)
            }

        self.cache.set_entry(path, 'getattr', return_dict)

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

        if path != "/": modified_path = path[1:].replace("/", "::")
        else: modified_path = ""

        logger.info("readdir: 1. %s", modified_path)

        cached_entry = self.cache.get_entry(modified_path, 'readdir')
        if cached_entry is not None:
            dirents = cached_entry
        else:
            dirents = ['.', '..']
            # Need to check if path is a dir but
            # I am checking if it a file or not
            if self._is_file(modified_path) is False:
                dirents = _readdir(modified_path)
            logger.info("readdir: 2. files and folders %s", ",".join(dirents))
            self.cache.set_entry(modified_path, 'readdir', dirents)

        for r in dirents:
            yield r

    # Read Only
    def statfs(self, path):
        return_dict = {'f_bsize': 4096, 'f_bavail': 22106710, 'f_favail': 6104275,
                       'f_files': 6365184, 'f_frsize': 4096, 'f_blocks': 24930076,
                       'f_ffree': 6104275, 'f_bfree': 23383842, 'f_namemax': 255,
                       'f_flag': 4097}
        # f_flag has been changes to read only and to not use uids. Rest of the data is junk
        return return_dict

    # File methods
    # ============
    # Read Only
    def open(self, path, flags):
        # return a dummy because path is passed in during read
        return -1

    # Read Only
    def read(self, path, length, offset, fh):
        print "read: ", path, length, offset
        data = self.read_cache.get_data(path, offset, offset+length)
        if data == 0:
            print "EOF reached"
            return 0
        return data

    # Read Only
    def release(self, path, fh):
        return -1


def main(mountpoint, ip="10.239.227.6"):
    FUSE(Passthrough(ip), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    # Usage: python passthrough_hpcc.py ip mountpoint
    # try:
    main(sys.argv[2], sys.argv[1])
    # except:
        # print "Usage: python passthrough_hpcc.py ip mountpoint"
