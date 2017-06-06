#!/usr/bin/env python

from __future__ import with_statement

import sys
import utility
import logging
import os
from fuse import FUSE,  Operations
from cache import cache
from read_cache import ReadCache
import ConfigParser

# Temporary Directory to store the files. This to help in reading
TEMP_DIR = "./.AUX/TEMP"

# Adding logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# create a file handler
handler = logging.FileHandler('HISTORY.log')
handler.setLevel(logging.DEBUG)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)


class Passthrough(Operations):
    def __init__(self, ip, port="8010"):
        config = ConfigParser.ConfigParser()
        config.read("./config.ini")
        self._config_check(config)
        self.ip = ip
        self.port = port
        self._cleanup()
        self.cache = cache(ip, logger)
        self.read_cache = ReadCache(logger, ip, port)
        self.exact_filesize = True if config.get('AUX', 'extact_filesize') == "True" else False

    # Helpers
    # =======
    def _config_check(self, config):
        def represent_int(number):
            try:
                int(number)
                return True
            except:
                return False

        aux_folder = config.get('AUX', 'folder')
        # Check if the folder exists if not throw error
        if os.path.isdir(aux_folder) is False:
            logger.error("_config_check: Auxillary File does not exist")
            raise ValueError('folder does not exist -- check config.ini')

        exact_filesize = config.get('AUX', 'extact_filesize')
        if exact_filesize == "True" or exact_filesize  == "False": pass
        else:
            logger.error("_config_check: extact_filesize should either be True or False. Value passed: " + exact_filesize)
            raise ValueError('Exact Filesize should be either True or False -- check config.ini')

        initial_fetch = config.get('PageTable', 'initial_fetch')
        if initial_fetch.lstrip('-+').isdigit() is False:
            logger.error("_config_check: initial_fetch should be a number. Value passed: " + initial_fetch)
            raise ValueError('initial_fetch should be a number -- check config.ini')
        if represent_int(initial_fetch) is False:
            logger.error("_config_check: initial_fetch should be a integer. Value passed: " + initial_fetch)
            raise ValueError('initial_fetch should be an integer -- check config.ini')
        if int(initial_fetch) < 10:
            logger.error("_config_check: initial_fetch should be greater than 10: " + initial_fetch)
            raise ValueError('initial_fetch should greater than 10 -- check config.ini')

        parts_per_cache = config.get('PageTable', 'parts_per_cache')
        if parts_per_cache.lstrip('-+').isdigit() is False:
            logger.error("_config_check: parts_per_cache should be a number. Value passed: " + parts_per_cache)
            raise ValueError('parts_per_cache Number should be a number -- check config.ini')
        if represent_int(parts_per_cache) is False:
            logger.error("_config_check: parts_per_cache should be an integer. Value passed: " + parts_per_cache)
            raise ValueError('parts_per_cache should be an integer -- check config.ini')
        if int(parts_per_cache) < 2:
            logger.error("_config_check: parts_per_cache should be greater than 2. Value passed: " + parts_per_cache)
            raise ValueError('parts_per_cache should greater than 2 -- check config.ini')

        cache_size = config.get('PageTable', 'cache_size')
        if cache_size.lstrip('-+').isdigit() is False:
            logger.error("_config_check: cache_size should be a number. Value passed: " + cache_size)
            raise ValueError('cache_size Number should be a posivite number -- check config.ini')
        if float(cache_size) < 0:
            logger.error("_config_check: cache_size should be a positive number. Value passed: " + cache_size)
            raise ValueError('cache_size should be positive -- check config.ini')


    def _cleanup(self):
        """Cleaning up the files stored from the last session"""
        # TODO: Need to add functionality so that the files are deleted at SIGINT
        logger.debug("_cleanup: Clean up started")
        os.system("rm -rf " + TEMP_DIR + "/*")
        os.system("rm -rf ./.AUX/*.p")
        os.system("rm -rf ./.AUX/*")
        logger.debug("_cleanup: Clean up finished")

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
        logger.debug("_is_file| pathname: " + str(pathname))
        if pathname == "": modified_path = ""
        elif pathname != "/":
            modified_path = pathname[0] + pathname[1:].replace("/", "::")
        else:
            modified_path = ""
        logger.info("_is_file| modified_path: " + str(modified_path))
        url = self._get_url() + "WsDfu/DFUFileView?ver_=1.31&wsdl"
        result = utility.get_result(url, modified_path, logger)

        if 'DFULogicalFiles' in result.keys():
            logger.info("_is_file: True, Pathname: " + pathname)
            return False
        else:
            logger.info("_is_file: False, Pathname: " + pathname)
            return True

    def _get_data(self, filename):
        """ Getting data from a filename"""
        url = self._get_url() + "WsDfu/DFUBrowseData?ver_=1.31&wsdl"
        logger.info("_get_data: url:" + url)
        result = utility.get_data(url, filename)
        logger.info("_get_data: Data returned is " + result)
        return result.encode('utf-8').strip()

    # Filesystem methods
    # ==================
    # Read Only
    def access(self, path, mode):
        logger.info("access| path: " + str(path))
        return False  # Assumption: All the files are readable by the user

    # Read Only
    def getattr(self, path, fh=None):
        def _is_dir(result):
            logger.info("getattr, _is_dir| result: " + ",".join(result.keys()))
            if 'DFULogicalFiles' in result.keys(): return True
            else: return False

        def _get_ctimed(result):
            all_ctime = [utility.unix_time(element['Modified']) for element in result['DFULogicalFiles'] \
                ['DFULogicalFile'] if element['isDirectory'] is False]
            logger.info("getattr, _get_ctimed| result: " + ",".join(result.keys()))
            if len(all_ctime) == 0: return 0  # The root directory sometimes have no files
            logger.info("getattr, _get_ctimed| all_ctime: " + ",".join(map(str, all_ctime)))
            return max(all_ctime)

        def _get_ctimef(result, path):
            if path.split('/')[-1][0] == '.': return -1
            logger.info("getattr, _get_ctimef| " + "path: " + path)
            if 'FileDetail' not in result.keys(): return -1
            return utility.unix_time(result['FileDetail']['Modified'])

        def _get_sizef(result):
            logger.info("getattr, _get_sizef|")
            return 10**10
            # TODO return correct filesize
            # return -1
            # if 'FileDetail' not in result.keys(): return
            # return int(result['FileDetail']['Filesize'].replace(',', ''))

        def _get_nlinksf(result):
            logger.info("getattr, _get_nlinksf|")
            return 1  # the n_links for a file is always 1

        def _get_st_modef(result):
            logger.info("getattr, _get_st_modef|")
            return 33188

        def _get_nlinks():
            logger.info("getattr, _get_nlinks|")
            """ Counts the number of folders in the folder + 2"""
            return len([element['Directory'] for element in result['DFULogicalFiles'] \
                ['DFULogicalFile'] if element['isDirectory'] is True]) + 2

        def _get_st_mode():
            logger.info("getattr, _get_st_mode|")
            """ refer to http://stackoverflow.com/questions/35375084/c-unix-how-to-extract-the-bits-from-st-mode """
            return 16877

        logger.debug("getattr| path: " + str(path))

        cached_entry = self.cache.get_entry(path, 'getattr')
        if cached_entry is not None:
            logger.info("getattr| path: " + str(path) + " is cached")
            return cached_entry
        else:
            logger.info("getattr| path: "+ str(path) + " is not cached")

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

        logger.info("getattr| Modified Path: %s", str(modified_path))
        url = self._get_url() + "WsDfu/DFUFileView?ver_=1.31&wsdl"
        logger.info("getattr| URL requested : " + str(url))
        result = utility.get_result(url, modified_path, logger)
        if _is_dir(result):
            logger.info("getattr| It is a directory")
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
            result = utility.get_result(url, modified_path, logger)
            logger.info("getattr|. It is a file" + str(_get_sizef(result)))
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
            result = utility.get_result(url, modified_path, logger)
            files = [element['Name'].split("::")[-1] for element in result['DFULogicalFiles'] \
                ['DFULogicalFile'] if element['isDirectory'] is False]
            folders = [element['Directory'] for element in result['DFULogicalFiles'] \
                ['DFULogicalFile'] if element['isDirectory'] is True]
            # result = [f for f in files.extend(folders) if f.split('/')[0] != '.']
            return files + folders

        logger.debug("readdir| path: " + str(path))
        if path != "/": modified_path = path[1:].replace("/", "::")
        else: modified_path = ""

        logger.info("readdir| modified_path: %s" + modified_path)

        cached_entry = self.cache.get_entry(modified_path, 'readdir')
        if cached_entry is not None:
            logger.info("readdir| Entry was cached| modified_path: %s" + modified_path)
            dirents = cached_entry
            logger.info("readdir| Entry was cached| Files and Folders %s", ",".join(dirents))
        else:
            logger.info("readdir| Entry was not cached| modified_path: %s" + modified_path)
            dirents = ['.', '..']
            # Need to check if path is a dir but
            # I am checking if it a file or not
            if self._is_file(modified_path) is False:
                dirents = _readdir(modified_path)
            logger.info("readdir| Files and Folders %s", ",".join(dirents))
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
        logger.info("statfs| return_dict.keys():  %s", ",".join(return_dict))
        return return_dict

    # File methods
    # ============
    # Read Only
    def open(self, path, flags):
        # return a dummy because path is passed in during read
        if self.exact_filesize is True:
            logger.debug("open: Exact File size to be fetched for  : " + path)
            existing_dict = self.cache.get_entry(path, 'getattr')
            file_size = self.read_cache.get_file_size(path)
            logger.info("open: Exact File size: " + str(file_size))
            # Modifying the file size
            existing_dict['st_size'] = file_size
            self.cache.set_entry(path, 'getattr', existing_dict)
        return -1

    # Read Only
    def read(self, path, length, offset, fh):
        logger.debug("read| path: " +  path + " length: " + str(length) + " offset: " + str(offset))
        data = self.read_cache.get_data(path, offset, offset+length)
        if data == 0:
            logger.debug("read| EOF reached")
            return 0
        logger.info("read| EOF reached")
        return data

    # Read Only
    def release(self, path, fh):
        logger.debug("release|")
        return -1


def main(mountpoint, ip="10.239.227.6", port="8010"):
    def check_connection(ip, port):
        from urllib2 import urlopen, URLError
        try:
            urlopen("http://" + ip + ":" + port, timeout=1)
            return True
        except URLError as err:
            return False

    if check_connection(ip, port) is False:
        raise ValueError('Cannot connect to Cluster: ' + ip + ":" + port)
    FUSE(Passthrough(ip, port), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    # try:
        main(sys.argv[3], sys.argv[1], sys.argv[2])
    # except:
    #     print "Usage: python passthrough_hpcc.py ip port mountpoint"
