from __future__ import division
from page_table import PageTable
import ConfigParser
import utility
import os
import sys

class ReadCache:
    def __init__(self, logger, ip, port="8010"):
        self.ip = ip
        self.port = port
        self.logger = logger
        self.page_table = PageTable(logger)
        config = ConfigParser.ConfigParser()
        config.read("./config.ini")
        self.records_per_part = int(config.get('PageTable', 'records_per_part'))
        self.parts_per_cache = {}
        self.aux_folder = str(config.get('AUX', 'folder'))
        self.cache_size = float(config.get('PageTable', 'cache_size'))

    def _get_url(self):
        if self.ip != "":
            url = "http://" + self.ip + ":" + self.port + "/"
            self.logger.info("ReadCache: _geturl: The url is: " + url)
        else:
            self.logger.info("ReadCache: _geturl: Ip address is invalid")
            raise ValueError("ReadCache: Ip address is invalid")
        return url

    def _get_data(self, filename, start):
        """ Getting data from a filename"""
        url = self._get_url() + "WsWorkunits/WUResult.json?LogicalName="+ filename + "&Cluster=" + str(self.ip) + "&Start=" + str(start) + "&Count=" + str(self.records_per_part)
        self.logger.info("ReadCache: _get_data(): {0}".format(url))
        self.logger.info("ReadCache: _get_data(): Filename: " + filename + " Start: " + str(start) + " count: " + str(self.records_per_part))
        result, total_count = utility.get_data(url)
        # self.logger.info("ReadCache: _get_data(): Data returned is " + result)
        ret_val = result.encode('utf-8').strip() + "\n" if len(result) > 0 else result.encode('utf-8').strip()
        # if total_count <= start + self.records_per_part and len(result) > 0:
        #     self.logger.info("ReadCache: _get_data(): Trailing character added")
        #     ret_val += '\0' # This is a workaround so that application is able to recognize the EOF
        return ret_val

    def if_cached(self, path, start_byte, end_byte):
        # keys of the page table
        abs_left = self.page_table.get_cache_left(path)
        abs_right = self.page_table.get_cache_right(path)
        start = False
        end = False
        if abs_left.start_byte <= start_byte < abs_right.end_byte:
            start = True
        if abs_left.start_byte <= end_byte < abs_right.end_byte:
            end = True
        if end_byte > abs_right.end_byte and abs_right.get_eof() is True:
            end = True
        return start and end

    def _fetch_data(self, path, start_byte, end_byte):
        """
        Assumption: This assumes that there is no holes in the cache
        :param path:
        :param start_byte:
        :param end_byte:
        :return:
        """
        def read_data(path, offset, length):
            f = open(path, 'r')
            assert(offset > -1), "Something is wrong"
            f.seek(offset)
            return_data = f.read(length)
            f.close()
            return return_data

        eof_char = ''
        # keys of the page table
        keys = self.page_table.get_parts(path)
        ranges = [self.page_table.get_ranges_of_parts(path, key) for key in keys if self.page_table.if_eof(path, key) is False]

        eof = [self.page_table.get_ranges_of_parts(path, key) for key in keys if self.page_table.if_eof(path, key) is True]
        if len(eof) > 0:
            eof = eof[-1]
            if end_byte > eof[0]:
                eof_char = '\n'

        # Since we are iterating through the ranges need to make sure that the ranges are sorted
        ranges = sorted(ranges, key=lambda x: x[0])

        data = ""
        # get to start position
        for range in ranges:
            if range[0] <= start_byte < range[1]: break

        for part_no, range in enumerate(ranges):
            if start_byte < range[0] and range[1] < end_byte:
                assert (len(data) > 0), "Something is wrong"
                part = self.page_table.get_part(path, part_no)
                cache_file_path = part.get_cache_file_path()
                length = part.get_end_byte() - part.get_start_byte()
                data += read_data(cache_file_path, 0, length)

            elif range[0] <= start_byte < range[1] and range[0] <= end_byte < range[1]:
                # parts = self.page_table.get_parts_based_byte_position(path, start_byte, end_byte)
                part = self.page_table.get_part(path, part_no)
                cache_file_path = part.get_cache_file_path()
                offset = start_byte - part.get_start_byte()
                # if all the data is in one page
                length = end_byte - start_byte
                data += read_data(cache_file_path, offset, length)

            elif start_byte < range[0] <= end_byte < range[1]:
                part = self.page_table.get_part(path, part_no)
                cache_file_path = part.get_cache_file_path()
                length = end_byte - part.get_start_byte()
                data += read_data(cache_file_path, 0, length)
                break  # reading is complete

            elif range[0] <= start_byte < range[1] < end_byte:
                part = self.page_table.get_part(path, part_no)
                cache_file_path = part.get_cache_file_path()
                offset = start_byte - part.get_start_byte()
                length = part.get_end_byte() - start_byte
                assert(len(data) == 0), "Something is wrong"
                data += read_data(cache_file_path, offset, length)

        self.logger.info("ReadCache: _fetch_data: Data has been fetched | Path: " + path +
                         " Start Byte: " + str(start_byte) + " End Byte: " + str(end_byte))
        # print data
        return data + eof_char

    def delete_file(self, filepath):
        print "Delete File: ", filepath
        open(filepath, 'w').write("")

    def invalidate_all_parts(self, path):
        self.logger.info("ReadCache: invalidate_all_parts(): Invalidating all cached parts")
        parts = self.page_table.get_cache_parts(path)
        for part in parts:
            self.logger.info("ReadCache: invalidate_all_parts(): Part invalidated: " + str(part.get_part_no()))
            cache_file_path = self.page_table.part_invalidate_page(path, part.get_part_no())
            self.delete_file(cache_file_path)
        # raise RuntimeError('xxx')

    def invalidate_extreme(self, path, right=True):
        """ To move cache to the left - This invalidates the extreme part"""
        if right is True:
            self.logger.info("ReadCache: invalidate_extreme(): Invalidate Right Extreme")
        else:
            self.logger.info("ReadCache: invalidate_extreme(): Invalidate Left Extreme")

        parts = self.page_table.get_cache_parts(path)
        if right is True:
            start_end_part = max(parts, key=lambda x: x.part_no)
        else:
            start_end_part = min(parts, key=lambda x: x.part_no)
        cache_file_path = self.page_table.part_invalidate_page(path, start_end_part.get_part_no())
        self.delete_file(cache_file_path)
        self.logger.info("ReadCache: invalidate_extreme(): Cache file invalidated " + cache_file_path)

    def build_cache(self, path, start_count, part_no):
        self.logger.info("ReadCache: build_cache(): Building Cache Start| Path:  " + path + " start_count: " + str(start_count) + " part_no: " + str(part_no))
        # Fetch 1000 pages and store it
        modified_path = path[1:].replace("/", "::")
        data = self._get_data(modified_path, start_count)
        # if there is no more data to be fetched return
        if len(data) == 0:
            self.logger.info("ReadCache: get_data(): EOF reached for file  " + path)
            self.logger.info("ReadCache: get_data(): Creating a EOF entry for  " + path)
            total_size = sum([self.page_table.get_part(path, part).get_end_byte() -
                              self.page_table.get_part(path, part).get_start_byte()
                              for part in self.page_table.get_parts(path)])
            self.page_table.create_entry(path, start_count, start_count, total_size,
                                         total_size, part_no=part_no, eof=True)
            return -1
        # generating part file name. Since this is the first file, the part number is assigned as 1
        part_path_file = self.aux_folder + path[1:] + "_" + str(part_no)
        self.logger.info("ReadCache: get_data(): File Created: " + part_path_file)
        open(part_path_file, 'w').write(data)

        total_size = sum([self.page_table.get_part(path, part).get_end_byte() -
                          self.page_table.get_part(path, part).get_start_byte()
                          for part in self.page_table.get_parts(path)])
        # Adding a page entry
        self.page_table.create_entry(path, start_count, start_count + self.records_per_part, total_size,
                                     total_size + os.stat(part_path_file).st_size, part_no=part_no)

    def update_cache_file(self, path, part_no):
        part = self.page_table.get_part(path, part_no)
        start_count = part.get_start_record()
        # Fetch 1000 pages and store it
        modified_path = path[1:].replace("/", "::")
        data = self._get_data(modified_path, start_count)
        # generating part file name. Since this is the first file, the part number is assigned as 1
        part_path_file = self.aux_folder + path[1:] + "_" + str(part_no)
        self.logger.info("ReadCache: get_data(): File Created: " + part_path_file)
        open(part_path_file, 'w').write(data)

        # Updating a page entry
        self.page_table.update_entry(path, part_no)

    # def fill_page_table(self, path):
    #     self.logger.info("ReadCache: fill_page_table(): Path: " + path)
    #     if self.page_table.path_exists(path) is True:
    #         assert(False, "This should not happen")
    #     else:

    def get_data(self, path, start_byte, end_byte):
        self.logger.info("ReadCache: get_data(): First Stop: " + str(start_byte) + " " + str(end_byte))
        # if end_byte - start_byte > self.page_table.get_cache_size(path):
        #     assert(False), "This functionality is not supported"
        # Check if this file has been fetched before
        if self.page_table.path_exists(path) is False:
            # assert(start_byte == -1 and end_byte == -1), "This should only happen when the file is opened"
            # Instantiate first part
            # Fetch 1000 pages and store it
            modified_path = path[1:].replace("/", "::")

            data = self._get_data(modified_path, 0)

            # generating part file name. Since this is the first file, the part number is assigned as 1
            part_path_file = self.aux_folder + path[1:] + "_" + str(0)
            parent_path = '/'.join(part_path_file.split('/')[:-1])
            if not os.path.exists(parent_path): os.makedirs(parent_path)

            f = open(part_path_file, 'w')
            f.write(data)
            f.close()

            self.logger.info("ReadCache: get_data(): File Created: " + part_path_file)
            # Adding a page entry
            self.page_table.create_entry(path, 1, self.records_per_part, 0, os.stat(part_path_file).st_size, 0)

            count = 1
            total_size = 0
            while total_size < self.cache_size: # since the first part has been fetched
                abs_right = self.page_table.get_cache_right(path)
                abs_right_part_no = abs_right.get_part_no()
                to_fetch_part_no = abs_right_part_no + 1
                ret_val = self.build_cache(path, abs_right.end_record, to_fetch_part_no)
                self.logger.info("ReadCache: get_data(): Getting other data: " + str(1 + 1 + count))
                # if ret_val is -1 means the whole file has been fetched
                if ret_val == -1 :
                    self.logger.info("ReadCache: get_data(): EOF has been reached during initial fetch: " + str(to_fetch_part_no))
                    break
                self.page_table.part_validate_page(path, to_fetch_part_no)
                count += 1
                total_size = sum([self.page_table.get_part(path, part).get_end_byte() -
                          self.page_table.get_part(path, part).get_start_byte()
                          for part in self.page_table.get_parts(path)])/1024/1024

            self.parts_per_cache[path] = count
            if ret_val != -1:
                assert(len(self.page_table.get_cache_parts(path)) == self.parts_per_cache[path]), "Initialization has failed"

        self.logger.info("Number of parts in cache: " + str(len(self.page_table.get_parts(path))))
        self.logger.info("Number of cached parts in cache: " + str(len(self.page_table.get_cache_parts(path))))
        # Check if the data exists in the local repository
        while self.if_cached(path, start_byte, end_byte) is not True:
            self.logger.info("ReadCache: get_data(): Cache Miss : Start Byte " + str(start_byte) + "End Byte: " + str(end_byte))
            # Need to decide if should move to left or right
            # Get absolute left
            abs_left = self.page_table.get_cache_left(path)
            # Get absolute right
            abs_right = self.page_table.get_cache_right(path)
            self.logger.info(
                "ReadCache: get_data(): Cache Miss : Cache Start " + str(abs_left.start_byte) + "Cache End: " + str(abs_right.end_byte))
            if self.page_table.get_eof_entry(path) != -1 and start_byte > self.page_table.get_eof_entry(path).get_start_byte() and end_byte > self.page_table.get_eof_entry(path).get_start_byte():
                self.logger.info("ReadCache: get_data(): EOF File has been reached: " + str(start_byte) + " " + str(end_byte))
                return 0
            # if start_byte and end_byte is both to the left or right of the cache - invalidate all the pages
            if (start_byte < abs_left.start_byte and end_byte < abs_left.start_byte) or \
                    (start_byte > abs_right.end_byte and end_byte > abs_right.end_byte):
                self.logger.info("ReadCache: get_data(): Not present in Cache All together " + str(start_byte) + "End Byte: " + str(end_byte))
                #TODO if the parts have not been fetched in the past then we need to try and iteratively build the cache
                # fetch all the parts have the data
                parts = self.page_table.get_parts_based_byte_position(path, start_byte, end_byte)

                if parts == -1: # these parts have never been fetched before
                    self.logger.info("ReadCache: get_data(): These parts have never been fetched before. "
                                     "Hence, Random Access")
                    # TODO: Add a sequential access
                    assert(False), "This functionality is not supported"

                # invalidate all parts
                self.invalidate_all_parts(path)
                self.logger.info("ReadCache: get_data(): New parts")

                # validate the parts, which has been previously fetched
                for part in parts:
                    self.logger.info("ReadCache: get_data(): Update the parts which belong the start and end byte "
                                     + str(start_byte) + "End Byte: " + str(end_byte))
                    self.update_cache_file(path, part.get_part_no())
                    self.page_table.part_validate_page(path, part.get_part_no())

                # check if the new parts are equal to self.parts_per_cache
                if len(parts) < self.parts_per_cache[path]:
                    self.logger.info("ReadCache: get_data(): Fill up all the cache parts - self.parts_per_cache ")
                    # fetch part numbers
                    part_numbers = [part.get_part_no() for part in parts]

                    # Add new parts which should be added to the cache
                    fetch_part_numbers = []
                    while len(part_numbers) + len(fetch_part_numbers) != self.parts_per_cache[path]:
                        if len(fetch_part_numbers) == 0:
                            fetch_part_numbers.append(max(part_numbers)+1)
                        else:
                            fetch_part_numbers.append(max(fetch_part_numbers) + 1)

                    # parts, which has been fetched before
                    old_parts = [fetch_part for fetch_part in fetch_part_numbers if self.page_table.if_accessed_before(path, fetch_part) is True]
                    self.logger.info("ReadCache: get_data(): Number of parts that has been fetched before is {0}".format(str(len(old_parts))))

                    # parts, which have never been fetched before
                    new_parts = [fetch_part for fetch_part in fetch_part_numbers if self.page_table.if_accessed_before(path, fetch_part) is False]
                    self.logger.info("ReadCache: get_data(): Parts which have never been fetched before: {0}".format(','.join(map(str, new_parts))))

                    # Update the cache file - i.e. download the latest cache
                    self.logger.info("ReadCache: get_data(): Download latest cache for older parts")
                    for old_part in old_parts:
                        self.update_cache_file(path, old_part)
                        self.page_table.part_validate_page(path, old_part)

                    # Build parts for new parts and download the latest cache
                    self.logger.info("ReadCache: get_data(): Fetch data and update cache for newer parts")
                    for new_part in sorted(new_parts):
                        abs_right = self.page_table.get_cache_right(path)
                        abs_right_part_no = abs_right.get_part_no()
                        assert(abs_right_part_no == new_part), "Something is wrong"
                        to_fetch_part_no = abs_right_part_no + 1
                        self.build_cache(path, abs_right.end_record, to_fetch_part_no)
                        part = self.page_table.get_part(path, to_fetch_part_no)
                        self.page_table.part_validate_page(path, part)

            # To Left: if the start byte is to the left of cache and end_byte is within cache
            elif abs_left > start_byte and end_byte <= abs_right:
                self.logger.info("ReadCache: get_data(): Data is to left of the cache window. Start Byte: "
                                 + str(start_byte) + "End Byte: " + str(end_byte) + " abs_left: " + str(abs_left)
                                 + " abs_right: " + str(abs_right))
                # if the entry for left part number does not exist
                abs_left_part_no = abs_left.get_part_no()
                to_fetch_part_no = abs_left_part_no - 1
                all_part_no = [part.get_part_no() for part in self.page_table.get_parts(path)]
                if to_fetch_part_no not in all_part_no:
                    self.logger.info("ReadCache: get_data(): This part has never been fetched before. "
                                     "Random Access before populating")
                    assert(False), "This functionality is not supported"
                else:
                    self.invalidate_extreme(path, right=True)
                    part = self.page_table.get_part(path, to_fetch_part_no)
                    self.update_cache_file(path, part.get_part_no())
                    self.page_table.part_validate_page(path, part)

            # To Right
            elif abs_left <= start_byte and end_byte > abs_right:
                self.logger.info("ReadCache: get_data(): Data is to right of the cache window. Start Byte: "
                                 + str(start_byte) + "End Byte: " + str(end_byte))
                self.invalidate_extreme(path, right=False)
                # if the entry for left part number does not exist
                abs_right_part_no = abs_right.get_part_no()
                to_fetch_part_no = abs_right_part_no + 1
                all_part_no = [self.page_table.get_part(path, part_no).get_part_no() for part_no in self.page_table.get_parts(path)]
                if to_fetch_part_no not in all_part_no:
                    self.logger.info("ReadCache: get_data(): Part has not been fetched before hence move to right")
                    ret_val = self.build_cache(path, abs_right.end_record, to_fetch_part_no)
                    if ret_val == -1:
                        self.logger.info("ReadCache: get_data(): EOF has been reached: " + str(to_fetch_part_no))
                        break
                    part = self.page_table.get_part(path, to_fetch_part_no)
                else:
                    part = self.page_table.get_part(path, to_fetch_part_no)
                    self.update_cache_file(path, part.get_part_no())

                self.page_table.part_validate_page(path, part.get_part_no())

        # Get data using the page table
        data = self._fetch_data(path, start_byte, end_byte)
        return data



# for testing purposes
def test1(read_cache):
    """ Check if a new part can be instantiated"""
    # path = "/vivek/c2_f2_clustering.csv"
    path = "/vn/dsoutput"
    start_byte = 0
    end_byte = 16384
    data = read_cache.get_data(path, start_byte, end_byte)
    print data
    if len(data) > 0: return True
    else: return False


def test2(read_cache):
    """ Check if a new part can be instantiated and can be accessed"""
    path = "/vivek/c2_f2_clustering.csv"
    start_byte = 0
    end_byte = 16384
    read_cache.get_data(path, start_byte, end_byte)
    data = read_cache.get_data(path, start_byte, end_byte)
    if len(data) > 0: return True
    else: return False


def test3(read_cache):
    """ This test the right cache sweep """
    import time
    path = "/vivek/21049.csv"
    end_byte = 45103604
    length = 16384
    old = 0
    ranges = list()
    while old <= int(0.1 * end_byte):
        ranges.append([old, old+length])
        old += length
    for start_byte, end_byte in ranges:
        # print "# " * 40
        # print "start_byte: " + str(start_byte) + " end_byte: " + str(end_byte)
        # t_start = time.time()
        try:
            read_cache.get_data(path, start_byte, end_byte)
        except:
            return False
        # print "start_byte: ", start_byte, " end_byte: ", end_byte, "Fetch Time: " + str(time.time() - t_start)
        # print "# " * 40
        # print "Extreme Left: ", read_cache.page_table.get_cache_left(path).start_byte, \
        #     " Extreme Right: ", read_cache.page_table.get_cache_right(path).end_byte
    return True


def test4(read_cache):
    """ This test the right cache sweep followed by the left cache sweep"""
    import time
    path = "/vivek/21049.csv"
    end_byte = 45103604
    length = 16384
    old = 0
    right_ranges = list()
    # Ranges for Right Cache Sweep
    while old <= int(0.1 * end_byte):
        right_ranges.append([old, old+length])
        old += length

    # Ranges for Left Cache Sweep
    left_ranges = list(reversed(right_ranges))

    ranges = right_ranges + left_ranges

    for start_byte, end_byte in ranges:
        # print "# " * 40
        # print "start_byte: " + str(start_byte) + " end_byte: " + str(end_byte)
        # t_start = time.time()
        try:
            read_cache.get_data(path, start_byte, end_byte)
        except:
            return False

        # print "start_byte: ", start_byte, " end_byte: ", end_byte, "Fetch Time: " + str(time.time() - t_start)
        # print "# " * 40
        # print "Extreme Left: ", read_cache.page_table.get_cache_left(path).start_byte, \
        #     " Extreme Right: ", read_cache.page_table.get_cache_right(path).end_byte
    return True

if __name__ == '__main__':
    import logging
    # Temporary Directory to store the files. This to help in reading
    TEMP_DIR = "./.AUX/TEMP"

    # Adding logger
    logging.basicConfig(level=logging.ERROR)
    logger = logging.getLogger(__name__)

    # create a file handler
    handler = logging.FileHandler('HISTORY.log')
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    ip = "10.239.227.6"
    port = "8010"
    read_cache = ReadCache(logger, ip, port)


    tests = [test1(read_cache)]#, test2(read_cache), test3(read_cache), test4(read_cache)]
    for i, test in enumerate(tests):
        if test is not True: print "Test " + str(i+1) + " has failed"
        else: print "Test " + str(i+1) + " has passed"
        sys.stdout.flush()
