from page_table import PageTable
import ConfigParser
import utility
import os


def delete_file(filepath):
    os.remove(filepath)

class ReadCache:
    def __init__(self, logger):
        self.logger = logger
        self.page_table = PageTable(logger)
        config = ConfigParser.ConfigParser()
        config.read("./config.ini")
        self.record_per_page = int(config.get('PageTable', 'record_per_page'))
        self.aux_folder = str(config.get('AUX', 'folder'))

    def _get_data(self, filename, start):
        """ Getting data from a filename"""
        url = self._get_url() + "WsDfu/DFUBrowseData?ver_=1.31&wsdl"
        self.logger.info("ReadCache: _get_data(): {0}".format(url))
        result = utility.get_data(url, filename, start=start, count=self.record_per_page)
        self.logger.info("ReadCache: _get_data(): Data returned is " + result)
        return result.encode('utf-8').strip()

    def _create_file(self, ):

    def if_cached(self, path, start_byte, end_byte):
        # keys of the page table
        abs_left = self.page_table.get_cache_left(path)
        abs_right = self.page_table.get_right(path)
        start = False
        end = False
        if abs_left.start_byte <= start_byte < abs_right.end_byte:
            start = True
        if abs_left.start_byte <= end_byte < abs_right.end_byte:
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

        # keys of the page table
        keys = self.page_table.get_parts(path)
        ranges = [map(int, key.split('--')) for key in keys]

        # Since we are iterating through the ranges need to make sure that the ranges are sorted
        ranges = sorted(ranges, key=lambda x: x[0])

        data = ""
        # get to start position
        for range in ranges:
            if range[0] <= start_byte < range[1]: break

        for range in ranges:
            if range[0] <= start_byte < range[1]:
                page = self.page_table.fetch_page(path, start_byte, end_byte)
                cache_file_path = page.get_cache_file_path()
                offset = start_byte - page.get_start_byte()
                # if all the data is in one page
                if end_byte <= page.get_end_byte():
                    length = end_byte - start_byte
                else: # if data is spread around multiple pages
                    length = page.get_end_byte() - start_byte
                data += read_data(cache_file_path, offset, length)
            elif range[0] <= end_byte < range[1]:
                page = self.page_table.fetch_page(path, start_byte, end_byte)
                cache_file_path = page.get_cache_file_path()
                length = end_byte - page.get_start_byte()
                data += read_data(cache_file_path, 0, length)
                break  # reading is complete
            else:
                assert(len(data) > 0), "Someothing is wrong"
                page = self.page_table.fetch_page(path, start_byte, end_byte)
                cache_file_path = page.get_cache_file_path()
                length = page.get_end_byte() - page.get_start_byte()
                data += read_data(cache_file_path, 0, length)
        return data

    def invalidate_all_parts(self, path):
        parts = self.page_table.get_cache_parts(path)
        for part in parts:
            cache_file_path = self.page_table.part_invalidate_page(path, part)
            delete_file(cache_file_path)

    def invalidate_extreme(self, path, right=True):
        """ To move cache to the left - This invalidates the rightmost part"""
        parts = self.page_table.get_cache_parts(path)
        if right is True:
            start_end_part = max(parts, key=lambda x: x.part_no)
        else:
            start_end_part = min(parts, key=lambda x: x.part_no)
        cache_file_path = self.page_table.part_invalidate_page(path, start_end_part)
        delete_file(cache_file_path)

    def build_cache(self, path, start_count, part_no):
        # Fetch 1000 pages and store it
        modified_path = path[1:].replace("/", "::")
        data = self._get_data(modified_path, start_count)
        # generating part file name. Since this is the first file, the part number is assigned as 1
        part_path_file = self.aux_folder + path + "_" + str(part_no)
        self.logger.info("ReadCache: get_data(): File Created: " + part_path_file)
        open(part_path_file, 'w').write(data)

        total_size = sum([part.end_size - part.start_size for part in self.page_table.get_parts(path)])
        # Adding a page entry
        self.page_table.create_entry(path, start_count, start_count + self.record_per_page,total_size,
                        total_size+os.stat(part_path_file).st_size, part_no=part_no)

    def update_cache_file(self, path, start_count, part_no):
        # Fetch 1000 pages and store it
        modified_path = path[1:].replace("/", "::")
        data = self._get_data(modified_path, start_count)
        # generating part file name. Since this is the first file, the part number is assigned as 1
        part_path_file = self.aux_folder + path + "_" + str(part_no)
        self.logger.info("ReadCache: get_data(): File Created: " + part_path_file)
        open(part_path_file, 'w').write(data)

        # Updating a page entry
        self.page_table.update_entry(path, part_no)

    def rebuild_cache(self):
        # TODO

    def get_data(self, path, start_byte=-1, end_byte=-1):
        if end_byte - start_byte > self.get_cache_size(path):
            assert(True), "This functionality is not supported"
        # Check if this file has been fetched before
        if self.page_table.path_exists(path) is False:
            #TODO This part of the condition need to be reformatted
            assert(start_byte == -1 and end_byte == -1), "This should only happen when the file is opened"
            # Fetch 1000 pages and store it
            modified_path = path[1:].replace("/", "::")
            data = self._get_data(modified_path)
            # generating part file name. Since this is the first file, the part number is assigned as 1
            part_path_file = self.aux_folder + path + "_" + str(1)
            self.logger.info("ReadCache: get_data(): File Created: " + part_path_file)
            open(part_path_file, 'w').write(data)

            # Adding a page entry
            self.page_table.create_entry(path, 1, 1000, 0, os.stat(part_path_file).st_size, 0)

        # Check if the data exists in the local repository
        while self.if_cached(path, start_byte, end_byte) is not True:
            # Need to decide if should move to left or right
            # Get absolute left
            abs_left = self.page_table.get_cache_left(path)
            # Get absolute right
            abs_right = self.page_table.get_right(path)

            # if start_byte and end_byte is both to the left or right of the cache - invalidate all the pages
            if (start_byte < abs_left.start_byte and end_byte < abs_left.start_byte) or \
                    (start_byte > abs_right.end_byte and end_byte > abs_right.end_byte):
                # invalidate all parts
                self.invalidate_all_parts(path)
                #TODO implement rebuild_cache() - get the part number based on start and end byte

            # To Left: if the start byte is to the left of cache and end_byte is within cache
            elif abs_left > start_byte and end_byte <= abs_right:
                # if the entry for left part number does not exist
                abs_left_part_no = abs_left.get_part_no()
                to_fetch_part_no = abs_left_part_no - 1
                all_part_no = [part.get_part_no() for part in self.page_table.get_parts(path)]
                if to_fetch_part_no not in all_part_no:
                    assert(True), "This functionality is not supported"
                else:
                    self.invalidate_extreme(path, right=True)
                    part = self.page_table.get_part(path, to_fetch_part_no)
                    self.update_cache_file(path, part.get_start_record(), part.get_part_no())
                    self.page_table.part_validate_page(part)

            # To Right
            elif abs_left <= start_byte and end_byte > abs_right:
                self.invalidate_extreme(path, right=False)
                # if the entry for left part number does not exist
                abs_right_part_no = abs_right.get_part_no()
                to_fetch_part_no = abs_right_part_no + 1
                all_part_no = [part.get_part_no() for part in self.page_table.get_parts(path)]
                if to_fetch_part_no not in all_part_no:
                    self.invalidate_extreme(path, right=False)
                    self.build_cache(path, abs_right.end_record+1, to_fetch_part_no)
                    part = self.page_table.get_part(path, to_fetch_part_no)
                    self.page_table.part_validate_page(part)
                else:
                    self.invalidate_extreme(path, right=True)
                    part = self.page_table.get_part(path, to_fetch_part_no)
                    self.update_cache_file(path, part.get_start_record(), part.get_part_no())
                    self.page_table.part_validate_page(part)

                pick up here

        data = self._fetch_data(path, start_byte, end_byte)
        # Get data using the page table