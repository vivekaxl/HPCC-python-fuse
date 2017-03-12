from __future__ import division
from page_table_entry import PageTableEntry
import ConfigParser


class PageTable:
    def __init__(self, logger):
        """
        This is the data structure that holds the page table
        :param logger:
        :param window_size:
                                        left
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~--------------------------~~~~~~~~~~~~~~~~~~~~~~
                                         |                        |
            Data                         |       Cache            |
                                         |                        |
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~--------------------------~~~~~~~~~~~~~~~~~~~~~~
                                                                right
        """
        self.logger = logger
        """
        The page table is designed as follows:
        page_table
        | path <- Key
        |   | 'start_byte -- end_byte' <- Key
        |   |   | PageTableEntry
        """
        self.page_table = {}
        self.aux_folder = self._get_aux_folder()

    @staticmethod
    def _get_aux_folder():
        config = ConfigParser.ConfigParser()
        config.read("./config.ini")
        return str(config.get('AUX', 'folder'))

    def create_entry(self, path, start_record, end_record, start_byte, end_byte, part_no):
        # creating a key for the page table
        page_key = part_no
        if path not in self.page_table.keys(): self.page_table[path] = {}
        if page_key in self.page_table[path]:
            self.logger.info('PageTable: create_entry(): The entry already exists. Something is wrong')
        else:
            cache_file_path = self.aux_folder + path[1:] + "_" + str(part_no)
            self.page_table[path][page_key] = PageTableEntry(cache_file_path, start_record, end_record, start_byte, end_byte, part_no)

    def delete_entry(self, path, part_no):
        if path not in self.page_table.keys():
            self.logger.info('PageTable: delete_entry(): The entry does not exists for the path. Something is wrong')
        if part_no not in self.page_table[path].keys():
            self.logger.info('PageTable: delete_entry(): The entry already exists. Something is wrong')
        else:
            del self.page_table[path][part_no]
            # if the file does not have any other pages then delete the entry for the file
            if len(self.page_table[path].keys()) == 0:
                del self.page_table[path]

    def update_entry(self, path, part_no):
        if path not in self.page_table.keys():
            self.logger.info('PageTable: delete_entry(): The entry does not exists for the path. Something is wrong')
        if part_no not in self.page_table[path].keys():
            self.logger.info('PageTable: delete_entry(): The entry already exists. Something is wrong')
        else:
            # updating only the cache file path.
            cache_file_path = self.aux_folder + path + "_" + str(part_no)
            self.page_table[path][part_no].set_cache_file_path(cache_file_path)

    def path_exists(self, path):
        if path in self.page_table.keys():
            self.logger.info('PageTable: path_exists(): Path exists')
            return True
        self.logger.info('PageTable: path_exists(): Path does not exist')
        return False

    def lru_invalidate_page(self, path):
        # Find all the [cache_key, access_time] that has cached data
        cached_parts = self.get_cache_parts(path)
        # The entry of the page which is cached and was least recently used
        lru_page = min(cached_parts, key=lambda x: x.access_time)
        # invalidate cache
        lru_page.invalidate_cache()
        return lru_page.get_cache_file_path()

    def part_invalidate_page(self, path, part_no):
        part = self.get_part(path, part_no)
        assert (part.if_cached is True), "Something is wrong"
        part.invalidate_cache()
        assert (self.get_part(path, part_no).if_cached is False), "Something is wrong"

        part.invalidate_cache()
        return part.get_cache_file_path()

    def part_validate_page(self, path, part_no):
        part = self.get_part(path, part_no)
        part.validate_cache()
        assert(self.get_part(path, part_no).if_cached is True), "Something is wrong"
        return part.get_cache_file_path()

    def get_parts(self, path):
        """ Get all the part numbers"""
        return self.page_table[path].keys()

    def get_cache_left(self, path):
        """
        :param path: Path of the file
        :return: The part which is to the left of the cache
        """
        all_start_positions = self.get_cache_parts(path)
        return min(all_start_positions, key=lambda x: x.part_no)

    def get_cache_right(self, path):
        """
        :param path: Path of the file
        :return: The part which is to the right of the cache
        """
        all_start_positions = self.get_cache_parts(path)
        return max(all_start_positions, key=lambda x: x.part_no)

    def get_page_table_left(self, path):
        return min(self.get_parts(path), lambda x: x.part_no)

    def get_page_table_right(self, path):
        return max(self.get_parts(path), lambda x: x.part_no)

    def get_cache_parts(self, path):
        """
        :param path: Path of the file
        :return: The parts of the cache
        """
        cached_parts = [self.page_table[path][key] for key in self.page_table[path].keys() if self.page_table[path][key].get_cache_status() is True]
        return cached_parts

    def get_cache_size(self, path):
        cache_parts = self.get_cache_parts(path)
        total_size = sum([part.end_byte - part.start_byte for part in cache_parts])
        return total_size

    def get_part(self, path, part_no):
        """ Return the PageTableEntry of a specific part_no"""
        return self.page_table[path][part_no]

    def get_parts_based_byte_position(self, path, start_byte, end_byte):
        parts = self.get_parts(path)
        object_parts = [self.get_part(path, part) for part in parts]
        return_parts = []
        for object_part in object_parts:
            if object_part.get_start_byte() <= start_byte <= object_part.get_end_byte():
                if object_part.get_part_no() not in [return_part.get_part_no() for return_part in return_parts]:
                    return_parts.append(object_part)
            if object_part.get_start_byte() <= end_byte <= object_part.get_end_byte():
                if object_part.get_part_no() not in [return_part.get_part_no() for return_part in return_parts]:
                    return_parts.append(object_part)

        # the start byte and end byte position has never been fetched before
        if len(return_parts) == 0: return -1
        else: return return_parts

    def if_accessed_before(self, path, part_no):
        """
        Checks if the part_no has been fetched before
        :param path: path
        :param part_no:
        :return: True or False
        """
        if part_no in self.page_table[path].keys(): return True
        return False

    def get_ranges_of_parts(self, path, part_no):
        part = self.get_part(path, part_no)
        return [part.start_byte, part.end_byte]