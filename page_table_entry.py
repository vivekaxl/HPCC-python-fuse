from datetime import datetime


class PageTableEntry:
    def __init__(self, path, start_record, end_record, start_byte, end_byte, part_no, eof=False):
        """
        This is an entry which holds data of each page of a file
        :param path: thor file path
        :param start_record:
        :param end_record:
        :param start_byte:
        :param end_byte:
        """
        self.part_no = part_no
        self.start_byte = start_byte
        self.end_byte = end_byte
        self.start_record = start_record
        self.end_record = end_record
        self.cache_file_path = path
        self.if_cached = True  # if the data exists in the local repo
        self.access_time = datetime.now().microsecond  # last time when the page was accessed
        self.EOF = eof # If this entry is the end of file

    def update_access_time(self):
        self.access_time = datetime.now().microsecond

    def invalidate_cache(self):
        self.if_cached = False

    def validate_cache(self):
        self.if_cached = True

    def get_cache_status(self):
        return self.if_cached

    def get_access_time(self):
        return self.access_time

    def get_cache_file_path(self):
        return self.cache_file_path

    def set_cache_file_path(self, cache_file_path):
        self.cache_file_path = cache_file_path

    def get_start_byte(self):
        return self.start_byte

    def get_end_byte(self):
        return self.end_byte

    def get_part_no(self):
        return self.part_no

    def get_start_record(self):
        return self.start_record

    def get_eof(self):
        return self.EOF
