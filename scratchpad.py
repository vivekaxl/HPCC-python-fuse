
import ConfigParser
config = ConfigParser.ConfigParser()
config.read("./config.ini")
exact_filesize = True if config.get('AUX', 'extact_filesize') == "True" else False

print exact_filesize, type(exact_filesize)