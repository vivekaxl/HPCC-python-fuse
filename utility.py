from suds.client import Client
from suds.sudsobject import asdict
from datetime import datetime
from dateutil import parser
from bs4 import BeautifulSoup


def recursive_translation(d):
    result = {}
    for k, v in asdict(d).iteritems():
        if hasattr(v, '__keylist__'):
            result[k] = recursive_translation(v)
        elif isinstance(v, list):
            result[k] = []
            for item in v:
                if hasattr(item, '__keylist__'):
                    result[k].append(recursive_translation(item))
                else:
                    result[k].append(item)
        else:
            result[k] = v
    return result


def get_result(url, scope):
    client = Client(url)
    try:
        response = client.service.DFUFileView(Scope=scope)
    except:
        response = client.service.DFUInfo(Name=scope)
    dict = recursive_translation(response)
    return dict


def unix_time(time_string):
    dt = parser.parse(time_string)
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0


# def get_data(url, scope, start=0, count=100):
#     def return_tag(line, tag="line"):
#         soup = BeautifulSoup(line, "html.parser")
#         return soup.line.string
#
#     client = Client(url)
#     response = client.service.DFUBrowseData(LogicalName=scope, Start=start, Count=count)
#     results = response.Result.split('\n')
#     # only get the lines which has the tag <Row>
#     results = [result for result in results if '<Row>' in result]
#     # try:
#     results = [return_tag(result) for result in results]
#     if len(results) == 0: return ""
#     print ">> " * 10 , results[0]
#     no_of_fields = len(results[0].split(','))
#     lines = []
#     for result in results:
#         r = result.split(',')
#         d = {}
#         for i in xrange(no_of_fields):
#             d['field' + str(i + 1)] = r[i]
#         import json
#         lines.append(json.dumps(d, sort_keys=True))
#     lines = '\n'.join(lines)
#     print lines
#     return lines
    # except:
    #     import xmltodict, json
    #     collector = "<dataset>" + "".join(result) + "</dataset>"
    #     o = xmltodict.parse(collector)
    #     lines = [json.dumps(line) for line in o['dataset']['Row']]
    #     lines = '\n'.join(lines)
    #     return lines

def get_data(url, scope, start=0, count=100):
    def return_tag(line, tag="line"):
        soup = BeautifulSoup(line, "html.parser")
        return soup.line.string

    client = Client(url)
    response = client.service.DFUBrowseData(LogicalName=scope, Start=start, Count=count)
    results = response.Result.split('\n')
    # only get the lines which has the tag <Row>
    result = [result for result in results if '<Row>' in result]
    try:
        # Get the data between line
        lines = '\n'.join([return_tag(line) for line in result])
        return lines
    except:
        import xmltodict, json
        collector = "<dataset>" + "".join(result) + "</dataset>"
        o = xmltodict.parse(collector)
    return json.dumps(o)
