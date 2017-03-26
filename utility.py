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

def get_data(url):
    total_count = -1
    import urllib2
    url = url.replace(' ', '%20')
    # Download the content of the page
    response = urllib2.urlopen(url)
    html = response.read()
    data = html.split('\"Row\": ')[-1][1:-4].replace('\n','').replace('},', '}||')
    try:
        # This would only work for thor files
        total_count = [int(l.strip().split(":")[-1].strip()) for l in html.split(',') if "Total" in l][-1]
    except:
        pass
    return '\n'.join([d.strip() for d in data.split('||')]), total_count
