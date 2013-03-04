
# CONFIGURE THE LOGGER
from os.path import exists
from urllib2 import urlopen
import json
import logging
import sys

logging.basicConfig(level=logging.DEBUG, stream=sys.stderr,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%b-%d %H:%M:%S')


def get_project_host_map(usecache=True):
    cache_name = 'project_host_map.json'
    if not exists(cache_name) or not usecache:
        cluster_url_fmt = 'https://gerrit.wikimedia.org/r/' \
                          'gitweb?p=operations/mediawiki-' \
                          'config.git;a=blob_plain;f=s%d.' \
                          'dblist;hb=HEAD'
        #host_fmt = 's%d-analytics-slave.eqiad.wmnet'
        host_fmt = 's%d'
        project_host_map = {}
        for i in range(1, 8):
            host = host_fmt % i
            url = cluster_url_fmt % i
            projects = urlopen(url).read().splitlines()
            for project in projects:
                project_host_map[project] = host
        json.dump(project_host_map, open(cache_name, 'w'))
    else:
        project_host_map = json.load(open(cache_name))
    return project_host_map
