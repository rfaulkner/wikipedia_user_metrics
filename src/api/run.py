#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
    Entry point for flask web server implementin Wikimedia Metrics API.

    Process states: ::
        * 'pending' - The request has yet to be fully processed and exposed but is underway
        * 'success' - The request has finished processing and is exposed at the url
        * 'failure' - The result has finished processing but dailed to expose results
"""

import sys
sys.path.insert(0,"/Users/dartar/git/E3_analysis")
from flask import Flask, render_template, Markup, jsonify, \
    redirect, url_for, make_response, request, escape

import cPickle
from re import search
from config import logging
import os
import json
from urlparse import urlparse
import config.settings as settings
import multiprocessing as mp
import collections

import src.etl.data_loader as dl
import src.metrics.metrics_manager as mm
from src.api import COHORT_REGEX, parse_cohorts

app = Flask(__name__)

# hash for jobs in queue_data dict
global global_id
global_id = 0

# Stores cached requests (this should eventually be replaced with a proper cache)
global pkl_data
pkl_data = dict()

# stores data in Queue objects that are active
global queue_data
queue_data = dict()

global error_codes
error_codes = {
    0 : 'Job already running.'
}

global processQ
processQ = list()

QStructClass = collections.namedtuple('QStruct', 'id process url queue status')

@app.route('/')
def api_root():
    """ View for root url - API instructions """
    conn = dl.Connector(instance='slave')
    conn._cur_.execute('select utm_name from usertags_meta')
    data = [r[0] for r in conn._cur_]
    del conn
    return render_template('index.html', cohort_data=data)

@app.route('/about/')
def about():
    return render_template('about.html')

@app.route('/contact/')
def contact():
    return render_template('contact.html')

@app.route('/tags/')
def tags():
    """ View for tag definitions where cohort meta data can be reviewed """

    conn = dl.Connector(instance='slave')
    conn._cur_.execute('select * from usertags_meta')

    f = dl.DataLoader().cast_elems_to_string
    utm = [escape(", ".join(f(r))) for r in conn._cur_]

    del conn
    return render_template('tags.html', procs=utm)

@app.route('/cohorts/')
def cohorts():
    """ View for listing and selecting cohorts """
    conn = dl.Connector(instance='slave')
    conn._cur_.execute('select distinct utm_name from usertags_meta')
    o = [r[0] for r in conn._cur_]

    del conn
    return render_template('cohorts.html', data=o)

@app.route('/metrics')
@app.route('/metrics/<string:cohort>')
def metrics(cohort=''):
    """ View for listing and selecting metrics """
    if not cohort:
        return redirect(url_for('cohorts'))
    else:
        return render_template('metrics.html', c_str=cohort, m_list=mm.get_metric_names())

@app.route('/metrics/<string:cohort>/<string:metric>')
def output(cohort, metric):
    """ View corresponding to a data request - all of the setup and execution for a request happens here. """

    global global_id
    extra_params = list()   # stores extra query parameters that may be valid outside of metric parameters

    url = request.url.split(request.url_root)[1]

    # GET - parse query string
    arg_dict = dict()
    for key in request.args: arg_dict[key] = request.args[key]

    refresh = False
    if 'refresh' in arg_dict:
        try:
            if int(arg_dict['refresh']): refresh = True
        except ValueError: pass

    aggregator = arg_dict['aggregator'] if 'aggregator' in arg_dict else ''
    aggregator_key = mm.get_agg_key(aggregator, metric)
    if mm.aggregator_dict.has_key(aggregator_key):
        extra_params.append('aggregator')

    if 'time_series' in arg_dict: extra_params.extend(['time_series', 'date_start', 'date_end', 'interval'])

    # Format the query string
    metric_params = mm.get_param_types(metric)
    url = strip_query_string(url, metric_params['init'].keys() + metric_params['process'].keys() + extra_params)

    if url in pkl_data and not refresh:
        return pkl_data[url]
    else:

        # Ensure that the job for this url is not already running
        is_pending_job = False
        for p in processQ:
            if not cmp(url, p.url) and p.status[0] == 'pending': is_pending_job = True

        if not is_pending_job: # Queue the job

            q = mp.Queue()
            p = mp.Process(target=process_metrics, args=(url, cohort, metric, aggregator, q, arg_dict))
            p.start()

            global_id += 1

            logging.info(__name__ + '::Appending request %s to the queue...' % url)
            processQ.append(QStructClass(global_id,p,url,q,['pending']))

            return render_template('processing.html', url_str=url)
        else:
            return redirect(url_for('job_queue') + '?error=0')

@app.route('/job_queue/')
def job_queue():
    """ View for listing current jobs working """

    error = ''
    if 'error' in request.args:
        try:
            error = error_codes[int(request.args['error'])]
        except KeyError: pass
        except ValueError: pass

    def error_class(em):
    	return {
        	'failure': 'error',
        	'pending': 'warning',
        	'success': 'success'
        	}.get(em, '') 

    p_list = list()
    p_list.append(Markup('<thead><tr><th>is_alive</th><th>PID</th><th>url</th><th>status</th></tr></thead>\n<tbody>\n'))
    for p in processQ:
        try:

            # Pull data off of the queue and add it to the queue data
            while not p.queue.empty():
                if not queue_data.has_key(p.id):
                    queue_data[p.id] = json.loads(p.queue.get().data)
                else:
                    for k,v in queue_data[p.id]:
                        if hasattr(v,'__iter__'): queue_data[p.id][k].extend(v)

            # once a process has finished working remove it and put its contents into the cache
            if not p.process.is_alive() and p.status[0] == 'pending':
                q_response = make_response(jsonify(queue_data[p.id]))
                del queue_data[p.id]
                pkl_data[p.url] = q_response

                p.status[0] = 'success'
                logging.info(__name__ + '::Completed request %s.' % p.url)

        except Exception as e:
            p.status[0] = 'failure'
            logging.error(__name__ + "::Could not update request: %s.  Exception: %s" % (p.url, e.message) )

        # Log the status of the job
        response_url = "".join(['<a href="', request.url_root, p.url + '">', p.url, '</a>'])
	
        p_list.append(Markup('<tr class="'+ error_class(p.status[0])+'"><td>'))
        p_list.append("</td><td>".join([str(p.process.is_alive()), str(p.process.pid),
                                  escape(Markup(response_url)), p.status[0]]))
        p_list.append(Markup('</td></tr>'))

    p_list.append(Markup('\n</tbody>'))

    if error:
        return render_template('queue.html', procs=p_list, error=error)
    else:
        return render_template('queue.html', procs=p_list)

@app.route('/all_urls')
def all_urls():
    """ View for listing all requests """

    url_list = list()
    for url in pkl_data.keys():
        url_list.append("".join(['<a href="', request.url_root, url + '">', url, '</a>']))
    return render_template('all_urls.html', urls=url_list)

def strip_query_string(url, valid_items):
    """ Strips the query string down to the relevant items defined by the list of string objects `valid_items` """

    # parse the url then remove the query string
    url_obj = urlparse(url)
    url = url.split('?')[0]

    q_params = dict()
    new_q_params = list()

    # Compile the query string elements
    for q_items in url_obj.query.split('&'):
        k = q_items.split('=')
        try: q_params[k[0]] = k[1]
        except IndexError: pass

    # Hardcode time series var if it is included
    q_params['time_series'] = 'True'

    # Filter the parameters
    for item in valid_items:
        if q_params.has_key(item): new_q_params.append(item+'='+q_params[item])

    # synthesize and return the new url
    if new_q_params:
        return url.split('?')[0] + '?' + "&".join(new_q_params)
    else:
        return url

def process_metrics(url, cohort, metric, aggregator, p, args):
    """ Worker process for requests - this will typically operate in a forked process """

    conn = dl.Connector(instance='slave')
    logging.info(__name__ + '::START JOB %s (PID = %s)' % (url, os.getpid()))

    # process metrics
    users = get_users(cohort)
    results = mm.process_data_request(metric, users, agg_handle=aggregator, **args)

    p.put(jsonify(results))
    del conn
    logging.info(__name__ + '::END JOB %s (PID = %s)' % (url, os.getpid()))

def get_users(cohort_exp):
    """ get users from cohort """

    if search(COHORT_REGEX, cohort_exp):
        logging.info(__name__ + '::Processing cohort by expression.')
        users = [user for user in parse_cohorts(cohort_exp)]
    else:
        logging.info(__name__ + '::Processing cohort by tag name.')
        conn = dl.Connector(instance='slave')
        try:
            conn._cur_.execute('select utm_id from usertags_meta where utm_name = "%s"' % str(cohort_exp))
            res = conn._cur_.fetchone()[0]
            conn._cur_.execute('select ut_user from usertags where ut_tag = "%s"' % res)
        except IndexError:
            redirect(url_for('cohorts'))
        users = [r[0] for r in conn._cur_]
        del conn
    return users

class APIMethods(object):

    __instance = None   # Singleton instance

    def __new__(cls):
        """ This class is Singleton, return only one instance """
        if not cls.__instance:
            cls.__instance = super(APIMethods, cls).__new__(cls)
        return cls.__instance

    def __init__(self):

        global pkl_data

        # Open the pickled data for reading.
        try:
            pkl_file = open(settings.__data_file_dir__ + 'api_data.pkl', 'rb')
        except IOError:
            pkl_file = None

        # test whether the open was successful
        if pkl_file:
            pkl_data = cPickle.load(pkl_file)
            pkl_file.close()

    def __del__(self):

        global pkl_data

        pkl_file = None
        try:
            pkl_file = open(settings.__data_file_dir__ + 'api_data.pkl', 'wb')
            cPickle.dump(pkl_data, pkl_file)
        except Exception:
            logging.error(__name__ + '::Could not pickle data.')
        finally:
            if hasattr(pkl_file, 'close'): pkl_file.close()

if __name__ == '__main__':

    a = APIMethods() # initialize API data
    try:
        app.run()
    finally:
        del a



