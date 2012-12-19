#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
    Entry point for flask web server implementin Wikimedia Metrics API
"""

from flask import Flask, render_template, Markup, jsonify, redirect, url_for, make_response
import src.etl.data_loader as dl
import cPickle
import logging
import sys
import os
import re
import json
import config.settings as settings
import multiprocessing as mp
import collections

import src.metrics.threshold as th
import src.metrics.blocks as b
import src.metrics.bytes_added as ba
import src.metrics.survival as sv
import src.metrics.revert_rate as rr
import src.metrics.time_to_threshold as ttt

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr,
    format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

global global_id
global_id = 0

app = Flask(__name__)
metric_dict = {
    'threshold' : th.Threshold,
    'survival' : sv.Survival,
    'revert' : rr.RevertRate,
    'bytes_added' : ba.BytesAdded,
    'blocks' : b.Blocks,
    'time_to_threshold' : ttt.TimeToThreshold
}

processQ = list()
QStructClass = collections.namedtuple('QStruct', 'id process url queue status')
# gl_lock = mp.Lock()    # global lock

@app.route('/')
def api_root():
    return render_template('index.html')

@app.route('/login')
def login():
    return render_template('login.html')

@app.route('/tag_definitions')
def tag_definitions():

    usertag_meta_data = ''
    conn = dl.Connector(instance='slave')
    conn._cur_.execute('select * from usertags_meta')

    for r in conn._cur_:
        usertag_meta_data += " ".join(dl.DataLoader().cast_elems_to_string(list(r))) + '<br>'

    del conn
    return render_template('tag_definitions.html', data=Markup(usertag_meta_data))

@app.route('/cohorts')
def cohorts():
    conn = dl.Connector(instance='slave')
    conn._cur_.execute('select distinct utm_name from usertags_meta')
    o = [r[0] for r in conn._cur_]

    del conn
    return render_template('cohorts.html', data=o)

@app.route('/metrics')
@app.route('/metrics/<cohort>')
def metrics(cohort=''):
    if not cohort:
        return redirect(url_for('cohorts'))
    else:
        return render_template('metrics.html', c_str=cohort, m_list=metric_dict.keys())

@app.route('/metrics/<cohort>/<metric>')
def output(cohort, metric):
    global global_id
    url = "".join(['/metrics/', cohort, '/', metric])

    if url in pkl_data:
        return pkl_data[url]
    else:
        q = mp.Queue()
        p = mp.Process(target=process_metrics, args=(url, cohort, metric, q))
        p.start()

        global_id += 1

        logging.info('Appending request %s to the queue...' % url)
        processQ.append(QStructClass(global_id,p,url,q,['pending']))

        return render_template('processing.html', url_str=url)

@app.route('/job_queue')
def job_queue():

    p_list = list()
    p_list.append(Markup('<u><b>is_alive , PID, url, status</b></u><br>'))
    for p in processQ:
        try:

            while not p.queue.empty():
                if not queue_data.has_key(p.id):
                    queue_data[p.id] = json.loads(p.queue.get().data)
                else:
                    print queue_data[p.id]
                    for k,v in queue_data[p.id]:
                        if hasattr(v,'__iter__'): queue_data[p.id][k].extend(v)

            if not p.process.is_alive():
                q_response = make_response(jsonify(queue_data[p.id]))
                del queue_data[p.id]
                pkl_data[p.url] = q_response

                p.status[0] = 'success'
                logging.info('Completed request %s.' % p.url)
        except Exception as e:
            p.status[0] = 'failure'
            logging.error("Could not update request: %s.  Exception: %s" % (p.url, e.message) )

        # Log the status of the job
        p_list.append(" , ".join([str(p.process.is_alive()), str(p.process.pid), p.url, p.status[0]]))

    return render_template('queue.html', procs=p_list)


def process_metrics(url, cohort, metric, p):

    conn = dl.Connector(instance='slave')
    try:
        conn._cur_.execute('select utm_id from usertags_meta where utm_name = "%s"' % str(cohort))
        res = conn._cur_.fetchone()[0]
        conn._cur_.execute('select ut_user from usertags where ut_tag = "%s"' % res)
    except IndexError:
        redirect(url_for('cohorts'))

    # Get metric
    metric_obj = None
    try:
        metric_obj = metric_dict[metric]()
    except KeyError:
        logging.error('Bad metric handle: %s' % url)
        redirect(url_for('cohorts'))

    if not metric_obj: return redirect(url_for('cohorts'))

    results = dict()
    results['header'] = " ".join(metric_obj.header())
    for key in metric_obj.__dict__:
        if re.search(r'_.*_', key):
            results[str(key[1:-1])] = str(metric_obj.__dict__[key])
    results['metric'] = dict()

    users = [r[0] for r in conn._cur_]

    logging.debug('Processing results for %s... (PID = %s)' % (url, os.getpid()))

    for m in metric_obj.process(users, num_threads=50, rev_threads=50):
        results['metric'][m[0]] = " ".join(dl.DataLoader().cast_elems_to_string(m[1:]))

    p.put(jsonify(results))
    del conn
    logging.info('Processing complete for %s... (PID = %s)' % (url, os.getpid()))


if __name__ == '__main__':

    queue_data = dict()

    # Open the pickled data for reading.  Then
    pkl_file = open(settings.__data_file_dir__ + 'api_data.pkl', 'rb')
    pkl_data = cPickle.load(pkl_file)
    pkl_file.close()

    flush_process = None
    try:
        app.run()
    finally:
        try:
            pkl_file = open(settings.__data_file_dir__ + 'api_data.pkl', 'wb')
            cPickle.dump(pkl_data, pkl_file)
        except Exception:
            logging.error('Could not pickle data.')
            pass
        pkl_file.close()


