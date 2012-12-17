#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
    Entry point for flask web server implementin Wikimedia Metrics API
"""

from flask import Flask, render_template, Markup, jsonify, redirect, url_for
import src.etl.data_loader as dl
import cPickle
import logging
import  sys
import config.settings as settings

import src.metrics.threshold as th
# import src.metrics.blocks as b
# import src.metrics.bytes_added as ba
# import src.metrics.survival as sv
# import src.metrics.time_to_threshold

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr,
    format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

# Global instance of pickle file and results data
pkl_file = None
pkl_data = None

app = Flask(__name__)

def get_metric(handle):
    if handle == 'threshold':
        return th.Threshold()
    return ''

@app.route('/')
def api_root():
    return 'Welcome to the user metrics API'

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
        return render_template('metrics.html', c_str=cohort, m_list=['threshold'])

@app.route('/metrics/<cohort>/<metric>')
def output(cohort, metric):

    url = "".join(['/metrics/', cohort, '/', metric])
    if url in pkl_data:
        results = pkl_data[url]
    else:
        # Get users
        conn = dl.Connector(instance='slave')
        try:
            conn._cur_.execute('select utm_id from usertags_meta where utm_name = "%s"' % str(cohort))
            res = conn._cur_.fetchone()[0]
            conn._cur_.execute('select ut_user from usertags where ut_tag = "%s"' % res)
        except IndexError:
            redirect(url_for('cohorts'))

        # Get metric
        metric_obj = get_metric(metric)
        if not metric_obj: return redirect(url_for('cohorts'))

        results = dict()
        results['header'] = " ".join(metric_obj.header())
        results['metric'] = dict()

        users = [r[0] for r in conn._cur_]

        for m in metric_obj.process(users, num_threads=20):
            results['metric'][m[0]] = " ".join(dl.DataLoader().cast_elems_to_string(m[1:]))

        del conn
        results = jsonify(results)
        pkl_data[url] = results

    return results

if __name__ == '__main__':

    # Open the pickled data for reading.  Then
    pkl_file = open(settings.__data_file_dir__ + 'api_data.pkl', 'rb')
    pkl_data = cPickle.load(pkl_file)
    pkl_file.close()

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


