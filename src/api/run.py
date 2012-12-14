#!/usr/bin/python

from flask import Flask, render_template # url_for, jsonify, 

app = Flask(__name__)

@app.route('/')
def api_root():
    return 'Welcome to the user metrics API'

@app.route('/login')
def login():
    return render_template('login.html')

@app.route('/tag_definitions')
def tag_definitions(): pass

@app.route('/cohorts')
def api_cohorts():
    return 'Please choose a cohort: '

@app.route('/metrics')
def api_article():
    return 'Choose some metrics '


if __name__ == '__main__':
    app.run()
