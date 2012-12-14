#!/usr/bin/python

from flask import Flask, url_for, jsonify, Response
app = Flask(__name__)

@app.route('/')
def api_root():
    return 'Welcome to the user metrics API'

@app.route('/articles')
def api_articles():
    return 'List of ' + url_for('api_articles')

@app.route('/articles/<articleid>')
def api_article(articleid):
    return 'You are reading ' + articleid

@app.route('/users')
def api_users():
    return 'List of ' + url_for('api_users')

@app.route('/users/<userid>', methods = ['GET'])
def api_user(userid):
    users = {'1':'John', '2':'Steve', '3':'Bill'}
    
    if userid in users:
        return jsonify({userid:users[userid]})
    else:
        return jsonify('{}')

if __name__ == '__main__':
    app.run()
