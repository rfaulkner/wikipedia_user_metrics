
Overview
========

To setup the dependencies for the e3_analysis module and the metrics API.

Before doing this (exit if you have already entered the env) install MySQL server: ::

    root:~# apt-get install mysql-server

System Wide Install
--------------------

Install setuptools: ::

    root:~# wget http://pypi.python.org/packages/source/s/setuptools/setuptools-0.6c11.tar.gz
    root:~# tar xzf setuptools-0.6c11.tar.gz
    root:~# cd setuptools-0.6c11
    root:~# python setup.py install 

Install pip: ::

    root:~# curl -O http://pypi.python.org/packages/source/p/pip/pip-1.0.tar.gz
    root:~# tar xvfz pip-1.0.tar.gz
    root:~# cd pip-1.0
    root:~# python setup.py install # may need to be root

Now install Flask, MySQLdb, mod_wsgi, scipy, and numpy: ::

    root:~# pip install Flask
    root:~# apt-get install libmysqlclient-dev
    root:~# apt-get install python-mysqldb
    root:~# apt-get install libapache2-mod-wsgi
    root:~# apt-get install python-numpy
    root:~# apt-get install python-scipy

Clear out the setup files: ::

    root:~# rm -rf numpy-1.6.2
    root:~# rm numpy-1.6.2.tar.gz 
    root:~# rm -rf pip-1.0
    root:~# rm -rf setuptools-0.6c11
    root:~# rm setuptools-0.6c11.tar.gz 
    root:~# rm pip-1.0.tar.gz 

Now we are ready to clone E3_analysis Github repository: ::

   root:~# git clone https://github.com/rfaulkner/E3_analysis.git

After the repo has been successfully cloned under <app_home>/src/api/ you will find api.wsgi.  This file should be moved to directory under the web server root: ::

    root:~# cd /var/www
    root:~# mkdir api
    root:~# cd api
    root:~# mv /home/<username>/E3_analysis/src/api/api.wsgi .

You will need to set the PROJECT_PATH in `api.wsgi` to the location of the cloned `E3_analysis` project. ::

	import sys
	sys.stdout = sys.stderr     # replace the stdout stream
	from src.api.run import app as application

Once this is complete go to the home path of the package under the `config` subfolder and create a file settings.py (there should be an example file called settings.py.example) and configure your path and database settings. The
example file will help to guide you as will the instructions in the :doc:`install` section of this documentation.  

Finally, you'll need to setup apache to point to the `api` application.  In your apache root config (/etc/apache2/apache2.conf) ensure the following line is appended: ::

    Include sites-enabled/ 

Now go to /etc/apache2/sites-available and create a file `00*-metrics-api` containing the following configuration: ::

	<VirtualHost *:80>
    		ServerName metrics-api.wikimedia.org
 
    		WSGIDaemonProcess api user=stats group=wikidev threads=5 python-path=/a/e3/E3Analysis
    		WSGIScriptAlias / /srv/org.wikimedia.metrics-api/api.wsgi
 
    		<Directory /srv/org.wikimedia.metrics-api>
        		WSGIProcessGroup api
        		WSGIApplicationGroup %{GLOBAL}
        		Order deny,allow
        		Allow from all
    		</Directory>
	</VirtualHost>

Subsequently create a soft link: ::

  	root:~# cd ../sites-enabled
	root:~# ln -s ../sites-available/00*-metrics-api

Finally, restart the server: ::

    root:~# apachectl restart

Now, the server should be working however the project and database credentials will need to be set in `/<project_home>/config.settings` (e.g. __home__ = '/a/E3/', __project_home__ = '/a/E3/E3Analysis/') in order to build the internal references for the project.  To complete this step see `<project_home>/config/settings.py.example`, the fields in this file need to be set so that the project home and local database credentials are supplied.  The double starred lines below indicate which fields to change:  ::

	# Project settings
	# ================
	**__home__ = '<user_home>'
  	**__project_home__ = ''.join([__home__, '<project home>'])
  	__web_home__ = ''.join([__project_home__, 'web_interface/'])
  	__sql_home__ = ''.join([__project_home__, 'SQL/'])
	__server_log_local_home__ = ''.join([__project_home__, 'logs/'])
	__data_file_dir__ = ''.join([__project_home__, 'data/'])

	__web_app_module__ = 'api'
	__system_user__ = 'rfaulk'

	# Database connection settings
	# ============================

	connections = {
    		'slave': {
        		'user' : 'research',
        		**'host' : '127.0.0.1',
        		'db' : 'staging',
        		**'passwd' : 'xxxx',
        		'port' : 3307},
    		'slave-2': {
        		'user' : 'rfaulk',
        		'host' : '127.0.0.1',
        		'db' : 'rfaulk',
        		'passwd' : 'xxxx',
        		'port' : 3307}
		}
	}
 
Finally, rename the file to simply `settings.py`.  Now you are ready to test the instance, first tail the Apache logs to ensure there are no errors: ::

Next, modify your /etc/hosts file to contain the line **"127.0.0.1 wikimedia-metrics-api"** (assuming that the instance is running locally) and run curl to verify the content is being properly served: ::

	root:~# sudo nano /etc/hosts
	root:~# curl wikimedia-metrics-api

You should get something back that looks like: ::

	<!doctype html>
	<title>Wikimedia Metrics API</title>
	<link rel=stylesheet type=text/css href="/static/style.css">
	<div class=page>
 		<h1>Wikimedia Metrics API</h1>
  			<div class=metanav>
    			<a href="/login">log in</a>
  			</div>
  
		<h3>Welcome to the user metrics API!</h3>
		…

You should be all set!  If you need further assistance contact the author, Ryan Faulkner at rfaulkner@wikimedia.org.  


Virtual Environment Install
---------------------------

Setup a virtual environment to get pip running.  For example: ::
    
    $ curl -O https://raw.github.com/pypa/virtualenv/master/virtualenv.py
    $ python virtualenv.py my_new_env

To use the new virtual environment: ::

    $ . my_new_env/bin/activate


