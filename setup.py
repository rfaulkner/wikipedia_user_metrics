#!/usr/bin/python
# -*- coding: utf-8 -*-

from distutils.core import setup

with open('README.md') as file:
    long_description = file.read()

setup(
    name='e3_analysis',
    version='0.1-dev',
    email="e3-team@lists.wikimedia.org",
    long_description=long_description,
    description='Data Analysis for Wikipedia User data.',
    url='http://www.github.com/rfaulkner/E3_analysis',
    author='Ryan Faulkner',
    author_email='rfaulkner@wikimedia.org',
    long_description=__doc__,
    packages=['e3_analysis.src', 'e3_analysis.src.etl',
              'e3_analysis.src.metrics','e3_analysis.config'],
    scripts=['e3_analysis/scripts/e3_data_wrangle',
             'e3_analysis/scripts/e3_experiment_definitions.py',
             'e3_analysis/scripts/call_metric_on_users',
             'e3_analysis/scripts/daily_active_users',
             'e3_analysis/scripts/id2user.sh',
             'e3_analysis/scripts/user2id.sh',
             'e3_analysis/scripts/e3_settings.py'],
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    data_files=[('readme', ['README.md'])]
)
