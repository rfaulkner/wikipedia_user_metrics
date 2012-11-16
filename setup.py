#!/usr/bin/python

from distutils.core import setup

with open('README.md') as file:
    long_description = file.read()

setup(
    name='e3_analysis',
    version='0.1.4',
    long_description=long_description,
    description='Data handling code for Wikimedia Editor Engagement Experiments.',
    url='http://www.github.com/rfaulkner/E3_analysis',
    author='Ryan Faulkner',
    author_email='rfaulkner@wikimedia.org',
    packages=['e3_analysis.src.etl', 'e3_analysis.src.metrics',
              'e3_analysis.config'],
    scripts=['e3_analysis/scripts/e3_data_wrangle', 'e3_analysis/scripts/e3_experiment_definitions.py',
             'e3_analysis/scripts/call_metric_on_users', 'e3_analysis/scripts/daily_active_users',
             'e3_analysis/scripts/id2user.sh', 'e3_analysis/scripts/user2id.sh', 'e3_analysis/scripts/e3_settings.py'],
    data_files=[('readme', ['README.md'])]
)

