#!/usr/bin/python
# -*- coding: utf-8 -*-

from distutils.core import setup

with open('README.md') as file:
    long_description = file.read()

__version__ = '0.1-dev'

setup(
    name='e3_analysis',
    version=__version__,
    long_description=long_description,
    description='Data Analysis for Wikipedia User data.',
    url='http://www.github.com/rfaulkner/E3_analysis',
    author="Wikimedia Foundation",
    author_email="e3-team@lists.wikimedia.org",
    packages=['e3_analysis.src',
              'e3_analysis.src.etl',
              'e3_analysis.src.metrics',
              'e3_analysis.config'],
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
