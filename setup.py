# -*- coding: utf-8 -*-
"""
The Wikimedia Foundation's E3 Data Analysis Library

"""

from setuptools import setup, find_packages



setup(
    name='e3analysis',
    packages=find_packages(),
    version='0.1-dev',
    url='http://meta.wikimedia.org/wiki/E3',
    license='GPL',
    description='E3 Data Analysis Library',
    author='Wikimedia Foundation -- E3 Team'
    email="e3-team@lists.wikimedia.org",
    long_description=__doc__,
    entry_points = {
        'console_scripts': [ 'ct2csv = e3analysis.ct2csv:main' ],
    },
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
