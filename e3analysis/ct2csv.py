#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    ct2csv - Generate CSV from ClickTracking logs

    Usage: ct2csv [OPTIONS] [file ...]

      files                  Raw or gzip/bzip2-compressed log files; if missing
                             or '-', will read from standard input

      --pattern              Event name pattern (substring or regexp)
      --sep SEP              Extra subfield separator
      --subfields FIELDS     Comma-separated subfield names

    Notes:

      * Output is written to stdout.
      * Input files may be raw text or compressed gzip/bzip2 archives.
      * If the 'extra' column comprises multiple values, the 'sep' and 'subfields'
        optional arguments specify its format.

    Example:

      Generate a CSV of all entries in clicktracking.log, writing to stdout:

          ct2csv clicktracking.log

      Grep for click events in clicktracking.log.gz, splitting the 'extra'
      field into subfields 'Referrer' and 'Target', and write to clicks.csv:

          ct2csv --sep="@" --subfields="Referrer,Target" \\
                  --pattern="[Cc]lick" clicktracking.log.gz > clicks.csv

"""
__author__ = "Ori Livneh <ori@wikimedia.org>"
__license__ = "GPL (version 2 or later)"

import argparse
import csv
import datetime
import fileinput
import re
import sys
import textwrap



standard_fields = (
    'Prefix', 'Event Name', 'Timestamp', 'Registered', 'Token',
    'Namespace', 'Edits (lifetime)', 'Edits (6 months)', 'Edits (3 months)',
    'Edits (last month)'
)


def parse_timestamp(timestamp):
    """Parse MediaWiki timestamps into Datetime objects"""
    return datetime.datetime.strptime(timestamp, "%Y%m%d%H%M%S")


def split(line, sep=None):
    """Split a ClickTracking log line"""
    values = line.strip().split(None, 10)
    if sep is not None:
        values.extend(values.pop().split(sep))
    return values


def write_report(files, pattern, subfields, sep):
    """Parse raw ClickTracking data into mappings"""
    fields = list(standard_fields) + list(subfields)
    writer = csv.writer(sys.stdout)
    writer.writerow(list(standard_fields) + list(subfields))
    for line in fileinput.input(*files, openhook=fileinput.hook_compressed):
        values = split(line, sep)
        if pattern.search(values[1]):
            values[2] = parse_timestamp(values[2])
            writer.writerow(values)


def main():
    """Parse command-line arguments and generate report"""

    def comma_delim(str):
        return str.split(',')

    def format_help():
        help = __doc__.lstrip('\n')
        return textwrap.dedent(help)

    parser = argparse.ArgumentParser(prog='ct2csv')
    parser.format_help = format_help  # Override auto-generated help

    # Required arguments
    parser.add_argument('files', nargs='*', default=('-',))

    # Optional arguments
    parser.add_argument('--pattern', type=re.compile, default='.*')
    parser.add_argument('--subfields', default=('Extra',), type=comma_delim)
    parser.add_argument('--sep')

    options = vars(parser.parse_args())
    write_report(**options)


if __name__ == '__main__':
    main()
