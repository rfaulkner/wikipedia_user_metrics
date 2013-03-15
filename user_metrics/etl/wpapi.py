"""
    This module defines a class for interfacing with
        http://www.mediawiki.org/wiki/API:Main_page

    Example: ::

        >>> import libraries.etl.WPAPI
        >>> api = WPAPI.WPAPI()
        >>> api.getDiff(515866670)
        (u'[[Category:People from Palermo]] [[Category:Sportspeople from
        Sicily|Palermo]] [[Category:Sport in Palermo|People]] [[Category:
        Sportspeople by city in Italy|Palermo]]', True    )
"""

__author__ = "Ryan Faulkner and Aaron Halfaker"
__date__ = "October 3rd, 2012"
__license__ = "GPL (version 2 or later)"

import sys
import types
import re
import time
import urllib
import urllib2
import json
import htmlentitydefs
from user_metrics.config import logging


class WPAPI:
    """
        The class itself implements functionality that allows a user to
        examine revision text.  The initializerallows the user ot specify
        the particular API.
    """

    DIFF_ADD_RE = re.compile(
        r'<td class="diff-addedline"><div>(.+)</div></td>')

    def __init__(self, uri='http://en.wikipedia.org/w/api.php'):
        self.uri = uri

    def getDiff(self, revId, retries=20):
        attempt = 0
        is_content = False

        while attempt < retries:
            try:
                # e.g. url: http://en.wikipedia.org/w/api.php?format=xml&
                # action=query&prop=revisions&revids=472419240&rvprop=ids&
                # rvdiffto=prev&format=json
                response = urllib2.urlopen(
                    self.uri,
                    urllib.urlencode({
                        'action': 'query',
                        'prop': 'revisions',
                        'revids': revId,
                        'rvprop': 'ids',
                        'rvdiffto': 'prev',
                        'format': 'json'
                    })
                )

                result = json.load(response)

                diff = result['query']['pages'].values()[0][
                    'revisions'][0]['diff']['*']

                # The diff will not exist if it included the creation of the
                # user talk page in this case simply load the content of the
                # page at this revision
                if type(diff) not in types.StringTypes or diff == '':

                    # e.g. url: http://en.wikipedia.org/w/api.php?format=xml&
                    # action=query&prop=revisions&revids=474338555&format=json&
                    # rvprop=content
                    response = urllib2.urlopen(
                        self.uri,
                        urllib.urlencode({
                            'action': 'query',
                            'prop': 'revisions',
                            'revids': revId,
                            'rvprop': 'content',
                            'format': 'json'
                        })
                    )

                    result = json.load(response)
                    try:
                        diff = result['query']['pages'].\
                            values()[0]['revisions'][0]['*']
                    except KeyError:
                        sys.stderr.write("x")
                        diff = ''
                        pass

                    # Add the diff tags such that the content is parsed as if
                    # it were a diff
                    if type(diff) not in types.StringTypes:
                        diff = ''

                    is_content = True

                return diff, is_content
            except urllib2.HTTPError as e:
                time.sleep(2**attempt)
                attempt += 1
                logging.error("HTTP Error: %s.  Retry #%s in %s seconds..." % (
                    e, attempt, 2**attempt))

    def getAdded(self, revId):
        diff, is_content = self.getDiff(revId)

        if is_content:
            return diff
        else:
            return self.unescape(
                "\n".join(
                    match.group(1)
                    for match in WPAPI.DIFF_ADD_RE.finditer(diff)
                )
            )

    def unescape(self, text):
        def fixup(m):
            text = m.group(0)
            if text[:2] == "&#":
                # character reference
                try:
                    if text[:3] == "&#x":
                        return unichr(int(text[3:-1], 16))
                    else:
                        return unichr(int(text[2:-1]))
                except ValueError:
                    pass
            else:
                # named entity
                try:
                    text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
                except KeyError:
                    pass
            # leave as is
            return text
        return re.sub("&#?\w+;", fixup, text)
