"""
    Any general purpose utilities useful in the project are defined here.
"""

MW_TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"
from dateutil.parser import parse as date_parse


def format_mediawiki_timestamp(timestamp_repr):
    """
        Convert representation to mediawiki timestamps.  Returns a sring
         timestamp in the MediaWiki Format.

        Parameters
        ~~~~~~~~~~

        timestamp_repr : str|datetime
           Datetime representation to convert.
    """
    if hasattr(timestamp_repr, 'strftime'):
        return timestamp_repr.strftime(MW_TIMESTAMP_FORMAT)
    else:
        return date_parse(timestamp_repr).strftime(
            MW_TIMESTAMP_FORMAT)
