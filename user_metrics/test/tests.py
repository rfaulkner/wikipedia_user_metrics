"""
    Test user metrics methods.  This file contains method definitions to be
    invoked by nose_.  The testing framework focuses on ensuring that
    UserMetric and API functionality functions correctly.

    .. _nose: https://nose.readthedocs.org/en/latest/
"""

__author__ = "ryan faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "02/14/2012"
__license__ = "GPL (version 2 or later)"

from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
from collections import namedtuple

from user_metrics.metrics import edit_count
from user_metrics.metrics.users import UMP_MAP, USER_METRIC_PERIOD_TYPE
from user_metrics.config import settings
from user_metrics.etl.data_loader import Connector, ConnectorError

from user_metrics.metrics import revert_rate

# User Metric tests
# =================


def test_blocks():
    assert False  # TODO: implement your test here


def test_namespace_of_edits():
    assert False  # TODO: implement your test here


def test_time_to_threshold():
    assert False  # TODO: implement your test here


def test_edit_rate():
    assert False  # TODO: implement your test here


def test_edit_count():
    """ Test the edit count metric results """
    results = {
        '13234584': 18,
        '13234503': 2,
        '13234565': 0,
        '13234585': 2,
        '13234556': 6,
        }
    e = edit_count.EditCount(t=10000)

    # Check edit counts against
    index = 0
    for res in e.process(results.keys()):
        assert res[1] == results[str(res[0])]
        index += 1


def test_live_account():
    assert False  # TODO: implement your test here


def test_threshold():
    assert False  # TODO: implement your test here


def test_survival():
    assert False  # TODO: implement your test here


def test_bytes_added():
    assert False  # TODO: implement your test here



def test_revert_rate():
    r = revert_rate.RevertRate()
    users = {
        '17792132': 0.0,
        '17797320': 0.5,
        '17792130': 0.0,
        '17792131': 0.0,
        '17792136': 0.0,
        '17792137': 0.0,
        '17792134': 0.0,
        '17797328': 0.0,
        '17797329': 0.0,
        '17792138': 1.0
    }

    for r in r.process(users.keys(), k_=1, kr_=1, log_=True):
        if not float(r[1]) == float(users[str(r[0])]):
            assert False
    assert True


def test_user():
    assert False  # TODO: implement your test here


def test_user_UMPRegistration():
    """
        Test for UMPRegistration in user_metrics.metrics.users module.

        1) ``rec.user`` was in the original list
        2) That ``rec.start`` and ``rec.end`` reflect ``rec.users``'s
            registration date and reg date + ``o.t`` hours respectively
    """
    o = namedtuple('nothing', 't project datetime_start, datetime_end')

    o.t = 1000
    o.project = 'enwiki'

    users = ['13234590', '13234584']
    for rec in UMP_MAP[USER_METRIC_PERIOD_TYPE.REGISTRATION](users, o):
        assert str(rec.user) in users
        # @TODO check if user's reg date and reg date + t is start and end


def test_user_UMPInput():
    """
        Test for UMPInput in user_metrics.metrics.users module.

        1) ``rec.user`` was in the original list
        2) That ``rec.start`` and ``rec.end`` reflect the
            ``o.datetime_start`` and ``o.datetime_start`` respectively
    """
    o = namedtuple('nothing', 't project datetime_start, datetime_end')

    o.project = 'enwiki'
    o.datetime_start = datetime.now()
    o.datetime_end = datetime.now() + timedelta(days=30)

    users = ['13234590', '13234584']
    # If datetimes are less than a second apart the test passes
    for rec in UMP_MAP[USER_METRIC_PERIOD_TYPE.INPUT](users, o):
        assert str(rec.user) in users
        assert abs((date_parse(rec.start) - o.datetime_start).
            total_seconds()) < 1
        assert abs((date_parse(rec.end) - o.datetime_end).
            total_seconds()) < 1


def test_user_UMPRegInput():
    """
        Test for UMPRegInput in user_metrics.metrics.users module.

        This method tests that:

            1) ``rec.user`` was in the original list
            2) Users whose reg date falls within ``o.datetime_start`` and
                ``o.datetime_end`` are included while others are excluded
            3) That ``rec.start`` and ``rec.end`` reflect the
                ``o.datetime_start`` and ``o.datetime_start`` respectively
    """
    o = namedtuple('nothing', 't project datetime_start, datetime_end')

    o.project = 'enwiki'
    o.t = '24'
    o.datetime_start = datetime(year=2010, month=10, day=1)
    o.datetime_end = o.datetime_start + timedelta(days=30)

    users = ['13234590', '13234584']
    for rec in UMP_MAP[USER_METRIC_PERIOD_TYPE.REGINPUT](users, o):
        assert str(rec.user) in users
        assert date_parse(rec.start) == o.datetime_start
        assert date_parse(rec.end) == o.datetime_end
        # @TODO check whether user's reg date is within input dates


# Query call tests
# ================


import user_metrics.query.query_calls_sql as qSQL

UID_1 = 13234584
UID_2 = 15013214
PROJECT = 'enwiki'

def test_rev_count_query():
    """
    Test revision count query.
    """
    rev_count = qSQL.rev_count_query(UID_1, False, [0], PROJECT,
                                     '20100101000000', '20130301000000')
    assert rev_count == 14


def test_live_account_query():
    """
    Test Live account query.
    """
    res = qSQL.live_account_query([UID_2], PROJECT,
        namedtuple('x', 'namespace')([0]))
    assert not cmp("[(15013214L, '20110725223731', '20110725223838')]",
                   str(res))


def test_rev_query():
    """
    Test revision query.
    """
    res = qSQL.rev_query([UID_1], PROJECT, namedtuple('x',
        'namespace date_start date_end')([0], '20100101000000',
        '20130301000000'))
    assert not cmp("[(15013214L, '20110725223731', '20110725223838')]",
        str(res))


def test_rev_len_query():
    """
    Test revision length query.
    """
    assert 17039 == qSQL.rev_len_query(412553375, 'enwiki')


# ETL tests
# =========


def test_connect_to_dbs():
    for key in settings.connections:
        try:
            conn = Connector(instance=key, retries=1)
            del conn
        except ConnectorError as e:
            print e.message
            assert False
        assert True


# API tests
# =========


def test_cohort_parse():
    assert False  # TODO: implement your test here


# Utilities tests
# ===============


def test_recordtype():
    assert False  # TODO: implement your test here


if __name__ == '__main__':
    test_revert_rate()
