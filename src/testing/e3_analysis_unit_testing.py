"""
    Unit Testing Framework for E3 Analytics
"""

__author__ = "Ryan Faulkner"
__date__ = "September 18, 2012"
__license__ = "GPL (version 2 or later)"

import sys
import unittest
import src.etl.experiments_loader as el
# import src.metrics.time_to_threshold as ttt

class TestTimeToThreshold(unittest.TestCase):
    """ Class that defines unit tests across the TimeToThreshold Metrics class """

    def setUp(self):
        self.uid = 13234584 # Renklauf
        self.ttt = None
        # self.ttt = ttt.TimeToThreshold(ttt.TimeToThreshold.EDIT_COUNT_THRESHOLD, first_edit=1, threshold_edit=2)

    def test_time_diff_greater_than_a_day(self):
        """ Ensure that the time to threshold when exceeding one day reports the correct value """

        # self.assertEqual(self.ttt.process(self.uid)[0][1], 3367)
        pass

class TestExperimentsLoader(unittest.TestCase):
    """ Class that defines unit tests across the TimeToThreshold Metrics class """

    def setUp(self):
        self.el = el.ExperimentsLoader()
        self.data = {'samples' : [1,1,1], 'buckets' : ['ctrl','test1','test2'], 'bins' : [1,2,3]}

    def test_write_sample_aggregates(self):

        out = self.el.write_sample_aggregates(self.data['samples'], self.data['buckets'], self.data['bins'])
        self.assertEqual(out['y0'], [1,0,0])


def main(args):
    # Execute desired unit tests
    unittest.main()

# Call Main
if __name__ == "__main__":
    sys.exit(main([]))