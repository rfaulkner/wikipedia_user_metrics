"""
    Unit Testing Framework for E3 Analytics
"""

__author__ = "Ryan Faulkner"
__date__ = "September 18, 2012"
__license__ = "GPL (version 2 or later)"

import sys
import unittest
import random
import libraries.etl.ExperimentsLoader as EL
import libraries.metrics.TimeToThreshold as TTT

class TestSequenceFunctions(unittest.TestCase):
    """
        Some Sample tests to ensure that the unit testing module is functioning properly
    """
    def setUp(self):
        self.seq = range(10)

    def test_shuffle(self):
        # make sure the shuffled sequence does not lose any elements
        random.shuffle(self.seq)
        self.seq.sort()
        self.assertEqual(self.seq, range(10))

        # should raise an exception for an immutable sequence
        self.assertRaises(TypeError, random.shuffle, (1,2,3))

    def test_choice(self):
        element = random.choice(self.seq)
        self.assertTrue(element in self.seq)

    def test_sample(self):
        with self.assertRaises(ValueError):
            random.sample(self.seq, 20)
        for element in random.sample(self.seq, 5):
            self.assertTrue(element in self.seq)


class TestTimeToThreshold(unittest.TestCase):
    """
        Class that defines unit tests across the TimeToThreshold Metrics class
    """

    def setUp(self):
        self.uid = 13234584 # Renklauf
        self.ttt = TTT.TimeToThreshold(TTT.TimeToThreshold.EDIT_COUNT_THRESHOLD, first_edit=1, threshold_edit=2)

    def test_time_diff_greater_than_a_day(self):
        """
            Ensure that the time to threshold when exceeding one day reports the correct value
        """

        self.assertEqual(self.ttt.process(self.uid)[0][1], 3367)


class TestExperimentsLoader(unittest.TestCase):
    """
        Class that defines unit tests across the TimeToThreshold Metrics class
    """

    def setUp(self):
        self.el = EL.ExperimentsLoader()
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