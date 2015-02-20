'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''
import unittest
import pandas
import array
from graphlab.cython.cy_dataframe import _dataframe
from pandas.util.testing import assert_frame_equal


class DataFrameTest(unittest.TestCase):
    def test_empty(self):
        expected = pandas.DataFrame()
        assert_frame_equal(_dataframe(expected), expected)
        expected['int'] = []
        expected['float'] = []
        expected['str'] = []
        assert_frame_equal(_dataframe(expected), expected)

    def test_simple_dataframe(self):
        expected = pandas.DataFrame()
        expected['int'] = [i for i in range(10)]
        expected['float'] = [float(i) for i in range(10)]
        expected['str'] = [str(i) for i in range(10)]
        expected['unicode'] = [unicode(i) for i in range(10)]
        expected['array'] = [array.array('d', [i]) for i in range(10)]
        expected['ls'] = [[str(i)] for i in range(10)]
        assert_frame_equal(_dataframe(expected), expected)

    def test_sparse_dataframe(self):
        expected = pandas.DataFrame()
        expected['sparse_int'] = [i if i % 2 == 0 else None for i in range(10)]
        expected['sparse_float'] = [float(i) if i % 2  == 1 else None for i in range(10)]
        expected['sparse_str'] = [str(i) if i % 3 == 0 else None for i in range(10)]
        expected['sparse_array'] = [array.array('d', [i]) if i % 5 == 0 else None for i in range(10)]
        expected['sparse_list'] = [[str(i)] if i % 7 == 0 else None for i in range(10)]
        assert_frame_equal(_dataframe(expected), expected)
