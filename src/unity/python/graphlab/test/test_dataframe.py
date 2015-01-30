'''
Copyright (C) 2015 Dato, Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
