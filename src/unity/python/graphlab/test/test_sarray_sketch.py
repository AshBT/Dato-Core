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
# from nose import with_setup
# -*- coding: utf-8 -*-
from graphlab.data_structures.sarray import SArray
import pandas as pd
import numpy as np
import unittest
import random
import copy
import os
import math
import shutil
import array
import util
import time
import itertools

#######################################################
# Metrics tracking tests are in test_usage_metrics.py #
#######################################################

class SArraySketchTest(unittest.TestCase):
    def setUp(self):
        pass

    def __validate_sketch_result(self, sketch, sa, delta = 1E-7):
        df = pd.DataFrame(list(sa.dropna()))
        pds = pd.Series(list(sa.dropna()))
        if (sa.dtype() == int or sa.dtype() == float):
            if (len(sa) == 0):
                self.assertTrue(math.isnan(sketch.min()))
                self.assertTrue(math.isnan(sketch.min()))
                self.assertEquals(sketch.sum(), 0.0)
                self.assertEquals(sketch.mean(), 0.0)
                self.assertEquals(sketch.var(), 0.0)
                self.assertEquals(sketch.std(), 0.0)
            else:
                self.assertEquals(sketch.min(), sa.min())
                self.assertEquals(sketch.max(), sa.max())
                self.assertEquals(sketch.sum(), sa.sum())
                self.assertAlmostEqual(sketch.mean(), sa.dropna().mean(), delta=delta)
                self.assertAlmostEqual(sketch.var(), sa.dropna().var(), delta=delta)
                self.assertAlmostEqual(sketch.std(), sa.dropna().std(), delta=delta)
                self.assertAlmostEqual(sketch.quantile(0.5), df.quantile(0.5)[0], delta=1)
                self.assertEqual(sketch.quantile(0), df.quantile(0)[0])
                self.assertEqual(sketch.quantile(1), df.quantile(1)[0])

                self.assertEqual(sketch.frequent_items(), SArray(pds).sketch_summary().frequent_items())
                for item in pds.value_counts().index:
                    self.assertEqual(sketch.frequency_count(item), pds.value_counts()[item])

                self.assertAlmostEqual(sketch.num_unique(), len(sa.unique()), delta=3)
        else:
            with self.assertRaises(RuntimeError):
                sketch.quantile((0.5))


        self.assertEqual(sketch.num_undefined(), sa.num_missing())
        self.assertEqual(sketch.size(), len(sa))
        self.assertEqual(sketch.sketch_ready(), True)
        self.assertEqual(sketch.num_elements_processed(), sketch.size())

    def __validate_nested_sketch_result(self, sa):
        sketch = sa.sketch_summary()
        self.__validate_sketch_result(sketch, sa)

        # element length summary
        t = sketch.element_length_summary()
        len_sa = sa.dropna().item_length()
        self.__validate_sketch_result(t, len_sa)

    def test_sketch_int(self):
        int_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, None]
        sa = SArray(data=int_data)
        self.__validate_sketch_result(sa.sketch_summary(), sa)

    def test_sketch_float(self):
        int_data = [1.2, 3,.4, 6.789, None]
        sa = SArray(data=int_data)
        self.__validate_sketch_result(sa.sketch_summary(), sa)

    def test_vector_sketch(self):
        vector_data = [[], [1,2], [3], [4,5,6,7], [8,9,10], None]
        sa = SArray(data=vector_data)

        sketch = sa.sketch_summary();
        self.__validate_sketch_result(sketch, sa)
        self.__validate_sketch_result(sketch.element_length_summary(), sa.dropna().item_length())

        flattened = list(itertools.chain.from_iterable(list(sa.dropna())))
        self.__validate_sketch_result(sketch.element_summary(), SArray(flattened))

        fi = sketch.frequent_items()
        self.assertEqual(len(fi), 5)
        self.assertEqual((fi['[1 2]']), 1)
        self.assertEqual((fi['[4 5 6 7]']), 1)

        # sub sketch with one key
        s = sa.sketch_summary(sub_sketch_keys = 1).element_sub_sketch(1)
        expected = sa.vector_slice(1)
        self.__validate_sketch_result(s, expected)

        # sub sketch with multiple keys
        keys = [1,3]
        s = sa.sketch_summary(sub_sketch_keys = keys).element_sub_sketch(keys)
        self.assertEqual(len(s), len(keys))
        for key in keys:
            self.assertTrue(s.has_key(key))
            expected = sa.vector_slice(key)
            self.__validate_sketch_result(s[key], expected)

        indexes = range(0,10)
        s = sa.sketch_summary(sub_sketch_keys = indexes).element_sub_sketch()
        self.assertEqual(len(s), len(indexes))

    def test_list_sketch(self):
        list_data = [[], [1,2],[1,2], ['a', 'a', 'a', 'b'], [ 1 ,1 , 2], None]
        sa = SArray(list_data)
        self.__validate_nested_sketch_result(sa)
        sketch = sa.sketch_summary();

        self.assertEqual(sketch.num_unique(), 4)
        element_summary = sketch.element_summary()
        another_rep = list(itertools.chain.from_iterable(list(sa.dropna())))
        self.__validate_sketch_result(element_summary, SArray(another_rep, str))

        fi = sketch.frequent_items()
        self.assertEqual(len(fi), 4)
        self.assertEqual((fi['[1,2]']), 2)
        self.assertEqual((fi['["a","a","a","b"]']), 1)

    def test_dict_sketch_int_value(self):
        dict_data = [{}, {'a':1, 'b':2}, {'a':1, 'b':2}, {'a':3, 'c':1}, {'a': 1, 'b': 2, 'c': 3}, None]
        sa = SArray(data=dict_data)
        self.__validate_nested_sketch_result(sa)

        sketch = sa.sketch_summary()
        self.assertEqual(sketch.num_unique(), 4)
        fi = sketch.frequent_items()
        self.assertEqual(len(fi), 4)
        self.assertEqual((fi['{"a":1, "b":2}']), 2)
        self.assertEqual((fi['{"a":3, "c":1}']), 1)

        # Get dict key sketch
        key_summary = sketch.dict_key_summary()
        another_rep = list(itertools.chain.from_iterable(list(sa.dict_keys().dropna())))
        self.__validate_sketch_result(key_summary, SArray(another_rep))

        # Get dict value sketch
        value_summary = sketch.dict_value_summary()
        another_rep = list(itertools.chain.from_iterable(list(sa.dict_values().dropna())))
        self.__validate_sketch_result(value_summary, SArray(another_rep))

        # sub sketch with one key
        s = sa.sketch_summary(sub_sketch_keys ='a').element_sub_sketch('a')
        expected = sa.unpack(column_name_prefix="")['a']
        self.__validate_sketch_result(s, expected)

        s = sa.sketch_summary(sub_sketch_keys ='Nonexist').element_sub_sketch('Nonexist')
        self.assertEqual(s.num_undefined(), len(sa))

        # sub sketch with multiple keys
        keys = ['a', 'b']
        s = sa.sketch_summary(sub_sketch_keys =keys).element_sub_sketch(keys)
        self.assertEqual(len(s), len(keys))
        for key in keys:
            self.assertTrue(s.has_key(key))
            expected = sa.unpack(column_name_prefix="")[key]
            self.__validate_sketch_result(s[key], expected)

    def test_dict_sketch_str_value(self):
        # Dict value sketch type should be auto inferred
        dict_data = [{'a':'b', 'b':'c'}, {'a':'b', 'b':'c'}, {'a':'d', 'b':'4'}, None]
        sa = SArray(data=dict_data)
        self.__validate_nested_sketch_result(sa)

        sketch = sa.sketch_summary()
        fi = sketch.frequent_items()
        self.assertEqual(len(fi), 2)
        self.assertEqual(fi['{"a":"b", "b":"c"}'], 2)
        self.assertEqual(fi['{"a":"d", "b":"4"}'], 1)

        # Get dict key sketch
        key_summary = sketch.dict_key_summary()
        another_rep = list(itertools.chain.from_iterable(list(sa.dict_keys().dropna())))
        self.__validate_sketch_result(key_summary, SArray(another_rep))

        # Get dict value sketch
        value_summary = sketch.dict_value_summary()
        another_rep = list(itertools.chain.from_iterable(list(sa.dict_values().dropna())))
        self.__validate_sketch_result(value_summary, SArray(another_rep))

        # sub sketch with one key
        s = sa.sketch_summary(sub_sketch_keys ='a').element_sub_sketch('a')
        expected = sa.unpack(column_name_prefix="")['a']
        self.__validate_sketch_result(s, expected)

        s = sa.sketch_summary(sub_sketch_keys ='Nonexist').element_sub_sketch('Nonexist')
        self.assertEqual(s.num_undefined(), len(sa))

        # sub sketch with multiple keys
        keys = ['a', 'b']
        s = sa.sketch_summary(sub_sketch_keys =keys).element_sub_sketch(keys)
        self.assertEqual(len(s), len(keys))
        for key in keys:
            self.assertTrue(s.has_key(key))
            expected = sa.unpack(column_name_prefix="")[key]
            self.__validate_sketch_result(s[key], expected)

        # allow pass in empty keys, which will retrieve all keys
        s = sa.sketch_summary(sub_sketch_keys =keys).element_sub_sketch()
        self.assertEqual(len(s), len(keys))
        for key in keys:
            self.assertTrue(s.has_key(key))
            expected = sa.unpack(column_name_prefix="")[key]
            self.__validate_sketch_result(s[key], expected)


    def test_str_sketch(self):
        str_data = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", None]
        sa = SArray(data=str_data)
        sketch = sa.sketch_summary()
        with self.assertRaises(RuntimeError):
          sketch.min()
        with self.assertRaises(RuntimeError):
          sketch.max()
        with self.assertRaises(RuntimeError):
          sketch.sum()
        with self.assertRaises(RuntimeError):
          sketch.mean()
        with self.assertRaises(RuntimeError):
          sketch.var()
        with self.assertRaises(RuntimeError):
          sketch.std()

        self.assertAlmostEqual(sketch.num_unique(), 10, delta=3)
        self.assertEqual(sketch.num_undefined(), 1)
        self.assertEqual(sketch.size(), len(str_data))

        with self.assertRaises(RuntimeError):
          sketch.quantile(0.5)
        self.assertEqual(sketch.frequency_count("1"), 1)
        self.assertEqual(sketch.frequency_count("2"), 1)
        t = sketch.frequent_items()
        self.assertEqual(len(t), 10)

    def test_empty_sketch(self):
        int_data = []
        sa = SArray(data=int_data)
        sketch = sa.sketch_summary()
        self.assertTrue(math.isnan(sketch.min()))
        self.assertTrue(math.isnan(sketch.max()))
        self.assertEquals(sketch.sum(), 0)
        self.assertEqual(sketch.mean(), 0)
        self.assertEqual(sketch.var(), 0)
        self.assertEqual(sketch.std(), 0)
        self.assertEqual(sketch.num_unique(), 0)
        self.assertEqual(sketch.num_undefined(),0)
        self.assertEqual(sketch.size(), 0)
        with self.assertRaises(RuntimeError):
          sketch.quantile(0.5)

        t = sketch.frequent_items()
        self.assertEqual(len(t), 0)

    def test_background_sketch(self):
        dict_data = [{str(i):1} for i in range(1,10000)]
        sa = SArray(dict_data)
        s = sa.sketch_summary(background=True, sub_sketch_keys=[str(i ) for i in range(100,200)])
        s.sketch_ready() # cannot check the actual value as it depends on the speed of processing
        t = s.element_sub_sketch([str(i) for i in range(100, 105)])
        self.assertEqual(len(t), 5)


    def test_large_value_sketch(self):
        sa = SArray([1234567890 for i in range(100)])
        sk = sa.sketch_summary();
        self.__validate_sketch_result(sa.sketch_summary(), sa, 1E-5)

    def test_cancelation(self):
        sa = SArray(range(1,10000))
        s = sa.sketch_summary(background=True)
        s.cancel()
        # this can be rather non-deterministic, so there is very little
        # real output validation that can be done...
